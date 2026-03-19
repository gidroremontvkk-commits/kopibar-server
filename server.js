/**
 * KopiBar — прокси-сервер для 5 бирж
 * Binance, Bybit, OKX, Gate.io, Bitget
 * Запуск: node server.js
 */

const express = require('express');
const cors    = require('cors');
const https   = require('https');

const app  = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

// --- Keep-alive агент (переиспользуем TCP соединения) -------------------------
const agent = new https.Agent({ keepAlive: true, maxSockets: 20 });

const sleep = ms => new Promise(r => setTimeout(r, ms));

// --- HTTP запрос с обработкой статус-кодов ------------------------------------
function httpGet(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { hostname, path, method: 'GET', agent,
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip' } },
      (res) => {
        // Отдаём статус код наружу чтобы retry мог его видеть
        const status = res.statusCode;
        let body = '';
        res.on('data', c => body += c);
        res.on('end', () => {
          if (status === 429 || status === 418) {
            return reject(Object.assign(new Error('RateLimit'), { status }));
          }
          if (status >= 400) {
            return reject(Object.assign(new Error(`HTTP ${status}`), { status }));
          }
          try { resolve(JSON.parse(body)); }
          catch (e) { reject(new Error('JSON parse: ' + body.slice(0, 120))); }
        });
      }
    );
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

// --- Rate-limiter — очередь запросов для каждой биржи ------------------------
// Позволяет не более `rps` запросов в секунду на биржу
class RateLimiter {
  constructor(rps) {
    this.interval = 1000 / rps; // мс между запросами
    this.queue    = [];
    this.running  = false;
  }

  schedule(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      if (!this.running) this._run();
    });
  }

  async _run() {
    this.running = true;
    while (this.queue.length) {
      const { fn, resolve, reject } = this.queue.shift();
      try { resolve(await fn()); } catch (e) { reject(e); }
      if (this.queue.length) await sleep(this.interval);
    }
    this.running = false;
  }
}

// Лимиты: Binance ~20/s, Bybit ~10/s, OKX ~4/s, GateIO ~10/s, Bitget ~10/s
const limiters = {
  binance: new RateLimiter(18),
  bybit:   new RateLimiter(10),
  okx:     new RateLimiter(4),
  gateio:  new RateLimiter(10),
  bitget:  new RateLimiter(10),
};

// --- Retry с экспоненциальным backoff ----------------------------------------
async function fetchWithRetry(exchange, hostname, path, retries = 4) {
  const limiter = limiters[exchange];
  let delay = 500;
  for (let i = 0; i < retries; i++) {
    try {
      return await (limiter
        ? limiter.schedule(() => httpGet(hostname, path))
        : httpGet(hostname, path));
    } catch (e) {
      const isRetryable = e.status === 429 || e.status === 418 || e.status >= 500 || e.message === 'Timeout';
      if (!isRetryable || i === retries - 1) throw e;
      console.warn(`[${exchange}] retry ${i+1}/${retries} (${e.message}), ждём ${delay}ms`);
      await sleep(delay);
      delay *= 2; // 500 > 1000 > 2000 > 4000
    }
  }
}

// --- Дедупликация одновременных запросов -------------------------------------
// Если два клиента одновременно запросили один и тот же символ —
// делаем только один реальный запрос к бирже
const inFlight = new Map();

function dedupe(key, fn) {
  if (inFlight.has(key)) return inFlight.get(key);
  const p = fn().finally(() => inFlight.delete(key));
  inFlight.set(key, p);
  return p;
}

// --- Адаптеры бирж ------------------------------------------------------------
// Единый формат свечи: { time(сек), open, high, low, close, volume, openTime(мс) }

const Binance = {
  name: 'binance',
  host: 'fapi.binance.com',

  async getSymbols() {
    const d = await fetchWithRetry('binance', this.host, '/fapi/v1/exchangeInfo');
    return d.symbols
      .filter(s => s.status === 'TRADING' && s.quoteAsset === 'USDT')
      .map(s => s.symbol);
  },

  async getTickers() {
    const d = await fetchWithRetry('binance', this.host, '/fapi/v1/ticker/24hr');
    return d.map(t => ({
      symbol: t.symbol,
      price: parseFloat(t.lastPrice),
      priceChangePercent: parseFloat(t.priceChangePercent),
      quoteVolume: parseFloat(t.quoteVolume),
      count: parseInt(t.count) || 0,
    }));
  },

  tfMap: { '1m':'1m','5m':'5m','15m':'15m','1h':'1h','4h':'4h','1d':'1d' },

  async getKlines(symbol, interval, limit = 1000, endTime = null) {
    const tf = this.tfMap[interval] || interval;
    let qs = `symbol=${symbol}&interval=${tf}&limit=${limit}`;
    if (endTime) qs += `&endTime=${endTime}`;
    const d = await fetchWithRetry('binance', this.host, `/fapi/v1/klines?${qs}`);
    return d.map(c => ({
      time: c[0]/1000, open:+c[1], high:+c[2], low:+c[3], close:+c[4], volume:+c[5], openTime:c[0]
    }));
  },
};

const Bybit = {
  name: 'bybit',
  host: 'api.bybit.com',

  async getSymbols() {
    const d = await fetchWithRetry('bybit', this.host, '/v5/market/instruments-info?category=linear&limit=1000');
    return (d.result?.list || [])
      .filter(s => s.status === 'Trading' && s.quoteCoin === 'USDT')
      .map(s => s.symbol);
  },

  async getTickers() {
    const d = await fetchWithRetry('bybit', this.host, '/v5/market/tickers?category=linear');
    return (d.result?.list || [])
      .filter(t => t.symbol.endsWith('USDT'))
      .map(t => ({
        symbol: t.symbol,
        price: parseFloat(t.lastPrice),
        priceChangePercent: parseFloat(t.price24hPcnt) * 100,
        quoteVolume: parseFloat(t.turnover24h),
        count: 0,
      }));
  },

  tfMap: { '1m':'1','5m':'5','15m':'15','1h':'60','4h':'240','1d':'D' },

  async getKlines(symbol, interval, limit = 1000, endTime = null) {
    const tf = this.tfMap[interval] || interval;
    let qs = `category=linear&symbol=${symbol}&interval=${tf}&limit=${limit}`;
    if (endTime) qs += `&end=${endTime}`;
    const d = await fetchWithRetry('bybit', this.host, `/v5/market/kline?${qs}`);
    return (d.result?.list || []).reverse().map(c => ({
      time: +c[0]/1000, open:+c[1], high:+c[2], low:+c[3], close:+c[4], volume:+c[5], openTime:+c[0]
    }));
  },
};

const OKX = {
  name: 'okx',
  host: 'www.okx.com',

  async getSymbols() {
    const d = await fetchWithRetry('okx', this.host, '/api/v5/public/instruments?instType=SWAP');
    return (d.data || [])
      .filter(s => s.state === 'live' && s.ctType === 'linear' && s.settleCcy === 'USDT')
      .map(s => s.instId);
  },

  async getTickers() {
    const d = await fetchWithRetry('okx', this.host, '/api/v5/market/tickers?instType=SWAP');
    return (d.data || [])
      .filter(t => t.instId.endsWith('-USDT-SWAP'))
      .map(t => ({
        symbol: t.instId,
        price: parseFloat(t.last),
        priceChangePercent: parseFloat(t.open24h) > 0
          ? ((parseFloat(t.last) - parseFloat(t.open24h)) / parseFloat(t.open24h)) * 100
          : 0,
        quoteVolume: parseFloat(t.volCcy24h),
        count: 0,
      }));
  },

  tfMap: { '1m':'1m','5m':'5m','15m':'15m','1h':'1H','4h':'4H','1d':'1D' },

  async getKlines(symbol, interval, limit = 300, endTime = null) {
    const tf = this.tfMap[interval] || interval;
    let qs = `instId=${symbol}&bar=${tf}&limit=${limit}`;
    if (endTime) qs += `&after=${endTime}`;
    const d = await fetchWithRetry('okx', this.host, `/api/v5/market/candles?${qs}`);
    return (d.data || []).reverse().map(c => ({
      time: +c[0]/1000, open:+c[1], high:+c[2], low:+c[3], close:+c[4], volume:+c[5], openTime:+c[0]
    }));
  },
};

const GateIO = {
  name: 'gateio',
  host: 'api.gateio.ws',

  async getSymbols() {
    const d = await fetchWithRetry('gateio', this.host, '/api/v4/futures/usdt/contracts');
    return (Array.isArray(d) ? d : [])
      .filter(s => !s.in_delisting)
      .map(s => s.name);
  },

  async getTickers() {
    const d = await fetchWithRetry('gateio', this.host, '/api/v4/futures/usdt/tickers');
    return (Array.isArray(d) ? d : []).map(t => ({
      symbol: t.contract,
      price: parseFloat(t.last),
      priceChangePercent: parseFloat(t.change_percentage),
      quoteVolume: parseFloat(t.volume_24h_quote || t.volume_24h_settle || 0),
      count: 0,
    }));
  },

  tfMap: { '1m':'1m','5m':'5m','15m':'15m','1h':'1h','4h':'4h','1d':'1d' },

  async getKlines(symbol, interval, limit = 1000, endTime = null) {
    const tf = this.tfMap[interval] || interval;
    let qs = `contract=${symbol}&interval=${tf}&limit=${limit}`;
    if (endTime) qs += `&to=${Math.floor(endTime / 1000)}`;
    const d = await fetchWithRetry('gateio', this.host, `/api/v4/futures/usdt/candlesticks?${qs}`);
    return (Array.isArray(d) ? d : []).map(c => ({
      time: +c.t, open:+c.o, high:+c.h, low:+c.l, close:+c.c, volume:+c.v, openTime:+c.t*1000
    }));
  },
};

const Bitget = {
  name: 'bitget',
  host: 'api.bitget.com',

  // Bitget: symbols и tickers в одном эндпоинте — кэшируем результат
  _tickersCache: null,
  _tickersCachedAt: 0,

  async _fetchTickers() {
    const now = Date.now();
    if (this._tickersCache && now - this._tickersCachedAt < 30_000) return this._tickersCache;
    const d = await fetchWithRetry('bitget', this.host, '/api/v2/mix/market/tickers?productType=USDT-FUTURES');
    this._tickersCache = d.data || [];
    this._tickersCachedAt = now;
    return this._tickersCache;
  },

  async getSymbols() {
    const data = await this._fetchTickers();
    return data.map(s => s.symbol);
  },

  async getTickers() {
    const data = await this._fetchTickers();
    return data.map(t => ({
      symbol: t.symbol,
      price: parseFloat(t.lastPr),
      priceChangePercent: parseFloat(t.change24h) * 100,
      quoteVolume: parseFloat(t.quoteVolume || t.usdtVolume || 0),
      count: 0,
    }));
  },

  tfMap: { '1m':'1m','5m':'5m','15m':'15m','1h':'1h','4h':'4h','1d':'1d' },

  async getKlines(symbol, interval, limit = 1000, endTime = null) {
    const tf = this.tfMap[interval] || interval;
    let qs = `symbol=${symbol}&granularity=${tf}&limit=${limit}&productType=usdt-futures`;
    if (endTime) qs += `&endTime=${endTime}`;
    const d = await fetchWithRetry('bitget', this.host, `/api/v2/mix/market/candles?${qs}`);
    return (d.data || []).reverse().map(c => ({
      time: +c[0]/1000, open:+c[1], high:+c[2], low:+c[3], close:+c[4], volume:+c[5], openTime:+c[0]
    }));
  },
};

const EXCHANGES = { binance: Binance, bybit: Bybit, okx: OKX, gateio: GateIO, bitget: Bitget };

// --- Умный кэш свечей ---------------------------------------------------------
// Исторические свечи (закрытые) — не меняются никогда > храним бессрочно
// Только последнюю открытую свечу обновляем по TTL
const kCache = {};  // kCache[exchange][symbol][interval] = { candles[], lastOpenTime, updatedAt }

const REFRESH_TTL = {
  '1m': 30_000,   '5m': 60_000,  '15m': 90_000,
  '1h': 120_000,  '4h': 180_000, '1d': 300_000,
};

function getCached(exchange, symbol, interval) {
  return kCache[exchange]?.[symbol]?.[interval] || null;
}

function setCached(exchange, symbol, interval, candles) {
  if (!kCache[exchange]) kCache[exchange] = {};
  if (!kCache[exchange][symbol]) kCache[exchange][symbol] = {};
  const lastOpenTime = candles.length ? candles[candles.length - 1].openTime : 0;
  kCache[exchange][symbol][interval] = { candles, lastOpenTime, updatedAt: Date.now() };
}

function needsRefresh(cached, interval) {
  if (!cached) return true;
  const ttl = REFRESH_TTL[interval] || 60_000;
  return Date.now() - cached.updatedAt > ttl;
}

// --- Загрузка истории (несколько батчей назад) --------------------------------
async function loadHistory(exchange, symbol, interval) {
  const ex = EXCHANGES[exchange];
  // OKX даёт 300 свечей за раз > нужно 13 проходов для ~3900 свечей (~1 мес на 5m)
  // Остальные дают 1000 > 4 прохода = ~4000 свечей
  const passes  = exchange === 'okx' ? 13 : 4;
  const limit   = exchange === 'okx' ? 300 : 1000;

  let all = [];
  let endTime = null;

  for (let i = 0; i < passes; i++) {
    const batch = await ex.getKlines(symbol, interval, limit, endTime);
    if (!batch || batch.length === 0) break;
    all = [...batch, ...all];
    endTime = batch[0].openTime - 1;
    // Небольшая пауза только если продолжаем (не последний батч)
    if (i < passes - 1 && batch.length === limit) await sleep(80);
    else break; // получили меньше лимита — история закончилась
  }

  // Убираем дубли, сортируем
  const seen = new Set();
  return all
    .filter(c => { if (seen.has(c.time)) return false; seen.add(c.time); return true; })
    .sort((a, b) => a.time - b.time);
}

// --- Инкрементальное обновление кэша -----------------------------------------
// Вместо перезагрузки всей истории — подтягиваем только новые свечи
async function refreshCache(exchange, symbol, interval, cached) {
  const ex = EXCHANGES[exchange];
  // Запрашиваем свечи начиная с последней закрытой (без endTime = последние N)
  const fresh = await ex.getKlines(symbol, interval, exchange === 'okx' ? 300 : 1000);
  if (!fresh || fresh.length === 0) return cached.candles;

  // Мержим: берём все старые закрытые свечи + все новые
  const cutoff = cached.candles.length > 1
    ? cached.candles[cached.candles.length - 2].openTime  // до предпоследней включительно
    : 0;
  const oldPart = cached.candles.filter(c => c.openTime <= cutoff);
  const merged  = [...oldPart, ...fresh];

  // Дедуп + сортировка
  const seen = new Set();
  return merged
    .filter(c => { if (seen.has(c.time)) return false; seen.add(c.time); return true; })
    .sort((a, b) => a.time - b.time);
}

// --- Кэш метаданных (символы + тикеры) ---------------------------------------
const metaCache = {};
const META_TTL  = 5 * 60_000; // 5 минут

async function getMeta(exchange) {
  const now = Date.now();
  if (metaCache[exchange] && now - metaCache[exchange].updatedAt < META_TTL) {
    return metaCache[exchange];
  }
  const ex = EXCHANGES[exchange];
  const [symbols, tickers] = await Promise.all([ex.getSymbols(), ex.getTickers()]);
  metaCache[exchange] = { symbols, tickers, updatedAt: now };
  return metaCache[exchange];
}

// --- Роуты --------------------------------------------------------------------

app.get('/ping', (req, res) => {
  const cacheStats = {};
  for (const ex of Object.keys(kCache)) {
    let syms = 0, candles = 0;
    for (const sym of Object.keys(kCache[ex])) {
      syms++;
      for (const tf of Object.keys(kCache[ex][sym])) {
        candles += kCache[ex][sym][tf].candles.length;
      }
    }
    cacheStats[ex] = { syms, candles };
  }
  res.json({ ok: true, uptime: Math.round(process.uptime()), cache: cacheStats });
});

app.get('/exchanges', (req, res) => res.json(Object.keys(EXCHANGES)));

// GET /symbols?exchange=binance
app.get('/symbols', async (req, res) => {
  const { exchange = 'binance' } = req.query;
  if (!EXCHANGES[exchange]) return res.status(400).json({ error: 'Неизвестная биржа' });
  try {
    const meta = await getMeta(exchange);
    res.json(meta.symbols);
  } catch (e) {
    console.error('[/symbols]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /tickers?exchange=binance
app.get('/tickers', async (req, res) => {
  const { exchange = 'binance' } = req.query;
  if (!EXCHANGES[exchange]) return res.status(400).json({ error: 'Неизвестная биржа' });
  try {
    const meta = await getMeta(exchange);
    res.json(meta.tickers);
  } catch (e) {
    console.error('[/tickers]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /klines?exchange=binance&symbol=BTCUSDT&interval=5m
app.get('/klines', async (req, res) => {
  const { exchange = 'binance', symbol, interval = '5m' } = req.query;
  if (!symbol)              return res.status(400).json({ error: 'Нужен symbol' });
  if (!EXCHANGES[exchange]) return res.status(400).json({ error: 'Неизвестная биржа' });

  const cached = getCached(exchange, symbol, interval);

  // Кэш свежий — отдаём сразу
  if (!needsRefresh(cached, interval)) {
    return res.json(cached.candles);
  }

  // Дедупликация: один реальный запрос даже при параллельных обращениях
  const key = `${exchange}:${symbol}:${interval}`;
  try {
    let candles;
    if (!cached) {
      // Первая загрузка — полная история
      candles = await dedupe(key, () => loadHistory(exchange, symbol, interval));
    } else {
      // Обновление — только новые свечи
      candles = await dedupe(key, () => refreshCache(exchange, symbol, interval, cached));
    }
    setCached(exchange, symbol, interval, candles);
    res.json(candles);
  } catch (e) {
    console.error(`[/klines] ${exchange} ${symbol} ${interval}:`, e.message);
    // Отдаём устаревший кэш если есть
    if (cached) return res.json(cached.candles);
    res.status(500).json({ error: e.message });
  }
});

// GET /cache-status — детальная статистика кэша
app.get('/cache-status', (req, res) => {
  const result = {};
  for (const ex of Object.keys(kCache)) {
    result[ex] = { symbols: 0, candles: 0, entries: [] };
    for (const sym of Object.keys(kCache[ex])) {
      result[ex].symbols++;
      for (const tf of Object.keys(kCache[ex][sym])) {
        const entry = kCache[ex][sym][tf];
        result[ex].candles += entry.candles.length;
        result[ex].entries.push({
          symbol: sym, tf,
          candles: entry.candles.length,
          age: Math.round((Date.now() - entry.updatedAt) / 1000) + 's'
        });
      }
    }
  }
  res.json(result);
});

// --- Запуск -------------------------------------------------------------------
app.listen(PORT, '0.0.0.0', () => {
  console.log(`? KopiBar сервер запущен: http://0.0.0.0:${PORT}`);
  console.log(`   Биржи: ${Object.keys(EXCHANGES).join(', ')}`);
  console.log(`   Проверка: http://77.239.105.144:${PORT}/ping`);
});