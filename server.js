/**
 * KopiBar Server v2.0
 * SQLite хранилище + полная история Binance + автообновление
 */

const express  = require('express');
const cors     = require('cors');
const https    = require('https');
const zlib     = require('zlib');
const path     = require('path');
const fs       = require('fs');
const Database = require('better-sqlite3');

const app  = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

// ─── Rate limiting ────────────────────────────────────────────────────────────
const rateLimit = require('express-rate-limit');
app.use('/klines',  rateLimit({ windowMs:60000, max:600, handler:(req,res)=>res.status(429).json({error:'Too many requests'}) }));
app.use('/symbols', rateLimit({ windowMs:60000, max:120, handler:(req,res)=>res.status(429).json({error:'Too many requests'}) }));
app.use('/tickers', rateLimit({ windowMs:60000, max:120, handler:(req,res)=>res.status(429).json({error:'Too many requests'}) }));

// ─── SQLite ───────────────────────────────────────────────────────────────────
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(path.join(DATA_DIR, 'candles.db'));
db.pragma('journal_mode = WAL');   // быстрая запись
db.pragma('synchronous = NORMAL'); // баланс скорость/надёжность
db.pragma('cache_size = -64000');  // 64 МБ кэш SQLite
db.pragma('temp_store = MEMORY');

// Создаём таблицы
db.exec(`
  CREATE TABLE IF NOT EXISTS candles (
    exchange TEXT NOT NULL,
    symbol   TEXT NOT NULL,
    tf       TEXT NOT NULL,
    time     INTEGER NOT NULL,
    open     REAL NOT NULL,
    high     REAL NOT NULL,
    low      REAL NOT NULL,
    close    REAL NOT NULL,
    volume   REAL NOT NULL,
    PRIMARY KEY (exchange, symbol, tf, time)
  );
  CREATE INDEX IF NOT EXISTS idx_candles_query ON candles(exchange, symbol, tf, time DESC);

  CREATE TABLE IF NOT EXISTS download_state (
    exchange TEXT NOT NULL,
    symbol   TEXT NOT NULL,
    tf       TEXT NOT NULL,
    oldest_time  INTEGER DEFAULT NULL,
    newest_time  INTEGER DEFAULT NULL,
    full_loaded  INTEGER DEFAULT 0,
    PRIMARY KEY (exchange, symbol, tf)
  );
`);

// Подготовленные запросы
const stmtInsert = db.prepare(`
  INSERT OR REPLACE INTO candles (exchange,symbol,tf,time,open,high,low,close,volume)
  VALUES (@exchange,@symbol,@tf,@time,@open,@high,@low,@close,@volume)
`);
const stmtGetNewest = db.prepare(`SELECT MAX(time) as t FROM candles WHERE exchange=? AND symbol=? AND tf=?`);
const stmtGetOldest = db.prepare(`SELECT MIN(time) as t FROM candles WHERE exchange=? AND symbol=? AND tf=?`);
const stmtGetState  = db.prepare(`SELECT * FROM download_state WHERE exchange=? AND symbol=? AND tf=?`);
const stmtSetState  = db.prepare(`
  INSERT OR REPLACE INTO download_state (exchange,symbol,tf,oldest_time,newest_time,full_loaded)
  VALUES (@exchange,@symbol,@tf,@oldest_time,@newest_time,@full_loaded)
`);
const stmtCount = db.prepare(`SELECT COUNT(*) as n FROM candles WHERE exchange=? AND symbol=? AND tf=?`);

// Пакетная вставка
const insertMany = db.transaction((rows) => {
  for (const row of rows) stmtInsert.run(row);
});

// ─── HTTP запрос к Binance ────────────────────────────────────────────────────
const agent = new https.Agent({ keepAlive: true, maxSockets: 10 });

function httpGet(path) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { hostname: 'fapi.binance.com', path, method: 'GET',
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip, deflate' },
        agent },
      (res) => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          const buf = Buffer.concat(chunks);
          const enc = res.headers['content-encoding'];
          const parse = (data) => {
            try { resolve(JSON.parse(data)); }
            catch(e) { reject(new Error('JSON parse: ' + data.toString().slice(0,100))); }
          };
          if (enc === 'gzip')    zlib.gunzip(buf, (e,d) => e ? reject(e) : parse(d.toString()));
          else if (enc === 'deflate') zlib.inflate(buf, (e,d) => e ? reject(e) : parse(d.toString()));
          else parse(buf.toString());
        });
      }
    );
    req.on('error', reject);
    req.setTimeout(20000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Rate limiter для Binance (не более 18 запросов/сек)
class RateLimiter {
  constructor(rps) { this.interval = 1000/rps; this.queue = []; this.running = false; }
  schedule(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({fn, resolve, reject});
      if (!this.running) this._run();
    });
  }
  async _run() {
    this.running = true;
    while (this.queue.length) {
      const {fn, resolve, reject} = this.queue.shift();
      try { resolve(await fn()); } catch(e) { reject(e); }
      if (this.queue.length) await sleep(this.interval);
    }
    this.running = false;
  }
}
const limiter = new RateLimiter(10); // консервативно 10 rps при скачивании

// ─── Прогресс-бар ─────────────────────────────────────────────────────────────
function progressBar(current, total, width = 30) {
  const pct  = total > 0 ? current / total : 0;
  const filled = Math.round(pct * width);
  const bar  = '█'.repeat(filled) + '░'.repeat(width - filled);
  return `${bar} ${Math.round(pct*100)}% (${current}/${total})`;
}

// ─── Binance API ──────────────────────────────────────────────────────────────
const TF_MAP = { '1m':'1m','5m':'5m','15m':'15m','1h':'1h','4h':'4h','1d':'1d' };
const TIMEFRAMES = ['1m','5m','15m','1h','4h','1d'];

async function binanceKlines(symbol, interval, limit=1000, endTime=null) {
  let qs = `symbol=${encodeURIComponent(symbol)}&interval=${TF_MAP[interval]}&limit=${limit}`;
  if (endTime) qs += `&endTime=${endTime}`;
  const data = await limiter.schedule(() => httpGet(`/fapi/v1/klines?${qs}`));
  if (!Array.isArray(data)) throw new Error('Не массив: ' + JSON.stringify(data).slice(0,100));
  return data.map(c => ({
    time:   Math.floor(c[0]/1000),
    open:   parseFloat(c[1]),
    high:   parseFloat(c[2]),
    low:    parseFloat(c[3]),
    close:  parseFloat(c[4]),
    volume: parseFloat(c[5]),
    openTime: c[0]
  }));
}

async function binanceSymbols() {
  const d = await httpGet('/fapi/v1/exchangeInfo');
  return d.symbols
    .filter(s => s.status === 'TRADING' && s.quoteAsset === 'USDT')
    .map(s => s.symbol);
}

async function binanceTickers() {
  const d = await httpGet('/fapi/v1/ticker/24hr');
  if (!Array.isArray(d)) return [];
  return d.map(t => ({
    symbol: t.symbol,
    price: parseFloat(t.lastPrice),
    priceChangePercent: parseFloat(t.priceChangePercent),
    quoteVolume: parseFloat(t.quoteVolume),
    count: parseInt(t.count) || 0,
  }));
}

// Лимиты глубины истории (в миллисекундах от текущего момента)
const TF_DEPTH_MS = {
  '1m':  3   * 24 * 60 * 60 * 1000, // 3 дня
  '5m':  14  * 24 * 60 * 60 * 1000, // 2 недели
  '15m': 30  * 24 * 60 * 60 * 1000, // 1 месяц
  '1h':  180 * 24 * 60 * 60 * 1000, // 6 месяцев
  '4h':  365 * 24 * 60 * 60 * 1000, // 1 год
  '1d':  0,                          // 0 = без лимита (вся история)
};

// Примерное кол-во свечей на TF (для прогресс-бара)
const TF_ESTIMATE = { '1m':4320,'5m':4032,'15m':2880,'1h':4320,'4h':2190,'1d':2000 };

// ─── Скачивание ПОЛНОЙ истории одной монеты/TF ────────────────────────────────
async function downloadFullHistory(symbol, tf) {
  const state = stmtGetState.get('binance', symbol, tf);
  if (state && state.full_loaded) return 0; // уже скачано полностью

  // Вычисляем нижнюю границу времени (не качаем глубже)
  const depthMs  = TF_DEPTH_MS[tf] || 0;
  const minTime  = depthMs > 0 ? Date.now() - depthMs : 0; // unix ms

  let endTime   = state?.oldest_time ? (state.oldest_time * 1000 - 1) : null;
  let totalNew  = 0;
  let batchNum  = 0;
  const estimate = TF_ESTIMATE[tf] || 5000;
  let lastLogBatch = -1;

  while (true) {
    let batch;
    try {
      batch = await binanceKlines(symbol, tf, 1000, endTime);
    } catch(e) {
      if (e.message.includes('429') || e.message.includes('418')) {
        console.warn(`[download]   ${tf}: rate limit, ждём 5 сек...`);
        await sleep(5000);
        continue;
      }
      throw e;
    }

    if (!batch || batch.length === 0) break;

    // Обрезаем батч если достигли нижней границы
    let reachedLimit = false;
    if (minTime > 0) {
      const filtered = batch.filter(c => c.openTime >= minTime);
      if (filtered.length < batch.length) reachedLimit = true;
      batch = filtered;
    }

    if (batch.length > 0) {
      const rows = batch.map(c => ({
        exchange: 'binance', symbol, tf,
        time: c.time, open: c.open, high: c.high,
        low: c.low, close: c.close, volume: c.volume
      }));
      insertMany(rows);
      totalNew += rows.length;
      batchNum++;
    }

    // Обновляем статус для /download-status
    const pct = Math.min(Math.round(totalNew / estimate * 100), 99);
    downloadStatus.currentTf  = tf;
    downloadStatus.currentPct = pct;
    downloadStatus.currentCandles = totalNew;
    downloadStatus.currentEstimate = estimate;

    // Логируем каждые 5 батчей
    if (batchNum % 5 === 0 && batchNum !== lastLogBatch) {
      lastLogBatch = batchNum;
      const bar = progressBar(Math.min(totalNew, estimate), estimate, 24);
      console.log(`[download]   ${tf}: ${bar} (${totalNew} свечей)`);
    }

    // Достигли лимита глубины — считаем полностью скачанным
    if (reachedLimit) {
      const oldest = stmtGetOldest.get('binance', symbol, tf)?.t;
      const newest = stmtGetNewest.get('binance', symbol, tf)?.t;
      stmtSetState.run({ exchange:'binance', symbol, tf,
        oldest_time: oldest, newest_time: newest, full_loaded: 1 });
      break;
    }

    // Следующий батч — ещё глубже в историю
    endTime = batch.length > 0 ? batch[0].openTime - 1 : null;
    if (!endTime) break;

    // Если меньше 1000 — дошли до начала истории биржи
    if (batch.length < 1000) {
      const oldest = stmtGetOldest.get('binance', symbol, tf)?.t;
      const newest = stmtGetNewest.get('binance', symbol, tf)?.t;
      stmtSetState.run({ exchange:'binance', symbol, tf,
        oldest_time: oldest, newest_time: newest, full_loaded: 1 });
      break;
    }

    // Сохраняем состояние каждые 5 батчей
    if (batchNum % 5 === 0) {
      const oldest = stmtGetOldest.get('binance', symbol, tf)?.t;
      const newest = stmtGetNewest.get('binance', symbol, tf)?.t;
      stmtSetState.run({ exchange:'binance', symbol, tf,
        oldest_time: oldest, newest_time: newest, full_loaded: 0 });
    }

    await sleep(100);
  }

  // Финальная строка (только если были новые свечи)
  if (totalNew > 0) {
    console.log(`[download]   ${tf}: ${'█'.repeat(24)} 100% (${totalNew} свечей) ✓`);
  }
  return totalNew;
}

// ─── Обновление новых свечей (инкрементально, все пропуски) ───────────────────
async function updateSymbol(symbol, tf) {
  const newest = stmtGetNewest.get('binance', symbol, tf)?.t;
  let startTime = newest ? (newest * 1000) : null; // включаем последнюю свечу чтобы перезаписать незакрытую
  let totalNew = 0;

  while (true) {
    let qs = `symbol=${encodeURIComponent(symbol)}&interval=${TF_MAP[tf]}&limit=1000`;
    if (startTime) qs += `&startTime=${startTime}`;

    let batch;
    try {
      batch = await limiter.schedule(() => httpGet(`/fapi/v1/klines?${qs}`));
    } catch(e) { break; }

    if (!Array.isArray(batch) || batch.length === 0) break;

    const rows = batch.map(c => ({
      exchange:'binance', symbol, tf,
      time: Math.floor(c[0]/1000), open: parseFloat(c[1]),
      high: parseFloat(c[2]), low: parseFloat(c[3]),
      close: parseFloat(c[4]), volume: parseFloat(c[5])
    }));
    insertMany(rows);
    totalNew += rows.length;

    if (batch.length < 1000) break;
    startTime = batch[batch.length - 1][0] + 1;
  }

  if (totalNew > 0) {
    const newNewest = stmtGetNewest.get('binance', symbol, tf)?.t;
    const oldest    = stmtGetOldest.get('binance', symbol, tf)?.t;
    const state     = stmtGetState.get('binance', symbol, tf);
    stmtSetState.run({ exchange:'binance', symbol, tf,
      oldest_time: oldest, newest_time: newNewest,
      full_loaded: state?.full_loaded || 0 });
  }

  return totalNew;
}

// ─── Мета-кэш (символы и тикеры) ─────────────────────────────────────────────
let metaCache = { symbols: [], tickers: [], updatedAt: 0 };

async function getMeta() {
  if (Date.now() - metaCache.updatedAt < 5 * 60_000) return metaCache;
  const [symbols, tickers] = await Promise.all([binanceSymbols(), binanceTickers()]);
  metaCache = { symbols, tickers, updatedAt: Date.now() };
  return metaCache;
}

// ─── Первичная загрузка всей истории ─────────────────────────────────────────
let downloadInProgress = false;
let downloadStatus = { total: 0, done: 0, current: '', currentTf: '', currentPct: 0, failed: [] };

async function runFullDownload() {
  if (downloadInProgress) return;
  downloadInProgress = true;

  console.log('\n[download] ═══════════════════════════════════════');
  console.log('[download] Начинаю загрузку полной истории Binance');
  console.log('[download] ═══════════════════════════════════════\n');

  const meta = await getMeta();
  const symbols = meta.symbols;
  downloadStatus.total = symbols.length;
  downloadStatus.done  = 0;
  downloadStatus.failed = [];

  for (let i = 0; i < symbols.length; i++) {
    const symbol = symbols[i];
    downloadStatus.current = symbol;
    downloadStatus.done    = i;

    console.log(`\n[download] Общий прогресс: ${progressBar(i, symbols.length)}`);
    console.log(`[download] Монета: ${symbol}`);

    for (const tf of TIMEFRAMES) {
      const state = stmtGetState.get('binance', symbol, tf);
      if (state && state.full_loaded) {
        console.log(`[download]   ${tf}: уже скачан ✓`);
        continue;
      }

      downloadStatus.currentTf = tf;
      let attempts = 0;

      while (attempts < 3) {
        try {
          await downloadFullHistory(symbol, tf);
          break;
        } catch(e) {
          attempts++;
          console.warn(`[download]   ${tf}: ошибка (попытка ${attempts}/3): ${e.message}`);
          if (attempts < 3) await sleep(3000);
          else {
            downloadStatus.failed.push(`${symbol}:${tf}`);
            console.error(`[download]   ${tf}: пропускаем ✗`);
          }
        }
      }

      await sleep(200); // пауза между TF
    }
  }

  downloadStatus.done = symbols.length;
  console.log(`\n[download] ═══════════════════════════════════════`);
  console.log(`[download] ✅ Загрузка завершена! ${symbols.length} монет`);
  if (downloadStatus.failed.length > 0) {
    console.log(`[download] ⚠ Не загружено: ${downloadStatus.failed.length} позиций`);
    console.log(`[download]   ${downloadStatus.failed.join(', ')}`);
  }
  console.log(`[download] ═══════════════════════════════════════\n`);

  downloadInProgress = false;
}

// ─── Автообновление новых свечей каждую минуту ───────────────────────────────
async function runUpdater() {
  while (true) {
    await sleep(60_000); // каждую минуту
    if (downloadInProgress) continue; // не мешаем первичной загрузке

    try {
      const meta = await getMeta();
      let totalNew = 0;
      for (const symbol of meta.symbols) {
        for (const tf of TIMEFRAMES) {
          const state = stmtGetState.get('binance', symbol, tf);
          if (!state || !state.full_loaded) continue; // ещё не скачано
          const n = await updateSymbol(symbol, tf);
          totalNew += n;
          await sleep(50);
        }
      }
      if (totalNew > 0) console.log(`[update] +${totalNew} новых свечей`);
    } catch(e) {
      console.warn('[update] Ошибка:', e.message);
    }
  }
}

// ─── Серверный расчёт статистики (NATR / волатильность / корреляция) ──────────

// statsCache[tf][symbol] = { natr, volat, corr, updatedAt }
const statsCache = {};

// Периоды (в часах) — используем дефолтные значения как на фронте
const NATR_PERIOD_H  = 2;
const VOLAT_PERIOD_H = 6;
const CORR_PERIOD_H  = 1;

function calcNATR(candles, periodH, tfMin) {
  const n = Math.max(2, Math.round((periodH * 60) / tfMin));
  const slice = candles.slice(-n);
  if (slice.length < 2) return 0;
  const lastPrice = slice[slice.length - 1].close;
  if (!lastPrice) return 0;
  let totalTR = 0;
  for (let i = 1; i < slice.length; i++) {
    totalTR += Math.max(
      slice[i].high - slice[i].low,
      Math.abs(slice[i].high - slice[i-1].close),
      Math.abs(slice[i].low  - slice[i-1].close)
    );
  }
  return (totalTR / (slice.length - 1) / lastPrice) * 100;
}

function calcVolat(candles, periodH, tfMin) {
  const n = Math.max(2, Math.round((periodH * 60) / tfMin));
  const slice = candles.slice(-n);
  if (slice.length < 2) return 0;
  const rets = [];
  for (let i = 1; i < slice.length; i++) {
    if (slice[i-1].close > 0 && slice[i].close > 0)
      rets.push(Math.log(slice[i].close / slice[i-1].close));
  }
  if (rets.length < 2) return 0;
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  return Math.sqrt(rets.reduce((s, r) => s + (r - mean) ** 2, 0) / (rets.length - 1)) * 100;
}

function calcCorr(candles, btcCandles, periodH, tfMin) {
  const n = Math.max(10, Math.round((periodH * 60) / tfMin));
  const slice    = candles.slice(-n);
  const btcMap   = new Map(btcCandles.map(c => [c.time, c.close]));
  const x = [], y = [];
  for (let i = 1; i < slice.length; i++) {
    const btcPrev = btcMap.get(slice[i-1].time);
    const btcCur  = btcMap.get(slice[i].time);
    if (!btcPrev || !btcCur) continue;
    x.push((slice[i].close - slice[i-1].close) / slice[i-1].close);
    y.push((btcCur - btcPrev) / btcPrev);
  }
  if (x.length < 2) return null;
  const n2 = x.length;
  const mx = x.reduce((a, b) => a + b, 0) / n2;
  const my = y.reduce((a, b) => a + b, 0) / n2;
  let num = 0, dx2 = 0, dy2 = 0;
  for (let i = 0; i < n2; i++) {
    const dx = x[i] - mx, dy = y[i] - my;
    num += dx * dy; dx2 += dx * dx; dy2 += dy * dy;
  }
  if (!dx2 || !dy2) return null;
  return Math.round((num / Math.sqrt(dx2 * dy2)) * 100);
}

function getCandles(symbol, tf, limit) {
  return db.prepare(`
    SELECT time, open, high, low, close, volume
    FROM candles WHERE exchange='binance' AND symbol=? AND tf=?
    ORDER BY time DESC LIMIT ?
  `).all(symbol, tf, limit).reverse();
}

const TF_MIN_MAP = { '1m':1,'5m':5,'15m':15,'1h':60,'4h':240,'1d':1440 };

async function computeAllStats(tf) {
  const tfMin   = TF_MIN_MAP[tf] || 5;
  const needed  = Math.max(
    Math.round((NATR_PERIOD_H  * 60) / tfMin),
    Math.round((VOLAT_PERIOD_H * 60) / tfMin),
    Math.round((CORR_PERIOD_H  * 60) / tfMin)
  ) + 10;

  // Загружаем BTC для корреляции
  const btcSymbol = 'BTCUSDT';
  const btcCandles = getCandles(btcSymbol, tf, needed);

  const meta    = await getMeta();
  const result  = {};

  for (const symbol of meta.symbols) {
    try {
      const candles = getCandles(symbol, tf, needed);
      if (candles.length < 2) continue;

      const isBtc = symbol.replace(/[-_].*/,'').toUpperCase().startsWith('BTC');
      result[symbol] = {
        natr:  parseFloat(calcNATR(candles, NATR_PERIOD_H, tfMin).toFixed(2)),
        volat: parseFloat(calcVolat(candles, VOLAT_PERIOD_H, tfMin).toFixed(2)),
        corr:  isBtc ? 100 : calcCorr(candles, btcCandles, CORR_PERIOD_H, tfMin),
      };
    } catch(e) { /* пропускаем */ }
  }

  if (!statsCache[tf]) statsCache[tf] = {};
  statsCache[tf] = { data: result, updatedAt: Date.now() };
  return result;
}

// Пересчёт статистики каждые 60 секунд для активного TF
async function runStatsUpdater() {
  // Начальный прогрев — считаем для всех TF
  await sleep(10_000); // ждём пока сервер поднимется
  for (const tf of TIMEFRAMES) {
    try { await computeAllStats(tf); console.log(`[stats] ${tf} готов`); }
    catch(e) { console.warn(`[stats] ошибка ${tf}:`, e.message); }
  }

  // Обновляем каждую минуту
  while (true) {
    await sleep(60_000);
    if (downloadInProgress) continue;
    for (const tf of TIMEFRAMES) {
      try { await computeAllStats(tf); }
      catch(e) { console.warn(`[stats] ошибка ${tf}:`, e.message); }
    }
  }
}

// ─── Роуты ───────────────────────────────────────────────────────────────────

// POST /redownload?symbol=BRUSDT — полная перекачка одной монеты
app.post('/redownload', async (req, res) => {
  const { symbol } = req.query;
  if (!symbol) return res.status(400).json({ error: 'Нужен symbol' });
  res.json({ ok: true, message: `Перекачка ${symbol} запущена, смотри логи` });

  console.log(`[redownload] Удаляем старые данные ${symbol}...`);
  for (const tf of ['1m','5m','15m','1h','4h','1d']) {
    db.prepare(`DELETE FROM candles WHERE exchange='binance' AND symbol=? AND tf=?`).run(symbol, tf);
    db.prepare(`DELETE FROM download_state WHERE exchange='binance' AND symbol=? AND tf=?`).run(symbol, tf);
  }
  console.log(`[redownload] Данные удалены, начинаем загрузку...`);
  try {
    await downloadSymbol(symbol);
    console.log(`[redownload] ✅ ${symbol} перекачан`);
  } catch(e) {
    console.error(`[redownload] ❌ Ошибка: ${e.message}`);
  }
});

// POST /fix-gaps — одноразовое заполнение всех пропусков в БД
let fixGapsRunning = false;
app.post('/fix-gaps', async (req, res) => {
  if (fixGapsRunning) return res.json({ ok: false, message: 'Уже запущено' });
  res.json({ ok: true, message: 'Запущено в фоне, смотри логи' });

  fixGapsRunning = true;
  const meta = await getMeta().catch(() => ({ symbols: [] }));
  console.log(`[fix-gaps] Начинаю проверку ${meta.symbols.length} монет...`);

  for (const symbol of meta.symbols) {
    for (const tf of ['1m','5m','15m','1h']) {
      try {
        const rows = db.prepare(`
          SELECT time, (time*1000) as openTime
          FROM candles WHERE exchange='binance' AND symbol=? AND tf=?
          ORDER BY time ASC
        `).all(symbol, tf);
        if (rows.length < 2) continue;

        const step = TF_STEP_SEC[tf];
        const tolerance = step * 1.5;
        let hadGaps = false;

        for (let i = 1; i < rows.length; i++) {
          const diff = rows[i].time - rows[i-1].time;
          if (diff <= tolerance) continue;
          hadGaps = true;
          try {
            const startTime = (rows[i-1].time + step) * 1000;
            const endTime   = (rows[i].time - 1) * 1000;
            const qs = `symbol=${encodeURIComponent(symbol)}&interval=${TF_MAP[tf]}&startTime=${startTime}&endTime=${endTime}&limit=1000`;
            const data = await limiter.schedule(() => httpGet(`/fapi/v1/klines?${qs}`));
            if (!Array.isArray(data) || data.length === 0) continue;
            const newRows = data.map(c => ({
              exchange:'binance', symbol, tf,
              time: Math.floor(c[0]/1000), open: parseFloat(c[1]),
              high: parseFloat(c[2]), low: parseFloat(c[3]),
              close: parseFloat(c[4]), volume: parseFloat(c[5])
            }));
            insertMany(newRows);
            console.log(`[fix-gaps] ${symbol} ${tf}: +${newRows.length} свечей`);
          } catch(e) { /* пропускаем */ }
          await sleep(50);
        }
      } catch(e) { /* пропускаем монету */ }
    }
  }

  console.log('[fix-gaps] ✅ Готово!');
  fixGapsRunning = false;
});


app.get('/stats', async (req, res) => {
  const tf = req.query.tf || '5m';
  if (statsCache[tf] && statsCache[tf].data) {
    return res.json(statsCache[tf].data);
  }
  // Если кэш ещё не готов — считаем прямо сейчас
  try {
    const data = await computeAllStats(tf);
    res.json(data);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});


app.get('/ping', (req, res) => {
  const dbSize = (() => {
    try {
      const stat = fs.statSync(path.join(DATA_DIR, 'candles.db'));
      return Math.round(stat.size / 1024 / 1024) + ' МБ';
    } catch { return '0 МБ'; }
  })();
  res.json({
    ok: true,
    uptime: Math.round(process.uptime()),
    download: {
      inProgress: downloadInProgress,
      total: downloadStatus.total,
      done:  downloadStatus.done,
      current: downloadStatus.current,
      pct: downloadStatus.total > 0 ? Math.round(downloadStatus.done / downloadStatus.total * 100) : 0,
      failed: downloadStatus.failed.length
    },
    dbSize
  });
});

// Статус загрузки
app.get('/download-status', (req, res) => {
  const cur = downloadStatus.currentCandles || 0;
  const est = downloadStatus.currentEstimate || 1000;
  res.json({
    inProgress:  downloadInProgress,
    total:       downloadStatus.total,
    done:        downloadStatus.done,
    failed:      downloadStatus.failed,
    current:     downloadStatus.current,
    currentTf:   downloadStatus.currentTf,
    // Общий прогресс
    totalBar:    progressBar(downloadStatus.done, downloadStatus.total),
    totalPct:    downloadStatus.total > 0 ? Math.round(downloadStatus.done / downloadStatus.total * 100) : 0,
    // Прогресс текущей монеты/TF
    coinBar:     progressBar(Math.min(cur, est), est, 24),
    coinPct:     downloadStatus.currentPct || 0,
    coinCandles: cur,
  });
});

app.get('/symbols', async (req, res) => {
  try {
    const meta = await getMeta();
    res.json(meta.symbols);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/tickers', async (req, res) => {
  try {
    const meta = await getMeta();
    res.json(meta.tickers);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Размер шага в секундах для каждого TF
const TF_STEP_SEC = { '1m':60,'5m':300,'15m':900,'1h':3600,'4h':14400,'1d':86400 };

// Заполнение пропусков в данных из Binance
async function fillGaps(symbol, tf, rows) {
  if (rows.length < 2) return rows;
  const step = TF_STEP_SEC[tf] || 60;
  const tolerance = step * 1.5; // допуск 1.5 шага

  // Находим пропуски
  const gaps = [];
  for (let i = 1; i < rows.length; i++) {
    const diff = rows[i].time - rows[i-1].time;
    if (diff > tolerance) {
      gaps.push({ from: rows[i-1].time, to: rows[i].time });
    }
  }
  if (gaps.length === 0) return rows;

  console.log(`[gaps] ${symbol} ${tf}: найдено ${gaps.length} пропусков, заполняем...`);

  // Заполняем каждый пропуск
  for (const gap of gaps) {
    try {
      const startTime = (gap.from + step) * 1000;
      const endTime   = (gap.to - 1) * 1000;
      const qs = `symbol=${encodeURIComponent(symbol)}&interval=${TF_MAP[tf]}&startTime=${startTime}&endTime=${endTime}&limit=1000`;
      const data = await httpGet(`/fapi/v1/klines?${qs}`);
      if (!Array.isArray(data) || data.length === 0) continue;

      const newRows = data.map(c => ({
        exchange:'binance', symbol, tf,
        time: Math.floor(c[0]/1000), open: parseFloat(c[1]),
        high: parseFloat(c[2]), low: parseFloat(c[3]),
        close: parseFloat(c[4]), volume: parseFloat(c[5])
      }));
      insertMany(newRows);
      console.log(`[gaps] ${symbol} ${tf}: заполнено ${newRows.length} свечей`);
    } catch(e) {
      console.warn(`[gaps] ошибка заполнения: ${e.message}`);
    }
  }

  // Перечитываем данные из БД после заполнения
  return null; // сигнал что нужно перечитать
}

// GET /klines?symbol=BTCUSDT&interval=5m&limit=1000&before=<timestamp_sec>
app.get('/klines', async (req, res) => {
  const { symbol, interval = '5m', limit = 1000, before } = req.query;
  if (!symbol) return res.status(400).json({ error: 'Нужен symbol' });

  const lim = Math.min(parseInt(limit) || 1000, 2000);

  const fetchRows = () => {
    if (before) {
      return db.prepare(`
        SELECT time,open,high,low,close,volume,(time*1000) as openTime
        FROM candles
        WHERE exchange='binance' AND symbol=? AND tf=? AND time < ?
        ORDER BY time DESC LIMIT ?
      `).all(symbol, interval, parseInt(before), lim);
    } else {
      return db.prepare(`
        SELECT time,open,high,low,close,volume,(time*1000) as openTime
        FROM candles
        WHERE exchange='binance' AND symbol=? AND tf=?
        ORDER BY time DESC LIMIT ?
      `).all(symbol, interval, lim);
    }
  };

  try {
    let rows = fetchRows();
    rows.reverse();

    // Подгружаем свежие свечи если есть разрыв с текущим временем
    if (rows.length > 0 && !before) {
      const lastTime = rows[rows.length - 1].time;
      const step     = TF_STEP_SEC[interval] || 60;
      const nowSec   = Math.floor(Date.now() / 1000);
      if (nowSec - lastTime > step * 2) {
        try {
          const n = await updateSymbol(symbol, interval);
          if (n > 0) { rows = fetchRows(); rows.reverse(); }
        } catch(e) { /* игнорируем */ }
      }
    }

    // Заполняем внутренние пропуски синхронно и перечитываем
    if (['1m','5m','15m','1h'].includes(interval) && rows.length > 1) {
      const result = await fillGaps(symbol, interval, rows);
      if (result === null) {
        rows = fetchRows();
        rows.reverse();
      }
    }

    res.json(rows);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Статистика БД
app.get('/db-stats', (req, res) => {
  const total = db.prepare(`SELECT COUNT(*) as n FROM candles`).get().n;
  const byTf  = db.prepare(`SELECT tf, COUNT(*) as n FROM candles WHERE exchange='binance' GROUP BY tf`).all();
  const dbSize = (() => {
    try { return Math.round(fs.statSync(path.join(DATA_DIR,'candles.db')).size/1024/1024) + ' МБ'; }
    catch { return '?'; }
  })();
  res.json({ total, byTf, dbSize });
});

// ─── Запуск ───────────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ KopiBar v2.0 запущен: http://0.0.0.0:${PORT}`);
  console.log(`   БД: ${path.join(DATA_DIR, 'candles.db')}`);
  console.log(`   Ping: http://77.239.105.144:${PORT}/ping`);

  // Запускаем загрузку истории в фоне
  setTimeout(() => {
    runFullDownload().catch(e => console.error('[download] Критическая ошибка:', e.message));
  }, 3000);

  // Запускаем обновление новых свечей
  runUpdater().catch(e => console.error('[update] Критическая ошибка:', e.message));

  // Запускаем серверный расчёт статистики
  runStatsUpdater().catch(e => console.error('[stats] Критическая ошибка:', e.message));
});