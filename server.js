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
  let qs = `symbol=${symbol}&interval=${TF_MAP[interval]}&limit=${limit}`;
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

// Примерное кол-во свечей на TF (для прогресс-бара)
const TF_ESTIMATE = { '1m':280000,'5m':56000,'15m':19000,'1h':4500,'4h':1200,'1d':300 };

// ─── Скачивание ПОЛНОЙ истории одной монеты/TF ────────────────────────────────
async function downloadFullHistory(symbol, tf) {
  const state = stmtGetState.get('binance', symbol, tf);
  if (state && state.full_loaded) return 0; // уже скачано полностью

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

    // Сохраняем батч в БД
    const rows = batch.map(c => ({
      exchange: 'binance', symbol, tf,
      time: c.time, open: c.open, high: c.high,
      low: c.low, close: c.close, volume: c.volume
    }));
    insertMany(rows);
    totalNew += rows.length;
    batchNum++;

    // Обновляем статус для /download-status
    const pct = Math.min(Math.round(totalNew / estimate * 100), 99);
    downloadStatus.currentTf  = tf;
    downloadStatus.currentPct = pct;
    downloadStatus.currentCandles = totalNew;
    downloadStatus.currentEstimate = estimate;

    // Логируем каждые 10 батчей (10000 свечей)
    if (batchNum % 10 === 0 && batchNum !== lastLogBatch) {
      lastLogBatch = batchNum;
      const bar = progressBar(Math.min(totalNew, estimate), estimate, 24);
      console.log(`[download]   ${tf}: ${bar} (${totalNew} свечей)`);
    }

    // Следующий батч — ещё глубже в историю
    endTime = batch[0].openTime - 1;

    // Если меньше 1000 — дошли до начала истории
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

// ─── Обновление новых свечей (инкрементально) ─────────────────────────────────
async function updateSymbol(symbol, tf) {
  const newest = stmtGetNewest.get('binance', symbol, tf)?.t;
  const startTime = newest ? (newest * 1000 + 1) : null;

  let qs = `symbol=${symbol}&interval=${TF_MAP[tf]}&limit=1000`;
  if (startTime) qs += `&startTime=${startTime}`;

  let batch;
  try {
    batch = await limiter.schedule(() => httpGet(`/fapi/v1/klines?${qs}`));
  } catch(e) { return 0; }

  if (!Array.isArray(batch) || batch.length === 0) return 0;

  const rows = batch.map(c => ({
    exchange:'binance', symbol, tf,
    time: Math.floor(c[0]/1000), open: parseFloat(c[1]),
    high: parseFloat(c[2]), low: parseFloat(c[3]),
    close: parseFloat(c[4]), volume: parseFloat(c[5])
  }));
  insertMany(rows);

  // Обновляем newest_time
  const newNewest = stmtGetNewest.get('binance', symbol, tf)?.t;
  const oldest    = stmtGetOldest.get('binance', symbol, tf)?.t;
  const state     = stmtGetState.get('binance', symbol, tf);
  stmtSetState.run({ exchange:'binance', symbol, tf,
    oldest_time: oldest, newest_time: newNewest,
    full_loaded: state?.full_loaded || 0 });

  return rows.length;
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

// ─── Роуты ───────────────────────────────────────────────────────────────────

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

// GET /klines?symbol=BTCUSDT&interval=5m&limit=1000&before=<timestamp_sec>
app.get('/klines', async (req, res) => {
  const { symbol, interval = '5m', limit = 1000, before } = req.query;
  if (!symbol) return res.status(400).json({ error: 'Нужен symbol' });

  const lim = Math.min(parseInt(limit) || 1000, 2000);

  try {
    let rows;
    if (before) {
      // Lazy loading — свечи ДО указанного времени
      rows = db.prepare(`
        SELECT time,open,high,low,close,volume,
               (time * 1000) as openTime
        FROM candles
        WHERE exchange='binance' AND symbol=? AND tf=? AND time < ?
        ORDER BY time DESC LIMIT ?
      `).all(symbol, interval, parseInt(before), lim);
    } else {
      // Последние N свечей
      rows = db.prepare(`
        SELECT time,open,high,low,close,volume,
               (time * 1000) as openTime
        FROM candles
        WHERE exchange='binance' AND symbol=? AND tf=?
        ORDER BY time DESC LIMIT ?
      `).all(symbol, interval, lim);
    }

    // Возвращаем в хронологическом порядке
    rows.reverse();
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
});