/**
 * Polymarket BTC 5m Up/Down：服务端 CLOB Market WSS + RTDS Chainlink，
 * 每秒写入 MySQL；Express 提供静态页与 JSON API。
 */

import express from "express";
import http from "http";
import path from "path";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "url";
import mysql from "mysql2/promise";
import WebSocket, { WebSocketServer } from "ws";
import {
  resolveRollingMarketFromGamma,
  tokenIdsForUpDown,
  marketEndIso,
  EVENT_SERIES_PREFIX,
  GAMMA_BASE,
} from "./lib/gammaRolling.mjs";
import { createBookState } from "./lib/polymarketBook.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** 读取项目根目录 `.env`（不覆盖已在 shell 里设置的变量） */
function loadDotEnvFromFile() {
  try {
    const envPath = path.join(__dirname, ".env");
    if (!existsSync(envPath)) return;
    let raw = readFileSync(envPath, "utf8");
    if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq <= 0) continue;
      const key = trimmed.slice(0, eq).trim();
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;
      let val = trimmed.slice(eq + 1).trim();
      if (
        (val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))
      ) {
        val = val.slice(1, -1);
      }
      if (process.env[key] === undefined) process.env[key] = val;
    }
  } catch {
    /* ignore */
  }
}

loadDotEnvFromFile();

const PORT = Number(process.env.PORT) || 3840;
const CLOB_TIME_URL = (process.env.CLOB_TIME_URL || "https://clob.polymarket.com/time").replace(/\/$/, "");
const PM_MARKET_WSS = process.env.PM_MARKET_WS || "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const RTDS_WS_URL = process.env.RTDS_WS_URL || "wss://ws-live-data.polymarket.com";
const RTDS_CHAINLINK_SYMBOL = (process.env.RTDS_CHAINLINK_SYMBOL || "btc/usd").trim().toLowerCase();

const DB_HOST = process.env.DB_HOST || "127.0.0.1";
const DB_USER = process.env.DB_USER || "poly";
const DB_PASSWORD = process.env.DB_PASSWORD || "poly";
const DB_NAME = process.env.DB_NAME || "poly";

const TICK_MS = Math.max(200, Number(process.env.TICK_MS) || 1000);
const GAMMA_REFRESH_MS = Math.max(3000, Number(process.env.GAMMA_REFRESH_MS) || 10000);

/** @type {number} */
let timeOffsetMs = 0;

function offsetMsFromClobTimePayload(raw) {
  if (raw == null) return null;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    const serverMs = raw < 1e12 ? raw * 1000 : raw;
    return Math.round(serverMs - Date.now());
  }
  if (typeof raw === "string" && /^\s*[+-]?\d+(\.\d+)?\s*$/.test(raw)) {
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    const serverMs = n < 1e12 ? n * 1000 : n;
    return Math.round(serverMs - Date.now());
  }
  if (typeof raw !== "object") return null;
  let n = Number(raw.server_time ?? raw.time);
  if (!Number.isFinite(n)) return null;
  const serverMs = n < 1e12 ? n * 1000 : n;
  return Math.round(serverMs - Date.now());
}

async function syncServerTime() {
  try {
    const res = await fetch(CLOB_TIME_URL);
    if (!res.ok) return;
    const raw = await res.json();
    const off = offsetMsFromClobTimePayload(raw);
    if (off != null) timeOffsetMs = off;
  } catch {
    timeOffsetMs = 0;
  }
}

const book = createBookState();

function parseMetadataMaybe(raw) {
  if (raw == null) return null;
  if (typeof raw === "object" && !Array.isArray(raw)) return raw;
  if (typeof raw === "string") {
    const t = raw.trim();
    if (!t) return null;
    try {
      const p = JSON.parse(t);
      if (typeof p === "object" && p != null && !Array.isArray(p)) return p;
    } catch {
      return null;
    }
  }
  return null;
}

function pickNumericFromObjects(keys, ...objs) {
  for (const obj of objs) {
    if (!obj || typeof obj !== "object") continue;
    for (const k of keys) {
      if (!(k in obj)) continue;
      const v = obj[k];
      if (v == null || v === "") continue;
      const n = typeof v === "number" ? v : Number(String(v).replace(/,/g, "").trim());
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}

/** Gamma / market 上的「开盘参考价 / Price to Beat」（与 BTC5Mins snapshot 一致） */
function pickPriceToBeatFromMarket(m) {
  if (!m || typeof m !== "object") return null;
  const em = parseMetadataMaybe(m.eventMetadata);
  return pickNumericFromObjects(
    [
      "priceToBeat",
      "price_to_beat",
      "openPrice",
      "open_price",
      "referencePrice",
      "reference_price",
      "strikePrice",
      "strike_price",
      "startPriceUsd",
      "underlyingPrice",
      "underlying_price",
    ],
    em ?? {},
    m,
  );
}

/** @type {{ slug: string; market: object; upId: string; downId: string; endMs: number | null; btcOpenUsd: number | null } | null} */
let active = null;

/** @type {number | null} */
let lastBtcUsd = null;
/** @type {number | null} */
let lastBtcTs = null;

/** @type {WebSocket | null} */
let pmWs = null;
let pmPingTimer = null;
/** @type {WebSocket | null} */
let rtdsWs = null;
let rtdsPingTimer = null;

function connectPolymarketWs(assetIds) {
  if (pmPingTimer) {
    clearInterval(pmPingTimer);
    pmPingTimer = null;
  }
  if (pmWs) {
    try {
      pmWs.removeAllListeners();
      pmWs.close();
    } catch {
      /* noop */
    }
    pmWs = null;
  }
  if (!assetIds.length) return;

  book.reset();
  const ws = new WebSocket(PM_MARKET_WSS);
  pmWs = ws;

  ws.on("open", () => {
    const sub = {
      assets_ids: assetIds,
      type: "market",
      custom_feature_enabled: true,
      initial_dump: true,
      level: 2,
    };
    ws.send(JSON.stringify(sub));
    pmPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send("PING");
        } catch {
          /* noop */
        }
      }
    }, 10_000);
  });

  ws.on("message", (buf) => {
    const text = Buffer.isBuffer(buf) ? buf.toString("utf8") : String(buf);
    const t = text.trim();
    if (t === "PONG" || t === "pong" || t === "") return;
    let parsed;
    try {
      parsed = JSON.parse(t);
    } catch {
      return;
    }
    book.ingestParsed(parsed);
  });

  ws.on("close", () => {
    if (pmPingTimer) {
      clearInterval(pmPingTimer);
      pmPingTimer = null;
    }
    pmWs = null;
    setTimeout(() => {
      if (active) connectPolymarketWs([active.upId, active.downId].filter(Boolean));
    }, 2500);
  });

  ws.on("error", () => {
    try {
      ws.close();
    } catch {
      /* noop */
    }
  });
}

function connectRtds() {
  if (rtdsPingTimer) {
    clearInterval(rtdsPingTimer);
    rtdsPingTimer = null;
  }
  if (rtdsWs) {
    try {
      rtdsWs.removeAllListeners();
      rtdsWs.close();
    } catch {
      /* noop */
    }
    rtdsWs = null;
  }

  const ws = new WebSocket(RTDS_WS_URL);
  rtdsWs = ws;

  ws.on("open", () => {
    ws.send(
      JSON.stringify({
        action: "subscribe",
        subscriptions: [
          {
            topic: "crypto_prices_chainlink",
            type: "*",
            filters: JSON.stringify({ symbol: RTDS_CHAINLINK_SYMBOL }),
          },
        ],
      }),
    );
    rtdsPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send("PING");
        } catch {
          /* noop */
        }
      }
    }, 5000);
  });

  ws.on("message", (buf) => {
    const text = Buffer.isBuffer(buf) ? buf.toString("utf8") : String(buf);
    const t = text.trim();
    if (t === "PONG" || t === "pong") return;
    let msg;
    try {
      msg = JSON.parse(t);
    } catch {
      return;
    }
    if (
      msg.topic !== "crypto_prices_chainlink" ||
      msg.type !== "update" ||
      !msg.payload ||
      typeof msg.payload !== "object"
    ) {
      return;
    }
    const pl = msg.payload;
    if (String(pl.symbol || "").toLowerCase() !== RTDS_CHAINLINK_SYMBOL) return;
    const p = Number(pl.value);
    if (!Number.isFinite(p)) return;
    const tsRaw = pl.timestamp != null ? Number(pl.timestamp) : NaN;
    lastBtcTs = Number.isFinite(tsRaw) ? tsRaw : Date.now();
    lastBtcUsd = p;
    if (active && active.btcOpenUsd == null && Number.isFinite(p)) {
      active.btcOpenUsd = p;
    }
  });

  ws.on("close", () => {
    if (rtdsPingTimer) {
      clearInterval(rtdsPingTimer);
      rtdsPingTimer = null;
    }
    rtdsWs = null;
    setTimeout(connectRtds, 2000);
  });

  ws.on("error", () => {
    try {
      ws.close();
    } catch {
      /* noop */
    }
  });
}

async function activateMarketFromGamma() {
  const rolled = await resolveRollingMarketFromGamma(timeOffsetMs);
  if (!rolled) {
    console.error("[charts-btc] Gamma: no rolling market found");
    return false;
  }
  const { market, slug } = rolled;
  const { upId, downId } = tokenIdsForUpDown(market);
  if (!upId || !downId) {
    console.error("[charts-btc] Missing clob token ids for", slug);
    return false;
  }
  const endIso = marketEndIso(market);
  const endMs = endIso && Number.isFinite(Date.parse(endIso)) ? Date.parse(endIso) : null;

  const same = active && active.slug === slug && active.upId === upId && active.downId === downId;
  let btcOpenUsd = pickPriceToBeatFromMarket(market);
  if (btcOpenUsd == null && same && active?.btcOpenUsd != null) btcOpenUsd = active.btcOpenUsd;
  active = { slug, market, upId, downId, endMs, btcOpenUsd };
  if (!same) {
    console.log(`[charts-btc] Active market: ${slug} up=${upId.slice(0, 12)}… down=${downId.slice(0, 12)}…`);
    connectPolymarketWs([upId, downId]);
  }
  return true;
}

function marketExpired() {
  if (!active?.endMs) return false;
  return Date.now() + timeOffsetMs >= active.endMs;
}

// —— MySQL —— //

let pool = null;

async function ensureDb() {
  pool = mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASSWORD,
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
  });
  await pool.execute(`
    CREATE TABLE IF NOT EXISTS pm_book_ticks (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      ts_ms BIGINT NOT NULL,
      market_slug VARCHAR(512) NOT NULL,
      up_bid DECIMAL(14,8) NULL,
      up_ask DECIMAL(14,8) NULL,
      up_mid DECIMAL(14,8) NULL,
      down_bid DECIMAL(14,8) NULL,
      down_ask DECIMAL(14,8) NULL,
      down_mid DECIMAL(14,8) NULL,
      btc_usd DECIMAL(24,8) NULL,
      PRIMARY KEY (id),
      KEY idx_ts (ts_ms),
      KEY idx_slug_ts (market_slug(191), ts_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
}

async function insertTick(row) {
  if (!pool) return;
  const sql = `INSERT INTO pm_book_ticks
    (ts_ms, market_slug, up_bid, up_ask, up_mid, down_bid, down_ask, down_mid, btc_usd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
  await pool.execute(sql, [
    row.ts_ms,
    row.market_slug,
    row.up_bid,
    row.up_ask,
    row.up_mid,
    row.down_bid,
    row.down_ask,
    row.down_mid,
    row.btc_usd,
  ]);
}

// —— Express —— //

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function buildHealthPayload() {
  const btcUsd = lastBtcUsd;
  const btcOpenUsd = active?.btcOpenUsd ?? null;
  const btcDeltaUsd =
    btcUsd != null && btcOpenUsd != null ? btcUsd - btcOpenUsd : null;
  return {
    ok: true,
    btcUsd,
    btcOpenUsd,
    btcDeltaUsd,
    active: active
      ? {
          slug: active.slug,
          endMs: active.endMs,
          btcOpenUsd,
          pmWs: pmWs?.readyState === WebSocket.OPEN,
          rtds: rtdsWs?.readyState === WebSocket.OPEN,
        }
      : null,
    series: EVENT_SERIES_PREFIX,
    gamma: GAMMA_BASE,
  };
}

app.get("/api/health", (_req, res) => {
  res.json(buildHealthPayload());
});

function clampTickLimit(raw) {
  const x = Math.floor(Number(raw));
  if (!Number.isFinite(x)) return 3600;
  return Math.min(50_000, Math.max(1, x));
}

function clampWindowListLimit(raw) {
  const x = Math.floor(Number(raw));
  if (!Number.isFinite(x)) return 96;
  return Math.min(500, Math.max(1, x));
}

/**
 * @param {{ safeLimit: number; sinceMs: number | null; slug: string | null }} q
 * @returns {Promise<unknown[]>}
 */
async function fetchTicksRows(q) {
  if (!pool) return [];
  const { safeLimit, sinceMs, slug } = q;
  if (sinceMs != null && Number.isFinite(sinceMs)) {
    const [rows] = await pool.execute(
      `SELECT ts_ms, market_slug, up_bid, up_ask, up_mid, down_bid, down_ask, down_mid, btc_usd
       FROM pm_book_ticks WHERE ts_ms >= ? ORDER BY ts_ms ASC LIMIT ${safeLimit}`,
      [Math.floor(sinceMs)],
    );
    return rows;
  }
  if (slug) {
    const [rows] = await pool.execute(
      `SELECT ts_ms, market_slug, up_bid, up_ask, up_mid, down_bid, down_ask, down_mid, btc_usd
       FROM pm_book_ticks WHERE market_slug = ? ORDER BY ts_ms ASC LIMIT ${safeLimit}`,
      [slug],
    );
    return rows;
  }
  const [rows] = await pool.query(
    `SELECT ts_ms, market_slug, up_bid, up_ask, up_mid, down_bid, down_ask, down_mid, btc_usd
     FROM pm_book_ticks ORDER BY ts_ms DESC LIMIT ${safeLimit}`,
  );
  return rows.reverse();
}

/**
 * 已入库的各 5 分钟市场（按 slug 聚合），用于前端切换归档图表。
 */
app.get("/api/market-windows", async (req, res) => {
  if (!pool) {
    res.status(503).json({ error: "database_unavailable" });
    return;
  }
  const safeLimit = clampWindowListLimit(req.query.limit ?? 96);
  try {
    const [rows] = await pool.query(
      `SELECT market_slug AS slug,
              MIN(ts_ms) AS min_ts_ms,
              MAX(ts_ms) AS max_ts_ms,
              COUNT(*) AS tick_count
       FROM pm_book_ticks
       GROUP BY market_slug
       ORDER BY max_ts_ms DESC
       LIMIT ${safeLimit}`,
    );
    res.json({ windows: rows });
  } catch (e) {
    console.error("[charts-btc] /api/market-windows", e);
    res.status(500).json({ error: String(e instanceof Error ? e.message : e) });
  }
});

app.get("/api/ticks", async (req, res) => {
  if (!pool) {
    res.status(503).json({ error: "database_unavailable" });
    return;
  }
  /** `LIMIT ?` 在部分 MySQL/MariaDB 上会报 ER_WRONG_ARGUMENTS；用校验后的字面量。 */
  const safeLimit = clampTickLimit(req.query.limit ?? 3600);
  const sinceMs = req.query.since != null ? Number(req.query.since) : null;
  const slugRaw = req.query.slug != null ? String(req.query.slug).trim() : "";
  const slug = slugRaw !== "" ? slugRaw : null;

  try {
    const ticks = await fetchTicksRows({
      safeLimit,
      sinceMs: sinceMs != null && Number.isFinite(sinceMs) ? sinceMs : null,
      slug,
    });
    res.json({ ticks });
  } catch (e) {
    console.error("[charts-btc] /api/ticks", e);
    res.status(500).json({ error: String(e instanceof Error ? e.message : e) });
  }
});

const server = http.createServer(app);

/** 浏览器图表页 /ws/chart：订阅后推送快照，实时盘在每次入库后再推送一条。 */
const wssChart = new WebSocketServer({ server, path: "/ws/chart" });

/**
 * @param {import("ws").WebSocket} ws
 * @param {unknown} msg
 */
async function handleChartSubscribe(ws, msg) {
  if (!pool) {
    ws.send(JSON.stringify({ type: "error", message: "database_unavailable" }));
    return;
  }
  if (!msg || typeof msg !== "object") return;
  const m = /** @type {{ limit?: unknown; slug?: unknown }} */ (msg);
  const safeLimit = clampTickLimit(m.limit ?? 3600);
  const slugRaw = typeof m.slug === "string" ? m.slug.trim() : "";
  const explicitSlug = slugRaw !== "" ? slugRaw : null;
  let slug = explicitSlug;
  /** 未指定 slug → 跟随当前服务盘并接收增量 */
  let live = false;
  if (!slug) {
    slug = active?.slug ?? null;
    live = true;
  }
  if (!slug) {
    ws.send(JSON.stringify({ type: "error", message: "no_active_slug" }));
    ws._chartSub = undefined;
    return;
  }
  ws._chartSub = live ? { live: true, slug } : { live: false, slug };

  try {
    ws.send(JSON.stringify({ type: "health", ...buildHealthPayload() }));
  } catch (e) {
    console.error("[charts-btc] ws health before snapshot", e);
  }

  const ticks = await fetchTicksRows({ safeLimit, sinceMs: null, slug });
  ws.send(
    JSON.stringify({
      type: "snapshot",
      ticks,
      slug,
      mode: live ? "live" : "archive",
    }),
  );
}

function broadcastHealthToAllChartClients() {
  const payload = JSON.stringify({ type: "health", ...buildHealthPayload() });
  for (const client of wssChart.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    try {
      client.send(payload);
    } catch (e) {
      console.error("[charts-btc] ws health broadcast", e);
    }
  }
}

/**
 * @param {Record<string, unknown>} row
 */
function broadcastChartTick(row) {
  const slug = row.market_slug;
  if (typeof slug !== "string" || !slug) return;
  for (const client of wssChart.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    const sub = client._chartSub;
    if (!sub?.live || sub.slug !== slug) continue;
    try {
      client.send(JSON.stringify({ type: "tick", tick: row }));
    } catch (e) {
      console.error("[charts-btc] ws chart broadcast", e);
    }
  }
}

wssChart.on("connection", (ws) => {
  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }
    if (!msg || typeof msg !== "object") return;
    const op = /** @type {{ op?: string }} */ (msg).op;
    if (op !== "subscribe") return;
    try {
      await handleChartSubscribe(ws, msg);
    } catch (e) {
      console.error("[charts-btc] ws chart subscribe", e);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(
          JSON.stringify({
            type: "error",
            message: String(e instanceof Error ? e.message : e),
          }),
        );
      }
    }
  });
  ws.on("close", () => {
    ws._chartSub = undefined;
  });
});

async function tickLoop() {
  if (marketExpired()) {
    await activateMarketFromGamma().catch((e) => console.error("[charts-btc] rollover", e));
  }

  if (!active) {
    await activateMarketFromGamma().catch((e) => console.error("[charts-btc] activate", e));
  }

  if (!active) return;

  const odds = book.oddsFromIds(active.upId, active.downId);
  const ts = Date.now();
  const row = {
    ts_ms: ts,
    market_slug: active.slug,
    up_bid: odds.up.bestBid,
    up_ask: odds.up.bestAsk,
    up_mid: odds.up.mid,
    down_bid: odds.down.bestBid,
    down_ask: odds.down.bestAsk,
    down_mid: odds.down.mid,
    btc_usd: lastBtcUsd,
  };

  try {
    await insertTick(row);
    broadcastHealthToAllChartClients();
    broadcastChartTick(row);
  } catch (e) {
    console.error("[charts-btc] insert", e);
  }
}

async function main() {
  await ensureDb().catch((e) => {
    console.error("[charts-btc] MySQL init failed — is MySQL running with DB", DB_NAME, "?", e);
    process.exit(1);
  });

  await syncServerTime();
  setInterval(syncServerTime, 5 * 60_000);

  await activateMarketFromGamma();
  connectRtds();

  setInterval(tickLoop, TICK_MS);
  setInterval(() => {
    activateMarketFromGamma().catch((e) => console.error("[charts-btc] periodic gamma", e));
  }, GAMMA_REFRESH_MS);

  server.listen(PORT, () => {
    console.log(
      `[charts-btc] http://127.0.0.1:${PORT}/  ·  WSS /ws/chart  ·  MySQL ${DB_HOST}/${DB_NAME}  ·  tick ${TICK_MS}ms`,
    );
    tickLoop().catch((e) => console.error("[charts-btc] first tick", e));
  });
}

main();
