/**
 * Polymarket BTC 5m Up/Down：服务端 CLOB Market WSS + RTDS Chainlink，
 * 每秒写入 MySQL；Express 提供静态页与 JSON API。
 */

import express from "express";
import http from "http";
import path from "path";
import crypto from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
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
import {
  computeLegPnlFromRows,
  formatBatchMarketTimeRange,
  num,
  pnlDetailTag,
  WINDOW_SEC,
} from "./public/legPairPnl.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** 历史：单侧测算预设曾存于此；启动时若库表为空则一次性导入后仍可手动保留备份 */
const CALC_PRESETS_PATH = path.join(__dirname, "data", "calc-presets.json");
const UI_SETTINGS_PATH = path.join(__dirname, "data", "ui-settings.json");
const CALC_PRESETS_MAX = 200;
const CALC_PRESET_NAME_MAX = 80;

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

/** 均非空时启用整站登录（与 Wether 相同环境变量名） */
const LOGIN_USERNAME = process.env.LOGIN_USERNAME?.trim() || "";
const LOGIN_SECRET = process.env.LOGIN_SECRET?.trim() || "";
const AUTH_ENABLED = Boolean(LOGIN_USERNAME && LOGIN_SECRET);
const SESSION_TTL_MS = Math.max(
  10 * 60 * 1000,
  Number(process.env.CHARTS_SESSION_TTL_MS) ||
    Number(process.env.WETHER_SESSION_TTL_MS) ||
    7 * 24 * 60 * 60 * 1000,
);
const COOKIE_NAME = "chartsbtc_session";
const COOKIE_SECURE = process.env.COOKIE_SECURE === "1";

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

/**
 * 入库前 1 秒窗口内聚合 Polymarket 盘口采样（与 tickLoop 周期一致，默认 TICK_MS=1000）。
 * 规则（对 Up / Down 各自独立，用该秒内**最后一次**有效 mid 判定）：
 * - mid < 0.5 → 记入值 = 该秒内**卖一（best ask）**的**最小值**
 * - mid > 0.5 → 记入值 = 该秒内**买一（best bid）**的**最大值**
 * - mid === 0.5 → 记入值 = 当前最后一档 bid/ask 的算术均价（兜底）
 * `up_bid`/`up_ask`/`down_*` 取该秒内**最后一次**快照；`up_mid`/`down_mid` 为上述记入值。
 */
function createEmptySecondBuffer() {
  return {
    /** @type {{ bids: number[]; asks: number[]; lastBid: number | null; lastAsk: number | null; lastMid: number | null }} */
    up: { bids: [], asks: [], lastBid: null, lastAsk: null, lastMid: null },
    /** @type {{ bids: number[]; asks: number[]; lastBid: number | null; lastAsk: number | null; lastMid: number | null }} */
    down: { bids: [], asks: [], lastBid: null, lastAsk: null, lastMid: null },
    sampleCount: 0,
  };
}

let secondBuffer = createEmptySecondBuffer();

function recordBookSampleFromActive() {
  if (!active) return;
  const odds = book.oddsFromIds(active.upId, active.downId);
  const push = (
    /** @type {{ bids: number[]; asks: number[]; lastBid: number | null; lastAsk: number | null; lastMid: number | null }} */ side,
    /** @type {{ bestBid: number | null; bestAsk: number | null; mid: number | null }} */ o,
  ) => {
    if (o.bestBid != null && Number.isFinite(Number(o.bestBid))) {
      const b = Number(o.bestBid);
      side.bids.push(b);
      side.lastBid = b;
    }
    if (o.bestAsk != null && Number.isFinite(Number(o.bestAsk))) {
      const a = Number(o.bestAsk);
      side.asks.push(a);
      side.lastAsk = a;
    }
    if (o.mid != null && Number.isFinite(Number(o.mid))) {
      side.lastMid = Number(o.mid);
    }
  };
  push(secondBuffer.up, odds.up);
  push(secondBuffer.down, odds.down);
  secondBuffer.sampleCount += 1;
}

/**
 * @param {{ bids: number[]; asks: number[]; lastBid: number | null; lastAsk: number | null; lastMid: number | null }} side
 * @param {{ bestBid: number | null; bestAsk: number | null; mid: number | null }} fallback
 */
function finalizeSideForDb(side, fallback) {
  const lb = side.lastBid ?? (fallback.bestBid != null ? Number(fallback.bestBid) : null);
  const la = side.lastAsk ?? (fallback.bestAsk != null ? Number(fallback.bestAsk) : null);
  const lm =
    side.lastMid ?? (fallback.mid != null && Number.isFinite(Number(fallback.mid)) ? Number(fallback.mid) : null);

  let midOut = null;
  if (lm != null && Number.isFinite(lm)) {
    if (lm < 0.5) {
      if (side.asks.length) midOut = Math.min(...side.asks);
      else if (fallback.bestAsk != null && Number.isFinite(Number(fallback.bestAsk))) {
        midOut = Number(fallback.bestAsk);
      } else midOut = lm;
    } else if (lm > 0.5) {
      if (side.bids.length) midOut = Math.max(...side.bids);
      else if (fallback.bestBid != null && Number.isFinite(Number(fallback.bestBid))) {
        midOut = Number(fallback.bestBid);
      } else midOut = lm;
    } else {
      midOut =
        lb != null && la != null && Number.isFinite(lb) && Number.isFinite(la)
          ? (lb + la) / 2
          : fallback.mid != null && Number.isFinite(Number(fallback.mid))
            ? Number(fallback.mid)
            : null;
    }
  } else {
    midOut =
      lb != null && la != null && Number.isFinite(lb) && Number.isFinite(la)
        ? (lb + la) / 2
        : fallback.mid != null && Number.isFinite(Number(fallback.mid))
          ? Number(fallback.mid)
          : null;
  }

  return { bid: lb, ask: la, mid: midOut };
}

/**
 * @param {number} ts_ms
 * @param {string} market_slug
 * @param {number | null} btc_usd
 */
function buildAggregatedTickRow(ts_ms, market_slug, btc_usd) {
  const odds = active ? book.oddsFromIds(active.upId, active.downId) : null;
  const upF = odds?.up ?? { bestBid: null, bestAsk: null, mid: null };
  const downF = odds?.down ?? { bestBid: null, bestAsk: null, mid: null };
  const up = finalizeSideForDb(secondBuffer.up, upF);
  const down = finalizeSideForDb(secondBuffer.down, downF);
  secondBuffer = createEmptySecondBuffer();
  return {
    ts_ms,
    market_slug,
    up_bid: up.bid,
    up_ask: up.ask,
    up_mid: up.mid,
    down_bid: down.bid,
    down_ask: down.ask,
    down_mid: down.mid,
    btc_usd,
  };
}

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
  secondBuffer = createEmptySecondBuffer();
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
    recordBookSampleFromActive();
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
    secondBuffer = createEmptySecondBuffer();
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
  await pool.execute(`
    CREATE TABLE IF NOT EXISTS charts_calc_presets (
      id CHAR(36) NOT NULL,
      name VARCHAR(80) NOT NULL,
      params_json JSON NOT NULL,
      updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (id),
      UNIQUE KEY uk_charts_calc_presets_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  await migrateCalcPresetsFromJsonIfNeeded();
}

/**
 * 首次部署：表为空时从 `data/calc-presets.json` 导入（与盘口数据同属 MySQL）。
 */
async function migrateCalcPresetsFromJsonIfNeeded() {
  if (!pool) return;
  const [cntRows] = await pool.query(`SELECT COUNT(*) AS c FROM charts_calc_presets`);
  const n = Number(/** @type {{ c?: unknown }[]} */ (cntRows)[0]?.c);
  if (!Number.isFinite(n) || n > 0) return;
  let doc;
  try {
    doc = await readCalcPresetsDoc();
  } catch {
    return;
  }
  const presets = Array.isArray(doc.presets) ? doc.presets : [];
  let imported = 0;
  for (const p of presets) {
    if (!p || typeof p !== "object") continue;
    const name = typeof /** @type {{ name?: unknown }} */ (p).name === "string" ? String(p.name).trim() : "";
    if (!name || name.length > CALC_PRESET_NAME_MAX) continue;
    const id =
      typeof /** @type {{ id?: unknown }} */ (p).id === "string" && /** @type {{ id: string }} */ (p).id.length >= 32
        ? /** @type {{ id: string }} */ (p).id
        : crypto.randomUUID();
    const rawParams = /** @type {{ params?: unknown }} */ (p).params;
    if (rawParams == null || typeof rawParams !== "object") continue;
    const params = normalizeCalcPresetParams(rawParams);
    if (!params) continue;
    let updatedAt = new Date();
    const u = /** @type {{ updatedAt?: unknown }} */ (p).updatedAt;
    if (typeof u === "string" && Number.isFinite(Date.parse(u))) updatedAt = new Date(u);
    try {
      await pool.execute(
        `INSERT INTO charts_calc_presets (id, name, params_json, updated_at) VALUES (?, ?, ?, ?)`,
        [id, name, JSON.stringify(params), updatedAt],
      );
      imported += 1;
    } catch (e) {
      const err = /** @type {{ code?: string }} */ (e);
      if (err.code === "ER_DUP_ENTRY") continue;
      throw e;
    }
  }
  if (imported > 0) {
    console.log(`[charts-btc] 已从 ${CALC_PRESETS_PATH} 导入 ${imported} 条测算预设至表 charts_calc_presets`);
  }
}

/**
 * @returns {Promise<Array<{ id: string; name: string; params: object; updatedAt: string }>>}
 */
async function fetchCalcPresetsFromDb() {
  if (!pool) return [];
  const [rows] = await pool.execute(
    `SELECT id, name, params_json, updated_at FROM charts_calc_presets ORDER BY updated_at DESC`,
  );
  return (Array.isArray(rows) ? rows : []).map((r) => {
    const row = /** @type {{ id: string; name: string; params_json: unknown; updated_at: unknown }} */ (r);
    let params = row.params_json;
    if (typeof params === "string") {
      try {
        params = JSON.parse(params);
      } catch {
        params = {};
      }
    }
    const ua = row.updated_at;
    const updatedAt =
      ua instanceof Date
        ? ua.toISOString()
        : typeof ua === "string"
          ? ua
          : new Date().toISOString();
    return { id: row.id, name: row.name, params, updatedAt };
  });
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

function timingSafeStringEq(a, b) {
  const x = Buffer.from(String(a), "utf8");
  const y = Buffer.from(String(b), "utf8");
  if (x.length !== y.length) return false;
  return crypto.timingSafeEqual(x, y);
}

function sessionSigningKeyBuf() {
  return crypto.createHash("sha256").update(`${LOGIN_SECRET}|chartsbtc.sid.v1`, "utf8").digest();
}

function signSessionToken() {
  const exp = Date.now() + SESSION_TTL_MS;
  const payloadB64 = Buffer.from(JSON.stringify({ exp }), "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", sessionSigningKeyBuf()).update(payloadB64).digest("base64url");
  return `${payloadB64}.${sig}`;
}

function readSessionTokenFromCookieHeader(cookieHeader) {
  if (!cookieHeader || typeof cookieHeader !== "string") return "";
  for (const part of cookieHeader.split(";")) {
    const s = part.trim();
    if (s.startsWith(`${COOKIE_NAME}=`)) {
      return decodeURIComponent(s.slice(COOKIE_NAME.length + 1).trim());
    }
  }
  return "";
}

function verifySessionToken(raw) {
  if (!raw || typeof raw !== "string") return false;
  const dot = raw.lastIndexOf(".");
  if (dot <= 0) return false;
  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expected = crypto.createHmac("sha256", sessionSigningKeyBuf()).update(payloadB64).digest("base64url");
  const sb = Buffer.from(sig, "utf8");
  const eb = Buffer.from(expected, "utf8");
  if (sb.length !== eb.length) return false;
  if (!crypto.timingSafeEqual(sb, eb)) return false;
  let data;
  try {
    data = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8"));
  } catch {
    return false;
  }
  if (typeof data.exp !== "number" || Number.isNaN(data.exp) || Date.now() > data.exp) return false;
  return true;
}

function isAuthenticated(req) {
  if (!AUTH_ENABLED) return true;
  return verifySessionToken(readSessionTokenFromCookieHeader(req.headers.cookie));
}

function wantsHtml(req) {
  const a = (req.headers.accept || "").toLowerCase();
  if (a.includes("application/json")) return false;
  return !a || a.includes("text/html") || a.includes("*/*");
}

function setSessionCookie(res, token) {
  const attrs = [
    `${COOKIE_NAME}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    `Max-Age=${Math.floor(SESSION_TTL_MS / 1000)}`,
    "SameSite=Lax",
  ];
  if (COOKIE_SECURE) attrs.push("Secure");
  res.setHeader("Set-Cookie", attrs.join("; "));
}

function clearSessionCookie(res) {
  const attrs = [`${COOKIE_NAME}=`, "Path=/", "HttpOnly", "Max-Age=0", "SameSite=Lax"];
  if (COOKIE_SECURE) attrs.push("Secure");
  res.setHeader("Set-Cookie", attrs.join("; "));
}

// —— Express —— //

const app = express();
app.use(express.json());

function buildHealthPayload() {
  const btcUsd = lastBtcUsd;
  const btcOpenUsd = active?.btcOpenUsd ?? null;
  const btcDeltaUsd =
    btcUsd != null && btcOpenUsd != null ? btcUsd - btcOpenUsd : null;
  return {
    ok: true,
    authEnabled: AUTH_ENABLED,
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

const api = express.Router();

api.post("/auth/login", (req, res) => {
  if (!AUTH_ENABLED) {
    res.status(400).json({
      error: "login_disabled",
      hint: "未同时设置 LOGIN_USERNAME 与 LOGIN_SECRET",
    });
    return;
  }
  const body = req.body && typeof req.body === "object" ? req.body : {};
  const username = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  if (!timingSafeStringEq(username, LOGIN_USERNAME) || !timingSafeStringEq(password, LOGIN_SECRET)) {
    res.status(401).json({ error: "invalid_credentials" });
    return;
  }
  setSessionCookie(res, signSessionToken());
  res.json({ ok: true });
});

api.get("/auth/status", (_req, res) => {
  res.json({
    authEnabled: AUTH_ENABLED,
    authenticated: !AUTH_ENABLED || isAuthenticated(_req),
  });
});

api.post("/auth/logout", (_req, res) => {
  clearSessionCookie(res);
  res.json({ ok: true });
});

api.use((req, res, next) => {
  if (!AUTH_ENABLED) return next();
  if (!isAuthenticated(req)) {
    res.status(401).json({ error: "unauthorized", needLogin: true });
    return;
  }
  next();
});

api.get("/health", (_req, res) => {
  res.json(buildHealthPayload());
});

/** 与页头「时间跨度（s）」一致：60～20000 */
function clampUiTimeSpanSeconds(raw) {
  const x = Math.floor(Number(raw));
  if (!Number.isFinite(x)) return 400;
  return Math.min(20_000, Math.max(60, x));
}

async function readUiSettingsDoc() {
  try {
    const raw = await readFile(UI_SETTINGS_PATH, "utf8");
    const j = JSON.parse(raw);
    return j && typeof j === "object" ? j : {};
  } catch (e) {
    const err = /** @type {NodeJS.ErrnoException} */ (e);
    if (err.code === "ENOENT") return {};
    throw e;
  }
}

/**
 * @param {Record<string, unknown>} doc
 */
async function atomicWriteUiSettings(doc) {
  const dir = path.dirname(UI_SETTINGS_PATH);
  await mkdir(dir, { recursive: true });
  const tmp = `${UI_SETTINGS_PATH}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(tmp, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  await rename(tmp, UI_SETTINGS_PATH);
}

/** 页头「时间跨度」读写在 `data/ui-settings.json` */
api.get("/ui-settings", async (_req, res) => {
  try {
    const doc = await readUiSettingsDoc();
    const timeSpanSeconds = clampUiTimeSpanSeconds(doc.timeSpanSeconds);
    res.json({ ok: true, timeSpanSeconds });
  } catch (e) {
    console.error("[charts-btc] /api/ui-settings GET", e);
    res.status(500).json({ ok: false, error: String(e instanceof Error ? e.message : e) });
  }
});

api.post("/ui-settings", async (req, res) => {
  try {
    const b = req.body && typeof req.body === "object" ? req.body : {};
    const timeSpanSeconds = clampUiTimeSpanSeconds(b.timeSpanSeconds);
    const doc = { ...(await readUiSettingsDoc()), timeSpanSeconds };
    await atomicWriteUiSettings(doc);
    res.json({ ok: true, timeSpanSeconds });
  } catch (e) {
    console.error("[charts-btc] /api/ui-settings POST", e);
    res.status(500).json({ ok: false, error: String(e instanceof Error ? e.message : e) });
  }
});

function clampTickLimit(raw) {
  const x = Math.floor(Number(raw));
  if (!Number.isFinite(x)) return 3600;
  return Math.min(50_000, Math.max(1, x));
}

function clampWindowListLimit(raw) {
  const x = Math.floor(Number(raw));
  if (!Number.isFinite(x)) return 96;
  return Math.min(20_000, Math.max(1, x));
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
 * 一次取多盘 ticks，每盘最多 `perSlugLimit` 条（按 ts_ms 升序）。需 MySQL 8+ / MariaDB 10.2+ 窗口函数。
 * @param {string[]} slugs
 * @param {number} perSlugLimit
 */
async function fetchTicksRowsForSlugs(slugs, perSlugLimit) {
  if (!pool || !slugs.length) return [];
  const lim = Math.min(50_000, Math.max(1, Math.floor(perSlugLimit)));
  const placeholders = slugs.map(() => "?").join(",");
  const [rows] = await pool.query(
    `SELECT z.ts_ms, z.market_slug, z.up_bid, z.up_ask, z.up_mid, z.down_bid, z.down_ask, z.down_mid, z.btc_usd
     FROM (
       SELECT t.ts_ms, t.market_slug, t.up_bid, t.up_ask, t.up_mid, t.down_bid, t.down_ask, t.down_mid, t.btc_usd,
         ROW_NUMBER() OVER (PARTITION BY t.market_slug ORDER BY t.ts_ms ASC) AS rn
       FROM pm_book_ticks t
       WHERE t.market_slug IN (${placeholders})
     ) z
     WHERE z.rn <= ?
     ORDER BY z.market_slug ASC, z.ts_ms ASC`,
    [...slugs, lim],
  );
  return rows;
}

/**
 * @param {unknown[]} rows
 * @returns {Map<string, unknown[]>}
 */
function groupTicksBySlug(rows) {
  /** @type {Map<string, unknown[]>} */
  const m = new Map();
  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const slug = /** @type {{ market_slug?: string }} */ (row).market_slug;
    if (typeof slug !== "string" || !slug) continue;
    let arr = m.get(slug);
    if (!arr) {
      arr = [];
      m.set(slug, arr);
    }
    arr.push(row);
  }
  return m;
}

/** JSON 安全数值（含 mysql2 BigInt） */
function jsonSafeFiniteNum(v) {
  if (v == null || v === "") return null;
  if (typeof v === "bigint") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * 全量测算导出用：单行 tick 纯 JSON 对象（无 BigInt）。
 * @param {unknown} row
 */
function serializeTickRowForCalcBatchExport(row) {
  if (!row || typeof row !== "object") return null;
  const r = /** @type {Record<string, unknown>} */ (row);
  const ts_ms = jsonSafeFiniteNum(r.ts_ms);
  if (ts_ms == null) return null;
  return {
    ts_ms,
    up_bid: jsonSafeFiniteNum(r.up_bid),
    up_ask: jsonSafeFiniteNum(r.up_ask),
    up_mid: jsonSafeFiniteNum(r.up_mid),
    down_bid: jsonSafeFiniteNum(r.down_bid),
    down_ask: jsonSafeFiniteNum(r.down_ask),
    down_mid: jsonSafeFiniteNum(r.down_mid),
    btc_usd: jsonSafeFiniteNum(r.btc_usd),
  };
}

/**
 * @param {unknown[]} rows
 * @returns {Record<string, number | null>[]}
 */
function serializeTicksForCalcBatchExport(rows) {
  const out = [];
  if (!Array.isArray(rows)) return out;
  for (const row of rows) {
    const o = serializeTickRowForCalcBatchExport(row);
    if (o) out.push(o);
  }
  return out;
}

/**
 * 单侧盈亏「全量」：一次拉齐 windows + 各盘 ticks，服务端套用与前端相同的 `computeLegPnlFromRows`。
 */
api.post("/calc-batch", async (req, res) => {
  if (!pool) {
    res.status(503).json({ ok: false, error: "database_unavailable" });
    return;
  }
  const b = req.body && typeof req.body === "object" ? req.body : {};
  const P_buyLimit = Number(b.P_buyLimit);
  let t0 = Number(b.t0);
  let t1 = Number(b.t1);
  const P_sellTarget = Number(b.P_sellTarget);
  const N = Number(b.N);
  const windowsLimit = clampWindowListLimit(b.windowsLimit ?? 500);
  const tickLimit = clampTickLimit(b.tickLimit ?? 50_000);

  if (
    !Number.isFinite(P_buyLimit) ||
    !Number.isFinite(t0) ||
    !Number.isFinite(t1) ||
    !Number.isFinite(P_sellTarget) ||
    !Number.isFinite(N) ||
    N <= 0
  ) {
    res.status(400).json({ ok: false, error: "invalid_params" });
    return;
  }
  if (t0 > t1) {
    const x = t0;
    t0 = t1;
    t1 = x;
  }
  t0 = Math.max(0, t0);
  t1 = Math.min(300, t1);

  try {
    const [winRows] = await pool.query(
      `SELECT market_slug AS slug,
              MIN(ts_ms) AS min_ts_ms,
              MAX(ts_ms) AS max_ts_ms,
              COUNT(*) AS tick_count
       FROM pm_book_ticks
       GROUP BY market_slug
       ORDER BY max_ts_ms DESC
       LIMIT ${windowsLimit}`,
    );
    const windows = Array.isArray(winRows) ? winRows : [];
    if (!windows.length) {
      res.json({
        ok: true,
        total: 0,
        nBuy: 0,
        nClosed: 0,
        nFloat: 0,
        nSkip: 0,
        marketCount: 0,
        details: [],
      });
      return;
    }

    const slugs = [];
    for (const w of windows) {
      const slug = w && typeof w === "object" && w.slug != null ? String(w.slug) : "";
      if (slug) slugs.push(slug);
    }

    const tickRows = await fetchTicksRowsForSlugs(slugs, tickLimit);
    const bySlug = groupTicksBySlug(tickRows);

    let total = 0;
    let nBuy = 0;
    let nClosed = 0;
    let nFloat = 0;
    let nSkip = 0;
    /** @type {unknown[]} */
    const details = [];

    for (const w of windows) {
      const slug = w && typeof w === "object" && w.slug != null ? String(w.slug) : "";
      if (!slug) continue;
      const timeRange = formatBatchMarketTimeRange(w, slug);
      const minMs = w.min_ts_ms != null ? Number(w.min_ts_ms) : NaN;
      const startMs = Number.isFinite(minMs) ? minMs : null;

      try {
        const ticks = bySlug.get(slug) ?? [];
        const calcOpts = normalizeLegPairOpts(b);
        const r = computeLegPnlFromRows(ticks, slug, P_buyLimit, t0, t1, P_sellTarget, N, calcOpts);
        total += r.netUsd;
        if (r.code === "no_buy" || r.code === "no_points" || r.code === "no_data") {
          nSkip += 1;
        } else if (r.code === "bad_entry" || r.code === "no_slug") {
          nSkip += 1;
        } else if (r.code === "closed") {
          nBuy += 1;
          nClosed += 1;
        } else if (r.code === "float") {
          nBuy += 1;
          nFloat += 1;
        }
        const tag = pnlDetailTag(r.code);
        /** @type {Record<string, unknown>} */
        const one = {
          slug,
          startMs,
          timeRange,
          tag,
          netUsd: r.netUsd,
          code: r.code,
        };
        if (r.code === "closed" || r.code === "float") {
          one.ticks = serializeTicksForCalcBatchExport(ticks);
        }
        details.push(one);
      } catch (e) {
        nSkip += 1;
        details.push({
          slug,
          startMs,
          timeRange,
          tag: "错误",
          netUsd: 0,
          code: "error",
          error: e instanceof Error ? e.message : String(e),
        });
      }
    }

    res.json({
      ok: true,
      total,
      nBuy,
      nClosed,
      nFloat,
      nSkip,
      marketCount: windows.length,
      details,
    });
  } catch (e) {
    console.error("[charts-btc] /api/calc-batch", e);
    res.status(500).json({
      ok: false,
      error: String(e instanceof Error ? e.message : e),
    });
  }
});

/**
 * 已入库的各 5 分钟市场（按 slug 聚合），用于前端切换归档图表。
 */
api.get("/market-windows", async (req, res) => {
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

/**
 * @param {unknown} raw
 * @returns {{ P_buyLimit: number, t0: number, t1: number, P_sellTarget: number, N: number, fullBatch: boolean, requireMinBidAboveLimit: boolean, pairBuyMinAbsChainlinkUsd: number, pairBuyMaxAbsChainlinkUsd: number, pairBuyMinPreEntryPeakAbsChainlinkUsd: number, pairBuyBtcRiseWindowSec: number, pairBuyBtcRiseMinUsd: number, advancedPairSell: boolean, pairChainlinkAbsAboveMarketSellUsd: number, pairStopPriceUsd: number } | null}
 */
function normalizeCalcPresetParams(raw) {
  const o = raw && typeof raw === "object" ? raw : {};
  const P_buyLimit = num(o.P_buyLimit);
  let t0 = num(o.t0);
  let t1 = num(o.t1);
  const P_sellTarget = num(o.P_sellTarget);
  const N = num(o.N);
  const fullBatch = Boolean(
    o.fullBatch === true || o.fullBatch === 1 || o.fullBatch === "1" || o.fullBatch === "true",
  );
  if (P_buyLimit == null || t0 == null || t1 == null || P_sellTarget == null || N == null) {
    return null;
  }
  if (N <= 0) return null;
  if (t0 > t1) {
    const x = t0;
    t0 = t1;
    t1 = x;
  }
  t0 = Math.max(0, t0);
  t1 = Math.min(WINDOW_SEC, t1);

  const legOpts = normalizeLegPairOpts(o);

  return {
    P_buyLimit,
    t0,
    t1,
    P_sellTarget,
    N,
    fullBatch,
    ...legOpts,
  };
}

/**
 * 单侧测算扩展项（与 `public/legPairPnl.mjs` 的 opts 一致）。
 * @param {unknown} raw
 */
function normalizeLegPairOpts(raw) {
  const o = raw && typeof raw === "object" ? raw : {};
  const requireMinBidAboveLimit = Boolean(o.requireMinBidAboveLimit);
  let minCl = num(o.pairBuyMinAbsChainlinkUsd);
  if (minCl == null || minCl <= 0) minCl = 0;
  else minCl = Math.min(9_999_999, Math.max(1, Math.floor(minCl)));
  let maxCl = num(o.pairBuyMaxAbsChainlinkUsd);
  if (maxCl == null || maxCl <= 0) maxCl = 0;
  else maxCl = Math.min(9_999_999, Math.max(1, Math.floor(maxCl)));
  let prePeak = num(o.pairBuyMinPreEntryPeakAbsChainlinkUsd);
  if (prePeak == null || prePeak <= 0) prePeak = 0;
  else prePeak = Math.min(9_999_999, Math.max(1, Math.floor(prePeak)));
  let riseW = num(o.pairBuyBtcRiseWindowSec);
  if (riseW == null || riseW <= 0) riseW = 0;
  else riseW = Math.min(WINDOW_SEC, Math.max(1, Math.floor(riseW)));
  let riseU = num(o.pairBuyBtcRiseMinUsd);
  if (riseU == null || riseU <= 0) riseU = 0;
  else riseU = Math.min(9_999_999, Math.max(1, Math.floor(riseU)));
  const advancedPairSell = Boolean(
    o.advancedPairSell === true || o.advancedPairSell === 1 || o.advancedPairSell === "1" || o.advancedPairSell === "true",
  );
  let dump = num(o.pairChainlinkAbsAboveMarketSellUsd);
  if (dump == null) dump = 0;
  dump = Math.max(0, Math.min(9_999_999, Math.floor(dump)));
  let stop = num(o.pairStopPriceUsd);
  if (stop == null || stop <= 0) stop = 0;
  stop = Math.max(0, Math.min(1, stop));
  if (stop >= 1) stop = 0.999999;
  return {
    requireMinBidAboveLimit,
    pairBuyMinAbsChainlinkUsd: minCl,
    pairBuyMaxAbsChainlinkUsd: maxCl,
    pairBuyMinPreEntryPeakAbsChainlinkUsd: prePeak,
    pairBuyBtcRiseWindowSec: riseW,
    pairBuyBtcRiseMinUsd: riseU,
    advancedPairSell,
    pairChainlinkAbsAboveMarketSellUsd: dump,
    pairStopPriceUsd: stop,
  };
}

async function readCalcPresetsDoc() {
  try {
    const raw = await readFile(CALC_PRESETS_PATH, "utf8");
    const j = JSON.parse(raw);
    if (!j || typeof j !== "object" || !Array.isArray(j.presets)) {
      return { version: 1, presets: [] };
    }
    return j;
  } catch (e) {
    const err = /** @type {NodeJS.ErrnoException} */ (e);
    if (err.code === "ENOENT") {
      return { version: 1, presets: [] };
    }
    throw e;
  }
}

/** 单侧测算参数预设（MySQL `charts_calc_presets`，与 `pm_book_ticks` 同库） */
api.get("/calc-presets", async (_req, res) => {
  try {
    if (!pool) {
      res.status(503).json({ ok: false, error: "database_unavailable" });
      return;
    }
    const presets = await fetchCalcPresetsFromDb();
    res.json({ ok: true, presets });
  } catch (e) {
    console.error("[charts-btc] /api/calc-presets GET", e);
    res.status(500).json({ ok: false, error: String(e instanceof Error ? e.message : e) });
  }
});

api.post("/calc-presets", async (req, res) => {
  try {
    if (!pool) {
      res.status(503).json({ ok: false, error: "database_unavailable" });
      return;
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const nameRaw = typeof body.name === "string" ? body.name.trim() : "";
    if (!nameRaw.length || nameRaw.length > CALC_PRESET_NAME_MAX) {
      res.status(400).json({ ok: false, error: "invalid_name" });
      return;
    }
    const params = normalizeCalcPresetParams(body.params);
    if (!params) {
      res.status(400).json({ ok: false, error: "invalid_params" });
      return;
    }
    const now = new Date();
    const [existingRows] = await pool.execute(
      `SELECT id FROM charts_calc_presets WHERE name = ? LIMIT 1`,
      [nameRaw],
    );
    const ex = Array.isArray(existingRows) && existingRows[0] ? /** @type {{ id: string }} */ (existingRows[0]) : null;
    if (ex) {
      await pool.execute(`UPDATE charts_calc_presets SET params_json = ?, updated_at = ? WHERE id = ?`, [
        JSON.stringify(params),
        now,
        ex.id,
      ]);
    } else {
      const [cntRows] = await pool.execute(`SELECT COUNT(*) AS c FROM charts_calc_presets`);
      const c = Number(/** @type {{ c?: unknown }} */ (/** @type {unknown[]} */ (cntRows)[0])?.c);
      if (Number.isFinite(c) && c >= CALC_PRESETS_MAX) {
        res.status(400).json({ ok: false, error: "too_many_presets" });
        return;
      }
      await pool.execute(
        `INSERT INTO charts_calc_presets (id, name, params_json, updated_at) VALUES (?, ?, ?, ?)`,
        [crypto.randomUUID(), nameRaw, JSON.stringify(params), now],
      );
    }
    const presets = await fetchCalcPresetsFromDb();
    const saved = presets.find((p) => p.name === nameRaw);
    if (!saved) {
      res.status(500).json({ ok: false, error: "save_failed" });
      return;
    }
    res.json({ ok: true, preset: saved });
  } catch (e) {
    console.error("[charts-btc] /api/calc-presets POST", e);
    res.status(500).json({ ok: false, error: String(e instanceof Error ? e.message : e) });
  }
});

api.delete("/calc-presets/:id", async (req, res) => {
  try {
    if (!pool) {
      res.status(503).json({ ok: false, error: "database_unavailable" });
      return;
    }
    const id = typeof req.params.id === "string" ? req.params.id.trim() : "";
    if (!id) {
      res.status(400).json({ ok: false, error: "invalid_id" });
      return;
    }
    const [result] = await pool.execute(`DELETE FROM charts_calc_presets WHERE id = ?`, [id]);
    const affected = /** @type {{ affectedRows?: number }} */ (result).affectedRows ?? 0;
    if (affected === 0) {
      res.status(404).json({ ok: false, error: "not_found" });
      return;
    }
    res.json({ ok: true });
  } catch (e) {
    console.error("[charts-btc] /api/calc-presets DELETE", e);
    res.status(500).json({ ok: false, error: String(e instanceof Error ? e.message : e) });
  }
});

api.get("/ticks", async (req, res) => {
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

function staticAuthGuard(req, res, next) {
  if (!AUTH_ENABLED) return next();
  if (req.path.startsWith("/api")) return next();
  const base = req.path.split("?")[0].replace(/\/$/, "") || "/";
  const publicPaths = new Set(["/login.html", "/login.js", "/styles.css"]);
  if (publicPaths.has(base)) return next();
  if (!isAuthenticated(req)) {
    if (wantsHtml(req)) {
      const dest = base === "/" ? "/" : base;
      res.redirect(302, `/login.html?next=${encodeURIComponent(dest)}`);
      return;
    }
    res.status(401).type("text/plain").send("Unauthorized");
    return;
  }
  next();
}

app.use(staticAuthGuard);
app.use("/api", api);
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);

/** 浏览器图表页 /ws/chart：订阅后推送快照，实时盘在每次入库后再推送一条。 */
const wssChart = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  let pathname = "/";
  try {
    pathname = new URL(request.url || "/", "http://localhost").pathname;
  } catch {
    socket.destroy();
    return;
  }
  if (pathname === "/ws/chart") {
    if (AUTH_ENABLED) {
      const tok = readSessionTokenFromCookieHeader(request.headers.cookie || "");
      if (!verifySessionToken(tok)) {
        socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
        socket.destroy();
        return;
      }
    }
    wssChart.handleUpgrade(request, socket, head, (ws) => {
      wssChart.emit("connection", ws, request);
    });
    return;
  }
  socket.destroy();
});

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

  const ts = Date.now();
  const row = buildAggregatedTickRow(ts, active.slug, lastBtcUsd);

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
    console.log(
      AUTH_ENABLED
        ? "[charts-btc] Login: 已启用（.env 中 LOGIN_USERNAME + LOGIN_SECRET；未登录将跳转 /login.html）"
        : "[charts-btc] Login: 未启用（同时设置 LOGIN_USERNAME 与 LOGIN_SECRET 后重启以开启）",
    );
    tickLoop().catch((e) => console.error("[charts-btc] first tick", e));
  });
}

main();
