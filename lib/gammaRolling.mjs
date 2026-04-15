/**
 * Gamma rolling market resolution (same idea as BTC5Mins server.js):
 * slug = `{EVENT_SERIES_PREFIX}-{unix}` aligned to ROLLING_WINDOW_SEC.
 */

const GAMMA_BASE = (process.env.GAMMA_BASE || "https://gamma-api.polymarket.com").replace(/\/$/, "");
const EVENT_SERIES_PREFIX = (process.env.EVENT_SERIES_PREFIX || "btc-updown-5m").trim();
const ROLLING_WINDOW_SEC = Math.max(30, Number(process.env.ROLLING_WINDOW_SEC) || 300);
const ROLLING_SLUG_SCAN_WINDOWS = Math.max(1, Math.min(96, Number(process.env.ROLLING_SLUG_SCAN_WINDOWS) || 12));

function gammaTimeToIso(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) {
    const ms = v < 1e12 ? v * 1000 : v;
    const t = new Date(ms).getTime();
    return Number.isNaN(t) ? null : new Date(t).toISOString();
  }
  if (typeof v === "string") {
    const s = v.trim();
    if (/^\d+$/.test(s)) {
      const n = Number(s);
      if (!Number.isFinite(n)) return null;
      const ms = n < 1e12 ? n * 1000 : n;
      const t = new Date(ms).getTime();
      return Number.isNaN(t) ? null : new Date(t).toISOString();
    }
    const t = Date.parse(s);
    return Number.isNaN(t) ? null : new Date(t).toISOString();
  }
  return null;
}

export function marketEndIso(m) {
  return (
    gammaTimeToIso(m?.endDate) ||
    gammaTimeToIso(m?.end_date) ||
    gammaTimeToIso(m?.endDateIso) ||
    gammaTimeToIso(m?.umaEndDateIso) ||
    gammaTimeToIso(m?.umaEndDate) ||
    gammaTimeToIso(m?.endTime) ||
    null
  );
}

function normalizeGammaMarketPayload(data) {
  if (data == null) return null;
  if (Array.isArray(data)) return data[0] ?? null;
  if (data.market && typeof data.market === "object") return data.market;
  return data;
}

export async function fetchGammaMarketBySlug(slug) {
  const url = `${GAMMA_BASE}/markets/slug/${encodeURIComponent(slug)}`;
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Gamma market HTTP ${res.status}: ${t.slice(0, 400)}`);
  }
  const raw = await res.json();
  return normalizeGammaMarketPayload(raw);
}

function rollingMarketLooksTradeable(m, nowMs) {
  if (!m || typeof m !== "object") return false;
  if (m.closed === true) return false;
  const endIso = marketEndIso(m);
  if (endIso) {
    const t = Date.parse(endIso);
    if (Number.isFinite(t) && nowMs >= t) return false;
  }
  const ao = m.acceptingOrders ?? m.accepting_orders;
  if (ao === false) return false;
  return true;
}

/**
 * @param {number} timeOffsetMs - server time correction vs Date.now()
 * @returns {Promise<{ market: object; slug: string } | null>}
 */
export async function resolveRollingMarketFromGamma(timeOffsetMs = 0) {
  const prefix = EVENT_SERIES_PREFIX;
  const w = ROLLING_WINDOW_SEC;
  const nowMs = Date.now() + (timeOffsetMs || 0);
  const nowSec = Math.floor(nowMs / 1000);
  const base = Math.floor(nowSec / w) * w;
  const order = [0];
  for (let i = 1; i <= ROLLING_SLUG_SCAN_WINDOWS; i++) {
    order.push(i, -i);
  }
  for (const k of order) {
    const ts = base + k * w;
    const slug = `${prefix}-${ts}`;
    const m = await fetchGammaMarketBySlug(slug);
    if (!m) continue;
    if (rollingMarketLooksTradeable(m, nowMs)) {
      return { market: m, slug };
    }
  }
  for (const k of order) {
    const ts = base + k * w;
    const slug = `${prefix}-${ts}`;
    const m = await fetchGammaMarketBySlug(slug);
    if (!m) continue;
    const endIso = marketEndIso(m);
    if (endIso) {
      const t = Date.parse(endIso);
      if (Number.isFinite(t) && nowMs < t) {
        return { market: m, slug };
      }
    } else if (m.closed !== true) {
      return { market: m, slug };
    }
  }
  return null;
}

export function parseTokenIds(market) {
  if (!market) return [];
  const src = market.clobTokenIds ?? market.clob_token_ids ?? null;
  if (src == null || src === "") return [];
  try {
    const raw = typeof src === "string" ? JSON.parse(src) : src;
    if (!Array.isArray(raw)) return [];
    return raw.map((x) => String(x));
  } catch {
    return [];
  }
}

export function parseOutcomes(market) {
  if (!market?.outcomes) return ["Up", "Down"];
  try {
    const o = typeof market.outcomes === "string" ? JSON.parse(market.outcomes) : market.outcomes;
    return Array.isArray(o) && o.length ? o : ["Up", "Down"];
  } catch {
    return ["Up", "Down"];
  }
}

export function tokenIdsForUpDown(market) {
  const ids = parseTokenIds(market);
  const outcomes = parseOutcomes(market);
  let upIdx = outcomes.findIndex((x) => String(x).toLowerCase() === "up");
  let downIdx = outcomes.findIndex((x) => String(x).toLowerCase() === "down");
  if (upIdx < 0) upIdx = 0;
  if (downIdx < 0) downIdx = 1;
  const upId = ids[upIdx] ?? ids[0] ?? null;
  const downId = ids[downIdx] ?? ids[1] ?? null;
  return { upId, downId, outcomes };
}

export { EVENT_SERIES_PREFIX, ROLLING_WINDOW_SEC, GAMMA_BASE };
