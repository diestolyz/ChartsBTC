/**
 * 单侧盈亏测算核心（浏览器与 Node 共用：Node 自 `public/legPairPnl.mjs` 导入）。
 */

export const WINDOW_SEC = 300;

export function num(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** `btc-updown-5m-{unix}` → 窗口起点 Unix 秒 */
export function windowStartSecFromSlug(slug) {
  if (!slug || typeof slug !== "string") return null;
  const m = slug.match(/-(\d{8,})$/);
  if (!m) return null;
  const sec = Number(m[1]);
  return Number.isFinite(sec) ? sec : null;
}

/**
 * @param {number} ts_ms
 * @param {string | null} slug
 * @param {unknown[]} rowsAsc - 升序，供无 slug 时退化为「首条为 0 秒」
 */
export function secondsFromWindowOpen(ts_ms, slug, rowsAsc) {
  const w0 = windowStartSecFromSlug(slug);
  if (w0 != null) return ts_ms / 1000 - w0;
  if (rowsAsc.length > 0) {
    const t0 = num(rowsAsc[0].ts_ms);
    if (t0 != null) return (ts_ms - t0) / 1000;
  }
  return 0;
}

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_entry?: number, t_exit?: number, floatLoss?: number }}
 */
export function computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N) {
  if (!slug || typeof slug !== "string") {
    return { code: "no_slug", netUsd: 0 };
  }
  if (!Array.isArray(rows) || !rows.length) {
    return { code: "no_data", netUsd: 0 };
  }
  const points = [];
  for (const r of rows) {
    const ts = num(r.ts_ms);
    if (ts == null) continue;
    const u = num(r.up_mid);
    const d = num(r.down_mid);
    if (u == null || d == null) continue;
    const sec = secondsFromWindowOpen(ts, slug, rows);
    if (sec < 0 || sec > WINDOW_SEC) continue;
    points.push({ sec, u, d });
  }
  if (!points.length) {
    return { code: "no_points", netUsd: 0 };
  }
  const buyIdx = points.findIndex(
    (p) => p.sec >= t0 && p.sec <= t1 && (p.u <= P_buyLimit || p.d <= P_buyLimit),
  );
  if (buyIdx < 0) {
    return { code: "no_buy", netUsd: 0 };
  }
  const pBuy = points[buyIdx];
  const legUp = pBuy.u <= P_buyLimit;
  const leg = legUp ? "up" : "down";
  const P_entry = leg === "up" ? pBuy.u : pBuy.d;
  const t_entry = pBuy.sec;
  if (P_entry <= 0) {
    return { code: "bad_entry", netUsd: 0 };
  }
  let sellIdx = -1;
  for (let j = buyIdx + 1; j < points.length; j++) {
    const q = points[j];
    if (q.sec > WINDOW_SEC) break;
    const px = leg === "up" ? q.u : q.d;
    if (px >= P_sellTarget) {
      sellIdx = j;
      break;
    }
  }
  const legLabel = leg === "up" ? "Up" : "Down";
  if (sellIdx >= 0) {
    const q = points[sellIdx];
    const P_exit = leg === "up" ? q.u : q.d;
    const t_exit = q.sec;
    const profit = N * (P_exit - P_entry);
    return {
      code: "closed",
      netUsd: profit,
      leg,
      legLabel,
      P_entry,
      P_exit,
      t_entry,
      t_exit,
    };
  }
  const floatLoss = P_entry * N;
  return {
    code: "float",
    netUsd: -floatLoss,
    leg,
    legLabel,
    P_entry,
    t_entry,
    floatLoss,
  };
}

/**
 * 全量明细用：`M/D HH:mm → HH:mm`
 * @param {{ min_ts_ms?: unknown; max_ts_ms?: unknown }} w
 * @param {string} slug
 */
export function formatBatchMarketTimeRange(w, slug) {
  const minMs = w.min_ts_ms != null ? Number(w.min_ts_ms) : NaN;
  let startMs = Number.isFinite(minMs) ? minMs : null;

  if (startMs == null) {
    const m = slug.match(/-(\d{8,})$/);
    if (m) {
      const sec = Number(m[1]);
      if (Number.isFinite(sec)) startMs = sec * 1000;
    }
  }
  if (startMs == null) return "—";

  const da = new Date(startMs);
  const mo = da.getMonth() + 1;
  const day = da.getDate();
  const h = da.getHours();
  const min = da.getMinutes();
  const mdHm = `${mo}/${day} ${String(h).padStart(2, "0")}:${String(min).padStart(2, "0")}`;
  const endMin = min + 5;
  const rightT = `${String(h).padStart(2, "0")}:${String(endMin).padStart(2, "0")}`;
  return `${mdHm} → ${rightT}`;
}

/**
 * @param {string} code
 */
export function pnlDetailTag(code) {
  return code === "closed"
    ? "平仓"
    : code === "float"
      ? "浮亏"
      : code === "no_buy"
        ? "未买"
        : code === "no_points" || code === "no_data"
          ? "无点"
          : code;
}
