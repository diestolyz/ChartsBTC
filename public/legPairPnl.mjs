/**
 * 单侧盈亏测算核心（浏览器与 Node 共用：Node 自 `public/legPairPnl.mjs` 导入）。
 */

export const WINDOW_SEC = 300;

/** 盘末若干秒盘口常失真：`computeLegPnlFromRows` 等测算不采纳 `sec` 严格大于此值的采样；图表仍按整窗 `WINDOW_SEC` 展示。 */
export const WINDOW_CHART_TRIM_END_SEC = 13;
export const WINDOW_EFFECTIVE_MAX_SEC = WINDOW_SEC - WINDOW_CHART_TRIM_END_SEC;

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
 * `float` 未平仓结算专用：仅使用 **裁切区间之后** 到窗末的采样，即
 * `sec ∈ (WINDOW_EFFECTIVE_MAX_SEC, WINDOW_SEC]`（对应「`WINDOW_CHART_TRIM_END_SEC`～300s」之间不入 `points`、也不参与买/卖扫描，**仅**在此取 **一条** 价做 >0.5 / <0.5 判定）。
 * 在该区间内取 **sec 最大**（最接近 `WINDOW_SEC`）的一条已买腿 mid；若该区间无有效价则返回 `null`（不读 `[0, WINDOW_EFFECTIVE_MAX_SEC]` 作未平仓结算价）。
 * @param {unknown[]} rowsAsc
 * @param {string | null} slug
 * @param {"up" | "down"} leg
 * @returns {{ sec: number; px: number } | null}
 */
function terminalFloatLegPxFromRows(rowsAsc, slug, leg) {
  const eps = 1e-12;
  /** @type {{ sec: number; px: number } | null} */
  let tailPick = null;
  for (const r of rowsAsc) {
    const ts = num(r.ts_ms);
    if (ts == null) continue;
    const sec = secondsFromWindowOpen(ts, slug, rowsAsc);
    if (sec <= WINDOW_EFFECTIVE_MAX_SEC || sec > WINDOW_SEC) continue;
    const u = num(r.up_mid);
    const d = num(r.down_mid);
    const px = leg === "up" ? u : d;
    if (px == null || !(px > 0 && px < 1 - eps)) continue;
    if (!tailPick || sec > tailPick.sec) tailPick = { sec, px };
  }
  return tailPick;
}

/**
 * 与 BTC5Mins `pair-limit-params` 对齐的可选约束（未传或默认时与旧版行为一致：仅 mid 触发 + 限价卖出）。
 * @typedef {object} LegPairPnlOpts
 * @property {number} [pairBuyMinAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| ≥ 该值（美元）的 tick 上允许触发买；无有效 btc 差价数据则不触发；0 关闭
 * @property {number} [pairBuyMaxAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| 严格小于该值（美元）的 tick 上允许触发买；0 关闭上界
 * @property {boolean} [advancedPairSell] — 为真时启用 `pairStopPriceUsd` 止损（参考买一）
 * @property {number} [pairStopPriceUsd] — 止损绝对价格（USD，0~1）；勾选 `advancedPairSell` 时：买入后仅扫描**已买入那一腿**的 mid（买 Up 只看 Up、买 Down 只看 Down）。若曾 **≤ P_stop(=本字段)** 则记止损平仓，并按该止损价结算：盈亏 = (P_stop − P_entry)×份数。**整窗回看**：若曾先达到卖出限价、之后同一窗口内仍出现破止损，则按**首次破止损**计，不按限价止盈盈利。
 * @property {number} [pairFixedLossUsd] — 固定亏损金额（USD）。默认 0 关闭；>0 时：只要最终处于 `float`（未平仓），浮亏固定为该金额（netUsd = −pairFixedLossUsd），不再随期末价变化。
 * @property {number} [feeUsd] — 固定手续费（USD）。只要触发买入（最终处于 closed/float），统一计入：netUsd = 原netUsd − feeUsd（即盈利扣手续费、亏损叠加手续费）。
 * @property {boolean} [pairHighBuyNoAboveBeforeCross] — 买入限价 &gt;0.5 时：为真（默认）则定侧后须自盘首至穿入前该侧 mid 从未严格高于限价，否则 `no_buy`；为假则不做该过滤。
 */

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @param {LegPairPnlOpts} [opts]
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_entry?: number, t_exit?: number, floatLoss?: number, exitKind?: "limit" | "stop" }} P_entry 为买入价（默认：触发买点当刻已买侧 mid；开异动链上条件且命中右端点时为异动结束当刻已买侧 mid）。`closed` 时 P_exit 为平仓价。`float`：窗口末未平仓时若有有效期末价则按「市场结束结算」规则给出 netUsd，并返回 P_exit/t_exit；无有效期末价时回退为全亏 −N×P_entry，若启用止损价则回退为 −N×max(P_entry−P_stop,0)，仅此时带 floatLoss。
 */
export function computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N, opts = {}) {
  if (!slug || typeof slug !== "string") {
    return { code: "no_slug", netUsd: 0 };
  }
  if (!Array.isArray(rows) || !rows.length) {
    return { code: "no_data", netUsd: 0 };
  }

  const minClUsd = num(opts.pairBuyMinAbsChainlinkUsd);
  const minAbsChainlinkOn = minClUsd != null && minClUsd > 0;
  const maxClUsd = num(opts.pairBuyMaxAbsChainlinkUsd);
  const maxAbsChainlinkOn = maxClUsd != null && maxClUsd > 0;

  const advancedPairSell = Boolean(opts.advancedPairSell);
  const stopRaw = num(opts.pairStopPriceUsd);
  const stopPx =
    advancedPairSell && stopRaw != null && stopRaw > 0 && stopRaw < 1 ? stopRaw : null;
  const stopOn = stopPx != null;

  const fixedLossRaw = num(opts.pairFixedLossUsd);
  const fixedLossUsd =
    fixedLossRaw != null && fixedLossRaw > 0 ? Math.max(0, Math.min(9_999_999, fixedLossRaw)) : 0;

  const feeRaw = num(opts.feeUsd);
  const feeUsd = feeRaw != null && feeRaw > 0 ? Math.max(0, Math.min(9_999_999, feeRaw)) : 0;

  const vNoAbove = opts.pairHighBuyNoAboveBeforeCross;
  const highBuyNoAboveBeforeCross =
    vNoAbove === false || vNoAbove === 0 || vNoAbove === "0" || vNoAbove === "false"
      ? false
      : true;

  /** 窗口内首条非空 Chainlink 现货，作「开盘」参考（避免首 tick 无 btc 导致全程无差价） */
  let openBtc = null;
  for (const r of rows) {
    const b = num(r.btc_usd);
    if (b != null) {
      openBtc = b;
      break;
    }
  }

  const points = [];
  /** 前向填充：无 btc 的 tick 沿用上一有效值，便于与盘口点对齐判断差价 */
  let lastBtc = openBtc;
  for (const r of rows) {
    const ts = num(r.ts_ms);
    if (ts == null) continue;
    const u = num(r.up_mid);
    const d = num(r.down_mid);
    if (u == null || d == null) continue;
    const ub = num(r.up_bid);
    const db = num(r.down_bid);
    const btcRow = num(r.btc_usd);
    if (btcRow != null) lastBtc = btcRow;
    const sec = secondsFromWindowOpen(ts, slug, rows);
    if (sec < 0 || sec > WINDOW_EFFECTIVE_MAX_SEC) continue;
    let absCl = null;
    if (openBtc != null && lastBtc != null) {
      absCl = Math.abs(lastBtc - openBtc);
    }
    points.push({ sec, u, d, ub, db, absCl, btc: lastBtc });
  }
  if (!points.length) {
    return { code: "no_points", netUsd: 0 };
  }

  const highBuyMode = P_buyLimit > 0.5 + 1e-12;

  let buyIdx = -1;
  /** 买价 &gt;0.5 时由下方循环写入：'up' | 'down' */
  let highBuyLeg = /** @type {"up" | "down" | null} */ (null);

  if (highBuyMode) {
    const eps = 1e-12;
    for (let i = 1; i < points.length; i++) {
      const p = points[i];
      const prev = points[i - 1];
      if (p.sec < t0 || p.sec > t1) continue;

      const uPrev = prev.u;
      const uCur = p.u;
      const dPrev = prev.d;
      const dCur = p.d;
      if (
        uPrev == null ||
        uCur == null ||
        dPrev == null ||
        dCur == null
      ) {
        continue;
      }

      const upCross =
        uPrev <= P_buyLimit + eps && uCur >= P_buyLimit - eps;
      const downCross =
        dPrev <= P_buyLimit + eps && dCur >= P_buyLimit - eps;
      if (!upCross && !downCross) continue;

      const side = upCross && downCross ? "up" : upCross ? "up" : "down";

      if (highBuyNoAboveBeforeCross) {
        for (let k = 0; k < i; k++) {
          const px = side === "up" ? points[k].u : points[k].d;
          if (px != null && px > P_buyLimit + eps) {
            return { code: "no_buy", netUsd: 0 };
          }
        }
      }

      if (minAbsChainlinkOn && (p.absCl == null || p.absCl < minClUsd - 1e-12)) {
        continue;
      }
      if (maxAbsChainlinkOn && p.absCl != null && p.absCl >= maxClUsd - 1e-12) {
        continue;
      }

      buyIdx = i;
      highBuyLeg = side;
      break;
    }
  } else {
    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      if (p.sec < t0 || p.sec > t1) continue;
      if (!(p.u <= P_buyLimit || p.d <= P_buyLimit)) continue;
      if (minAbsChainlinkOn && (p.absCl == null || p.absCl < minClUsd - 1e-12)) {
        continue;
      }
      if (maxAbsChainlinkOn && p.absCl != null && p.absCl >= maxClUsd - 1e-12) {
        continue;
      }
      buyIdx = i;
      break;
    }
  }

  if (buyIdx < 0) {
    return { code: "no_buy", netUsd: 0 };
  }
  const pBuy = points[buyIdx];
  const legUp = highBuyMode ? highBuyLeg === "up" : pBuy.u <= P_buyLimit;
  const leg = legUp ? "up" : "down";
  /** 默认：入账价 = 触发买点当刻已买侧 mid（实际买入价）；开异动且命中右端点：改为异动结束当刻该侧 mid */
  const pxAtBuyTick = leg === "up" ? pBuy.u : pBuy.d;
  let P_entry =
    pxAtBuyTick != null && pxAtBuyTick > 0 && pxAtBuyTick < 1 - 1e-12 ? pxAtBuyTick : P_buyLimit;
  let t_entry = pBuy.sec;
  if (P_entry <= 0) {
    return { code: "bad_entry", netUsd: 0 };
  }

  let sellIdx = -1;
  /** @type {"limit" | "stop" | undefined} */
  let exitKind;
  /** @type {number | undefined} */
  let exitPrice;

  const eps = 1e-12;
  /** 止损价（绝对 USD，0~1）。仅看买入腿 mid */
  const stopLinePx = stopOn ? stopPx : null;

  /** 各自首次触发的 tick 索引（整窗扫描，用于「先止盈后仍破止损」等回看） */
  let firstStopJ = -1;
  let firstLimitJ = -1;

  for (let j = buyIdx + 1; j < points.length; j++) {
    const q = points[j];
    if (q.sec > WINDOW_EFFECTIVE_MAX_SEC) break;
    /** 仅已买入侧 mid；与另一侧无关 */
    const px = leg === "up" ? q.u : q.d;
    const bid = leg === "up" ? q.ub : q.db;
    /** 库中买一常为空：止损/差价卖用买一，缺失时回退该侧 mid（与页面「价格」一致，避免误判成全亏未平仓） */
    const ref =
      bid != null && bid > 0 ? bid : px != null && px > 0 && px < 1 ? px : null;

    if (stopOn && stopLinePx != null && px != null && px <= stopLinePx + eps) {
      if (firstStopJ < 0) firstStopJ = j;
    }
    if (px != null && px >= P_sellTarget) {
      if (firstLimitJ < 0) firstLimitJ = j;
    }
  }

  /** 曾先达到限价止盈、之后又破止损 → 按首次破止损计，不按止盈 */
  const lateStopAfterLimit =
    stopOn && firstStopJ >= 0 && firstLimitJ >= 0 && firstStopJ > firstLimitJ;

  /** @type {"stop" | "limit" | null} */
  let exitPickKind = null;
  let exitPickJ = -1;

  if (lateStopAfterLimit) {
    exitPickKind = "stop";
    exitPickJ = firstStopJ;
  } else {
    /** 同 tick 内顺序与旧版逐 tick 一致：止损 → 限价 */
    /** @type {{ j: number; kind: "stop" | "limit"; ord: number }[]} */
    const cands = [];
    if (stopOn && firstStopJ >= 0) cands.push({ j: firstStopJ, kind: "stop", ord: 0 });
    if (firstLimitJ >= 0) cands.push({ j: firstLimitJ, kind: "limit", ord: 1 });
    cands.sort((a, b) => a.j - b.j || a.ord - b.ord);
    const best = cands[0];
    if (best) {
      exitPickKind = best.kind;
      exitPickJ = best.j;
    }
  }

  if (exitPickKind != null && exitPickJ >= 0) {
    const q = points[exitPickJ];
    const px = leg === "up" ? q.u : q.d;
    const bid = leg === "up" ? q.ub : q.db;
    const ref =
      bid != null && bid > 0 ? bid : px != null && px > 0 && px < 1 ? px : null;
    sellIdx = exitPickJ;
    if (exitPickKind === "stop") {
      exitKind = "stop";
      exitPrice = stopPx != null ? stopPx : ref != null ? ref : px;
    } else if (exitPickKind === "limit") {
      exitKind = "limit";
      exitPrice = P_sellTarget;
    }
  }
  const legLabel = leg === "up" ? "Up" : "Down";
  if (sellIdx >= 0 && exitPrice != null) {
    const q = points[sellIdx];
    const t_exit = q.sec;
    let profit =
      exitKind === "stop" && stopOn && stopPx != null
        ? N * (stopPx - P_entry)
        : N * (exitPrice - P_entry);
    if (fixedLossUsd > 0 && profit < 0) profit = -fixedLossUsd;
    if (feeUsd > 0) profit -= feeUsd;
    return {
      code: "closed",
      netUsd: profit,
      leg,
      legLabel,
      P_entry,
      P_exit: exitPrice,
      t_entry,
      t_exit,
      exitKind,
    };
  }
  /**
   * 「未平仓」：买/止盈/止损仍仅用 `points`（sec ≤ `WINDOW_EFFECTIVE_MAX_SEC`）。
   * 结算价 **只** 来自 `rows` 中 `sec ∈ (WINDOW_EFFECTIVE_MAX_SEC, WINDOW_SEC]` 的 **一条**（sec 最大、即最接近 300s）；**不用**该开区间内的其它逻辑聚合，也 **不** 用 `[0, WINDOW_EFFECTIVE_MAX_SEC]` 的价做未平仓二元判定。
   * - 该价 > 0.5：结算到 1 → netUsd = N×(1−P_entry)
   * - 该价 < 0.5：结算到 0 → netUsd = −N×P_entry
   * - == 0.5：盯市 N×(0.5−P_entry)
   * 若尾段无有效价：`terminal` 为 `null`，走下方全亏回退。
   */
  const terminal = terminalFloatLegPxFromRows(rows, slug, leg);
  if (terminal != null) {
    const pxEnd = terminal.px;
    const tEnd = terminal.sec;
    const settlePx = pxEnd > 0.5 + eps ? 1 : pxEnd < 0.5 - eps ? 0 : 0.5;
    let netUsdFloat =
      fixedLossUsd > 0
        ? pxEnd > 0.5 + eps
          ? N * (1 - P_entry)
          : pxEnd < 0.5 - eps
            ? -fixedLossUsd
            : N * (0.5 - P_entry)
        : N * (settlePx - P_entry);
    if (feeUsd > 0) netUsdFloat -= feeUsd;
    return {
      code: "float",
      netUsd: netUsdFloat,
      leg,
      legLabel,
      P_entry,
      P_exit: pxEnd,
      t_entry,
      t_exit: tEnd,
      ...(fixedLossUsd > 0 && pxEnd < 0.5 - eps ? { floatLoss: fixedLossUsd + feeUsd } : {}),
    };
  }
  const fullStakeUsd = P_entry * N;
  let floatLoss =
    fixedLossUsd > 0
      ? fixedLossUsd
      : 
    advancedPairSell && stopOn && stopPx != null
      ? Math.max(0, (P_entry - stopPx) * N)
      : fullStakeUsd;
  if (feeUsd > 0) floatLoss += feeUsd;
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
      ? "未平仓"
      : code === "no_buy"
        ? "未买"
        : code === "no_points" || code === "no_data"
          ? "无点"
          : code;
}
