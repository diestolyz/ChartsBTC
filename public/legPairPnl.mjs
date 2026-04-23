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
 * 与 BTC5Mins `pair-limit-params` 对齐的可选约束（未传或默认时与旧版行为一致：仅 mid 触发 + 限价卖出）。
 * @typedef {object} LegPairPnlOpts
 * @property {boolean} [requireMinBidAboveLimit] — 仅 **买价 ≤0.5** 时生效：N1(=t0) 前 Up/Down 买一最小值及买入当刻两买一均须严格大于买入限价
 * @property {number} [pairBuyMinAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| ≥ 该值（美元）的 tick 上允许触发买；无有效 btc 差价数据则不触发；0 关闭
 * @property {number} [pairBuyMaxAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| 严格小于该值（美元）的 tick 上允许触发买；0 关闭上界
 * @property {number} [pairBuyMinPreEntryPeakAbsChainlinkUsd] — >0 时：自本盘首条数据起至买点 tick 之前，|现货−开盘| 的历史最大值须 ≥ 该值（美元）；否则该候选买点无效并继续向后找；无有效差价参与峰值的 tick 则该候选不通过；0 关闭
 * @property {number} [pairBuyBtcRiseWindowSec] — 与 `pairBuyBtcRiseMinUsd` 同时 >0 时：**与**买入限价、**[t0,t1]** 内触发、该 tick 上 |现货−开盘| 上下界等**同时**作为买点前提；须在 **[t0, min(t1, 买点秒)]** 内已出现「时长 ≤ 本字段（秒）、现货上涨 ≥ `pairBuyBtcRiseMinUsd`」的异动（不晚于买点）。**成交计价**：满足时 **P_entry = 异动结束当刻（最晚一次满足条件的右端点）已买侧 mid**，不再用买入限价；止损线、浮亏基数等同理按 P_entry；任一为 0 则关闭
 * @property {number} [pairBuyBtcRiseMinUsd] — 见 `pairBuyBtcRiseWindowSec`；美元涨幅下界；0 关闭
 * @property {boolean} [advancedPairSell] — 为真时启用 `pairLossPctThreshold` 止损与 `pairChainlinkAbsAboveMarketSellUsd` 差价市价卖（参考买一）
 * @property {number} [pairChainlinkAbsAboveMarketSellUsd] — 买入后 |现货−开盘| **首次严格大于**该美元值即在该 tick 按参考价（买一，缺则用 mid）平仓；至窗口末从未超过则全亏；0 关闭
 * @property {number} [pairLossPctThreshold] — 负数 %（相对入账价的跌幅）；勾选 `advancedPairSell` 时：买入后仅扫描**已买入那一腿**的 mid（买 Up 只看 Up、买 Down 只看 Down）。止损价 **P_stop = P_entry×(1+阈值/100)**（例：−20 → 0.8×入账价）；若曾 **≤ P_stop** 则记止损亏损，亏损额 = 买价×(|阈值|/100)×份数；若至序列结束未触发则浮亏仍按同比例或全额（见实现）。**整窗回看**：若曾先达到卖出限价、之后同一窗口内仍出现破止损，则按**首次破止损**计，不按限价止盈盈利。
 */

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @param {LegPairPnlOpts} [opts]
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_entry?: number, t_exit?: number, floatLoss?: number, exitKind?: "limit" | "stop" | "dump" }} P_entry 为买入价（未开异动时为买入限价；开异动时为异动结束当刻已买侧 mid）。`closed` 时 P_exit 为平仓价。`float`：勾选 `advancedPairSell` 且有有效期末 mid 时 netUsd = N×(期末 mid−P_entry)（盯市）；未勾选高级卖时未平仓一律 netUsd = −N×P_entry（全额入账亏）；勾选但无期末 mid 时回退 −N×P_entry 或 −N×P_entry×|止损%|/100，仅此时带 floatLoss。
 */
/**
 * 买点索引之前（不含买点）各 tick 上已算好的 |现货−开盘| 峰值。
 * @param {{ absCl: number | null }[]} points
 * @param {number} idxExclusive
 * @returns {number | null}
 */
function maxAbsClStrictlyBefore(points, idxExclusive) {
  let m = null;
  for (let k = 0; k < idxExclusive; k++) {
    const v = points[k].absCl;
    if (v != null) m = m == null ? v : Math.max(m, v);
  }
  return m;
}

/**
 * 在 [tLo,tHi]（距开盘秒，闭区间）内是否存在：两采样点均落在此区间、时间差 ≤ winSec、现货涨幅 ≥ minUsd（仅上涨，以前点为基准）。
 * @param {{ sec: number, btc: number | null }[]} points
 */
function rangeHasBtcRiseInWindow(points, tLo, tHi, winSec, minUsd) {
  const eps = 1e-12;
  if (!(winSec > 0 && minUsd > 0)) return true;
  for (let i = 0; i < points.length; i++) {
    const a = points[i];
    if (a.sec < tLo - eps || a.sec > tHi + eps) continue;
    const btcA = a.btc;
    if (btcA == null) continue;
    for (let j = i; j < points.length; j++) {
      const b = points[j];
      if (b.sec > tHi + eps) break;
      if (b.sec - a.sec > winSec + eps) break;
      const btcB = b.btc;
      if (btcB != null && btcB - btcA >= minUsd - eps) return true;
    }
  }
  return false;
}

/**
 * 满足 `rangeHasBtcRiseInWindow` 的异动中，取**结束时刻最晚**（右端点 j 最大）的那次异动的 tick 索引；用于「买入价 = 异动结束当刻该侧 mid」。
 * @param {{ sec: number, btc: number | null }[]} points
 * @returns {number} 无则 -1
 */
function findLatestBtcRiseEndIndex(points, tLo, tHi, winSec, minUsd) {
  const eps = 1e-12;
  if (!(winSec > 0 && minUsd > 0)) return -1;
  let bestJ = -1;
  for (let i = 0; i < points.length; i++) {
    const a = points[i];
    if (a.sec < tLo - eps || a.sec > tHi + eps) continue;
    const btcA = a.btc;
    if (btcA == null) continue;
    for (let j = i; j < points.length; j++) {
      const b = points[j];
      if (b.sec > tHi + eps) break;
      if (b.sec - a.sec > winSec + eps) break;
      const btcB = b.btc;
      if (btcB != null && btcB - btcA >= minUsd - eps && j > bestJ) bestJ = j;
    }
  }
  return bestJ;
}

export function computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N, opts = {}) {
  if (!slug || typeof slug !== "string") {
    return { code: "no_slug", netUsd: 0 };
  }
  if (!Array.isArray(rows) || !rows.length) {
    return { code: "no_data", netUsd: 0 };
  }

  const requireMinBidAboveLimit = Boolean(opts.requireMinBidAboveLimit);
  const minClUsd = num(opts.pairBuyMinAbsChainlinkUsd);
  const minAbsChainlinkOn = minClUsd != null && minClUsd > 0;
  const maxClUsd = num(opts.pairBuyMaxAbsChainlinkUsd);
  const maxAbsChainlinkOn = maxClUsd != null && maxClUsd > 0;
  const prePeakUsd = num(opts.pairBuyMinPreEntryPeakAbsChainlinkUsd);
  const preEntryPeakMinOn = prePeakUsd != null && prePeakUsd > 0;
  const riseWinRaw = num(opts.pairBuyBtcRiseWindowSec);
  const riseUsdRaw = num(opts.pairBuyBtcRiseMinUsd);
  const riseWinSec =
    riseWinRaw != null && riseWinRaw > 0
      ? Math.min(WINDOW_SEC, Math.max(1, Math.floor(riseWinRaw)))
      : 0;
  const riseMinUsd =
    riseUsdRaw != null && riseUsdRaw > 0
      ? Math.min(9_999_999, Math.max(1, Math.floor(riseUsdRaw)))
      : 0;
  const riseFilterOn = riseWinSec > 0 && riseMinUsd > 0;

  const advancedPairSell = Boolean(opts.advancedPairSell);
  const dumpUsd = num(opts.pairChainlinkAbsAboveMarketSellUsd);
  const dumpOn = advancedPairSell && dumpUsd != null && dumpUsd > 0;
  const lossRaw = num(opts.pairLossPctThreshold);
  const lossThr =
    advancedPairSell && lossRaw != null && lossRaw < 0 && lossRaw >= -1000 ? lossRaw : null;
  const stopOn = lossThr != null;

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
    if (sec < 0 || sec > WINDOW_SEC) continue;
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

  if (requireMinBidAboveLimit && t0 > 0 && !highBuyMode) {
    let minUb = null;
    let minDb = null;
    for (const p of points) {
      if (p.sec >= t0) break;
      if (p.ub == null || p.db == null) {
        return { code: "no_buy", netUsd: 0 };
      }
      minUb = minUb == null ? p.ub : Math.min(minUb, p.ub);
      minDb = minDb == null ? p.db : Math.min(minDb, p.db);
    }
    if (minUb == null || minDb == null) {
      return { code: "no_buy", netUsd: 0 };
    }
    if (minUb <= P_buyLimit + 1e-12 || minDb <= P_buyLimit + 1e-12) {
      return { code: "no_buy", netUsd: 0 };
    }
  }

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

      for (let k = 0; k < i; k++) {
        const px = side === "up" ? points[k].u : points[k].d;
        if (px != null && px > P_buyLimit + eps) {
          return { code: "no_buy", netUsd: 0 };
        }
      }

      if (minAbsChainlinkOn && (p.absCl == null || p.absCl < minClUsd - 1e-12)) {
        continue;
      }
      if (maxAbsChainlinkOn && p.absCl != null && p.absCl >= maxClUsd - 1e-12) {
        continue;
      }
      if (preEntryPeakMinOn) {
        const peak = maxAbsClStrictlyBefore(points, i);
        if (peak == null || peak < prePeakUsd - 1e-12) continue;
      }
      if (
        riseFilterOn &&
        !rangeHasBtcRiseInWindow(points, t0, Math.min(t1, p.sec), riseWinSec, riseMinUsd)
      ) {
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
      if (preEntryPeakMinOn) {
        const peak = maxAbsClStrictlyBefore(points, i);
        if (peak == null || peak < prePeakUsd - 1e-12) continue;
      }
      if (
        riseFilterOn &&
        !rangeHasBtcRiseInWindow(points, t0, Math.min(t1, p.sec), riseWinSec, riseMinUsd)
      ) {
        continue;
      }
      if (requireMinBidAboveLimit) {
        if (p.ub == null || p.db == null) continue;
        if (p.ub <= P_buyLimit + 1e-12 || p.db <= P_buyLimit + 1e-12) continue;
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
  /** 未开异动：入账价 = 买入限价、时间 = 触发买点；开异动：入账价 = 异动结束当刻已买侧 mid、时间 = 该 tick */
  let P_entry = P_buyLimit;
  let t_entry = pBuy.sec;
  if (riseFilterOn) {
    const riseEndIdx = findLatestBtcRiseEndIndex(
      points,
      t0,
      Math.min(t1, pBuy.sec),
      riseWinSec,
      riseMinUsd,
    );
    if (riseEndIdx >= 0) {
      const qe = points[riseEndIdx];
      const pxSurge = leg === "up" ? qe.u : qe.d;
      if (pxSurge != null && pxSurge > 0 && pxSurge < 1 - 1e-12) {
        P_entry = pxSurge;
        t_entry = qe.sec;
      }
    }
  }
  if (P_entry <= 0) {
    return { code: "bad_entry", netUsd: 0 };
  }

  let sellIdx = -1;
  /** @type {"limit" | "stop" | "dump" | undefined} */
  let exitKind;
  /** @type {number | undefined} */
  let exitPrice;

  const eps = 1e-12;
  /** 止损价：入账价×(1+止损%/100)，与 BTC5Mins `pairLimitStrategyCore` 市价止损线一致。仅看买入腿 mid */
  const stopLinePx =
    stopOn && P_entry > 0 ? P_entry * (1 + lossThr / 100) : null;

  /** 各自首次触发的 tick 索引（整窗扫描，用于「先止盈后仍破止损」等回看） */
  let firstStopJ = -1;
  let firstLimitJ = -1;
  let firstDumpJ = -1;

  for (let j = buyIdx + 1; j < points.length; j++) {
    const q = points[j];
    if (q.sec > WINDOW_SEC) break;
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
    if (dumpOn && ref != null && q.absCl != null && q.absCl > dumpUsd + 1e-12) {
      if (firstDumpJ < 0) firstDumpJ = j;
    }
  }

  /** 曾先达到限价止盈、之后又破止损 → 按首次破止损计，不按止盈 */
  const lateStopAfterLimit =
    stopOn && firstStopJ >= 0 && firstLimitJ >= 0 && firstStopJ > firstLimitJ;

  /** @type {"stop" | "limit" | "dump" | null} */
  let exitPickKind = null;
  let exitPickJ = -1;

  if (lateStopAfterLimit) {
    exitPickKind = "stop";
    exitPickJ = firstStopJ;
  } else {
    /** 同 tick 内顺序与旧版逐 tick 一致：止损 → 限价 → 差价平 */
    /** @type {{ j: number; kind: "stop" | "limit" | "dump"; ord: number }[]} */
    const cands = [];
    if (stopOn && firstStopJ >= 0) cands.push({ j: firstStopJ, kind: "stop", ord: 0 });
    if (firstLimitJ >= 0) cands.push({ j: firstLimitJ, kind: "limit", ord: 1 });
    if (dumpOn && firstDumpJ >= 0) cands.push({ j: firstDumpJ, kind: "dump", ord: 2 });
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
      exitPrice = ref != null ? ref : px;
    } else if (exitPickKind === "limit") {
      exitKind = "limit";
      exitPrice = P_sellTarget;
    } else {
      exitKind = "dump";
      exitPrice = ref != null ? ref : px;
    }
  }
  const legLabel = leg === "up" ? "Up" : "Down";
  if (sellIdx >= 0 && exitPrice != null) {
    const q = points[sellIdx];
    const t_exit = q.sec;
    const profit =
      exitKind === "stop" && stopOn && lossThr != null
        ? -N * P_entry * (Math.abs(lossThr) / 100)
        : N * (exitPrice - P_entry);
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
   * 「窗口末」未触发限价/止损/差价平仓：
   * - 勾选 `advancedPairSell` 时：若有有效期末已买侧 mid，按盯市 netUsd = N×(期末 mid−P_entry)，可浮盈可浮亏，并返回 P_exit/t_exit。
   * - 未勾选高级卖：不按期末 mid 折算，一律按全额入账亏 netUsd = −N×P_entry（与「仅基础+限价卖出」直觉一致）。
   * - 勾选高级卖但无有效期末 mid：回退全亏 N×P_entry（若同时启用止损线则 ×|止损%|/100），netUsd 为负，并填 floatLoss。
   */
  let pxEnd = /** @type {number | null} */ (null);
  let tEnd = /** @type {number | null} */ (null);
  if (advancedPairSell) {
    for (let j = buyIdx; j < points.length; j++) {
      const q = points[j];
      if (q.sec > WINDOW_SEC) break;
      const px = leg === "up" ? q.u : q.d;
      if (px != null && px > 0 && px < 1 - eps) {
        pxEnd = px;
        tEnd = q.sec;
      }
    }
  }
  if (advancedPairSell && pxEnd != null && tEnd != null) {
    const netUsdFloat = N * (pxEnd - P_entry);
    return {
      code: "float",
      netUsd: netUsdFloat,
      leg,
      legLabel,
      P_entry,
      P_exit: pxEnd,
      t_entry,
      t_exit: tEnd,
    };
  }
  const fullStakeUsd = P_entry * N;
  const floatLoss =
    advancedPairSell && stopOn
      ? fullStakeUsd * (Math.abs(lossThr) / 100)
      : fullStakeUsd;
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
