/**
 * 单侧盈亏测算核心（浏览器与 Node 共用：Node 自 `public/legPairPnl.mjs` 导入）。
 * `up_mid`/`down_mid` 与 ChartsBTC 服务端一致：按秒末 mid 与 0.5 比较分档，记入卖一极小或卖一极大（非算术 mid）。
 */

export const WINDOW_SEC = 300;

/** 盘末若干秒盘口常失真：`computeLegPnlFromRows` 等测算不采纳 `sec` 严格大于此值的采样；图表仍按整窗 `WINDOW_SEC` 展示。 */
export const WINDOW_CHART_TRIM_END_SEC = 16;
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
    /** 盘末 winning 侧 mid 可能为 1；仍参与 >0.5 结算判定 */
    if (px == null || !(px > 0 && px <= 1 + eps)) continue;
    if (!tailPick || sec > tailPick.sec) tailPick = { sec, px };
  }
  return tailPick;
}

/** 卖价为 1 或 0.01 时：不做限价止盈扫描，固定走未平仓并按 Chainlink 差价结算（与页头一致）。 */
export function isSellPriceChainlinkFloatMode(P_sellTarget) {
  if (!Number.isFinite(P_sellTarget)) return false;
  const eps = 1e-8;
  return Math.abs(P_sellTarget - 1) <= eps || Math.abs(P_sellTarget - 0.01) <= eps;
}

/**
 * 与页头「Chainlink 差价」同源：**升序 `rows` 自尾向前**，首条带有效 `btc_usd` 的采样（缓冲物理最后一帧）。
 * @param {unknown[]} rowsAsc
 * @returns {{ v: number; ts_ms: number } | null}
 */
function lastBtcUsdFromBufferTail(rowsAsc) {
  for (let i = rowsAsc.length - 1; i >= 0; i--) {
    const r = rowsAsc[i];
    const ts = num(r.ts_ms);
    const v = num(r.btc_usd);
    if (ts != null && v != null) return { v, ts_ms: ts };
  }
  return null;
}

/**
 * 未平仓 Chainlink 结算：收盘差价 = **缓冲末条有效现货** `btc_usd` − **页头同源开盘**（`opts.chainlinkOpenUsd` 或首条 tick 现货）。
 * 正 Up 胜、负 Down 胜。
 * @param {"up" | "down"} leg
 * @param {unknown[]} rowsAsc
 * @param {string | null} slug
 * @param {number | null} openBtcFirstTick
 * @param {LegPairPnlOpts} opts
 * @returns {{ settlePx: number; delta: number; t_exit: number } | null}
 */
function chainlinkFloatSettlement(leg, rowsAsc, slug, openBtcFirstTick, opts) {
  const openCl = num(opts.chainlinkOpenUsd) ?? openBtcFirstTick;
  if (openCl == null) return null;
  const lastCl = lastBtcUsdFromBufferTail(rowsAsc);
  if (lastCl == null) return null;
  const delta = lastCl.v - openCl;
  const tsMs = lastCl.ts_ms;
  const epsD = 1e-9;
  let settlePx;
  if (delta > epsD) settlePx = leg === "up" ? 1 : 0;
  else if (delta < -epsD) settlePx = leg === "down" ? 1 : 0;
  else settlePx = 0.5;
  const t_exit =
    tsMs != null ? secondsFromWindowOpen(tsMs, slug, rowsAsc) : WINDOW_SEC;
  return { settlePx, delta, t_exit };
}

/**
 * 与 BTC5Mins `pair-limit-params` 对齐的可选约束（未传或默认时与旧版行为一致：仅 mid 触发 + 限价卖出）。
 * @typedef {object} LegPairPnlOpts
 * @property {number} [pairBuyMinAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| ≥ 该值（美元）的 tick 上允许触发买；无有效 btc 差价数据则不触发；0 关闭
 * @property {number} [pairBuyMaxAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| 严格小于该值（美元）的 tick 上允许触发买；0 关闭上界
 * @property {boolean} [advancedPairSell] — 为真时启用 `pairStopPriceUsd` 止损（参考买一）
 * @property {number} [pairStopPriceUsd] — 止损线（USD，0~1）；勾选 `advancedPairSell` 时：买入后仅扫描**已买入那一腿**的 mid。**下穿**触发：相邻 tick 上前一帧已买侧 mid **严格高于** P_stop、当前帧 **≤ P_stop**。平仓价取**下穿当刻的下一帧**该侧买一（无则用 mid）；若无下一帧则回退为下穿当刻买一/mid。盈亏 = (P_exit − P_entry)×份数。**整窗回看**：若曾先达到卖出限价、之后同一窗口内仍出现下穿止损，则按**首次下穿**计，不按限价止盈盈利。
 * @property {number} [pairFixedLossUsd] — 固定亏损金额（USD）。默认 0 关闭；>0 时：只要最终处于 `float`（未平仓），浮亏固定为该金额（netUsd = −pairFixedLossUsd），不再随期末价变化。
 * @property {number} [feeUsd] — 固定手续费（USD）。只要触发买入（最终处于 closed/float），统一计入：netUsd = 原netUsd − feeUsd（即盈利扣手续费、亏损叠加手续费）。
 * @property {boolean} [pairHighBuyNoAboveBeforeCross] — 买入限价 &gt;0.5 时：为真（默认）则定侧后须自盘首至穿入前该侧 mid 从未严格高于限价，否则 `no_buy`；为假则不做该过滤。
 * @property {boolean} [pairHighBuyMaxAbsClBeforeNotAboveBuy] — 买入限价 &gt;0.5 时：为真（默认）则穿入当刻须有有效 |现货−开盘|，且自盘首至穿入前各 tick 的 |现货−开盘| 最大值不得严格大于穿入当刻 |现货−开盘|；否则跳过该次穿入继续找下一候选。
 * @property {number} [pairExitAbsClBelowUsd] — &gt;0 时：买入后若某 tick 上 |现货−开盘| **严格小于** 该美元值则视为平仓，卖出价取该 tick 已买侧买一（无则用 mid）。与止损、限价按时间先后取最早；0 关闭。
 * @property {number} [chainlinkOpenUsd] — 与页头「开盘 BTC」（`#live-btc-open`）一致时传入（如归档快照 `btcOpenUsd`、实时 health 的开盘）。不传则用首条 tick 的 `btc_usd` 作开盘近似（与归档无官方开盘时页头一致）。
 * @property {boolean} [pairReverseDevOn] — 为真且 `pairReverseDevMaxRate` &gt;0 时：在其它买点条件均通过后，校验开盘→买点弦线反向偏离率 R（买 Down 看 Up 侧、买 Up 看 Down 侧）；R 严格大于上限则跳过该候选。
 * @property {number} [pairReverseDevMaxRate] — 反向偏离率 R 上限（无量纲，Up/Down 共用）；0 或未勾选时关闭。
 */

/**
 * 买点反向偏离率 R：开盘 (0,0) 与买点 (t_buy, Δ_buy) 连弦线；买 Down 取路径在弦线上方（偏 Up）的峰值，买 Up 取弦线下方（偏 Down）的峰值；R = 峰值 / max(|Δ_buy|, ε)。
 * @param {"up" | "down"} leg
 * @param {{ sec: number; delta: number | null }[]} points
 * @param {number} buyIdx
 * @returns {number | null} R；无法计算时 null
 */
export function reverseDevRatioRAtBuy(leg, points, buyIdx) {
  const eps = 1e-12;
  if (buyIdx < 0 || buyIdx >= points.length) return null;
  const pBuy = points[buyIdx];
  const deltaBuy = pBuy.delta;
  if (deltaBuy == null || !Number.isFinite(deltaBuy)) return null;
  const secBuy = pBuy.sec;
  if (secBuy <= eps) return null;
  const denom = Math.abs(deltaBuy);
  if (denom < eps) return null;

  let peakDev = 0;
  for (let k = 0; k <= buyIdx; k++) {
    const dk = points[k].delta;
    if (dk == null || !Number.isFinite(dk)) continue;
    const secK = points[k].sec;
    const onChord = deltaBuy * (secK / secBuy);
    const dev = leg === "down" ? Math.max(0, dk - onChord) : Math.max(0, onChord - dk);
    if (dev > peakDev) peakDev = dev;
  }
  return peakDev / denom;
}

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @param {LegPairPnlOpts} [opts]
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_buy?: number, t_entry?: number, t_exit?: number, floatLoss?: number, exitKind?: "limit" | "stop" | "abs_cl" }} `t_buy` 为触发买入当刻距窗开盘秒数（`pBuy.sec`）。P_entry 为盈亏基数：**高价买与低价买规则相同**——优先取距开盘秒数 ≥ 买点秒+1 的首条采样上已买侧 mid；若无则回退买点当刻已买侧 mid；若上述采样价 **低于** 页面「买入限价」则入账价 **按买入限价**（不低于限价计入）。再不行则用买入限价本身。止盈/止损扫描自所选入账采样对应 tick 之后开始。`closed` 时 P_exit 为平仓价（止损为下穿后下一帧价）。`float`：窗口末未平仓时若有有效期末价则按「市场结束结算」规则给出 netUsd，并返回 P_exit/t_exit；无有效期末价时回退为全亏 −N×P_entry，若启用止损线则回退为 −N×max(P_entry−P_stop,0)，仅此时带 floatLoss。
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

  const exitAbsClRaw = num(opts.pairExitAbsClBelowUsd);
  const exitAbsClOn = exitAbsClRaw != null && exitAbsClRaw > 0;
  const exitAbsClThreshold = exitAbsClOn ? Math.max(1e-9, exitAbsClRaw) : null;

  const vNoAbove = opts.pairHighBuyNoAboveBeforeCross;
  const highBuyNoAboveBeforeCross =
    vNoAbove === false || vNoAbove === 0 || vNoAbove === "0" || vNoAbove === "false"
      ? false
      : true;

  const vMaxClBefore = opts.pairHighBuyMaxAbsClBeforeNotAboveBuy;
  const highBuyMaxAbsClBeforeNotAboveBuy =
    vMaxClBefore === false ||
    vMaxClBefore === 0 ||
    vMaxClBefore === "0" ||
    vMaxClBefore === "false"
      ? false
      : true;

  const reverseDevOn = Boolean(
    opts.pairReverseDevOn === true ||
      opts.pairReverseDevOn === 1 ||
      opts.pairReverseDevOn === "1" ||
      opts.pairReverseDevOn === "true",
  );
  const reverseDevMaxRRaw = num(opts.pairReverseDevMaxRate);
  const reverseDevMaxR =
    reverseDevOn && reverseDevMaxRRaw != null && reverseDevMaxRRaw > 0
      ? Math.min(1_000, Math.max(1e-9, reverseDevMaxRRaw))
      : null;

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
    let delta = null;
    if (openBtc != null && lastBtc != null) {
      delta = lastBtc - openBtc;
      absCl = Math.abs(delta);
    }
    points.push({ sec, u, d, ub, db, absCl, delta, btc: lastBtc });
  }
  if (!points.length) {
    return { code: "no_points", netUsd: 0 };
  }

  const highBuyMode = P_buyLimit > 0.5 + 1e-12;
  const eps = 1e-12;

  let buyIdx = -1;
  /** 买价 &gt;0.5 时由下方循环写入：'up' | 'down' */
  let highBuyLeg = /** @type {"up" | "down" | null} */ (null);

  if (highBuyMode) {
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

      if (highBuyMaxAbsClBeforeNotAboveBuy) {
        const absAtBuy = p.absCl;
        if (absAtBuy == null) continue;
        let maxAbsBefore = 0;
        let hasAbsBefore = false;
        for (let k = 0; k < i; k++) {
          const ac = points[k].absCl;
          if (ac == null) continue;
          hasAbsBefore = true;
          if (ac > maxAbsBefore) maxAbsBefore = ac;
        }
        if (hasAbsBefore && maxAbsBefore > absAtBuy + eps) continue;
      }

      if (minAbsChainlinkOn && (p.absCl == null || p.absCl < minClUsd - 1e-12)) {
        continue;
      }
      if (maxAbsChainlinkOn && p.absCl != null && p.absCl >= maxClUsd - 1e-12) {
        continue;
      }

      if (reverseDevMaxR != null) {
        const rRev = reverseDevRatioRAtBuy(side, points, i);
        if (rRev == null || rRev > reverseDevMaxR + eps) continue;
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

      const side = p.u <= P_buyLimit + eps ? "up" : "down";
      if (reverseDevMaxR != null) {
        const rRev = reverseDevRatioRAtBuy(side, points, i);
        if (rRev == null || rRev > reverseDevMaxR + eps) continue;
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
  const pxAtBuyTick = leg === "up" ? pBuy.u : pBuy.d;
  const buyPxOk = pxAtBuyTick != null && pxAtBuyTick > 0 && pxAtBuyTick < 1 - eps;
  const t_signal = pBuy.sec;
  let entryIdx = buyIdx;
  for (let k = 0; k < points.length; k++) {
    if (points[k].sec >= t_signal + 1 - eps) {
      entryIdx = k;
      break;
    }
  }
  const pEntry = points[entryIdx];
  const pxAtEntryTick = leg === "up" ? pEntry.u : pEntry.d;
  const entryPxOk =
    pxAtEntryTick != null && pxAtEntryTick > 0 && pxAtEntryTick < 1 - eps;
  /** 原始入账采样：+1s 已买侧 mid；否则回退买点当刻 mid */
  let rawEntryPx;
  let t_entry;
  let exitStartJ;
  if (entryIdx !== buyIdx && entryPxOk) {
    rawEntryPx = pxAtEntryTick;
    t_entry = pEntry.sec;
    exitStartJ = entryIdx + 1;
  } else {
    rawEntryPx = buyPxOk ? pxAtBuyTick : null;
    t_entry = pBuy.sec;
    exitStartJ = buyIdx + 1;
  }
  /** 高价买 / 低价买一致：后一秒（或回退）采样价低于买入限价则按买入限价入账 */
  let P_entry;
  if (rawEntryPx != null && rawEntryPx > 0 && rawEntryPx < 1 - eps) {
    P_entry = rawEntryPx + eps < P_buyLimit ? P_buyLimit : rawEntryPx;
  } else {
    P_entry = P_buyLimit;
  }
  if (P_entry <= 0) {
    return { code: "bad_entry", netUsd: 0, t_buy: t_signal };
  }

  let sellIdx = -1;
  /** @type {"limit" | "stop" | "abs_cl" | undefined} */
  let exitKind;
  /** @type {number | undefined} */
  let exitPrice;

  /** 止损价（绝对 USD，0~1）。仅看买入腿 mid */
  const stopLinePx = stopOn ? stopPx : null;

  /** 各自首次触发的 tick 索引（整窗扫描，用于「先止盈后仍下穿止损」等回看）；止损为「下穿」当刻的 j */
  let firstStopJ = -1;
  let firstLimitJ = -1;
  let firstAbsClExitJ = -1;
  /** 卖价 1 / 0.01：不按限价在窗内止盈，固定未平仓并按 Chainlink 差价结算 */
  const suppressLimitExit = isSellPriceChainlinkFloatMode(P_sellTarget);

  /** 入账 tick 上一帧的已买侧 mid，用于首帧「下穿」判定 */
  let prevLegMid =
    exitStartJ > 0
      ? (() => {
          const pr = points[exitStartJ - 1];
          const v = leg === "up" ? pr.u : pr.d;
          return v != null && v > 0 && v < 1 - eps ? v : null;
        })()
      : null;

  for (let j = exitStartJ; j < points.length; j++) {
    const q = points[j];
    if (q.sec > WINDOW_EFFECTIVE_MAX_SEC) break;
    /** 仅已买入侧 mid；与另一侧无关 */
    const px = leg === "up" ? q.u : q.d;
    const bid = leg === "up" ? q.ub : q.db;
    /** 库中买一常为空：止损/差价卖用买一，缺失时回退该侧 mid（与页面「价格」一致，避免误判成全亏未平仓） */
    const ref =
      bid != null && bid > 0 ? bid : px != null && px > 0 && px < 1 ? px : null;

    if (
      stopOn &&
      stopLinePx != null &&
      prevLegMid != null &&
      prevLegMid > stopLinePx + eps &&
      px != null &&
      px <= stopLinePx + eps
    ) {
      if (firstStopJ < 0) firstStopJ = j;
    }
    if (px != null && px > 0 && px < 1 - eps) prevLegMid = px;
    if (
      exitAbsClOn &&
      exitAbsClThreshold != null &&
      q.absCl != null &&
      q.absCl < exitAbsClThreshold - 1e-12
    ) {
      if (firstAbsClExitJ < 0) firstAbsClExitJ = j;
    }
    if (!suppressLimitExit && px != null && px >= P_sellTarget) {
      if (firstLimitJ < 0) firstLimitJ = j;
    }
  }

  /** 曾先达到限价止盈、之后又下穿止损 → 按首次下穿计，不按止盈 */
  const lateStopAfterLimit =
    stopOn && firstStopJ >= 0 && firstLimitJ >= 0 && firstStopJ > firstLimitJ;

  /** @type {"stop" | "limit" | "abs_cl" | null} */
  let exitPickKind = null;
  let exitPickJ = -1;

  if (lateStopAfterLimit) {
    exitPickKind = "stop";
    exitPickJ = firstStopJ;
  } else {
    /** 同 tick 内顺序：止损 → |BTC差价|平仓 → 限价 */
    /** @type {{ j: number; kind: "stop" | "limit" | "abs_cl"; ord: number }[]} */
    const cands = [];
    if (stopOn && firstStopJ >= 0) cands.push({ j: firstStopJ, kind: "stop", ord: 0 });
    if (exitAbsClOn && firstAbsClExitJ >= 0) cands.push({ j: firstAbsClExitJ, kind: "abs_cl", ord: 1 });
    if (!suppressLimitExit && firstLimitJ >= 0) cands.push({ j: firstLimitJ, kind: "limit", ord: 2 });
    cands.sort((a, b) => a.j - b.j || a.ord - b.ord);
    const best = cands[0];
    if (best) {
      exitPickKind = best.kind;
      exitPickJ = best.j;
    }
  }

  if (exitPickKind != null && exitPickJ >= 0) {
    /** 止损：下穿为启动点，平仓价取启动点下一帧（无下一帧则回退下穿当刻） */
    const jExec =
      exitPickKind === "stop" && stopOn
        ? exitPickJ + 1 < points.length
          ? exitPickJ + 1
          : exitPickJ
        : exitPickJ;
    const q = points[jExec];
    const px = leg === "up" ? q.u : q.d;
    const bid = leg === "up" ? q.ub : q.db;
    const ref =
      bid != null && bid > 0 ? bid : px != null && px > 0 && px < 1 ? px : null;
    sellIdx = jExec;
    if (exitPickKind === "stop") {
      exitKind = "stop";
      exitPrice =
        ref != null ? ref : px != null && px > 0 && px <= 1 + eps ? px : P_entry;
    } else if (exitPickKind === "limit") {
      exitKind = "limit";
      exitPrice = P_sellTarget;
    } else if (exitPickKind === "abs_cl") {
      exitKind = "abs_cl";
      exitPrice =
        ref != null ? ref : px != null && px > 0 && px <= 1 + eps ? px : P_entry;
    }
  }
  const legLabel = leg === "up" ? "Up" : "Down";
  if (sellIdx >= 0 && exitPrice != null) {
    const q = points[sellIdx];
    const t_exit = q.sec;
    let profit = N * (exitPrice - P_entry);
    if (fixedLossUsd > 0 && profit < 0) profit = -fixedLossUsd;
    if (feeUsd > 0) profit -= feeUsd;
    return {
      code: "closed",
      netUsd: profit,
      leg,
      legLabel,
      P_entry,
      P_exit: exitPrice,
      t_buy: t_signal,
      t_entry,
      t_exit,
      exitKind,
    };
  }
  /**
   * 「未平仓」：优先按 **缓冲末条有效现货 − 页头同源开盘** 算 Chainlink 差价；正 Up 胜、负 Down 胜。
   * 无有效 btc 时回退：裁切后盘末一条已买腿 mid（`terminalFloatLegPxFromRows`）；再无则全亏回退。
   */
  const clTerm = chainlinkFloatSettlement(leg, rows, slug, openBtc, opts);
  if (clTerm != null) {
    const { settlePx, delta, t_exit } = clTerm;
    const win = settlePx >= 1 - eps;
    const lose = settlePx <= eps;
    let netUsdFloat =
      fixedLossUsd > 0
        ? win
          ? N * (1 - P_entry)
          : lose
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
      P_exit: settlePx,
      t_buy: t_signal,
      t_entry,
      t_exit,
      floatSettleSource: "chainlink",
      chainlinkDeltaUsd: delta,
      ...(fixedLossUsd > 0 && lose ? { floatLoss: fixedLossUsd + feeUsd } : {}),
    };
  }
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
      t_buy: t_signal,
      t_entry,
      t_exit: tEnd,
      floatSettleSource: "mid",
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
    t_buy: t_signal,
    t_entry,
    floatLoss,
    floatSettleSource: "fallback",
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
