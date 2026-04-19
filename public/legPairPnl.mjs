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
 * @property {number} [pairBuyMaxAbsChainlinkUsd] — >0 时：仅在 |现货−开盘| 严格小于该值（美元）的 tick 上允许触发买；0 关闭
 * @property {boolean} [advancedPairSell] — 为真时启用 `pairLossPctThreshold` 止损与 `pairChainlinkAbsAboveMarketSellUsd` 差价市价卖（参考买一）
 * @property {number} [pairChainlinkAbsAboveMarketSellUsd] — 买入后 |现货−开盘| **首次严格大于**该美元值即在该 tick 按参考价（买一，缺则用 mid）平仓；至窗口末从未超过则全亏；0 关闭
 * @property {number} [pairLossPctThreshold] — 负数 %；勾选 `advancedPairSell` 时：买入后仅扫描**已买入那一腿**的 mid（买 Up 只看 Up、买 Down 只看 Down），若曾低于 买价×(|阈值|/100) 则记止损亏损，亏损额 = 买价×(|阈值|/100)×份数；若至序列结束未触发则浮亏仍按同比例或全额（见实现）。
 */

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @param {LegPairPnlOpts} [opts]
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_entry?: number, t_exit?: number, floatLoss?: number, exitKind?: "limit" | "stop" | "dump" }} P_entry 为买入限价；平仓时 P_exit 为实际结算价（限价目标或买一）。
 */
export function computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N, opts = {}) {
  if (!slug || typeof slug !== "string") {
    return { code: "no_slug", netUsd: 0 };
  }
  if (!Array.isArray(rows) || !rows.length) {
    return { code: "no_data", netUsd: 0 };
  }

  const requireMinBidAboveLimit = Boolean(opts.requireMinBidAboveLimit);
  const maxClUsd = num(opts.pairBuyMaxAbsChainlinkUsd);
  const maxAbsChainlinkOn = maxClUsd != null && maxClUsd > 0;

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
    points.push({ sec, u, d, ub, db, absCl });
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
      if (maxAbsChainlinkOn && p.absCl != null && p.absCl >= maxClUsd - 1e-12) {
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
  const t_entry = pBuy.sec;
  /** 盈亏按设置的买入限价 / 卖出目标价与份数，不用触发时刻的 mid。 */
  if (P_buyLimit <= 0) {
    return { code: "bad_entry", netUsd: 0 };
  }

  let sellIdx = -1;
  /** @type {"limit" | "stop" | "dump" | undefined} */
  let exitKind;
  /** @type {number | undefined} */
  let exitPrice;

  const eps = 1e-12;
  /** 止损观察线：买价×(|止损%|/100)。仅看买入腿 mid（买 Up 只看 Up，买 Down 只看 Down），另一侧不参与 */
  const stopLinePx =
    stopOn && P_buyLimit > 0 ? P_buyLimit * (Math.abs(lossThr) / 100) : null;

  for (let j = buyIdx + 1; j < points.length; j++) {
    const q = points[j];
    if (q.sec > WINDOW_SEC) break;
    /** 仅已买入侧 mid；与另一侧无关 */
    const px = leg === "up" ? q.u : q.d;
    const bid = leg === "up" ? q.ub : q.db;
    /** 库中买一常为空：止损/差价卖用买一，缺失时回退该侧 mid（与页面「价格」一致，避免误判成全亏未平仓） */
    const ref =
      bid != null && bid > 0 ? bid : px != null && px > 0 && px < 1 ? px : null;

    if (stopOn && stopLinePx != null && px != null) {
      if (px < stopLinePx - eps) {
        sellIdx = j;
        exitKind = "stop";
        exitPrice = ref != null ? ref : px;
        break;
      }
    }
    if (px >= P_sellTarget) {
      sellIdx = j;
      exitKind = "limit";
      exitPrice = P_sellTarget;
      break;
    }
    if (dumpOn && ref != null && q.absCl != null && q.absCl > dumpUsd + 1e-12) {
      sellIdx = j;
      exitKind = "dump";
      exitPrice = ref;
      break;
    }
  }
  const legLabel = leg === "up" ? "Up" : "Down";
  if (sellIdx >= 0 && exitPrice != null) {
    const q = points[sellIdx];
    const t_exit = q.sec;
    const profit =
      exitKind === "stop" && stopOn && lossThr != null
        ? -N * P_buyLimit * (Math.abs(lossThr) / 100)
        : N * (exitPrice - P_buyLimit);
    return {
      code: "closed",
      netUsd: profit,
      leg,
      legLabel,
      P_entry: P_buyLimit,
      P_exit: exitPrice,
      t_entry,
      t_exit,
      exitKind,
    };
  }
  /**
   * 「窗口末」：买入之后按时间扫完本盘剩余 tick（本 5 分钟市场、sec≤WINDOW_SEC），仍未触发限价/止损/差价平仓，即到序列结束节点。
   * 若全程未到卖出价等，按模型记为未平仓（全亏语义下风险敞口为 全亏金额 = N×买入限价）。
   * 勾选高级卖且止损线有效时：记账浮亏 = 全亏金额 × (|止损%|/100)（与 N×限价×|止损%|/100 等价）；否则浮亏仍按全额 全亏金额。
   */
  const fullStakeUsd = P_buyLimit * N;
  const floatLoss =
    advancedPairSell && stopOn
      ? fullStakeUsd * (Math.abs(lossThr) / 100)
      : fullStakeUsd;
  return {
    code: "float",
    netUsd: -floatLoss,
    leg,
    legLabel,
    P_entry: P_buyLimit,
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
