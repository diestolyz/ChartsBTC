/**
 * 通过 WSS /ws/chart 订阅图表数据（快照 + 实时增量 + health 推送），用 Chart.js 绘制 Up mid / Down mid（左轴）与 Chainlink 差价（右轴）。
 * 仅 market-windows 与刷新用 HTTP；支持按市场 slug 切换归档时间段。
 */

const chartCanvas = document.getElementById("chart");
const limitInput = document.getElementById("limit-input");
const refreshBtn = document.getElementById("refresh-btn");
const marketSelect = document.getElementById("market-select");
const marketPrevBtn = document.getElementById("market-prev");
const marketNextBtn = document.getElementById("market-next");
const seriesLabel = document.getElementById("series-label");
const slugLabel = document.getElementById("slug-label");
const liveUp = document.getElementById("live-up");
const liveDown = document.getElementById("live-down");
const liveBtc = document.getElementById("live-btc");
const connLabel = document.getElementById("conn-label");

/** @type {string | null} */
let activeSlug = null;
/** 非空 = 查看该 slug 的归档；空 = 跟随当前服务盘 */
let chartSlugOverride = null;
/** 当前窗口开盘参考（Gamma / RTDS；仅「跟随实时」时使用） */
let chainlinkOpenUsd = null;
/** @type {{ btcDeltaUsd?: number | null } | null} */
let lastHealthJson = null;

/** 当前图表数据缓冲（与 WSS 快照 / 增量一致） */
let tickBuffer = [];

/** @type {WebSocket | null} */
let chartWs = null;
let chartWsReconnectTimer = null;

/** 状态栏：上游 CLOB/RTDS 与图表 WSS 分行合并显示 */
let upstreamStatusLine = "—";
let chartStatusLine = "";

/** 剩余 ≤ 此毫秒数视为「临近结束」，排程换盘刷新 */
const ROLLOVER_NEAR_END_MS = 5000;
/** 临近结束后延迟多久执行 loadMarketWindows + 重订阅 */
const ROLLOVER_REFRESH_AFTER_MS = 8000;

/** 当前盘结束前临近窗口内触发，排程一次换盘刷新 */
let rolloverRefreshTimer = null;
/** @type {string | null} */
let rolloverScheduledForSlug = null;

function paintConnLabel() {
  if (!connLabel) return;
  connLabel.textContent = chartStatusLine
    ? `${upstreamStatusLine} · ${chartStatusLine}`
    : upstreamStatusLine;
}

/** 与 Polymarket 5m 盘一致：横轴固定 0–300 秒 */
const WINDOW_SEC = 300;

function num(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** `btc-updown-5m-{unix}` → 窗口起点 Unix 秒 */
function windowStartSecFromSlug(slug) {
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
function secondsFromWindowOpen(ts_ms, slug, rowsAsc) {
  const w0 = windowStartSecFromSlug(slug);
  if (w0 != null) return ts_ms / 1000 - w0;
  if (rowsAsc.length > 0) {
    const t0 = num(rowsAsc[0].ts_ms);
    if (t0 != null) return (ts_ms - t0) / 1000;
  }
  return 0;
}

function formatWindowOption(w) {
  const min = new Date(Number(w.min_ts_ms));
  const max = new Date(Number(w.max_ts_ms));
  const n = Number(w.tick_count) || 0;
  const d0 = min.toLocaleString(undefined, {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
  const t1 = max.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  return `${d0} → ${t1} · ${n} 点`;
}

const chart = new Chart(chartCanvas, {
  type: "line",
  data: {
    datasets: [
      {
        label: "Up mid",
        borderColor: "rgba(74, 222, 128, 0.95)",
        backgroundColor: "rgba(74, 222, 128, 0.08)",
        yAxisID: "y",
        parsing: false,
        pointRadius: 0,
        borderWidth: 1.5,
        tension: 0.08,
      },
      {
        label: "Down mid",
        borderColor: "rgba(248, 113, 113, 0.95)",
        backgroundColor: "rgba(248, 113, 113, 0.06)",
        yAxisID: "y",
        parsing: false,
        pointRadius: 0,
        borderWidth: 1.5,
        tension: 0.08,
      },
      {
        label: "Chainlink 差价 (USD)",
        borderColor: "rgba(251, 191, 36, 0.5)",
        backgroundColor: "rgba(251, 191, 36, 0.04)",
        yAxisID: "y1",
        parsing: false,
        pointRadius: 0,
        borderWidth: 1.5,
        tension: 0.05,
      },
    ],
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: {
        labels: { color: "#c4c9d4" },
      },
      tooltip: {
        callbacks: {
          title(items) {
            const x = items[0]?.parsed?.x;
            if (x == null) return "";
            return `距开盘 ${Number(x).toFixed(2)} s`;
          },
        },
      },
    },
    scales: {
      x: {
        type: "linear",
        min: 0,
        max: WINDOW_SEC,
        title: { display: true, text: "距离开盘 (秒)", color: "#9ca3af" },
        grid: { color: "rgba(255,255,255,0.06)" },
        ticks: {
          color: "#8a8f98",
          maxRotation: 0,
          stepSize: 30,
          callback(v) {
            return `${v}`;
          },
        },
      },
      y: {
        position: "left",
        min: 0,
        max: 1,
        title: { display: true, text: "隐含价格 (mid)", color: "#9ca3af" },
        grid: { color: "rgba(255,255,255,0.06)" },
        ticks: { color: "#8a8f98" },
      },
      y1: {
        position: "right",
        title: { display: true, text: "相对开盘 (USD)", color: "#9ca3af" },
        grid: { drawOnChartArea: false },
        ticks: { color: "#d6b676" },
      },
    },
  },
});

function formatUsdDelta(delta) {
  const sign = delta >= 0 ? "+" : "";
  return (
    sign +
    delta.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })
  );
}

/**
 * @param {unknown[]} rows
 * @param {boolean} fromArchive - 用该盘首条记录的 btc 作开盘近似（服务端按时间升序返回）
 * @param {string | null} marketSlug - 用于解析窗口起点（slug 尾缀 unix）
 */
function applyRows(rows, fromArchive = false, marketSlug = null) {
  const tUp = [];
  const tDown = [];
  const tBtc = [];

  let open = chainlinkOpenUsd;
  if (fromArchive) {
    if (rows.length > 0) {
      const firstBtc = num(rows[0].btc_usd);
      open = firstBtc != null ? firstBtc : null;
    } else {
      open = null;
    }
  }

  for (const r of rows) {
    const ts = num(r.ts_ms);
    if (ts == null) continue;
    const sec = secondsFromWindowOpen(ts, marketSlug, rows);
    if (sec < 0 || sec > WINDOW_SEC) continue;
    const x = sec;
    const u = num(r.up_mid);
    const d = num(r.down_mid);
    const b = num(r.btc_usd);
    if (u != null) tUp.push({ x, y: u });
    if (d != null) tDown.push({ x, y: d });
    if (b != null) {
      if (open != null) tBtc.push({ x, y: b - open });
      else tBtc.push({ x, y: b });
    }
  }
  chart.data.datasets[0].data = tUp;
  chart.data.datasets[1].data = tDown;
  chart.data.datasets[2].data = tBtc;
  const ds2 = chart.data.datasets[2];
  if (open != null) {
    ds2.label = fromArchive ? "Chainlink 差价 (USD，首条为基准)" : "Chainlink 差价 (USD)";
    chart.options.scales.y1.title.text = fromArchive ? "相对首条记录 (USD)" : "相对开盘 (USD)";
  } else {
    ds2.label = "BTC USD (Chainlink)";
    chart.options.scales.y1.title.text = "BTC / USD";
  }
  chart.update("none");

  const last = rows[rows.length - 1];
  if (last) {
    liveUp.textContent = num(last.up_mid) != null ? num(last.up_mid).toFixed(4) : "—";
    liveDown.textContent = num(last.down_mid) != null ? num(last.down_mid).toFixed(4) : "—";
    const lastPx = num(last.btc_usd);
    if (lastPx != null && open != null) {
      liveBtc.textContent = formatUsdDelta(lastPx - open);
    } else if (lastPx != null) {
      liveBtc.textContent = "—";
    } else {
      liveBtc.textContent = "—";
    }
  } else if (
    !fromArchive &&
    lastHealthJson?.btcDeltaUsd != null &&
    Number.isFinite(Number(lastHealthJson.btcDeltaUsd))
  ) {
    liveBtc.textContent = formatUsdDelta(Number(lastHealthJson.btcDeltaUsd));
  }

}

function getTickLimit() {
  return Math.min(50000, Math.max(60, Number(limitInput.value) || 1800));
}

function chartWsUrl() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws/chart`;
}

function sendChartSubscribe() {
  if (!chartWs || chartWs.readyState !== WebSocket.OPEN) return;
  const lim = getTickLimit();
  const slug =
    chartSlugOverride != null && chartSlugOverride !== "" ? chartSlugOverride : "";
  chartWs.send(JSON.stringify({ op: "subscribe", limit: lim, slug }));
}

function connectChartWs() {
  if (chartWsReconnectTimer) {
    clearTimeout(chartWsReconnectTimer);
    chartWsReconnectTimer = null;
  }
  if (chartWs) {
    try {
      chartWs.close();
    } catch {
      /* noop */
    }
    chartWs = null;
  }

  const ws = new WebSocket(chartWsUrl());
  chartWs = ws;

  ws.addEventListener("open", () => {
    chartStatusLine = "图表WSS 同步中…";
    paintConnLabel();
    sendChartSubscribe();
  });

  ws.addEventListener("message", (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    if (msg.type === "health") {
      const { type: _t, ...health } = msg;
      applyHealthPayload(health);
      return;
    }
    if (msg.type === "snapshot") {
      tickBuffer = Array.isArray(msg.ticks) ? msg.ticks.slice() : [];
      const fromArchive = chartSlugOverride != null && chartSlugOverride !== "";
      let slugForTicks =
        chartSlugOverride != null && chartSlugOverride !== ""
          ? chartSlugOverride
          : activeSlug;
      if (!fromArchive && typeof msg.slug === "string" && msg.slug) {
        slugForTicks = msg.slug;
        activeSlug = msg.slug;
        if (slugLabel) slugLabel.textContent = msg.slug;
      }
      applyRows(tickBuffer, fromArchive, slugForTicks);
      const mode = fromArchive ? "归档" : "实时";
      const slugShow = slugForTicks ?? "—";
      chartStatusLine = `${tickBuffer.length} 条 · ${mode} · ${slugShow}`;
      paintConnLabel();
    } else if (msg.type === "tick") {
      const row = msg.tick;
      if (!row || typeof row !== "object") return;
      const lim = getTickLimit();
      tickBuffer.push(row);
      while (tickBuffer.length > lim) tickBuffer.shift();
      const rowSlug = typeof row.market_slug === "string" ? row.market_slug : null;
      let slugForTicks =
        chartSlugOverride != null && chartSlugOverride !== ""
          ? chartSlugOverride
          : rowSlug ?? activeSlug;
      if (!chartSlugOverride && rowSlug) {
        activeSlug = rowSlug;
        if (slugLabel) slugLabel.textContent = rowSlug;
      }
      applyRows(tickBuffer, false, slugForTicks);
      chartStatusLine = `${tickBuffer.length} 条 · 实时 · ${slugForTicks ?? "—"}`;
      paintConnLabel();
    } else if (msg.type === "error") {
      chartStatusLine = `图表WSS: ${msg.message ?? "error"}`;
      paintConnLabel();
    }
  });

  ws.addEventListener("close", () => {
    chartWs = null;
    chartStatusLine = "图表WSS 断开，重连中…";
    paintConnLabel();
    chartWsReconnectTimer = setTimeout(connectChartWs, 2000);
  });

  ws.addEventListener("error", () => {
    try {
      ws.close();
    } catch {
      /* noop */
    }
  });
}

function clearRolloverRefreshTimer() {
  if (rolloverRefreshTimer != null) {
    clearTimeout(rolloverRefreshTimer);
    rolloverRefreshTimer = null;
  }
}

/**
 * @param {Record<string, unknown>} j - 与 /api/health 相同字段（由 WSS `{ type: "health", ... }` 解析）
 */
function applyHealthPayload(j) {
  const prevSlug = activeSlug;
  if (seriesLabel) {
    seriesLabel.textContent = j.series != null ? String(j.series) : "—";
  }
  const a = j.active && typeof j.active === "object" ? j.active : null;
  chainlinkOpenUsd = j.btcOpenUsd != null ? num(j.btcOpenUsd) : null;
  lastHealthJson = j;
  if (a && "slug" in a && a.slug) {
    const slug = String(a.slug);
    if (slug !== prevSlug) {
      clearRolloverRefreshTimer();
      rolloverScheduledForSlug = null;
    }
    if (slugLabel) slugLabel.textContent = slug;
    activeSlug = slug;
  } else {
    if (slugLabel) slugLabel.textContent = "—";
    activeSlug = null;
  }
  upstreamStatusLine =
    a && "pmWs" in a && "rtds" in a
      ? `CLOB ${a.pmWs ? "on" : "off"} · RTDS ${a.rtds ? "on" : "off"}`
      : "无活跃盘";
  paintConnLabel();
  if (
    !chartSlugOverride &&
    chartWs?.readyState === WebSocket.OPEN &&
    prevSlug != null &&
    activeSlug != null &&
    prevSlug !== activeSlug
  ) {
    sendChartSubscribe();
  }
  maybeScheduleRolloverRefresh(j);
}

/**
 * 跟随实时盘时：距当前盘结束 ≤5s 则为本 slug 排程一次 8s 后刷新（拉 market-windows + 重订阅）。
 */
function maybeScheduleRolloverRefresh(j) {
  if (chartSlugOverride) return;
  const a = j.active && typeof j.active === "object" ? j.active : null;
  const endMs = a && "endMs" in a ? num(a.endMs) : null;
  const slug = a && "slug" in a && a.slug ? String(a.slug) : null;
  if (endMs == null || slug == null) return;
  const left = endMs - Date.now();
  if (left > ROLLOVER_NEAR_END_MS || left < -120_000) return;
  if (rolloverScheduledForSlug === slug) return;
  rolloverScheduledForSlug = slug;
  clearRolloverRefreshTimer();
  rolloverRefreshTimer = setTimeout(async () => {
    rolloverRefreshTimer = null;
    rolloverScheduledForSlug = null;
    await loadMarketWindows();
    if (chartWs?.readyState === WebSocket.OPEN) sendChartSubscribe();
    else connectChartWs();
  }, ROLLOVER_REFRESH_AFTER_MS);
}

async function loadMarketWindows() {
  if (!marketSelect) return;
  const prev = chartSlugOverride ?? "";
  try {
    const res = await fetch("/api/market-windows?limit=96");
    const j = await res.json();
    const windows = Array.isArray(j.windows) ? j.windows : [];
    marketSelect.innerHTML = "";
    const o0 = document.createElement("option");
    o0.value = "";
    o0.textContent = "当前盘（跟随实时）";
    marketSelect.appendChild(o0);
    for (const w of windows) {
      const o = document.createElement("option");
      o.value = w.slug;
      o.textContent = formatWindowOption(w);
      marketSelect.appendChild(o);
    }
    const hasPrev = prev !== "" && [...marketSelect.options].some((opt) => opt.value === prev);
    if (hasPrev) {
      marketSelect.value = prev;
      chartSlugOverride = prev;
    } else {
      marketSelect.value = "";
      chartSlugOverride = null;
    }
    updateMarketNavButtons();
  } catch {
    /* keep existing select */
  }
}

async function refreshAll() {
  await loadMarketWindows();
  if (chartWs?.readyState === WebSocket.OPEN) sendChartSubscribe();
  else connectChartWs();
}

refreshBtn.addEventListener("click", () => {
  refreshAll();
});

function updateMarketNavButtons() {
  if (!marketSelect || !marketPrevBtn || !marketNextBtn) return;
  const n = marketSelect.options.length;
  const i = marketSelect.selectedIndex;
  marketPrevBtn.disabled = n <= 1 || i <= 0;
  marketNextBtn.disabled = n <= 1 || i >= n - 1;
}

function syncChartFromMarketSelect() {
  if (!marketSelect) return;
  chartSlugOverride = marketSelect.value ? marketSelect.value : null;
  updateMarketNavButtons();
  if (chartWs?.readyState === WebSocket.OPEN) sendChartSubscribe();
  else connectChartWs();
}

function stepMarketSelect(delta) {
  if (!marketSelect || marketSelect.options.length === 0) return;
  const n = marketSelect.options.length;
  let i = marketSelect.selectedIndex;
  if (i < 0) i = 0;
  const next = Math.max(0, Math.min(n - 1, i + delta));
  if (next === i) return;
  marketSelect.selectedIndex = next;
  syncChartFromMarketSelect();
}

if (marketSelect) {
  marketSelect.addEventListener("change", syncChartFromMarketSelect);
}

if (marketPrevBtn) {
  marketPrevBtn.addEventListener("click", () => stepMarketSelect(-1));
}
if (marketNextBtn) {
  marketNextBtn.addEventListener("click", () => stepMarketSelect(1));
}

limitInput.addEventListener("change", () => {
  if (chartWs?.readyState === WebSocket.OPEN) sendChartSubscribe();
});

const calcPairPrice = document.getElementById("calc-pair-price");
const calcT0 = document.getElementById("calc-t0");
const calcT1 = document.getElementById("calc-t1");
const calcSellPrice = document.getElementById("calc-sell-price");
const calcShares = document.getElementById("calc-shares");
const calcSubmit = document.getElementById("calc-submit");
const calcResult = document.getElementById("calc-result");
const calcVerdict = document.getElementById("calc-verdict");
const calcFullBatch = document.getElementById("calc-full-batch");

/**
 * @param {"profit" | "loss" | "neutral" | "warn"} kind
 * @param {string} verdictText
 * @param {string} [detail]
 */
function setCalcOutcome(kind, verdictText, detail = "") {
  if (calcVerdict) {
    calcVerdict.textContent = verdictText;
    calcVerdict.className = `pair-calculator__verdict mono pair-calculator__verdict--${kind}`;
  }
  if (calcResult) calcResult.textContent = detail;
}

function fmtUsdCalc(v) {
  return v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
}

/**
 * 全量明细用：`M/D HH:mm → HH:mm`，右侧为同一小时、分钟 = 左侧分钟 +5（不进位，故可为 :60）。
 * 起点优先 min_ts_ms，否则 slug 尾缀 unix。
 * @param {{ min_ts_ms?: unknown; max_ts_ms?: unknown }} w
 * @param {string} slug
 */
function formatBatchMarketTimeRange(w, slug) {
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

function getCalcMarketSlug() {
  return chartSlugOverride != null && chartSlugOverride !== "" ? chartSlugOverride : activeSlug;
}

/**
 * @param {unknown[]} rows
 * @param {string | null} slug
 * @returns {{ code: string, netUsd: number, leg?: string, legLabel?: string, P_entry?: number, P_exit?: number, t_entry?: number, t_exit?: number, floatLoss?: number }}
 */
function computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N) {
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

function readCalcParams() {
  const P_buyLimit = num(calcPairPrice?.value);
  let t0 = num(calcT0?.value);
  let t1 = num(calcT1?.value);
  const P_sellTarget = num(calcSellPrice?.value);
  const N = num(calcShares?.value);
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
  return { P_buyLimit, t0, t1, P_sellTarget, N };
}

/**
 * @param {ReturnType<typeof computeLegPnlFromRows>} r
 * @param {{ P_buyLimit: number, t0: number, t1: number, P_sellTarget: number, N: number }} params
 */
function applySingleCalcResult(r, params) {
  const fmtUsd = fmtUsdCalc;
  const { t0, t1, P_buyLimit, P_sellTarget, N } = params;
  if (r.code === "no_data") {
    setCalcOutcome("warn", "无数据", "请先加载图表");
    return;
  }
  if (r.code === "no_points") {
    setCalcOutcome("neutral", "无有效 mid", "");
    return;
  }
  if (r.code === "no_buy") {
    setCalcOutcome(
      "neutral",
      "未成交 · 盈亏 0 USD",
      `[${t0}–${t1}]s 内无 Up/Down mid ≤ ${P_buyLimit.toFixed(4)}`,
    );
    return;
  }
  if (r.code === "bad_entry") {
    setCalcOutcome("warn", "买入价无效", "");
    return;
  }
  if (r.code === "closed" && r.P_entry != null && r.P_exit != null && r.t_entry != null && r.t_exit != null && r.legLabel) {
    const profit = r.netUsd;
    const sign = profit >= 0 ? "+" : "";
    setCalcOutcome(
      profit >= 0 ? "profit" : "loss",
      `${profit >= 0 ? "盈利" : "亏损"} ${sign}${fmtUsd(profit)} USD`,
      `${r.legLabel} 买入 ${r.P_entry.toFixed(4)} @${r.t_entry.toFixed(1)}s → 卖出 ${r.P_exit.toFixed(4)} @${r.t_exit.toFixed(1)}s · ${N} 份`,
    );
    return;
  }
  if (r.code === "float" && r.floatLoss != null && r.P_entry != null && r.t_entry != null && r.legLabel) {
    setCalcOutcome(
      "loss",
      `浮亏 ${fmtUsd(r.floatLoss)} USD`,
      `${r.legLabel} 买入 ${r.P_entry.toFixed(4)} @${r.t_entry.toFixed(1)}s · 未达卖出 (需 ${r.legLabel} ≥ ${P_sellTarget.toFixed(4)}) · ${N} 份`,
    );
  }
}

async function runLegPairCalculator() {
  if (!calcVerdict && !calcResult) return;
  const params = readCalcParams();
  if (!params) {
    setCalcOutcome("warn", "请补全参数", "");
    return;
  }
  const { P_buyLimit, t0, t1, P_sellTarget, N } = params;

  if (!calcFullBatch?.checked) {
    const slug = getCalcMarketSlug();
    const rows = tickBuffer;
    if (!rows.length) {
      setCalcOutcome("warn", "无数据", "请先加载图表");
      return;
    }
    const r = computeLegPnlFromRows(rows, slug, P_buyLimit, t0, t1, P_sellTarget, N);
    applySingleCalcResult(r, params);
    return;
  }

  if (calcSubmit) calcSubmit.disabled = true;
  setCalcOutcome("neutral", "全量计算中…", "");
  try {
    const winRes = await fetch("/api/market-windows?limit=500");
    if (!winRes.ok) throw new Error(`market-windows ${winRes.status}`);
    const winJ = await winRes.json();
    const windows = Array.isArray(winJ.windows) ? winJ.windows : [];
    if (!windows.length) {
      setCalcOutcome("warn", "无归档市场", "");
      return;
    }
    const tickLimit = Math.min(
      50000,
      Math.max(60, Number(limitInput?.value) || 50000),
    );
    let total = 0;
    let nBuy = 0;
    let nClosed = 0;
    let nFloat = 0;
    let nSkip = 0;
    const detailLines = [];

    for (const w of windows) {
      const slug = w.slug != null ? String(w.slug) : "";
      if (!slug) continue;
      const timeRange = formatBatchMarketTimeRange(w, slug);
      try {
        const tr = await fetch(
          `/api/ticks?slug=${encodeURIComponent(slug)}&limit=${tickLimit}`,
        );
        if (!tr.ok) throw new Error(String(tr.status));
        const tj = await tr.json();
        const ticks = Array.isArray(tj.ticks) ? tj.ticks : [];
        const r = computeLegPnlFromRows(ticks, slug, P_buyLimit, t0, t1, P_sellTarget, N);
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
        const tag =
          r.code === "closed"
            ? "平仓"
            : r.code === "float"
              ? "浮亏"
              : r.code === "no_buy"
                ? "未买"
                : r.code === "no_points" || r.code === "no_data"
                  ? "无点"
                  : r.code;
        detailLines.push(`${slug} · ${timeRange} · ${tag} · ${fmtUsdCalc(r.netUsd)}`);
      } catch (e) {
        detailLines.push(
          `${slug} · ${timeRange} · 错误 ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }

    const sign = total >= 0 ? "+" : "";
    setCalcOutcome(
      total >= 0 ? "profit" : "loss",
      `累计盈亏 ${sign}${fmtUsdCalc(total)} USD`,
      `共 ${windows.length} 个市场 · 触发买入 ${nBuy} · 已平仓 ${nClosed} · 仅浮亏 ${nFloat} · 其余 ${nSkip}\n` +
        detailLines.slice(0, 80).join("\n") +
        (detailLines.length > 80 ? `\n… 余 ${detailLines.length - 80} 条` : ""),
    );
  } catch (e) {
    setCalcOutcome("warn", "全量失败", e instanceof Error ? e.message : String(e));
  } finally {
    if (calcSubmit) calcSubmit.disabled = false;
  }
}

if (calcSubmit) {
  calcSubmit.addEventListener("click", () => {
    runLegPairCalculator();
  });
}

(async () => {
  await refreshAll();
})();
