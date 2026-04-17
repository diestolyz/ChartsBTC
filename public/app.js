/**
 * 通过 WSS /ws/chart 订阅图表数据（快照 + 实时增量 + health 推送），用 Chart.js 绘制 Up mid / Down mid（左轴）与 Chainlink 差价（右轴）。
 * 仅 market-windows 与刷新用 HTTP；支持按市场 slug 切换归档时间段。
 */

import {
  WINDOW_SEC,
  num,
  secondsFromWindowOpen,
  computeLegPnlFromRows,
} from "./legPairPnl.mjs";

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

/** 「计算全量」明细列表最多展示行数（超出部分在文案末尾提示条数） */
const CALC_BATCH_DETAIL_MAX_LINES = 400;

const AUTH_STATUS_URL = "/api/auth/status";

async function fetchAuthStatus() {
  try {
    const r = await fetch(AUTH_STATUS_URL, { credentials: "same-origin" });
    if (!r.ok) return { authEnabled: false, authenticated: true };
    const j = await r.json();
    return {
      authEnabled: Boolean(j.authEnabled),
      authenticated: Boolean(j.authenticated),
    };
  } catch {
    return { authEnabled: false, authenticated: true };
  }
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

/** 与页头「时间跨度」一致：归档市场个数（5m 盘）、`/api/market-windows` 与「计算全量」的 windowsLimit。 */
function getArchivedMarketsLimit() {
  return Math.min(20000, Math.max(1, Math.floor(Number(limitInput?.value) || 400)));
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
    const res = await fetch(`/api/market-windows?limit=${getArchivedMarketsLimit()}`, {
      credentials: "same-origin",
    });
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

async function loadUiSettingsIntoForm() {
  if (!limitInput) return;
  try {
    const r = await fetch("/api/ui-settings", { credentials: "same-origin" });
    if (!r.ok) return;
    const j = await r.json();
    const v = Number(j.timeSpanSeconds);
    if (!Number.isFinite(v)) return;
    limitInput.value = String(Math.min(20000, Math.max(60, Math.floor(v))));
  } catch {
    /* noop */
  }
}

async function saveUiTimeSpanToServer() {
  if (!limitInput) return;
  const timeSpanSeconds = Math.min(20000, Math.max(60, Math.floor(Number(limitInput.value) || 400)));
  const r = await fetch("/api/ui-settings", {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ timeSpanSeconds }),
  });
  if (!r.ok) {
    const j = await r.json().catch(() => ({}));
    throw new Error(typeof j.error === "string" ? j.error : `ui-settings ${r.status}`);
  }
}

/**
 * @param {{ saveTimeSpan?: boolean }} [options] 仅用户点「刷新」时传 `saveTimeSpan: true`，把当前时间跨度写入服务端
 */
async function refreshAll(options = {}) {
  if (options.saveTimeSpan === true) {
    try {
      await saveUiTimeSpanToServer();
    } catch (e) {
      console.warn("[charts] 保存时间跨度失败", e);
    }
  }
  await loadMarketWindows();
  if (chartWs?.readyState === WebSocket.OPEN) sendChartSubscribe();
  else connectChartWs();
}

refreshBtn.addEventListener("click", () => {
  void refreshAll({ saveTimeSpan: true });
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
  void loadMarketWindows();
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
const calcBatchCanvas = document.getElementById("calc-batch-chart");
const calcBatchSummary = document.getElementById("calc-batch-summary");
const calcPresetFilter = document.getElementById("calc-preset-filter");
const calcPresetSelect = document.getElementById("calc-preset-select");
const calcPresetName = document.getElementById("calc-preset-name");
const calcPresetSave = document.getElementById("calc-preset-save");
const calcPresetDelete = document.getElementById("calc-preset-delete");
const calcPresetMsg = document.getElementById("calc-preset-msg");

function setCalcBatchSummary(text) {
  if (!calcBatchSummary) return;
  const t = text != null ? String(text).trim() : "";
  if (!t) {
    calcBatchSummary.textContent = "";
    calcBatchSummary.hidden = true;
    return;
  }
  calcBatchSummary.textContent = t;
  calcBatchSummary.hidden = false;
}

/** @type {any} */
let calcBatchChart = null;

/** 全量盈亏图：X 轴与 tooltip 用本地 24 小时制 */
function formatBatchChartDateTime(ms, withSeconds) {
  const n = Number(ms);
  if (!Number.isFinite(n)) return "";
  return new Date(n).toLocaleString("zh-CN", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    ...(withSeconds ? { second: "2-digit" } : {}),
    hour12: false,
  });
}

function destroyCalcBatchChart() {
  if (calcBatchChart) {
    try {
      calcBatchChart.destroy();
    } catch {
      /* noop */
    }
    calcBatchChart = null;
  }
}

/**
 * 全量明细图：仅含已触发买入的盘——平仓（closed）与浮亏（float）；未买/无点等不绘制。
 * X = 窗口起点时间（ms），Y = 该盘 netUsd。
 * @param {unknown[]} details
 */
function updateCalcBatchChart(details) {
  if (!calcBatchCanvas || typeof Chart === "undefined") return;
  destroyCalcBatchChart();
  const rows = Array.isArray(details)
    ? details.filter((d) => {
        if (!d || typeof d !== "object") return false;
        const o = /** @type {{ startMs?: unknown; netUsd?: unknown; code?: unknown }} */ (d);
        const code = typeof o.code === "string" ? o.code : "";
        if (code !== "closed" && code !== "float") return false;
        return (
          typeof o.startMs === "number" &&
          Number.isFinite(o.startMs) &&
          typeof o.netUsd === "number" &&
          Number.isFinite(o.netUsd)
        );
      })
    : [];
  if (!rows.length) return;

  const sorted = /** @type {{ startMs: number; netUsd: number; timeRange?: string; tag?: string }[]} */ (
    [...rows].sort((a, b) => a.startMs - b.startMs)
  );
  const data = sorted.map((d) => ({ x: d.startMs, y: d.netUsd }));
  const colors = sorted.map((d) =>
    d.netUsd >= 0 ? "rgba(74, 222, 128, 0.85)" : "rgba(248, 113, 113, 0.85)",
  );

  calcBatchChart = new Chart(calcBatchCanvas, {
    type: "scatter",
    data: {
      datasets: [
        {
          label: "各盘盈亏 (USD)",
          data,
          parsing: false,
          pointRadius: 4,
          pointHoverRadius: 6,
          pointBackgroundColor: colors,
          pointBorderColor: "rgba(255,255,255,0.25)",
          pointBorderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: "#c4c9d4" },
        },
        tooltip: {
          callbacks: {
            title(items) {
              const x = items[0]?.parsed?.x;
              return formatBatchChartDateTime(x, true);
            },
            label(ctx) {
              const i = ctx.dataIndex;
              const d = sorted[i];
              const tr = d?.timeRange != null ? String(d.timeRange) : "";
              const tag = d?.tag != null ? String(d.tag) : "";
              const y = ctx.parsed.y;
              const usd =
                typeof y === "number" && Number.isFinite(y)
                  ? y.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 4 })
                  : String(y);
              const bits = [`盈亏 ${usd} USD`];
              if (tag) bits.push(tag);
              if (tr) bits.push(tr);
              return bits.join(" · ");
            },
          },
        },
      },
      scales: {
        x: {
          type: "linear",
          title: {
            display: true,
            text: "市场窗口起点（本地时间 · 24 小时制）",
            color: "#9ca3af",
          },
          grid: { color: "rgba(255,255,255,0.06)" },
          ticks: {
            color: "#8a8f98",
            maxRotation: 45,
            autoSkip: true,
            maxTicksLimit: 12,
            callback(v) {
              return formatBatchChartDateTime(v, false) || String(v);
            },
          },
        },
        y: {
          title: { display: true, text: "盈亏 (USD)", color: "#9ca3af" },
          grid: { color: "rgba(255,255,255,0.06)" },
          ticks: { color: "#8a8f98" },
        },
      },
    },
  });
}

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

function getCalcMarketSlug() {
  return chartSlugOverride != null && chartSlugOverride !== "" ? chartSlugOverride : activeSlug;
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

/** @type {{ id: string; name: string; updatedAt: string; params: Record<string, unknown> }[]} */
let calcPresetsCache = [];
let calcPresetSelectSilent = false;

/**
 * @param {string} text
 * @param {"neutral" | "warn" | "ok"} kind
 */
function setCalcPresetMsg(text, kind = "neutral") {
  if (!calcPresetMsg) return;
  calcPresetMsg.textContent = text || "";
  const extra =
    kind === "warn"
      ? " pair-calculator__preset-msg--warn"
      : kind === "ok"
        ? " pair-calculator__preset-msg--ok"
        : "";
  calcPresetMsg.className = `pair-calculator__preset-msg muted${extra}`;
}

function collectCalcParamsForSave() {
  const p = readCalcParams();
  if (!p) return null;
  return { ...p, fullBatch: Boolean(calcFullBatch?.checked) };
}

/**
 * @param {unknown} params
 */
function applyCalcPresetParams(params) {
  if (!params || typeof params !== "object") return;
  const p = /** @type {Record<string, unknown>} */ (params);
  if (calcPairPrice && p.P_buyLimit != null) calcPairPrice.value = String(p.P_buyLimit);
  if (calcT0 && p.t0 != null) calcT0.value = String(p.t0);
  if (calcT1 && p.t1 != null) calcT1.value = String(p.t1);
  if (calcSellPrice && p.P_sellTarget != null) calcSellPrice.value = String(p.P_sellTarget);
  if (calcShares && p.N != null) calcShares.value = String(p.N);
  if (calcFullBatch) {
    calcFullBatch.checked =
      p.fullBatch === true || p.fullBatch === 1 || p.fullBatch === "1" || p.fullBatch === "true";
  }
}

/**
 * @param {string} [filterRaw]
 */
function rebuildCalcPresetOptions(filterRaw) {
  if (!calcPresetSelect) return;
  const q = (filterRaw != null ? String(filterRaw) : "").trim().toLowerCase();
  const prev = calcPresetSelect.value;
  calcPresetSelectSilent = true;
  calcPresetSelect.innerHTML = "";
  const o0 = document.createElement("option");
  o0.value = "";
  o0.textContent = "— 选择方案 —";
  calcPresetSelect.appendChild(o0);
  const list = q.length
    ? calcPresetsCache.filter((x) => x.name.toLowerCase().includes(q))
    : calcPresetsCache;
  for (const pr of list) {
    const o = document.createElement("option");
    o.value = pr.id;
    const sub = pr.updatedAt ? ` · ${pr.updatedAt.slice(0, 10)}` : "";
    o.textContent = `${pr.name}${sub}`;
    const pp = pr.params;
    const pb = pp.P_buyLimit != null ? String(pp.P_buyLimit) : "—";
    const t0s = pp.t0 != null ? String(pp.t0) : "—";
    const t1s = pp.t1 != null ? String(pp.t1) : "—";
    o.title = `${pr.name} — 买入限价 ${pb} · [${t0s},${t1s}]s`;
    calcPresetSelect.appendChild(o);
  }
  const still = prev && [...calcPresetSelect.options].some((opt) => opt.value === prev);
  calcPresetSelect.value = still ? prev : "";
  calcPresetSelectSilent = false;
}

async function fetchCalcPresets() {
  const res = await fetch("/api/calc-presets", {
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || j.ok === false) {
    throw new Error(typeof j.error === "string" ? j.error : `calc-presets ${res.status}`);
  }
  const presets = Array.isArray(j.presets) ? j.presets : [];
  calcPresetsCache = presets
    .filter((x) => x && typeof x === "object" && typeof /** @type {{ id?: unknown }} */ (x).id === "string")
    .map((x) => {
      const o = /** @type {{ id: string; name?: unknown; updatedAt?: unknown; params?: unknown }} */ (x);
      return {
        id: o.id,
        name: typeof o.name === "string" ? o.name : "",
        updatedAt: typeof o.updatedAt === "string" ? o.updatedAt : "",
        params: o.params && typeof o.params === "object" ? /** @type {Record<string, unknown>} */ (o.params) : {},
      };
    })
    .filter((x) => x.name.length > 0);
  rebuildCalcPresetOptions(calcPresetFilter?.value ?? "");
}

async function saveCalcPreset() {
  const params = collectCalcParamsForSave();
  if (!params) {
    setCalcPresetMsg("请补全测算参数后再保存", "warn");
    return;
  }
  const name = (calcPresetName?.value ?? "").trim();
  if (!name) {
    setCalcPresetMsg("请填写方案名称", "warn");
    return;
  }
  if (calcPresetSave) calcPresetSave.disabled = true;
  setCalcPresetMsg("保存中…", "neutral");
  try {
    const res = await fetch("/api/calc-presets", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ name, params }),
    });
    const j = await res.json().catch(() => ({}));
    if (!res.ok || j.ok === false) {
      const err = typeof j.error === "string" ? j.error : `HTTP ${res.status}`;
      if (err === "too_many_presets") {
        throw new Error("已达保存上限（200 条），请先删除旧方案");
      }
      throw new Error(err);
    }
    await fetchCalcPresets();
    const preset = j.preset && typeof j.preset === "object" ? j.preset : null;
    const id =
      preset && "id" in preset && typeof /** @type {{ id?: unknown }} */ (preset).id === "string"
        ? /** @type {{ id: string }} */ (preset).id
        : null;
    if (id && calcPresetSelect) {
      calcPresetSelectSilent = true;
      calcPresetSelect.value = id;
      calcPresetSelectSilent = false;
    }
    setCalcPresetMsg("已保存", "ok");
  } catch (e) {
    setCalcPresetMsg(e instanceof Error ? e.message : String(e), "warn");
  } finally {
    if (calcPresetSave) calcPresetSave.disabled = false;
  }
}

async function deleteCalcPreset() {
  const id = calcPresetSelect?.value ?? "";
  if (!id) {
    setCalcPresetMsg("请先在下拉框中选择要删除的方案", "warn");
    return;
  }
  if (calcPresetDelete) calcPresetDelete.disabled = true;
  setCalcPresetMsg("删除中…", "neutral");
  try {
    const res = await fetch(`/api/calc-presets/${encodeURIComponent(id)}`, {
      method: "DELETE",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
    const j = await res.json().catch(() => ({}));
    if (!res.ok || j.ok === false) {
      throw new Error(typeof j.error === "string" ? j.error : `HTTP ${res.status}`);
    }
    await fetchCalcPresets();
    if (calcPresetName) calcPresetName.value = "";
    setCalcPresetMsg("已删除", "ok");
  } catch (e) {
    setCalcPresetMsg(e instanceof Error ? e.message : String(e), "warn");
  } finally {
    if (calcPresetDelete) calcPresetDelete.disabled = false;
  }
}

function onCalcPresetSelectChange() {
  if (calcPresetSelectSilent || !calcPresetSelect) return;
  const selId = calcPresetSelect.value;
  if (!selId) {
    setCalcPresetMsg("", "neutral");
    return;
  }
  const pr = calcPresetsCache.find((x) => x.id === selId);
  if (!pr) {
    setCalcPresetMsg("未找到该方案", "warn");
    return;
  }
  applyCalcPresetParams(pr.params);
  if (calcPresetName) calcPresetName.value = pr.name;
  setCalcPresetMsg(`已加载「${pr.name}」`, "ok");
}

if (calcPresetFilter) {
  calcPresetFilter.addEventListener("input", () => {
    rebuildCalcPresetOptions(calcPresetFilter.value);
  });
}
if (calcPresetSelect) {
  calcPresetSelect.addEventListener("change", onCalcPresetSelectChange);
}
if (calcPresetSave) {
  calcPresetSave.addEventListener("click", () => {
    void saveCalcPreset();
  });
}
if (calcPresetDelete) {
  calcPresetDelete.addEventListener("click", () => {
    void deleteCalcPreset();
  });
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
    destroyCalcBatchChart();
    setCalcBatchSummary("");
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
  destroyCalcBatchChart();
  setCalcBatchSummary("");
  setCalcOutcome("neutral", "全量计算中…", "");
  try {
    const tickLimit = Math.min(
      50000,
      Math.max(60, Number(limitInput?.value) || 50000),
    );
    const windowsLimit = getArchivedMarketsLimit();
    const batchRes = await fetch("/api/calc-batch", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({
        P_buyLimit,
        t0,
        t1,
        P_sellTarget,
        N,
        windowsLimit,
        tickLimit,
      }),
    });
    const batchJ = await batchRes.json().catch(() => ({}));
    if (!batchRes.ok || batchJ.ok === false) {
      throw new Error(
        typeof batchJ.error === "string" ? batchJ.error : `calc-batch ${batchRes.status}`,
      );
    }
    const total = Number(batchJ.total);
    const nBuy = Number(batchJ.nBuy) || 0;
    const nClosed = Number(batchJ.nClosed) || 0;
    const nFloat = Number(batchJ.nFloat) || 0;
    const nSkip = Number(batchJ.nSkip) || 0;
    const marketCount = Number(batchJ.marketCount) || 0;
    const details = Array.isArray(batchJ.details) ? batchJ.details : [];
    if (!marketCount) {
      setCalcBatchSummary("");
      setCalcOutcome("warn", "无归档市场", "");
      return;
    }
    const detailLines = details.map((d) => {
      const row = d && typeof d === "object" ? /** @type {Record<string, unknown>} */ (d) : {};
      const timeRange = row.timeRange != null ? String(row.timeRange) : "—";
      const tag = row.tag != null ? String(row.tag) : String(row.code ?? "");
      const nu = Number(row.netUsd);
      const usd = Number.isFinite(nu) ? nu : 0;
      const errS =
        row.code === "error" && row.error != null ? ` (${String(row.error)})` : "";
      return `${timeRange} · ${tag} · ${fmtUsdCalc(usd)}${errS}`;
    });
    const sign = total >= 0 ? "+" : "";
    const detailShown = detailLines.slice(0, CALC_BATCH_DETAIL_MAX_LINES);
    const detailOverflow =
      detailLines.length > CALC_BATCH_DETAIL_MAX_LINES
        ? `\n… 余 ${detailLines.length - CALC_BATCH_DETAIL_MAX_LINES} 条未列出`
        : "";
    const summaryLine = `共 ${marketCount} 个市场 · 触发买入 ${nBuy} · 已平仓 ${nClosed} · 仅浮亏 ${nFloat} · 其余 ${nSkip}`;
    setCalcBatchSummary(summaryLine);
    setCalcOutcome(
      total >= 0 ? "profit" : "loss",
      `累计盈亏 ${sign}${fmtUsdCalc(total)} USD`,
      detailShown.join("\n") + detailOverflow,
    );
    updateCalcBatchChart(details);
  } catch (e) {
    destroyCalcBatchChart();
    setCalcBatchSummary("");
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

const logoutBtn = document.getElementById("logout-btn");
logoutBtn?.addEventListener("click", async () => {
  try {
    await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
  } catch {
    /* noop */
  }
  location.href = "/login.html";
});

async function bootstrap() {
  const auth = await fetchAuthStatus();
  if (auth.authEnabled && !auth.authenticated) {
    const dest = `${location.pathname}${location.search || ""}` || "/";
    location.href = `/login.html?next=${encodeURIComponent(dest)}`;
    return;
  }
  if (logoutBtn) logoutBtn.hidden = !auth.authEnabled;
  await loadUiSettingsIntoForm();
  await refreshAll();
  try {
    await fetchCalcPresets();
  } catch (e) {
    setCalcPresetMsg(e instanceof Error ? e.message : String(e), "warn");
  }
}

void bootstrap();
