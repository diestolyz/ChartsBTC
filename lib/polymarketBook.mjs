/**
 * In-memory CLOB market book from Polymarket Market WSS (mirrors BTC5Mins browserUpstreamWs).
 */

function pmNum(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function bestBidFromLevels(bids) {
  if (!Array.isArray(bids) || bids.length === 0) return null;
  let best = null;
  for (const row of bids) {
    const p = pmNum(row?.price);
    if (p == null) continue;
    if (best == null || p > best) best = p;
  }
  return best;
}

function bestAskFromLevels(asks) {
  if (!Array.isArray(asks) || asks.length === 0) return null;
  let best = null;
  for (const row of asks) {
    const p = pmNum(row?.price);
    if (p == null) continue;
    if (best == null || p < best) best = p;
  }
  return best;
}

function pmEventType(msg) {
  return msg?.event_type ?? msg?.eventType ?? null;
}

function pmAssetIdFromRoot(msg) {
  const v = msg?.asset_id ?? msg?.assetId;
  return v != null && v !== "" ? String(v) : null;
}

function pmAssetIdFromChange(ch) {
  const v = ch?.asset_id ?? ch?.assetId;
  return v != null && v !== "" ? String(v) : null;
}

function effectiveMidFromBookRow(row) {
  if (!row || typeof row !== "object") return null;
  if (row.mid != null && Number.isFinite(Number(row.mid))) return Number(row.mid);
  const b = row.bestBid;
  const a = row.bestAsk;
  if (b != null && a != null && Number.isFinite(Number(b)) && Number.isFinite(Number(a))) {
    return (Number(b) + Number(a)) / 2;
  }
  if (row.lastTrade != null && Number.isFinite(Number(row.lastTrade))) {
    return Number(row.lastTrade);
  }
  return null;
}

export function createBookState() {
  /** @type {Record<string, { bestBid?: number|null; bestAsk?: number|null; mid?: number|null; lastTrade?: number|null }>} */
  const book = {};

  function handleOneMessage(msg) {
    if (!msg || typeof msg !== "object") return;
    if (msg.error != null || msg.status === "error") return;
    const t = pmEventType(msg);
    if (!t) return;

    if (t === "best_bid_ask") {
      const aid = pmAssetIdFromRoot(msg);
      if (!aid) return;
      const bid = msg.best_bid != null || msg.bestBid != null ? pmNum(msg.best_bid ?? msg.bestBid) : null;
      const ask = msg.best_ask != null || msg.bestAsk != null ? pmNum(msg.best_ask ?? msg.bestAsk) : null;
      let mid = null;
      if (bid != null && ask != null) mid = (bid + ask) / 2;
      book[aid] = { ...book[aid], bestBid: bid, bestAsk: ask, mid };
      return;
    }

    if (t === "book") {
      const aid = pmAssetIdFromRoot(msg);
      if (!aid) return;
      const bids = Array.isArray(msg.bids) ? msg.bids : [];
      const asks = Array.isArray(msg.asks) ? msg.asks : [];
      const bb = bestBidFromLevels(bids);
      const ba = bestAskFromLevels(asks);
      let mid = null;
      if (bb != null && ba != null) mid = (bb + ba) / 2;
      book[aid] = { ...book[aid], bestBid: bb, bestAsk: ba, mid };
      return;
    }

    const priceChanges = msg.price_changes ?? msg.priceChanges;
    if (t === "price_change" && Array.isArray(priceChanges)) {
      for (const ch of priceChanges) {
        const aid = pmAssetIdFromChange(ch);
        if (!aid) continue;
        const bbRaw = ch.best_bid ?? ch.bestBid;
        const baRaw = ch.best_ask ?? ch.bestAsk;
        const bid = bbRaw != null ? pmNum(bbRaw) : book[aid]?.bestBid;
        const ask = baRaw != null ? pmNum(baRaw) : book[aid]?.bestAsk;
        let mid = book[aid]?.mid ?? null;
        if (bid != null && ask != null) mid = (bid + ask) / 2;
        book[aid] = {
          ...book[aid],
          bestBid: bid ?? book[aid]?.bestBid,
          bestAsk: ask ?? book[aid]?.bestAsk,
          mid,
        };
      }
      return;
    }

    if (t === "last_trade_price") {
      const aid = pmAssetIdFromRoot(msg);
      if (!aid) return;
      const price = msg.price != null ? pmNum(msg.price) : null;
      book[aid] = { ...book[aid], lastTrade: price };
    }
  }

  function ingestParsed(parsed) {
    const parts = Array.isArray(parsed) ? parsed : [parsed];
    for (const p of parts) {
      if (p && typeof p === "object") handleOneMessage(p);
    }
  }

  function oddsFromIds(upId, downId) {
    const up = (upId != null ? book[String(upId)] : null) || {};
    const down = (downId != null ? book[String(downId)] : null) || {};
    return {
      up: {
        bestBid: up.bestBid ?? null,
        bestAsk: up.bestAsk ?? null,
        mid: effectiveMidFromBookRow(up),
        lastTrade: up.lastTrade ?? null,
      },
      down: {
        bestBid: down.bestBid ?? null,
        bestAsk: down.bestAsk ?? null,
        mid: effectiveMidFromBookRow(down),
        lastTrade: down.lastTrade ?? null,
      },
    };
  }

  function reset() {
    for (const k of Object.keys(book)) delete book[k];
  }

  return { ingestParsed, oddsFromIds, reset };
}
