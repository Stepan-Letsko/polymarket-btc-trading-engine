import asyncio
import json
import websockets
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# MESSAGE FILTER
# ─────────────────────────────────────────────
# Set SHOW_MESSAGE to the number of the message type you want to see.
# Set to 0 to show ALL message types.
#
# 0 = ALL messages
# 1 = TICKER        (best bid/ask + last price, fires on every trade)
# 2 = MARKET TRADES (every individual trade execution)
# 3 = LEVEL2        (orderbook updates)

SHOW_MESSAGE = 0


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

# Coinbase Advanced Trade WebSocket — no auth needed for market data
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"

# Coinbase uses "BTC-USD" format
SYMBOL = "BTC-USD"


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGES
# ─────────────────────────────────────────────

def build_ticker_subscription():
    """
    Subscribe to ticker channel.
    Fires on every trade event with best bid, best ask, last price.
    This is the fastest top-of-book signal from Coinbase.

    Note: Coinbase also has ticker_batch which only fires every 5 seconds
    — we use ticker (no batch) for maximum speed.
    """
    return json.dumps({
        "type":        "subscribe",
        "product_ids": [SYMBOL],
        "channel":     "ticker",
    })


def build_market_trades_subscription():
    """
    Subscribe to market_trades channel.
    Fires on every individual trade execution.

    Key fields per trade:
      price     — execution price
      size      — quantity traded (in BTC)
      side      — "BUY" or "SELL" (aggressor side)
      time      — when the trade happened on Coinbase's matching engine
    """
    return json.dumps({
        "type":        "subscribe",
        "product_ids": [SYMBOL],
        "channel":     "market_trades",
    })


def build_level2_subscription():
    """
    Subscribe to level2 channel.
    Sends a full snapshot on connect then delta updates.
    Guaranteed delivery — designed to keep an accurate local order book.

    Each update contains:
      side    — "bid" or "offer"
      price   — the price level that changed
      new_qty — new quantity at that level (0 means level removed)
    """
    return json.dumps({
        "type":        "subscribe",
        "product_ids": [SYMBOL],
        "channel":     "level2",
    })


# ─────────────────────────────────────────────
# LOCAL ORDER BOOK STATE (level2)
# ─────────────────────────────────────────────
# Coinbase level2 sends a full snapshot on connect then delta updates.
# We maintain the full book here so we can always serve the current top-5.
# Keys are price-level strings (e.g. "94123.45"), values are float quantities.

_l2_bids: dict = {}   # price_str → qty (bid side)
_l2_asks: dict = {}   # price_str → qty (ask side)


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def parse_message(raw):
    """
    Parse a raw Coinbase Advanced Trade WebSocket message.

    Coinbase message format:
    {
        "channel":    "ticker" | "market_trades" | "level2" | "subscriptions",
        "client_id":  "",
        "timestamp":  "2026-05-04T00:00:00.000000Z",  ← when Coinbase sent it
        "sequence_num": 0,
        "events": [ ... array of event objects ... ]
    }

    The outer timestamp is when Coinbase's server sent the message.
    Individual events inside may have their own timestamps.

    Sequence numbers allow you to detect dropped messages.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    now      = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts  = datetime.now(timezone.utc).timestamp() * 1000
    channel  = data.get("channel", "")
    events   = data.get("events", [])

    # Parse Coinbase's ISO timestamp to ms for latency calculation
    cb_ts_str = data.get("timestamp")
    cb_ts     = None
    latency_ms = None
    if cb_ts_str:
        try:
            from datetime import datetime as dt
            cb_dt      = dt.fromisoformat(cb_ts_str.replace("Z", "+00:00"))
            cb_ts      = cb_dt.timestamp() * 1000
            latency_ms = round(recv_ts - cb_ts, 2)
        except Exception:
            pass

    # ── Subscriptions confirmation ─────────────────────────────────
    if channel == "subscriptions":
        return {
            "time":    now,
            "channel": "control",
            "raw":     data,
        }

    # ── Heartbeat ──────────────────────────────────────────────────
    if channel == "heartbeats":
        return {
            "time":    now,
            "channel": "heartbeat",
        }

    # ── Ticker ─────────────────────────────────────────────────────
    if channel == "ticker":
        if not events:
            return None
        tickers = events[0].get("tickers", [])
        if not tickers:
            return None
        tick = tickers[0]

        bid  = float(tick.get("best_bid", 0)) if tick.get("best_bid") else None
        ask  = float(tick.get("best_ask", 0)) if tick.get("best_ask") else None
        mid  = round((bid + ask) / 2, 2) if bid and ask else None
        last = float(tick.get("price", 0)) if tick.get("price") else None

        return {
            "time":         now,
            "recv_ts":      recv_ts,
            "channel":      "ticker",
            "type":         events[0].get("type"),   # "snapshot" or "update"
            "symbol":       tick.get("product_id"),
            "best_bid":     bid,
            "best_bid_qty": float(tick.get("best_bid_quantity", 0)) if tick.get("best_bid_quantity") else None,
            "best_ask":     ask,
            "best_ask_qty": float(tick.get("best_ask_quantity", 0)) if tick.get("best_ask_quantity") else None,
            "mid_price":    mid,
            "last_price":   last,
            "volume_24h":   tick.get("volume_24_h"),
            "cb_ts":        cb_ts,
            "latency_ms":   latency_ms,
            "sequence_num": data.get("sequence_num"),
        }

    # ── Market Trades ──────────────────────────────────────────────
    if channel == "market_trades":
        if not events:
            return None

        all_trades = []
        for event in events:
            for t in event.get("trades", []):
                trade_ts_str = t.get("time")
                trade_ts     = None
                trade_lat    = None
                if trade_ts_str:
                    try:
                        from datetime import datetime as dt
                        trade_dt  = dt.fromisoformat(trade_ts_str.replace("Z", "+00:00"))
                        trade_ts  = trade_dt.timestamp() * 1000
                        trade_lat = round(recv_ts - trade_ts, 2)
                    except Exception:
                        pass

                all_trades.append({
                    "trade_id":   t.get("trade_id"),
                    "price":      float(t.get("price", 0)),
                    "size":       float(t.get("size", 0)),
                    "side":       t.get("side"),       # "BUY" or "SELL"
                    "trade_ts":   trade_ts,
                    "latency_ms": trade_lat,
                })

        return {
            "time":         now,
            "recv_ts":      recv_ts,
            "channel":      "market_trades",
            "type":         events[0].get("type") if events else None,
            "symbol":       SYMBOL,
            "trades":       all_trades,
            "cb_ts":        cb_ts,
            "latency_ms":   latency_ms,
            "sequence_num": data.get("sequence_num"),
        }

    # ── Level2 Orderbook ───────────────────────────────────────────
    if channel == "level2":
        if not events:
            return None

        event    = events[0]
        msg_type = event.get("type")   # "snapshot" or "update"
        updates  = event.get("updates", [])

        # Snapshot: reset the full local book before applying levels
        if msg_type == "snapshot":
            _l2_bids.clear()
            _l2_asks.clear()

        # Apply every update (or all snapshot levels) to the local book.
        # new_quantity == 0 means the level was removed.
        for u in updates:
            side  = u.get("side")
            price = u.get("price_level")
            qty   = float(u.get("new_quantity", 0))
            if side == "bid":
                if qty == 0.0:
                    _l2_bids.pop(price, None)
                else:
                    _l2_bids[price] = qty
            elif side == "offer":
                if qty == 0.0:
                    _l2_asks.pop(price, None)
                else:
                    _l2_asks[price] = qty

        # Derive top-5 from the full maintained book.
        # Format: (price_str, qty_float) — same indexing as Binance depth5.
        bids_top5 = sorted(_l2_bids.items(), key=lambda x: float(x[0]), reverse=True)[:5]
        asks_top5 = sorted(_l2_asks.items(), key=lambda x: float(x[0]))[:5]

        best_bid = float(bids_top5[0][0]) if bids_top5 else None
        best_ask = float(asks_top5[0][0]) if asks_top5 else None
        mid      = round((best_bid + best_ask) / 2, 2) if best_bid and best_ask else None

        return {
            "time":          now,
            "recv_ts":       recv_ts,
            "channel":       "level2",
            "type":          msg_type,
            "symbol":        SYMBOL,
            "bids":          bids_top5,   # [(price_str, qty_float), ...] top-5 from full book
            "asks":          asks_top5,
            "best_bid":      best_bid,
            "best_ask":      best_ask,
            "mid_price":     mid,
            "updates_count": len(updates),
            "cb_ts":         cb_ts,
            "latency_ms":    latency_ms,
            "sequence_num":  data.get("sequence_num"),
        }

    # ── Unknown ────────────────────────────────────────────────────
    return {
        "time":    now,
        "channel": f"unknown ({channel})",
        "raw":     data,
    }


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    if not msg:
        return

    t  = msg["time"]
    ch = msg["channel"]

    TYPE_MAP = {
        1: "ticker",
        2: "market_trades",
        3: "level2",
    }

    if SHOW_MESSAGE != 0 and ch != TYPE_MAP.get(SHOW_MESSAGE):
        return

    # Suppress heartbeats and control messages silently
    if ch in ("heartbeat", "control"):
        return

    if ch == "ticker":
        lat = f"  latency: {msg['latency_ms']}ms" if msg.get("latency_ms") is not None else ""
        print(f"[{t}] ⚡ COINBASE TICKER  "
              f"bid: {msg['best_bid']} ({msg['best_bid_qty']})  "
              f"ask: {msg['best_ask']} ({msg['best_ask_qty']})  "
              f"mid: {msg['mid_price']}  "
              f"last: {msg['last_price']}"
              f"{lat}")

    elif ch == "market_trades":
        for trade in msg["trades"]:
            icon = "🟢" if trade["side"] == "BUY" else "🔴"
            lat  = f"  latency: {trade['latency_ms']}ms" if trade.get("latency_ms") is not None else ""
            print(f"[{t}] {icon} COINBASE TRADE  "
                  f"price: {trade['price']}  "
                  f"size: {trade['size']} BTC  "
                  f"side: {trade['side']}"
                  f"{lat}")

    elif ch == "level2":
        snap = "SNAPSHOT" if msg["type"] == "snapshot" else "UPDATE"
        print(f"[{t}] 📖 COINBASE L2 {snap}  "
              f"best_bid: {msg['best_bid']}  "
              f"best_ask: {msg['best_ask']}  "
              f"mid: {msg['mid_price']}  "
              f"changes: {msg['updates_count']}")
        if msg["type"] == "snapshot":
            print(f"   bids (top 5): {msg['bids']}")
            print(f"   asks (top 5): {msg['asks']}")

    else:
        print(f"[{t}] ❓ UNKNOWN — {msg.get('raw', msg)}")


# ─────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────

async def heartbeat(ws):
    """
    Coinbase requires a subscribe message to be sent within 5 seconds
    of connecting or you are disconnected. After that, Coinbase manages
    the connection keep-alive automatically — but we send periodic
    pings every 30 seconds as an extra safety measure.
    """
    while True:
        await asyncio.sleep(30)
        try:
            await ws.ping()
        except Exception:
            break


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_coinbase(on_message=None):
    """
    Connect to Coinbase Advanced Trade WebSocket and stream:
      - ticker:        best bid/ask + last price on every trade
      - market_trades: every individual trade execution
      - level2:        full orderbook snapshot then delta updates

    No authentication required for market data channels.

    Parameters:
        on_message - optional callback, receives each parsed message dict.
                     If None, prints to terminal.

    Runs forever until interrupted (Ctrl+C).
    """
    # Clear stale local book from any previous connection
    _l2_bids.clear()
    _l2_asks.clear()

    print(f"\nConnecting to Coinbase Advanced Trade WebSocket...")
    print(f"URL:     {COINBASE_WS_URL}")
    print(f"Symbol:  {SYMBOL}")
    print(f"Streams: ticker | market_trades | level2\n")

    async with websockets.connect(COINBASE_WS_URL, max_size=10 * 1024 * 1024) as ws:

        # Must subscribe within 5 seconds or connection is dropped.
        # level2 sends a full book snapshot immediately on subscribe,
        # then delta updates on every change — we maintain the book locally.
        await ws.send(build_ticker_subscription())
        await ws.send(build_market_trades_subscription())
        await ws.send(build_level2_subscription())

        print("Subscribed. Streaming...\n")

        hb_task = asyncio.create_task(heartbeat(ws))

        try:
            async for raw in ws:
                msg = parse_message(raw)
                if not msg:
                    continue

                if on_message:
                    on_message(msg)
                else:
                    print_message(msg)
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass


# ─────────────────────────────────────────────
# RUN DIRECTLY
# ─────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(stream_coinbase())
    except KeyboardInterrupt:
        print("\nStopped.")