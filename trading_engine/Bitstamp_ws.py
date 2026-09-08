import asyncio
import json
from datetime import datetime, timezone

import websockets

# ─────────────────────────────────────────────
# MESSAGE FILTER
# ─────────────────────────────────────────────
# Set SHOW_MESSAGE to the number of the message type you want to see.
# Set to 0 to show ALL message types.
#
# 0 = ALL messages
# 1 = BOOK TOP    (best bid/ask from order_book)
# 2 = LIVE TRADES (every individual trade execution)
# 3 = ORDER BOOK  (top 10 orderbook levels)

SHOW_MESSAGE = 1


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

BITSTAMP_WS_URL = "wss://ws.bitstamp.net"

# Bitstamp uses lowercase pair names in channel ids.
SYMBOL = "btcusd"


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGES
# ─────────────────────────────────────────────

def build_subscription(channel):
    return json.dumps({
        "event": "bts:subscribe",
        "data": {
            "channel": channel,
        },
    })


def build_trade_subscription():
    return build_subscription(f"live_trades_{SYMBOL}")


def build_order_book_subscription():
    return build_subscription(f"order_book_{SYMBOL}")


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def safe_float(value, fallback=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _latency_ms(recv_ts, exchange_ts):
    if exchange_ts is None:
        return None
    return round(recv_ts - exchange_ts, 2)


def parse_message(raw):
    """
    Parse a raw Bitstamp WebSocket message.

    Bitstamp messages look like:
      {
        "event": "data" | "trade" | "bts:subscription_succeeded",
        "channel": "live_ticker_btcusd" | "live_trades_btcusd" | "order_book_btcusd",
        "data": { ... }
      }

    This module normalizes them to the same broad shape as the other exchange
    websocket files in this repo: ticker, trade, book, heartbeat/control.
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(msg, dict):
        return None

    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts = datetime.now(timezone.utc).timestamp() * 1000
    event = msg.get("event", "")
    channel = msg.get("channel", "")
    data = msg.get("data", {})

    if event == "bts:subscription_succeeded":
        return {
            "time": now,
            "recv_ts": recv_ts,
            "channel": "control",
            "raw": msg,
        }

    if event == "bts:error":
        return {
            "time": now,
            "recv_ts": recv_ts,
            "channel": "error",
            "raw": msg,
        }

    if not isinstance(data, dict):
        return None

    exchange_ts = safe_float(data.get("timestamp"))
    exchange_ts_ms = exchange_ts * 1000 if exchange_ts is not None else None

    # ── Trades ─────────────────────────────────────────────────────
    if channel == f"live_trades_{SYMBOL}":
        price = safe_float(data.get("price"))
        size = safe_float(data.get("amount"), 0.0)
        trade_type = data.get("type")
        side = "BUY" if trade_type == 0 else "SELL" if trade_type == 1 else None

        return {
            "time": now,
            "recv_ts": recv_ts,
            "channel": "trade",
            "type": event,
            "symbol": "BTC/USD",
            "trade_id": data.get("id"),
            "price": price,
            "size": size,
            "side": side,
            "exchange_ts": exchange_ts_ms,
            "latency_ms": _latency_ms(recv_ts, exchange_ts_ms),
        }

    # ── Order Book ─────────────────────────────────────────────────
    if channel == f"order_book_{SYMBOL}":
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        bids_sorted = sorted(bids, key=lambda level: safe_float(level[0], 0), reverse=True)
        asks_sorted = sorted(asks, key=lambda level: safe_float(level[0], 0))

        best_bid = safe_float(bids_sorted[0][0]) if bids_sorted else None
        best_ask = safe_float(asks_sorted[0][0]) if asks_sorted else None
        mid = round((best_bid + best_ask) / 2, 2) if best_bid is not None and best_ask is not None else None

        return {
            "time": now,
            "recv_ts": recv_ts,
            "channel": "book",
            "type": event,
            "symbol": "BTC/USD",
            "bids": bids_sorted[:10],
            "asks": asks_sorted[:10],
            "best_bid": best_bid,
            "best_bid_qty": safe_float(bids_sorted[0][1]) if bids_sorted else None,
            "best_ask": best_ask,
            "best_ask_qty": safe_float(asks_sorted[0][1]) if asks_sorted else None,
            "mid_price": mid,
            "exchange_ts": exchange_ts_ms,
            "latency_ms": _latency_ms(recv_ts, exchange_ts_ms),
        }

    return {
        "time": now,
        "recv_ts": recv_ts,
        "channel": f"unknown ({channel})",
        "raw": msg,
    }


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    if not msg:
        return

    t = msg["time"]
    channel = msg.get("channel")

    type_map = {
        1: "book",
        2: "trade",
        3: "book",
    }
    if SHOW_MESSAGE != 0 and channel != type_map.get(SHOW_MESSAGE):
        return

    if channel == "control":
        return

    if channel == "ticker":
        lat = f"  latency: {msg['latency_ms']}ms" if msg.get("latency_ms") is not None else ""
        print(
            f"[{t}] ⚡ BITSTAMP TICKER  "
            f"bid: {msg['best_bid']}  ask: {msg['best_ask']}  "
            f"mid: {msg['mid_price']}  last: {msg['last_price']}"
            f"{lat}"
        )

    elif channel == "trade":
        icon = "🟢" if msg.get("side") == "BUY" else "🔴"
        lat = f"  latency: {msg['latency_ms']}ms" if msg.get("latency_ms") is not None else ""
        print(
            f"[{t}] {icon} BITSTAMP TRADE  "
            f"price: {msg['price']}  size: {msg['size']} BTC  side: {msg['side']}"
            f"{lat}"
        )

    elif channel == "book":
        print(
            f"[{t}] 📖 BITSTAMP BOOK  "
            f"best_bid: {msg['best_bid']} ({msg['best_bid_qty']})  "
            f"best_ask: {msg['best_ask']} ({msg['best_ask_qty']})  "
            f"mid: {msg['mid_price']}"
        )

    else:
        print(f"[{t}] ❓ BITSTAMP UNKNOWN — {msg.get('raw', msg)}")


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_bitstamp(on_message=None):
    """
    Connect to Bitstamp WebSocket and stream:
      - live_trades_btcusd: every individual trade
      - order_book_btcusd: top book snapshot/updates used for bid/ask/mid

    Parameters:
        on_message - optional callback, receives each parsed message dict.
                     If None, prints to terminal.
    """
    print("\nConnecting to Bitstamp WebSocket...")
    print(f"URL:     {BITSTAMP_WS_URL}")
    print(f"Symbol:  {SYMBOL}")
    print("Streams: trades | order_book\n")

    async with websockets.connect(BITSTAMP_WS_URL) as ws:
        await ws.send(build_trade_subscription())
        await ws.send(build_order_book_subscription())

        print("Subscribed. Streaming...\n")

        async for raw in ws:
            msg = parse_message(raw)
            if not msg:
                continue

            if on_message:
                on_message(msg)
            else:
                print_message(msg)


# ─────────────────────────────────────────────
# RUN DIRECTLY
# ─────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(stream_bitstamp())
    except KeyboardInterrupt:
        print("\nStopped.")
