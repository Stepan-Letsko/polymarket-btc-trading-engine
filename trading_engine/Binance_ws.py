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
# 1 = AGG TRADE      (every trade execution - price, size, direction)
# 2 = BOOK TICKER    (best bid/ask update - fires on every top of book change)
# 3 = DEPTH          (top 5 orderbook levels updated every 100ms)

SHOW_MESSAGE = 2


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

# Binance spot WebSocket base URL
# We use a combined stream so all 3 streams come through one connection
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@aggTrade/"
    "btcusdt@bookTicker/"
    "btcusdt@depth5@100ms"
)

# No auth needed - all streams are public


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def parse_message(raw):
    """
    Parse a raw Binance WebSocket message.

    Binance combined stream messages are wrapped like:
    {
        "stream": "btcusdt@aggTrade",
        "data": { ... actual payload ... }
    }

    Stream types we subscribe to:

    aggTrade - aggregate trade. Multiple trades at the same price/time
               from the same taker order are combined into one message.
               This fires on every trade execution.
               Key fields: price (p), quantity (q), trade time (T),
               is_buyer_maker (m) - true means seller was aggressor (price fell)

    bookTicker - fires instantly whenever the best bid OR best ask changes.
                 This is the fastest signal for price movement.
                 Key fields: best bid price (b), best bid qty (B),
                             best ask price (a), best ask qty (A)

    depth5@100ms - top 5 levels of the orderbook, pushed every 100ms.
                   Shows you the full picture near the mid price.
                   Key fields: bids (list of [price, qty]),
                               asks (list of [price, qty])
    """
    data     = json.loads(raw)
    stream   = data.get("stream", "")
    payload  = data.get("data", {})
    now      = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts  = datetime.now(timezone.utc).timestamp() * 1000  # our receive time in ms

    if "@aggTrade" in stream:
        return {
            "time":            now,
            "recv_ts":         recv_ts,
            "stream":          "aggTrade",
            # price of the trade
            "price":           payload.get("p"),
            # quantity traded
            "quantity":        payload.get("q"),
            # timestamp of the trade on Binance (ms)
            "trade_time":      payload.get("T"),
            # event time - when Binance generated this message (ms)
            "event_time":      payload.get("E"),
            # if true, buyer was market maker (seller was aggressor = price move down)
            # if false, seller was market maker (buyer was aggressor = price move up)
            "buyer_is_maker":  payload.get("m"),
            # direction derived from buyer_is_maker for readability
            "direction":       "SELL" if payload.get("m") else "BUY",
            # aggregate trade ID
            "agg_trade_id":    payload.get("a"),
            # latency: difference between when Binance sent it and when we got it
            "latency_ms":      round(recv_ts - payload.get("E", recv_ts), 2),
        }

    elif "@bookTicker" in stream:
        return {
            "time":        now,
            "recv_ts":     recv_ts,
            "stream":      "bookTicker",
            # best bid price
            "best_bid":    payload.get("b"),
            # best bid quantity
            "best_bid_qty": payload.get("B"),
            # best ask price
            "best_ask":    payload.get("a"),
            # best ask quantity
            "best_ask_qty": payload.get("A"),
            # mid price calculated from best bid and ask
            "mid_price":   round((float(payload.get("b", 0)) + float(payload.get("a", 0))) / 2, 2),
            # event time - when Binance generated this message (ms)
            "event_time":  payload.get("E"),
            # order book update ID
            "update_id":   payload.get("u"),
            # latency
            "latency_ms":  round(recv_ts - payload.get("E", recv_ts), 2),
        }

    elif "@depth" in stream:
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        # bids come sorted best first already from Binance
        return {
            "time":         now,
            "recv_ts":      recv_ts,
            "stream":       "depth5",
            # top 5 bids: each is [price, quantity]
            "bids":         bids,
            # top 5 asks: each is [price, quantity]
            "asks":         asks,
            # best bid and ask pulled from top of each list
            "best_bid":     bids[0][0] if bids else None,
            "best_ask":     asks[0][0] if asks else None,
            # mid price
            "mid_price":    round((float(bids[0][0]) + float(asks[0][0])) / 2, 2) if bids and asks else None,
            # last update ID
            "last_update_id": payload.get("lastUpdateId"),
        }

    else:
        return {
            "time":   now,
            "stream": "unknown",
            "raw":    payload,
        }


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    t  = msg["time"]
    st = msg["stream"]

    TYPE_MAP = {
        1: "aggTrade",
        2: "bookTicker",
        3: "depth5",
    }

    # Filter to only show selected message type
    if SHOW_MESSAGE != 0 and st != TYPE_MAP.get(SHOW_MESSAGE):
        return

    if st == "aggTrade":
        direction_icon = "🟢" if msg["direction"] == "BUY" else "🔴"
        print(f"[{t}] {direction_icon} TRADE  "
              f"price: {msg['price']}  qty: {msg['quantity']}  "
              f"direction: {msg['direction']}  "
              f"latency: {msg['latency_ms']}ms  "
              f"trade_time: {msg['trade_time']}")

    elif st == "bookTicker":
        print(f"[{t}] ⚡ BOOK TICKER  "
              f"bid: {msg['best_bid']} ({msg['best_bid_qty']})  "
              f"ask: {msg['best_ask']} ({msg['best_ask_qty']})  "
              f"mid: {msg['mid_price']}  "
              f"latency: {msg['latency_ms']}ms")

    elif st == "depth5":
        print(f"[{t}] 📖 DEPTH  "
              f"best_bid: {msg['best_bid']}  best_ask: {msg['best_ask']}  "
              f"mid: {msg['mid_price']}")
        print(f"   bids: {msg['bids']}")
        print(f"   asks: {msg['asks']}")

    else:
        print(f"[{t}] ❓ UNKNOWN — {msg.get('raw', msg)}")


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_binance(on_message=None):
    """
    Connect to Binance combined WebSocket and stream:
    - aggTrade: every trade execution
    - bookTicker: best bid/ask changes
    - depth5: top 5 orderbook levels every 100ms

    Parameters:
        on_message - optional callback for use when importing this module.
                     Called with each parsed message dict.
                     If None, prints to terminal.

    Binance handles ping/pong automatically at the protocol level
    so we don't need a manual heartbeat like with Polymarket.
    Runs forever until interrupted (Ctrl+C).
    """
    print(f"\nConnecting to Binance WebSocket...")
    print(f"Streams: aggTrade | bookTicker | depth5@100ms")
    print(f"Symbol:  BTCUSDT\n")

    async with websockets.connect(BINANCE_WS_URL) as ws:
        print("Connected. Streaming...\n")

        async for raw in ws:
            msg = parse_message(raw)

            if on_message:
                on_message(msg)
            else:
                print_message(msg)


# ─────────────────────────────────────────────
# RUN DIRECTLY
# ─────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(stream_binance())
    except KeyboardInterrupt:
        print("\nStopped.")