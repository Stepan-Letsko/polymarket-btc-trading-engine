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
# 1 = TICKER     (best bid/ask + last price, fires on every trade)
# 2 = TRADE      (every individual trade execution)
# 3 = BOOK       (top 10 orderbook levels, updated on every change)

SHOW_MESSAGE = 0


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

# Kraken WebSocket API v2 — spot markets
# This is hosted in AWS eu-west-1 (Dublin) making it the closest
# major exchange WebSocket to Ireland/UK
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

# Kraken uses "BTC/USD" format (not BTCUSDT like Binance)
SYMBOL = "BTC/USD"


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGES
# ─────────────────────────────────────────────

def build_ticker_subscription():
    """
    Subscribe to ticker channel.
    Streams level 1 data — best bid, best ask, last price.
    Fires on every trade event (not on every orderbook change).
    This is the fastest top-of-book signal from Kraken.

    event_trigger options:
      "trades" — update on every trade (default, most frequent)
      "bbo"    — update on best bid/offer change
    """
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel":       "ticker",
            "symbol":        [SYMBOL],
            "event_trigger": "bbo",   # fire on any top-of-book change
            "snapshot":      True,    # send current state immediately on connect
        }
    })


def build_trade_subscription():
    """
    Subscribe to trade channel.
    Fires on every individual trade execution.
    Multiple trades may be batched in a single message.

    Key fields per trade:
      price     — execution price
      qty       — quantity traded (in BTC)
      side      — "buy" or "sell" (taker perspective)
      timestamp — when the trade happened on Kraken's matching engine
      ord_type  — "market" or "limit"
    """
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel":  "trade",
            "symbol":   [SYMBOL],
            "snapshot": False,  # no historical trades on connect
        }
    })


def build_book_subscription():
    """
    Subscribe to book channel (Level 2 orderbook).
    Streams top 10 price levels on each side.
    Sends a full snapshot on connect then delta updates.

    Each update includes a CRC32 checksum to verify
    your local book is in sync with Kraken's book.

    depth options: 10, 25, 100, 500, 1000
    We use 10 — enough for our purposes and lowest bandwidth.
    """
    return json.dumps({
        "method": "subscribe",
        "params": {
            "channel":  "book",
            "symbol":   [SYMBOL],
            "depth":    10,
            "snapshot": True,
        }
    })


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def parse_message(raw):
    """
    Parse a raw Kraken v2 WebSocket message.

    Kraken v2 message format:
    {
        "channel": "ticker" | "trade" | "book" | "heartbeat" | "status",
        "type":    "snapshot" | "update",
        "data":    [ ... array of data objects ... ]
    }

    Control messages (subscription confirmations, heartbeats) have
    a "method" or "channel": "heartbeat" format instead.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    now     = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts = datetime.now(timezone.utc).timestamp() * 1000
    channel = data.get("channel", "")
    msg_type = data.get("type", "")

    # ── Heartbeat ──────────────────────────────────────────────────
    # Kraken sends a heartbeat every second when no other data flows
    if channel == "heartbeat":
        return {
            "time":    now,
            "channel": "heartbeat",
        }

    # ── Status / control messages ──────────────────────────────────
    # Subscription confirmations and system status messages
    if channel == "status" or "method" in data:
        return {
            "time":    now,
            "channel": "control",
            "raw":     data,
        }

    # ── Ticker ─────────────────────────────────────────────────────
    if channel == "ticker":
        items = data.get("data", [])
        if not items:
            return None
        tick = items[0]  # always one item for a single symbol

        bid     = tick.get("bid")
        ask     = tick.get("ask")
        mid     = round((bid + ask) / 2, 2) if bid and ask else None
        last    = tick.get("last")

        # latency: difference between Kraken's timestamp and our receive time
        kraken_ts_str = tick.get("timestamp")
        kraken_ts     = None
        latency_ms    = None
        if kraken_ts_str:
            from datetime import datetime as dt
            kraken_dt  = dt.fromisoformat(kraken_ts_str.replace("Z", "+00:00"))
            kraken_ts  = kraken_dt.timestamp() * 1000
            latency_ms = round(recv_ts - kraken_ts, 2)

        return {
            "time":       now,
            "recv_ts":    recv_ts,
            "channel":    "ticker",
            "type":       msg_type,
            "symbol":     tick.get("symbol"),
            "best_bid":   bid,
            "best_bid_qty": tick.get("bid_qty"),
            "best_ask":   ask,
            "best_ask_qty": tick.get("ask_qty"),
            "mid_price":  mid,
            "last_price": last,
            "kraken_ts":  kraken_ts,
            "latency_ms": latency_ms,
        }

    # ── Trade ──────────────────────────────────────────────────────
    if channel == "trade":
        items = data.get("data", [])
        if not items:
            return None

        # Multiple trades can be batched in one message
        trades = []
        for t in items:
            kraken_ts_str = t.get("timestamp")
            kraken_ts     = None
            latency_ms    = None
            if kraken_ts_str:
                from datetime import datetime as dt
                kraken_dt  = dt.fromisoformat(kraken_ts_str.replace("Z", "+00:00"))
                kraken_ts  = kraken_dt.timestamp() * 1000
                latency_ms = round(recv_ts - kraken_ts, 2)

            trades.append({
                "price":      t.get("price"),
                "qty":        t.get("qty"),        # quantity in BTC
                "side":       t.get("side"),        # "buy" or "sell" (taker)
                "ord_type":   t.get("ord_type"),    # "market" or "limit"
                "trade_id":   t.get("trade_id"),
                "kraken_ts":  kraken_ts,
                "latency_ms": latency_ms,
            })

        return {
            "time":     now,
            "recv_ts":  recv_ts,
            "channel":  "trade",
            "type":     msg_type,
            "symbol":   SYMBOL,
            "trades":   trades,
        }

    # ── Book ───────────────────────────────────────────────────────
    if channel == "book":
        items = data.get("data", [])
        if not items:
            return None
        book = items[0]

        bids = book.get("bids", [])
        asks = book.get("asks", [])

        # Bids come sorted descending (best first), asks ascending (best first)
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        mid      = round((best_bid + best_ask) / 2, 2) if best_bid and best_ask else None

        return {
            "time":       now,
            "recv_ts":    recv_ts,
            "channel":    "book",
            "type":       msg_type,   # "snapshot" or "update"
            "symbol":     book.get("symbol"),
            "bids":       bids,       # list of {price, qty}
            "asks":       asks,       # list of {price, qty}
            "best_bid":   best_bid,
            "best_ask":   best_ask,
            "mid_price":  mid,
            "checksum":   book.get("checksum"),  # CRC32 for book validation
            "timestamp":  book.get("timestamp"),
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
        2: "trade",
        3: "book",
    }

    if SHOW_MESSAGE != 0 and ch != TYPE_MAP.get(SHOW_MESSAGE):
        return

    # Skip heartbeats and control messages when showing all
    if ch in ("heartbeat", "control"):
        if SHOW_MESSAGE == 0:
            return  # suppress these unless specifically requested
        print(f"[{t}] 💓 {ch.upper()}")
        return

    if ch == "ticker":
        lat = f"  latency: {msg['latency_ms']}ms" if msg.get("latency_ms") else ""
        print(f"[{t}] ⚡ KRAKEN TICKER  "
              f"bid: {msg['best_bid']} ({msg['best_bid_qty']})  "
              f"ask: {msg['best_ask']} ({msg['best_ask_qty']})  "
              f"mid: {msg['mid_price']}  "
              f"last: {msg['last_price']}"
              f"{lat}")

    elif ch == "trade":
        for trade in msg["trades"]:
            icon = "🟢" if trade["side"] == "buy" else "🔴"
            lat  = f"  latency: {trade['latency_ms']}ms" if trade.get("latency_ms") else ""
            print(f"[{t}] {icon} KRAKEN TRADE  "
                  f"price: {trade['price']}  "
                  f"qty: {trade['qty']} BTC  "
                  f"side: {trade['side']}  "
                  f"type: {trade['ord_type']}"
                  f"{lat}")

    elif ch == "book":
        snap = "SNAPSHOT" if msg["type"] == "snapshot" else "UPDATE"
        print(f"[{t}] 📖 KRAKEN BOOK {snap}  "
              f"best_bid: {msg['best_bid']}  "
              f"best_ask: {msg['best_ask']}  "
              f"mid: {msg['mid_price']}")
        if msg["type"] == "snapshot":
            print(f"   bids (top 5): {msg['bids'][:5]}")
            print(f"   asks (top 5): {msg['asks'][:5]}")

    else:
        print(f"[{t}] ❓ UNKNOWN — {msg.get('raw', msg)}")


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_kraken(on_message=None):
    """
    Connect to Kraken WebSocket API v2 and stream:
      - ticker:  best bid/ask + last price on every top-of-book change
      - trade:   every individual trade execution
      - book:    top 10 orderbook levels, snapshot then deltas

    Kraken's server is hosted in AWS eu-west-1 (Dublin, Ireland) making
    it the geographically closest major exchange WebSocket from Ireland/UK.
    Expected latency: 5-20ms from Ireland.

    Kraken handles WebSocket ping/pong at the protocol level automatically
    so no manual heartbeat is needed.

    Parameters:
        on_message - optional callback, receives each parsed message dict.
                     If None, prints to terminal.

    Runs forever until interrupted (Ctrl+C).
    """
    print(f"\nConnecting to Kraken WebSocket API v2...")
    print(f"URL:    {KRAKEN_WS_URL}")
    print(f"Symbol: {SYMBOL}")
    print(f"Streams: ticker (bbo) | trade | book (depth 10)\n")

    async with websockets.connect(KRAKEN_WS_URL) as ws:

        # Subscribe to all three channels
        await ws.send(build_ticker_subscription())
        await ws.send(build_trade_subscription())
        await ws.send(build_book_subscription())

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
        asyncio.run(stream_kraken())
    except KeyboardInterrupt:
        print("\nStopped.")