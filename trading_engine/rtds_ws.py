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
# 1 = BINANCE price   (BTC/USD price relayed from Binance via Polymarket)
# 2 = CHAINLINK price (BTC/USD price from Chainlink — this is the resolution oracle)

SHOW_MESSAGE = 0


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

RTDS_URL           = "wss://ws-live-data.polymarket.com"
HEARTBEAT_INTERVAL = 5  # docs say send PING every 5 seconds for RTDS


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGES
# ─────────────────────────────────────────────

def build_binance_subscription():
    """
    Subscribe to all Binance crypto prices - no filter.
    We filter to BTC only in print_message/parse_message.
    """
    return json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type":  "update"
            }
        ]
    })


def build_chainlink_subscription():
    """
    Subscribe to Chainlink BTC/USD price feed via Polymarket RTDS.
    This is the RESOLUTION oracle — what Polymarket uses to decide
    if the market resolves Up or Down at the end of each 5m window.
    Using empty filters to get all symbols rather than filtering to just BTC.
    """
    return json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic":   "crypto_prices_chainlink",
                "type":    "*",
                "filters": ""
            }
        ]
    })


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def parse_message(raw):
    """
    Parse a raw RTDS message.

    All RTDS messages follow this structure:
    {
        "topic":     string,   - which feed this is from
        "type":      string,   - message type (always "update" for prices)
        "timestamp": number,   - when RTDS sent this message (ms)
        "payload": {
            "symbol":    string,  - the trading pair
            "timestamp": number,  - when the SOURCE recorded this price (ms)
            "value":     number   - the price in USD
        }
    }

    Two timestamps exist:
    - outer "timestamp": when Polymarket's RTDS relay sent the message to us
    - payload "timestamp": when Binance/Chainlink originally recorded the price

    The difference between these two is the internal relay delay within Polymarket.
    The difference between payload timestamp and our receive time is total latency
    from source to us.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "time":   datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
            "source": "unknown",
            "topic":  "parse_error",
            "raw":    raw,
        }

    topic    = data.get("topic", "")
    now      = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts  = datetime.now(timezone.utc).timestamp() * 1000

    if topic == "crypto_prices":
        payload = data.get("payload", {})
        rtds_ts = data.get("timestamp")
        src_ts  = payload.get("timestamp")

        relay_delay   = round(rtds_ts - src_ts, 2) if rtds_ts and src_ts else None
        total_latency = round(recv_ts - src_ts, 2) if src_ts else None

        return {
            "time":               now,
            "recv_ts":            recv_ts,
            "source":             "BINANCE",
            "topic":              topic,
            "symbol":             payload.get("symbol"),
            "price":              payload.get("value"),
            "source_ts":          src_ts,
            "rtds_ts":            rtds_ts,
            "relay_delay_ms":     relay_delay,
            "total_latency_ms":   total_latency,
        }

    elif topic == "crypto_prices_chainlink":
        payload = data.get("payload", {})
        rtds_ts = data.get("timestamp")
        src_ts  = payload.get("timestamp")

        relay_delay   = round(rtds_ts - src_ts, 2) if rtds_ts and src_ts else None
        total_latency = round(recv_ts - src_ts, 2) if src_ts else None

        return {
            "time":               now,
            "recv_ts":            recv_ts,
            "source":             "CHAINLINK",
            "topic":              topic,
            "symbol":             payload.get("symbol"),
            "price":              payload.get("value"),
            "source_ts":          src_ts,
            "rtds_ts":            rtds_ts,
            "relay_delay_ms":     relay_delay,
            "total_latency_ms":   total_latency,
        }

    else:
        return {
            "time":   now,
            "source": "unknown",
            "topic":  topic,
            "raw":    data,
        }


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    t      = msg["time"]
    source = msg.get("source", "unknown")

    TYPE_MAP = {
        1: "BINANCE",
        2: "CHAINLINK",
    }

    if SHOW_MESSAGE != 0 and source != TYPE_MAP.get(SHOW_MESSAGE):
        return

    # Only show BTC price — filter out ETH, SOL, XRP etc
    symbol = msg.get("symbol", "")
    if symbol not in ("btcusdt", "btc/usd"):
        return

    if source == "BINANCE":
        print(f"[{t}] 🟡 BINANCE (via RTDS)  "
              f"price: ${msg['price']:,.2f}  "
              f"relay_delay: {msg['relay_delay_ms']}ms  "
              f"total_latency: {msg['total_latency_ms']}ms")

    elif source == "CHAINLINK":
        print(f"[{t}] 🔵 CHAINLINK (oracle)  "
              f"price: ${msg['price']:,.2f}  "
              f"relay_delay: {msg['relay_delay_ms']}ms  "
              f"total_latency: {msg['total_latency_ms']}ms")

    else:
        print(f"[{t}] ❓ UNKNOWN topic: {msg.get('topic')}  raw: {msg.get('raw')}")


# ─────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────

async def heartbeat(ws):
    """
    RTDS requires PING every 5 seconds or it closes the connection.
    This is different from the CLOB WebSocket which requires PING every 10s.
    """
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await ws.send("PING")


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_rtds(on_message=None):
    """
    Connect to Polymarket RTDS and stream:
    - Binance BTC/USD price relay
    - Chainlink BTC/USD price (the resolution oracle)

    Key insight: comparing Binance price here vs raw Binance feed in
    binance_ws.py tells you exactly how delayed Polymarket's information
    is. That delay is part of your edge window.

    Parameters:
        on_message - optional callback for use when importing this module.
                     Called with each parsed message dict.
                     If None, prints to terminal.

    Runs forever until interrupted (Ctrl+C).
    """
    print(f"\nConnecting to Polymarket RTDS...")
    print(f"Subscribing to: Binance BTC/USD + Chainlink BTC/USD\n")

    async with websockets.connect(RTDS_URL) as ws:

        # Subscribe to both feeds
        await ws.send(build_binance_subscription())
        await ws.send(build_chainlink_subscription())
        print("Subscribed. Streaming...\n")

        # Start heartbeat - RTDS needs PING every 5 seconds
        hb_task = asyncio.create_task(heartbeat(ws))

        try:
            async for raw in ws:

                # Ignore PONG responses to our heartbeat
                if raw == "PONG":
                    continue

                msg = parse_message(raw)

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
        asyncio.run(stream_rtds())
    except KeyboardInterrupt:
        print("\nStopped.")