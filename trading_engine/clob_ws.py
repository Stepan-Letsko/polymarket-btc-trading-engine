import asyncio
import json
import websockets
from datetime import datetime, timezone
from get_market import get_current_market

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

CLOB_WS_URL        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HEARTBEAT_INTERVAL = 9


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGE
# ─────────────────────────────────────────────

def build_subscription(token_up, token_down):
    return json.dumps({
        "assets_ids":             [token_up, token_down],
        "type":                   "market",
        "initial_dump":           True,
        "custom_feature_enabled": True
    })


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def safe_float(val, fallback=None):
    """Safely convert a value to float, returning fallback if it fails."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


def get_best_bid(bids):
    """
    Return the best (highest) bid price from a list of {price, size} dicts.
    We sort descending so the highest bid is at index 0.
    The server does NOT guarantee order, so we must sort ourselves.
    """
    if not bids:
        return None
    sorted_bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
    return sorted_bids[0]["price"]


def get_best_ask(asks):
    """
    Return the best (lowest) ask price from a list of {price, size} dicts.
    We sort ascending so the lowest ask is at index 0.
    The server does NOT guarantee order, so we must sort ourselves.
    """
    if not asks:
        return None
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 0)))
    return sorted_asks[0]["price"]


def label_side(asset_id, token_up, token_down):
    """Return UP, DOWN, or UNKNOWN based on which token this asset_id belongs to."""
    if asset_id == token_up:
        return "UP"
    elif asset_id == token_down:
        return "DOWN"
    return "UNKNOWN"


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────
def parse_message(raw, token_up, token_down):
    data = json.loads(raw)

    # Server sends one object per message, not a list
    # Wrap in a list so we can handle both cases uniformly
    events = data if isinstance(data, list) else [data]

    parsed = []
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

    for event in events:
        if not isinstance(event, dict):
            continue

        event_type = event.get("event_type", "unknown")

        if event_type == "book":
            asset_id = event.get("asset_id", "")
            bids = event.get("bids", [])
            asks = event.get("asks", [])
            bids_sorted = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
            asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 0)))
            parsed.append({
                "time":       now,
                "event_type": "book",
                "asset_id":   asset_id,
                "side":       label_side(asset_id, token_up, token_down),
                "market":     event.get("market"),
                "bids":       bids_sorted,
                "asks":       asks_sorted,
                "best_bid":   bids_sorted[0]["price"] if bids_sorted else None,
                "best_ask":   asks_sorted[0]["price"] if asks_sorted else None,
                "timestamp":  event.get("timestamp"),
                "hash":       event.get("hash"),
            })

        elif event_type == "price_change":
            # asset_id is INSIDE each price_change item, not at event level
            price_changes = event.get("price_changes", [])
            for change in price_changes:
                asset_id = change.get("asset_id", "")
                parsed.append({
                    "time":       now,
                    "event_type": "price_change",
                    "asset_id":   asset_id,
                    "side":       label_side(asset_id, token_up, token_down),
                    "market":     event.get("market"),
                    "price":      change.get("price"),
                    "size":       change.get("size"),
                    "order_side": change.get("side"),   # BUY or SELL
                    "best_bid":   change.get("best_bid"),
                    "best_ask":   change.get("best_ask"),
                    "hash":       change.get("hash"),
                    "timestamp":  event.get("timestamp"),
                })

        elif event_type == "last_trade_price":
            asset_id = event.get("asset_id", "")
            parsed.append({
                "time":             now,
                "event_type":       "last_trade_price",
                "asset_id":         asset_id,
                "side":             label_side(asset_id, token_up, token_down),
                "market":           event.get("market"),
                "price":            event.get("price"),
                "size":             event.get("size"),
                "trade_side":       event.get("side"),
                "fee_rate_bps":     event.get("fee_rate_bps"),
                "timestamp":        event.get("timestamp"),
                "transaction_hash": event.get("transaction_hash"),
            })

        elif event_type == "best_bid_ask":
            asset_id = event.get("asset_id", "")
            parsed.append({
                "time":       now,
                "event_type": "best_bid_ask",
                "asset_id":   asset_id,
                "side":       label_side(asset_id, token_up, token_down),
                "market":     event.get("market"),
                "best_bid":   event.get("best_bid"),
                "best_ask":   event.get("best_ask"),
                "best_bid_size": event.get("best_bid_size"),
                "best_ask_size": event.get("best_ask_size"),
                "spread":     event.get("spread"),
                "timestamp":  event.get("timestamp"),
            })

        elif event_type == "tick_size_change":
            asset_id = event.get("asset_id", "")
            parsed.append({
                "time":          now,
                "event_type":    "tick_size_change",
                "asset_id":      asset_id,
                "side":          label_side(asset_id, token_up, token_down),
                "market":        event.get("market"),
                "old_tick_size": event.get("old_tick_size"),
                "new_tick_size": event.get("new_tick_size"),
                "timestamp":     event.get("timestamp"),
            })

        elif event_type == "new_market":
            parsed.append({
                "time":           now,
                "event_type":     "new_market",
                "id":             event.get("id"),
                "question":       event.get("question"),
                "market":         event.get("market"),
                "condition_id":   event.get("condition_id"),
                "slug":           event.get("slug"),
                "assets_ids":     event.get("assets_ids"),
                "outcomes":       event.get("outcomes"),
                "clob_token_ids": event.get("clob_token_ids"),
                "tick_size":      event.get("order_price_min_tick_size"),
                "timestamp":      event.get("timestamp"),
            })

        elif event_type == "market_resolved":
            parsed.append({
                "time":             now,
                "event_type":       "market_resolved",
                "market":           event.get("market"),
                "winning_asset_id": event.get("winning_asset_id"),
                "winning_outcome":  event.get("winning_outcome"),
                "timestamp":        event.get("timestamp"),
            })

        else:
            parsed.append({
                "time":       now,
                "event_type": f"unknown ({event_type})",
                "raw":        event,
            })

    return parsed


# ─────────────────────────────────────────────
# MESSAGE FILTER
# ─────────────────────────────────────────────
# Set SHOW_MESSAGE to the number of the message type you want to see.
# Set to 0 to show ALL message types.
#
# 0 = ALL messages
# 1 = BOOK SNAPSHOT       (full orderbook on connect)
# 2 = PRICE CHANGE        (order placed or cancelled)
# 3 = TRADE               (actual execution)
# 4 = BEST BID/ASK        (top of book changed)
# 5 = TICK SIZE CHANGE    (rare - min price increment changed)
# 6 = NEW MARKET          (next 5m window opened)
# 7 = MARKET RESOLVED     (window closed, winner announced)

SHOW_MESSAGE = 4


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    t  = msg["time"]
    et = msg["event_type"]

    TYPE_MAP = {
        1: "book",
        2: "price_change",
        3: "last_trade_price",
        4: "best_bid_ask",
        5: "tick_size_change",
        6: "new_market",
        7: "market_resolved",
    }

    # If filtering, skip anything that doesn't match
    if SHOW_MESSAGE != 0 and et != TYPE_MAP.get(SHOW_MESSAGE):
        return

    if et == "book":
        print(f"\n[{t}] 📚 BOOK SNAPSHOT — {msg['side']} token")
        print(f"   best_bid: {msg['best_bid']}  |  best_ask: {msg['best_ask']}")
        print(f"   bids ({len(msg['bids'])} levels, sorted): {msg['bids'][:5]}")
        print(f"   asks ({len(msg['asks'])} levels, sorted): {msg['asks'][:5]}")
        print(f"   market:    {msg['market']}")
        print(f"   timestamp: {msg['timestamp']}  hash: {msg['hash']}")

    elif et == "price_change":
        removed = "(REMOVED)" if msg.get("size") == "0" else ""
        print(f"[{t}] 📊 PRICE CHANGE — {msg['side']} token  "
              f"price: {msg['price']}  size: {msg['size']} {removed}  "
              f"order_side: {msg['order_side']}  "
              f"best_bid: {msg['best_bid']}  best_ask: {msg['best_ask']}")

    elif et == "last_trade_price":
        print(f"[{t}] 💰 TRADE — {msg['side']} token")
        print(f"   price: {msg['price']}  size: {msg['size']}  "
              f"side: {msg['trade_side']}  fee_bps: {msg['fee_rate_bps']}")
        print(f"   timestamp: {msg['timestamp']}")
        if msg.get("transaction_hash"):
            print(f"   tx: {msg['transaction_hash']}")

    elif et == "tick_size_change":
        print(f"[{t}] 📏 TICK SIZE CHANGE — {msg['side']} token")
        print(f"   {msg['old_tick_size']} → {msg['new_tick_size']}")

    elif et == "best_bid_ask":
        print(f"[{t}] ⚡ BEST BID/ASK — {msg['side']} token  "
              f"bid: {msg['best_bid']}  ask: {msg['best_ask']}  "
              f"spread: {msg['spread']}")

    elif et == "new_market":
        print(f"\n[{t}] 🆕 NEW MARKET")
        print(f"   question:  {msg['question']}")
        print(f"   slug:      {msg['slug']}")
        print(f"   assets:    {msg['assets_ids']}")

    elif et == "market_resolved":
        print(f"\n[{t}] 🏁 MARKET RESOLVED — winner: {msg['winning_outcome']}")
        print(f"   winning_asset: {msg['winning_asset_id']}")

    else:
        print(f"[{t}] ❓ UNKNOWN — {msg.get('raw', msg)}")

# ─────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────

async def heartbeat(ws):
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await ws.send("PING")


# ─────────────────────────────────────────────
# MAIN WEBSOCKET FUNCTION
# ─────────────────────────────────────────────

async def stream_market(token_up, token_down, on_message=None):
    """
    Connect and stream all market data for the given token pair.
 
    Parameters:
        token_up   - UP token ID
        token_down - DOWN token ID
        on_message - optional callback, receives each parsed message dict.
                     If None, prints to terminal.
    """
    print(f"\nConnecting to CLOB WebSocket...")
    print(f"UP token:   {token_up[:20]}...")
    print(f"DOWN token: {token_down[:20]}...\n")
 
    async with websockets.connect(CLOB_WS_URL) as ws:
        await ws.send(build_subscription(token_up, token_down))
        print("Subscribed. Streaming...\n")
        hb_task = asyncio.create_task(heartbeat(ws))
 
        try:
            async for raw in ws:
                if raw == "PONG":
                    continue
 
                messages = parse_message(raw, token_up, token_down)
                for msg in messages:
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
# AUTO-ROTATING MARKET STREAM
# ─────────────────────────────────────────────

async def stream_current_market(on_message=None):
    """
    Like stream_market(), but handles the 5-minute market rotation itself:
    fetches the currently-open BTC Up/Down market, streams it until that
    window's end_date passes, then automatically fetches the new market
    and reconnects with its token IDs. Callers just call this once and
    never need to track which market is currently open themselves.

    Parameters:
        on_message - optional callback, receives each parsed message dict
                     (same shape as stream_market()). If None, prints to
                     terminal.
    """
    while True:
        # get_current_market() makes a blocking HTTP request (via
        # requests) — run it off the event loop so it doesn't stall
        # everything else while waiting on the network.
        market = await asyncio.to_thread(get_current_market)
        if not market:
            print("Could not find current market, retrying in 2s...")
            await asyncio.sleep(2)
            continue

        end_dt = datetime.fromisoformat(market["end_date"].replace("Z", "+00:00"))
        seconds_left = (end_dt - datetime.now(timezone.utc)).total_seconds()
        # +1s margin so we don't race the next market not existing yet
        # in the API right as this window ends.
        window_budget = max(seconds_left, 0) + 1

        stream_task = asyncio.create_task(
            stream_market(market["token_up"], market["token_down"], on_message=on_message)
        )
        try:
            done, _ = await asyncio.wait({stream_task}, timeout=window_budget)
            if stream_task in done:
                # The connection itself died before the window ended —
                # surface whatever killed it so the caller's own retry
                # logic (e.g. a reconnect wrapper) can handle it.
                stream_task.result()
        finally:
            if not stream_task.done():
                stream_task.cancel()
            try:
                await stream_task
            except (asyncio.CancelledError, Exception):
                pass
        # Loop back around: the window's over, fetch the new current
        # market and reconnect with its tokens.


# ─────────────────────────────────────────────
# RUN DIRECTLY
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("Fetching current BTC 5m market...")
    market = get_current_market()

    if not market:
        print("ERROR: Could not find current market.")
        exit(1)

    print(f"Market: {market['event_title']}")
    print(f"Ends:   {market['end_date']}")

    try:
        asyncio.run(stream_market(market["token_up"], market["token_down"]))
    except KeyboardInterrupt:
        print("\nStopped.")