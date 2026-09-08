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
# 1 = ORDER events  (placement, update, cancellation)
# 2 = TRADE events  (match, mined, confirmed, failed)

SHOW_MESSAGE = 0


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

USER_WS_URL        = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
HEARTBEAT_INTERVAL = 9  # server closes if no PING within 10s


# ─────────────────────────────────────────────
# BUILD SUBSCRIPTION MESSAGE
# ─────────────────────────────────────────────

def build_subscription(api_key, secret, passphrase, condition_ids=None):
    """
    Build the authenticated subscription message for the User Channel.

    Parameters:
        api_key       - L2 API key from build_client() in order_client.py
        secret        - L2 API secret
        passphrase    - L2 API passphrase
        condition_ids - optional list of market condition IDs to filter.
                        If None, receives events for ALL your markets.

    The auth credentials are the L2 credentials derived from your
    private key — NOT your private key itself. These come from
    client.creds after calling build_client().
    """
    msg = {
        "auth": {
            "apiKey":     api_key,
            "secret":     secret,
            "passphrase": passphrase,
        },
        "type": "user",
    }

    # Optionally filter to specific markets by condition ID
    if condition_ids:
        msg["markets"] = condition_ids

    return json.dumps(msg)


# ─────────────────────────────────────────────
# PARSE INCOMING MESSAGES
# ─────────────────────────────────────────────

def parse_message(raw):
    """
    Parse a raw User Channel WebSocket message.

    Two event types:

    ORDER — fires when one of your orders changes state:
      type: PLACEMENT   — your order was accepted and is now live on the book
      type: UPDATE      — your order was partially filled (size_matched increased)
      type: CANCELLATION — your order was cancelled (by you or expired)

      Key fields:
        id             — the order ID (hash)
        status         — LIVE, MATCHED, CANCELED
        order_type     — GTC, GTD, FOK
        side           — BUY or SELL
        price          — the price you placed at
        original_size  — how many shares you ordered
        size_matched   — how many shares have filled so far
        asset_id       — which token (UP or DOWN)
        market         — condition ID of the market

    TRADE — fires when a trade actually executes involving your order:
      status: MATCHED   — trade matched off-chain, waiting for on-chain
      status: MINED     — transaction submitted to Polygon blockchain
      status: CONFIRMED — transaction confirmed on-chain (final)
      status: RETRYING  — on-chain submission failed, retrying
      status: FAILED    — trade failed on-chain

      Key fields:
        id               — trade ID
        taker_order_id   — the order that took liquidity
        size             — how many shares traded
        price            — execution price
        trader_side      — TAKER or MAKER (were you the aggressor?)
        transaction_hash — Polygon tx hash (available after MINED)
        maker_orders     — details of the maker orders that were matched
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    event_type = data.get("event_type", "unknown")
    now        = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    recv_ts    = datetime.now(timezone.utc).timestamp() * 1000

    if event_type == "order":
        return {
            "time":          now,
            "recv_ts":       recv_ts,
            "event_type":    "order",
            # what happened to this order
            "type":          data.get("type"),          # PLACEMENT, UPDATE, CANCELLATION
            "status":        data.get("status"),        # LIVE, MATCHED, CANCELED
            "order_type":    data.get("order_type"),    # GTC, GTD, FOK
            # order identity
            "order_id":      data.get("id"),
            "market":        data.get("market"),        # condition ID
            "asset_id":      data.get("asset_id"),      # token ID
            # order details
            "side":          data.get("side"),          # BUY or SELL
            "price":         data.get("price"),
            "original_size": data.get("original_size"), # total shares ordered
            "size_matched":  data.get("size_matched"),  # shares filled so far
            "outcome":       data.get("outcome"),       # YES or NO
            "expiration":    data.get("expiration"),    # for GTD orders
            "created_at":    data.get("created_at"),
            "timestamp":     data.get("timestamp"),
            "maker_address": data.get("maker_address"),
            "owner":         data.get("owner"),
        }

    elif event_type == "trade":
        return {
            "time":             now,
            "recv_ts":          recv_ts,
            "event_type":       "trade",
            # trade identity
            "trade_id":         data.get("id"),
            "taker_order_id":   data.get("taker_order_id"),
            "market":           data.get("market"),
            "asset_id":         data.get("asset_id"),
            # execution details
            "side":             data.get("side"),       # BUY or SELL (taker perspective)
            "size":             data.get("size"),       # shares traded
            "price":            data.get("price"),      # execution price
            "fee_rate_bps":     data.get("fee_rate_bps"),
            "outcome":          data.get("outcome"),
            # settlement status — this progresses through MATCHED → MINED → CONFIRMED
            "status":           data.get("status"),
            "matchtime":        data.get("matchtime"),
            "last_update":      data.get("last_update"),
            "transaction_hash": data.get("transaction_hash"),  # available after MINED
            # were you the taker (aggressor) or maker (resting order)?
            "trader_side":      data.get("trader_side"),
            # breakdown of which maker orders were matched against
            "maker_orders":     data.get("maker_orders", []),
            "owner":            data.get("owner"),
            "timestamp":        data.get("timestamp"),
        }

    else:
        return {
            "time":       now,
            "event_type": f"unknown ({event_type})",
            "raw":        data,
        }


# ─────────────────────────────────────────────
# PRINT HELPERS
# ─────────────────────────────────────────────

def print_message(msg):
    if not msg:
        return

    t  = msg["time"]
    et = msg["event_type"]

    TYPE_MAP = {1: "order", 2: "trade"}
    if SHOW_MESSAGE != 0 and et != TYPE_MAP.get(SHOW_MESSAGE):
        return

    if et == "order":
        # Choose icon based on what happened
        icon_map = {
            "PLACEMENT":    "📋",
            "UPDATE":       "🔄",
            "CANCELLATION": "❌",
        }
        icon   = icon_map.get(msg["type"], "❓")
        filled = float(msg["size_matched"] or 0)
        total  = float(msg["original_size"] or 0)
        pct    = (filled / total * 100) if total > 0 else 0

        print(f"\n[{t}] {icon} ORDER {msg['type']} — {msg['status']}")
        print(f"   order_id:   {msg['order_id']}")
        print(f"   side:       {msg['side']}  price: {msg['price']}  "
              f"type: {msg['order_type']}")
        print(f"   filled:     {filled}/{total} shares  ({pct:.1f}%)")
        print(f"   outcome:    {msg['outcome']}  asset: {str(msg['asset_id'] or '')[:20]}...")
        print(f"   market:     {msg['market']}")
        if msg.get("expiration"):
            print(f"   expires:    {msg['expiration']}")
        print(f"   timestamp:  {msg['timestamp']}")

    elif et == "trade":
        # Choose icon based on settlement status
        status_icon = {
            "MATCHED":   "🤝",
            "MINED":     "⛏️ ",
            "CONFIRMED": "✅",
            "RETRYING":  "🔁",
            "FAILED":    "💀",
        }
        icon = status_icon.get(msg["status"], "❓")

        print(f"\n[{t}] {icon} TRADE {msg['status']} — {msg['trader_side']}")
        print(f"   trade_id:   {msg['trade_id']}")
        print(f"   side:       {msg['side']}  price: {msg['price']}  "
              f"size: {msg['size']} shares")
        print(f"   outcome:    {msg['outcome']}  fee_bps: {msg['fee_rate_bps']}")
        print(f"   market:     {msg['market']}")
        if msg.get("transaction_hash"):
            print(f"   tx_hash:    {msg['transaction_hash']}")
        if msg.get("maker_orders"):
            print(f"   makers:     {len(msg['maker_orders'])} order(s) matched")
            for mo in msg["maker_orders"]:
                print(f"     - {mo.get('matched_amount')} shares @ "
                      f"{mo.get('price')} ({mo.get('side')})")
        print(f"   timestamp:  {msg['timestamp']}")

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

async def stream_user(api_key, secret, passphrase,
                      condition_ids=None, on_message=None):
    """
    Connect to the User Channel and stream order and trade events
    for the authenticated account.

    Parameters:
        api_key       - from client.creds.api_key after build_client()
        secret        - from client.creds.secret
        passphrase    - from client.creds.passphrase
        condition_ids - optional list of condition IDs to filter
        on_message    - optional callback, receives each parsed message dict.
                        If None, prints to terminal.

    Order event lifecycle:
        1. You place order → ORDER PLACEMENT (status: LIVE)
        2. Partial fill    → ORDER UPDATE    (size_matched increases)
        3a. Full fill      → ORDER UPDATE    (status: MATCHED)
        3b. You cancel     → ORDER CANCELLATION (status: CANCELED)

    Trade event lifecycle:
        1. Match off-chain → TRADE MATCHED
        2. Submitted tx    → TRADE MINED
        3. Confirmed       → TRADE CONFIRMED  ← final, money moved

    Runs forever until interrupted (Ctrl+C).
    """
    print(f"\nConnecting to User Channel...")
    if condition_ids:
        print(f"Filtering to markets: {condition_ids}")
    else:
        print(f"Listening to all your markets.")
    print()

    async with websockets.connect(USER_WS_URL) as ws:
        sub = build_subscription(api_key, secret, passphrase, condition_ids)
        await ws.send(sub)
        print("Authenticated and subscribed. Waiting for order/trade events...\n")

        hb_task = asyncio.create_task(heartbeat(ws))

        try:
            async for raw in ws:
                if raw == "PONG":
                    continue

                msg = parse_message(raw)
                if msg:
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
    import os
    from dotenv import load_dotenv
    from Order_client import build_client

    load_dotenv()

    print("Building authenticated client to get API credentials...")
    client = build_client()

    # Extract L2 credentials from the client
    api_key    = client.creds.api_key
    secret     = client.creds.secret
    passphrase = client.creds.passphrase

    print(f"API key: {api_key[:8]}...")

    try:
        asyncio.run(stream_user(api_key, secret, passphrase))
    except KeyboardInterrupt:
        print("\nStopped.")