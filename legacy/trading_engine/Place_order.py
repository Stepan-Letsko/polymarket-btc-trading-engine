import asyncio
import os
import time
from dotenv import load_dotenv
from get_market import get_current_market
from Order_client import build_client, place_order
from User_ws import stream_user, parse_message, print_message

# ═════════════════════════════════════════════
# ✏️  CONFIGURE YOUR ORDER HERE
# ═════════════════════════════════════════════

# Which token to trade
# "UP"   = bet BTC goes UP in this 5 minute window
# "DOWN" = bet BTC goes DOWN in this 5 minute window
TRADE_SIDE_TOKEN = "UP"

# BUY  = open a new position (spend USDC to get tokens)
# SELL = close a position (sell tokens you already hold)
ORDER_SIDE = "BUY"

# Your limit price (probability between 0.01 and 0.99)
# Your order only fills at this price or better
# e.g. 0.30 means you pay 30 cents per share, win $1 if correct
PRICE = 0.85

# Number of shares (minimum is 5)
# Dollar cost = SIZE * PRICE
# e.g. 10 shares at 0.30 = $3.00
SIZE = 5

# Order type:
# "GTC" = Good Till Cancel  — rests on book until filled or you cancel
# "GTD" = Good Till Date    — auto-cancels at EXPIRY_SECONDS from now
# "FOK" = Fill Or Kill      — fill entirely right now or cancel immediately
# "FAK" = Fill And Kill     — fill what's available now, cancel the rest
ORDER_TYPE = "FAK"

# Only used for GTD orders — how many seconds from now before auto-cancel
# e.g. 240 = cancel in 4 minutes
EXPIRY_SECONDS = 240

# ═════════════════════════════════════════════
# DO NOT EDIT BELOW THIS LINE
# ═════════════════════════════════════════════

load_dotenv()

# Tracks the order ID once placed so we can filter user channel events
placed_order_id = None


def handle_user_event(msg):
    """
    Callback for the User Channel WebSocket.
    Receives every order and trade event for our account.
    Filters to only show events related to the order we placed.
    """
    if not msg:
        return

    et = msg.get("event_type")

    # ORDER events — order status changes
    if et == "order":
        order_id = msg.get("order_id")

        # Only print if this is our order
        if placed_order_id and order_id != placed_order_id:
            return

        t        = msg["time"]
        otype    = msg.get("type")    # PLACEMENT, UPDATE, CANCELLATION
        status   = msg.get("status") # LIVE, MATCHED, CANCELED
        filled   = float(msg.get("size_matched") or 0)
        total    = float(msg.get("original_size") or 0)
        pct      = (filled / total * 100) if total > 0 else 0

        icon_map = {
            "PLACEMENT":    "📋",
            "UPDATE":       "🔄",
            "CANCELLATION": "❌",
        }
        icon = icon_map.get(otype, "❓")

        print(f"\n[{t}] {icon} ORDER {otype} — {status}")
        print(f"   order_id:  {order_id}")
        print(f"   side:      {msg.get('side')}  "
              f"price: {msg.get('price')}  "
              f"type: {msg.get('order_type')}")
        print(f"   filled:    {filled}/{total} shares  ({pct:.1f}%)")
        print(f"   outcome:   {msg.get('outcome')}")
        if msg.get("expiration"):
            print(f"   expires:   {msg.get('expiration')}")

        # Alert on key state changes
        if status == "MATCHED":
            print(f"\n   🎯 ORDER FULLY FILLED at {msg.get('price')}")
        elif otype == "CANCELLATION":
            print(f"\n   ❌ ORDER CANCELLED — unfilled: {total - filled} shares")

    # TRADE events — actual execution and settlement
    elif et == "trade":
        # Filter to our order if we have an ID
        taker_id = msg.get("taker_order_id")
        if placed_order_id and taker_id != placed_order_id:
            # Also check maker orders
            maker_ids = [mo.get("order_id") for mo in msg.get("maker_orders", [])]
            if placed_order_id not in maker_ids:
                return

        t      = msg["time"]
        status = msg.get("status")

        status_icon = {
            "MATCHED":   "🤝",
            "MINED":     "⛏️ ",
            "CONFIRMED": "✅",
            "RETRYING":  "🔁",
            "FAILED":    "💀",
        }
        icon = status_icon.get(status, "❓")

        print(f"\n[{t}] {icon} TRADE {status} — {msg.get('trader_side')}")
        print(f"   trade_id:  {msg.get('trade_id')}")
        print(f"   executed:  {msg.get('size')} shares @ {msg.get('price')}")
        print(f"   outcome:   {msg.get('outcome')}  fee_bps: {msg.get('fee_rate_bps')}")

        if msg.get("transaction_hash"):
            print(f"   tx_hash:   {msg.get('transaction_hash')}")
            print(f"   view on Polygonscan: "
                  f"https://polygonscan.com/tx/{msg.get('transaction_hash')}")

        if status == "CONFIRMED":
            print(f"\n   ✅ TRADE FULLY CONFIRMED ON-CHAIN")
            cost = float(msg.get("size", 0)) * float(msg.get("price", 0))
            print(f"   Cost: {cost:.4f} USDC  "
                  f"({msg.get('size')} shares x {msg.get('price')})")

        if status == "FAILED":
            print(f"\n   💀 TRADE FAILED ON-CHAIN — check your wallet")

        if msg.get("maker_orders"):
            print(f"   makers:    {len(msg['maker_orders'])} order(s) matched")
            for mo in msg["maker_orders"]:
                print(f"     → {mo.get('matched_amount')} shares @ "
                      f"{mo.get('price')} ({mo.get('side')})")


async def main():
    global placed_order_id

    # ── Step 1: Build authenticated client ──
    print("Building client...")
    client = build_client()
    print("Client ready.\n")

    # ── Step 2: Fetch current market ──
    print("Fetching current BTC 5m market...")
    market = get_current_market()

    if not market:
        print("ERROR: No active market found. Exiting.")
        return

    print(f"Market:    {market['event_title']}")
    print(f"Ends:      {market['end_date']}")
    print(f"UP ask:    {market['best_ask']} (current best ask)")
    print(f"DOWN ask:  N/A (use clob_ws.py to check)")
    print(f"Neg risk:  {market['neg_risk']}")
    print()

    # ── Step 3: Select token based on config ──
    token_id = (
        market["token_up"]
        if TRADE_SIDE_TOKEN == "UP"
        else market["token_down"]
    )

    # ── Step 4: Print order summary ──
    cost = SIZE * PRICE
    print("=" * 50)
    print("  ORDER SUMMARY")
    print("=" * 50)
    print(f"  Market:     {market['event_title']}")
    print(f"  Token:      {TRADE_SIDE_TOKEN}  ({token_id[:20]}...)")
    print(f"  Side:       {ORDER_SIDE}")
    print(f"  Price:      {PRICE}  ({PRICE*100:.1f}% implied probability)")
    print(f"  Size:       {SIZE} shares")
    print(f"  Cost:       ${cost:.2f} USDC")
    print(f"  Type:       {ORDER_TYPE}")
    if ORDER_TYPE == "GTD":
        expiry = int(time.time()) + EXPIRY_SECONDS
        print(f"  Expires:    in {EXPIRY_SECONDS}s  (unix: {expiry})")
    print("=" * 50)
    print()

    # ── Step 5: Confirm before placing ──
    confirm = input("Place this order? (yes/no): ").strip().lower()
    if confirm not in ("yes", "y"):
        print("Order cancelled by user.")
        return

    # ── Step 6: Place the order ──
    print("\nPlacing order...")

    kwargs = dict(
        client     = client,
        token_id   = token_id,
        side       = ORDER_SIDE,
        price      = PRICE,
        size       = SIZE,
        order_type = ORDER_TYPE,
        neg_risk   = market["neg_risk"],
        tick_size  = str(market["tick_size"]),
    )

    # Add expiration for GTD
    if ORDER_TYPE == "GTD":
        kwargs["expiration"] = int(time.time()) + EXPIRY_SECONDS

    response = place_order(**kwargs)

    if not response:
        print("ERROR: Order placement failed. Check your balance and credentials.")
        return

    placed_order_id = response.get("orderID")
    status          = response.get("status")

    print(f"\n✅ Order placed successfully!")
    print(f"   Order ID: {placed_order_id}")
    print(f"   Status:   {status}")
    print()

    # ── Step 7: Stream user channel for live updates ──
    print("Listening for order and trade updates...")
    print("(Press Ctrl+C to stop)\n")

    api_key    = client.creds.api_key
    secret     = client.creds.api_secret
    passphrase = client.creds.api_passphrase

    # Filter user channel to only this market
    await stream_user(
        api_key       = api_key,
        secret        = secret,
        passphrase    = passphrase,
        condition_ids = [market["condition_id"]],
        on_message    = handle_user_event,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")