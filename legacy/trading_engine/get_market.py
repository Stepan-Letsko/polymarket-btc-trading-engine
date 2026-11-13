import requests
import json
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# DEBUG FLAGS
# ─────────────────────────────────────────────
# RAW_RESPONSE      — print the raw Gamma API event + market JSON, then exit
# RAW_RESPONSE_FULL — same but dumps the complete event including all nested fields

RAW_RESPONSE      = False
RAW_RESPONSE_FULL = True


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"


# ─────────────────────────────────────────────
# GET THE CURRENT 5-MINUTE SLUG
# ─────────────────────────────────────────────

def get_current_slug():
    """
    Build the slug for the currently active BTC 5m market.
    Rounds current UTC time down to the nearest 5-minute boundary
    and formats it as: btc-updown-5m-{unix_timestamp}
    """
    now = datetime.now(timezone.utc)
    minutes = (now.minute // 5) * 5
    rounded = now.replace(minute=minutes, second=0, microsecond=0)
    unix_ts = int(rounded.timestamp())
    return f"btc-updown-5m-{unix_ts}"


# ─────────────────────────────────────────────
# FETCH THE MARKET
# ─────────────────────────────────────────────

def fetch_neg_risk_from_clob(token_id):
    """
    Fetch the authoritative neg_risk flag from the CLOB API for a token.
    The CLOB API is the source of truth for order signing parameters.
    Falls back to False if the request fails.
    """
    try:
        resp = requests.get(f"{CLOB_API}/markets/{token_id}", timeout=5)
        resp.raise_for_status()
        return bool(resp.json().get("neg_risk", False))
    except Exception:
        return False


def fetch_market(slug):
    """
    Query the Gamma API for the event matching this slug.
    Returns a dict of everything we need for trading.
    Returns None if the market is not found.
    """
    response = requests.get(f"{GAMMA_API}/events", params={"slug": slug})
    response.raise_for_status()

    events = response.json()
    if not events:
        return None

    event  = events[0]
    market = event["markets"][0]
    token_ids = json.loads(market["clobTokenIds"])

    token_up = token_ids[0]

    # Fetch neg_risk from CLOB API — Gamma API can return null/false even for
    # markets that the exchange treats as negRisk, causing order_version_mismatch.
    neg_risk = fetch_neg_risk_from_clob(token_up)

    return {
        "slug":         slug,
        "event_title":  event["title"],
        "end_date":     market["endDate"],
        "condition_id": market["conditionId"],
        "token_up":     token_up,
        "token_down":   token_ids[1],
        "best_bid":     market.get("bestBid"),
        "best_ask":     market.get("bestAsk"),
        "last_price":   market.get("lastTradePrice"),
        "liquidity":    market.get("liquidityNum"),
        "tick_size":    market.get("orderPriceMinTickSize"),
        "min_size":     market.get("orderMinSize"),
        "neg_risk":     neg_risk,
        "accepting":    market.get("acceptingOrders"),
    }


# ─────────────────────────────────────────────
# MAIN PUBLIC FUNCTION
# ─────────────────────────────────────────────

def get_current_market():
    """
    The main function other scripts will import and call.

    Usage from another file:
        from get_market import get_current_market
        market = get_current_market()
        print(market["token_up"])
        print(market["best_ask"])

    Returns a dict with all market info, or None if not found.
    """
    slug = get_current_slug()
    return fetch_market(slug)


# ─────────────────────────────────────────────
# PRINT HELPER
# ─────────────────────────────────────────────

def print_market(market):
    """
    Pretty print the market info. Used when running this file directly.
    """
    now = datetime.now(timezone.utc)
    print("\n" + "="*60)
    print("  CURRENT BTC 5M MARKET")
    print("="*60)
    print(f"  Title:            {market['event_title']}")
    print(f"  Ends at:          {market['end_date']}")
    print(f"  Current UTC:      {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"  Accepting orders: {market['accepting']}")
    print(f"  Condition ID:     {market['condition_id']}")
    print("-"*60)
    print(f"  Token UP:         {market['token_up']}")
    print(f"  Token DOWN:       {market['token_down']}")
    print("-"*60)
    print(f"  Best Bid:         {market['best_bid']}")
    print(f"  Best Ask:         {market['best_ask']}")
    print(f"  Last Price:       {market['last_price']}")
    print(f"  Liquidity:        ${market['liquidity']}")
    print("-"*60)
    print(f"  Tick Size:        {market['tick_size']}")
    print(f"  Min Order:        ${market['min_size']}")
    print(f"  Neg Risk:         {market['neg_risk']}")
    print("="*60 + "\n")


# ─────────────────────────────────────────────
# RUN DIRECTLY - prints to screen for testing
# ─────────────────────────────────────────────

# This block only runs when you execute: python3 get_market.py
# It does NOT run when another script imports this file
if __name__ == "__main__":
    slug = get_current_slug()
    print(f"Fetching current BTC 5m market...")
    print(f"Slug: {slug}\n")

    if RAW_RESPONSE or RAW_RESPONSE_FULL:
        resp = requests.get(f"{GAMMA_API}/events", params={"slug": slug})
        resp.raise_for_status()
        events = resp.json()
        if not events:
            print("ERROR: No event found for this slug.")
        else:
            event  = events[0]
            market = event["markets"][0] if event.get("markets") else {}
            if RAW_RESPONSE_FULL:
                print("=== RAW EVENT (full) ===")
                print(json.dumps(event, indent=2))
            else:
                print("=== RAW EVENT (top-level fields, markets excluded) ===")
                print(json.dumps({k: v for k, v in event.items() if k != "markets"}, indent=2))
                print("\n=== RAW MARKET (first market) ===")
                print(json.dumps(market, indent=2))
    else:
        market = get_current_market()
        if not market:
            print("ERROR: No market found. May be between windows or closed.")
        else:
            print_market(market)
