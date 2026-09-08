import os
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import OrderArgs, OrderType, OpenOrderParams, OrderPayload
from py_clob_client_v2.order_builder.constants import BUY, SELL
from py_clob_client_v2.clob_types import ApiCreds

# ─────────────────────────────────────────────
# SETUP
# Before using this file you need:
#
# 1. Install the SDK:
#    pip3 install py-clob-client python-dotenv
#
# 2. Your .env file should contain:
#    PRIVATE_KEY=0xyour_private_key_here
#    FUNDER_ADDRESS=0xyour_wallet_address_here
#    POLY_API_KEY=your_api_key_here
#    POLY_SECRET=your_secret_here
#    POLY_PASSPHRASE=your_passphrase_here
#
# 3. Never commit your .env file to GitHub.
# ─────────────────────────────────────────────

HOST     = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet


# ─────────────────────────────────────────────
# BUILD THE CLIENT
# ─────────────────────────────────────────────

def build_client():
    """
    Initialise and return an authenticated Polymarket CLOB client.

    Loads credentials directly from your .env file.
    Uses your existing POLY_API_KEY, POLY_SECRET, POLY_PASSPHRASE
    rather than re-deriving them from the private key each time.

    Required .env variables:
        PRIVATE_KEY      — your Ethereum private key (for signing orders)
        FUNDER_ADDRESS   — your Polygon wallet address (holds your USDC.e)
        POLY_API_KEY     — your L2 API key
        POLY_SECRET      — your L2 API secret
        POLY_PASSPHRASE  — your L2 API passphrase
    """
    private_key    = os.getenv("PRIVATE_KEY")
    funder_address = os.getenv("FUNDER_ADDRESS")
    api_key        = os.getenv("POLY_API_KEY")
    secret         = os.getenv("POLY_SECRET")
    passphrase     = os.getenv("POLY_PASSPHRASE")

    # Check all required variables are present
    missing = []
    if not private_key:    missing.append("PRIVATE_KEY")
    if not funder_address: missing.append("FUNDER_ADDRESS")
    if not api_key:        missing.append("POLY_API_KEY")
    if not secret:         missing.append("POLY_SECRET")
    if not passphrase:     missing.append("POLY_PASSPHRASE")

    if missing:
        raise ValueError(
            f"Missing required .env variables: {', '.join(missing)}\n"
            f"Make sure your .env file contains all five variables."
        )

    # Build API credentials object from existing L2 credentials
    creds = ApiCreds(
        api_key    = api_key,
        api_secret = secret,
        api_passphrase = passphrase,
    )

    # Build the authenticated client
    client = ClobClient(
        HOST,
        key            = private_key,
        chain_id       = CHAIN_ID,
        creds          = creds,
        signature_type = 1,             # 1 = POLY_PROXY (Required for Google/Email accounts)
        funder         = funder_address,
    )

    return client


# ─────────────────────────────────────────────
# PLACE ORDER
# ─────────────────────────────────────────────

def place_order(
    client,
    token_id,
    side,
    price,
    size,
    order_type = "GTC",
    neg_risk   = False,
    tick_size  = "0.01",
    expiration = None,
    post_only  = False,
):
    """
    Place an order on the Polymarket CLOB.

    Parameters:
        client     - authenticated ClobClient from build_client()
        token_id   - the UP or DOWN token ID from get_market()
        side       - "BUY" or "SELL"
        price      - probability price between 0.01 and 0.99
                     e.g. 0.50 means paying 50 cents per share to win $1
        size       - number of SHARES to buy/sell (minimum is 5 shares)
                     dollar cost = size * price
                     e.g. 10 shares at 0.30 = $3.00
        order_type - one of:
                     "GTC" Good Till Cancel — rests on book until filled or cancelled
                     "GTD" Good Till Date   — rests until expiration timestamp
                     "FOK" Fill Or Kill     — fill entirely right now or cancel
                     "FAK" Fill And Kill    — fill what's available, cancel the rest
        neg_risk   - from market data, almost always False for BTC markets
        tick_size  - minimum price increment from market data (usually "0.01")
        expiration - Unix timestamp in seconds, only required for GTD orders

    Returns:
        response dict with orderID and status if successful
        None if failed
    """
    from py_clob_client_v2.clob_types import PartialCreateOrderOptions

    order_type_map = {
        "GTC": OrderType.GTC,
        "GTD": OrderType.GTD,
        "FOK": OrderType.FOK,
        "FAK": OrderType.FAK,
    }

    if order_type not in order_type_map:
        raise ValueError(
            f"Invalid order_type '{order_type}'. "
            f"Must be one of: GTC, GTD, FOK, FAK"
        )

    side_map = {"BUY": BUY, "SELL": SELL}
    if side not in side_map:
        raise ValueError(f"Invalid side '{side}'. Must be BUY or SELL")

    order_args = OrderArgs(
        token_id   = token_id,
        price      = price,
        size       = size,
        side       = side_map[side],
        expiration = expiration if expiration else 0,
    )

    # options must be a PartialCreateOrderOptions object, not a dict
    options = PartialCreateOrderOptions(
        tick_size = tick_size,
        neg_risk  = neg_risk,
    )

    try:
        # Step 1: build and sign the order locally
        signed_order = client.create_order(order_args, options)
        # Step 2: post it to the CLOB with the order type
        response = client.post_order(signed_order, order_type_map[order_type], post_only)
        return response
    except Exception as e:
        print(f"[order_client] Order failed: {e}")
        return None


# ─────────────────────────────────────────────
# CANCEL ORDER
# ─────────────────────────────────────────────

def cancel_order(client, order_id):
    """
    Cancel a single open order by its order ID.
    """
    try:
        return client.cancel_order(OrderPayload(orderID=order_id))
    except Exception as e:
        print(f"[order_client] Cancel failed: {e}")
        return None


def cancel_all_orders(client):
    """
    Cancel ALL open orders across all markets.
    Use with caution — this cancels everything.
    """
    try:
        return client.cancel_all()
    except Exception as e:
        print(f"[order_client] Cancel all failed: {e}")
        return None


# ─────────────────────────────────────────────
# QUERY ORDERS
# ─────────────────────────────────────────────

def get_open_orders(client, market=None, token_id=None):
    """
    Get your currently open (unfilled) orders.

    Parameters:
        market   - optional condition ID to filter by market
        token_id - optional token ID to filter by specific token
    """
    try:
        params = OpenOrderParams()
        if market:
            params.market = market
        if token_id:
            params.asset_id = token_id
        return client.get_open_orders(params)
    except Exception as e:
        print(f"[order_client] Get orders failed: {e}")
        return []


def get_order(client, order_id):
    """
    Get the current status and details of a specific order by ID.
    """
    try:
        return client.get_order(order_id)
    except Exception as e:
        print(f"[order_client] Get order failed: {e}")
        return None


# ─────────────────────────────────────────────
# ACCOUNT INFO
# ─────────────────────────────────────────────

def get_balance(client):
    """
    Get your current USDC.e balance available for trading.
    """
    try:
        return client.get_balance_allowance()
    except Exception as e:
        print(f"[order_client] Get balance failed: {e}")
        return None


# ─────────────────────────────────────────────
# RUN DIRECTLY — test connection and credentials
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("Building authenticated client...")
    client = build_client()
    print("Client ready.\n")

    print("Checking balance...")
    balance = get_balance(client)
    print(f"Available balance: ${balance}\n")

    print("Checking open orders...")
    orders = get_open_orders(client)
    print(f"Open orders: {len(orders)}")
    for o in orders:
        print(f"  {o}")
    print("\nAll good — credentials working correctly.")