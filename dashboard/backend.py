import asyncio
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

# trading_engine/ has no __init__.py — it's a folder of scripts, not an
# installable package. We add it to the import path so we can reuse
# each exchange feed's stream_X() function instead of duplicating that
# connection logic here.
ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, ENGINE_DIR)

from Binance_ws import stream_binance    # noqa: E402
from Coinbase_ws import stream_coinbase  # noqa: E402
from kraken_ws import stream_kraken      # noqa: E402
from Bitstamp_ws import stream_bitstamp  # noqa: E402
from rtds_ws import stream_rtds          # noqa: E402

# Every browser tab that has this page open gets one WebSocket connection
# in here. When a new price arrives we loop over this set and push it to
# everyone currently watching.
connected_clients: set[WebSocket] = set()


def broadcast(source: str, time_ms: float, price: float) -> None:
    payload = {
        "source": source,
        # lightweight-charts wants whole seconds since 1970, not our
        # millisecond receive timestamp.
        "time": int(time_ms / 1000),
        "value": price,
    }
    for client in list(connected_clients):
        asyncio.create_task(_safe_send(client, payload))


async def _safe_send(client: WebSocket, payload: dict) -> None:
    try:
        await client.send_json(payload)
    except Exception:
        # Client likely disconnected between the loop above and this send —
        # just drop it, no need to crash the whole feed over one dead tab.
        connected_clients.discard(client)


# ── Per-exchange message handlers ──────────────────────────────────
# Every exchange script in trading_engine/ hands back its own message
# shape (different field/type names), so each of these picks out "is
# this a price update, and what's the price" before calling broadcast().

def on_binance(msg: dict) -> None:
    if msg.get("stream") == "bookTicker" and msg.get("mid_price") is not None:
        broadcast("binance", msg["recv_ts"], msg["mid_price"])


def on_coinbase(msg: dict) -> None:
    if msg.get("channel") == "ticker" and msg.get("mid_price") is not None:
        broadcast("coinbase", msg["recv_ts"], msg["mid_price"])


def on_kraken(msg: dict) -> None:
    if msg.get("channel") == "ticker" and msg.get("mid_price") is not None:
        broadcast("kraken", msg["recv_ts"], msg["mid_price"])


def on_bitstamp(msg: dict) -> None:
    if msg.get("channel") == "book" and msg.get("mid_price") is not None:
        broadcast("bitstamp", msg["recv_ts"], msg["mid_price"])


def on_rtds(msg: dict) -> None:
    # rtds_ws.py carries two sources in one connection: Binance's price
    # relayed through Polymarket's own infrastructure, and Chainlink —
    # the actual oracle Polymarket uses to resolve these markets.
    # Chainlink is its own independent price (an aggregate across
    # multiple exchanges), not just a copy of the Binance number.
    #
    # Both topics are subscribed to with NO symbol filter (see
    # build_binance_subscription()/build_chainlink_subscription() in
    # rtds_ws.py) — they carry every coin Polymarket tracks, not just
    # BTC. Without filtering here, ETH/SOL/etc prices (often a few
    # cents to a few dollars) get plotted on the same axis as BTC
    # (~$100k+) and look like a flat line pinned near zero.
    if msg.get("price") is None or msg.get("recv_ts") is None:
        return
    if str(msg.get("symbol", "")).lower() not in ("btcusdt", "btc/usd"):
        return
    if msg.get("topic") == "crypto_prices":
        broadcast("binance_relay", msg["recv_ts"], msg["price"])
    elif msg.get("topic") == "crypto_prices_chainlink":
        broadcast("chainlink", msg["recv_ts"], msg["price"])


async def run_forever(name: str, stream_fn, on_message) -> None:
    """Any of these exchange connections can drop (network blip, laptop
    sleep, etc). This wraps each one in a retry loop so one dropped
    connection doesn't kill that source's line forever — it just
    reconnects after a short pause instead of giving up."""
    while True:
        try:
            await stream_fn(on_message=on_message)
        except Exception as exc:
            print(f"{name} feed dropped ({exc!r}), reconnecting in 2s...")
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start every source's feed once, in the background, when the server
    # starts — not per browser tab that connects. All 5 run at once,
    # regardless of which lines are currently visible in the browser.
    feeds = [
        ("binance", stream_binance, on_binance),
        ("coinbase", stream_coinbase, on_coinbase),
        ("kraken", stream_kraken, on_kraken),
        ("bitstamp", stream_bitstamp, on_bitstamp),
        ("rtds", stream_rtds, on_rtds),
    ]
    for name, stream_fn, on_message in feeds:
        asyncio.create_task(run_forever(name, stream_fn, on_message))
    yield


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/price")
async def price_socket(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        # We never expect the browser to send us anything on this socket —
        # this just keeps the connection open until the browser closes it.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(websocket)


# Serves index.html (and anything else in static/) for every other path.
# Registered last so it doesn't swallow the /ws/price route above.
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
