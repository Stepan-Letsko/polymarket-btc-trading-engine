import asyncio
import os
import sys
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

# trading_engine/ has no __init__.py — it's a folder of scripts, not an
# installable package. We add it to the import path so we can reuse
# each exchange feed's stream_X() function instead of duplicating that
# connection logic here.
ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, ENGINE_DIR)

from Binance_ws import stream_binance                  # noqa: E402
from Coinbase_ws import stream_coinbase                # noqa: E402
from rtds_ws import stream_rtds                        # noqa: E402
from clob_ws import stream_current_market              # noqa: E402
from twap import compute_twap                          # noqa: E402
from indicators import calc_obi, calc_trade_delta      # noqa: E402

# Every browser tab that has this page open gets one WebSocket connection
# in here. When a new price arrives we loop over this set and push it to
# everyone currently watching.
connected_clients: set[WebSocket] = set()


def broadcast(source: str, time_ms: float, price: float) -> None:
    payload = {
        "type": "tick",
        "source": source,
        # lightweight-charts wants whole seconds since 1970, not our
        # millisecond receive timestamp.
        "time": int(time_ms / 1000),
        "value": price,
    }
    for client in list(connected_clients):
        asyncio.create_task(_safe_send(client, payload))


def broadcast_boundary(price: float) -> None:
    payload = {"type": "boundary", "value": price}
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

# Latest Binance top-5 order book (for OBI) and a rolling buffer of
# recent trades (for trade-flow delta) — fed by on_binance() below, read
# by broadcast_indicators_forever() to compute indicators.py's pure
# calc_obi()/calc_trade_delta() functions periodically.
_binance_depth = {"bids": [], "asks": []}
_binance_trades: deque[tuple[float, float, str]] = deque()
_TRADE_BUFFER_MAX_AGE_MS = 5000  # comfortably more than any delta window we'd use


def on_binance(msg: dict) -> None:
    stream = msg.get("stream")
    if stream == "bookTicker" and msg.get("mid_price") is not None:
        broadcast("binance", msg["recv_ts"], msg["mid_price"])
    elif stream == "depth5":
        _binance_depth["bids"] = msg.get("bids") or []
        _binance_depth["asks"] = msg.get("asks") or []
    elif stream == "aggTrade":
        qty, direction, recv_ts = msg.get("quantity"), msg.get("direction"), msg.get("recv_ts")
        if qty is not None and direction and recv_ts is not None:
            _binance_trades.append((recv_ts, float(qty), direction))
            cutoff = recv_ts - _TRADE_BUFFER_MAX_AGE_MS
            while _binance_trades and _binance_trades[0][0] < cutoff:
                _binance_trades.popleft()


def on_coinbase(msg: dict) -> None:
    if msg.get("channel") == "ticker" and msg.get("mid_price") is not None:
        broadcast("coinbase", msg["recv_ts"], msg["mid_price"])


# Latest full order book snapshot per token side, for CLOB OBI. clob_ws.py
# sends a fresh "book" snapshot on subscribe and again after every trade —
# frequent enough that we don't need to maintain our own incrementally
# updated book from price_change deltas, just use whatever's most recent.
_clob_books = {"UP": {"bids": [], "asks": []}, "DOWN": {"bids": [], "asks": []}}


def on_clob(msg: dict) -> None:
    # clob_ws.py's stream_current_market() already handles fetching the
    # right market and rotating to a new one every 5 minutes.
    event_type = msg.get("event_type")

    if event_type == "book":
        side = msg.get("side")
        if side in ("UP", "DOWN"):
            _clob_books[side]["bids"] = msg.get("bids") or []
            _clob_books[side]["asks"] = msg.get("asks") or []
        return

    if event_type != "best_bid_ask":
        return
    # Picks out best_bid_ask updates and turns them into a single "price"
    # (the midpoint between best bid and best ask) per token.
    side = msg.get("side")
    best_bid, best_ask = msg.get("best_bid"), msg.get("best_ask")
    if side not in ("UP", "DOWN") or best_bid is None or best_ask is None:
        return
    mid_price = (float(best_bid) + float(best_ask)) / 2
    timestamp = msg.get("timestamp")
    time_ms = float(timestamp) if timestamp else time.time() * 1000
    broadcast("clob_up" if side == "UP" else "clob_down", time_ms, mid_price)


def _book_levels_to_pairs(levels):
    # clob_ws.py's book levels are {"price": ..., "size": ...} dicts;
    # calc_obi() expects (price, quantity) pairs like Binance_ws.py's
    # depth5 gives — adapt at the call site rather than inside calc_obi()
    # itself, so that stays a plain, format-agnostic pure function.
    return [(lvl.get("price"), lvl.get("size")) for lvl in levels]


async def broadcast_indicators_forever() -> None:
    while True:
        await asyncio.sleep(0.5)
        payload = {
            "type": "indicators",
            "obi_binance": calc_obi(_binance_depth["bids"], _binance_depth["asks"]),
            "delta_binance": calc_trade_delta(list(_binance_trades), window_ms=1000),
            "obi_clob_up": calc_obi(
                _book_levels_to_pairs(_clob_books["UP"]["bids"]),
                _book_levels_to_pairs(_clob_books["UP"]["asks"]),
            ),
            "obi_clob_down": calc_obi(
                _book_levels_to_pairs(_clob_books["DOWN"]["bids"]),
                _book_levels_to_pairs(_clob_books["DOWN"]["asks"]),
            ),
        }
        for client in list(connected_clients):
            asyncio.create_task(_safe_send(client, payload))


# Polymarket's BTC Up/Down markets run in fixed 5-minute (300 second)
# windows. Polymarket resolves each window against a 60-second Chainlink
# TWAP captured right as the window opens — the actual math for that
# lives in trading_engine/twap.py, shared with the real trading scripts
# rather than duplicated here.
#
# This tracks which window's start time we're currently in (not "did we
# just cross a boundary") — a state comparison rather than an edge
# trigger. That matters because a feed reconnect can occasionally
# redeliver a message, and an edge-triggered "did this tick cross the
# line" check would fire again for it; a state comparison just no-ops
# since we're already recorded as being in that window.
_current_window_start: float | None = None

# Rolling buffer of recent Chainlink ticks — (timestamp_seconds, price) —
# used to compute the 60s TWAP once a boundary is crossed. Kept slightly
# longer than the 60s window itself so there's always at least one tick
# from before the window starts to "carry in" as the window's opening
# price — see compute_twap()'s docstring in twap.py for why that matters.
_chainlink_ticks: deque[tuple[float, float]] = deque()
_TICK_BUFFER_MAX_AGE = 70.0  # seconds

# The boundary broadcast fires exactly once, at the instant a new window
# opens. If a browser's WebSocket happened to be mid-reconnect at that
# exact moment, it would simply miss that one message and never see it
# again until the next boundary, 5 minutes later. Keeping the current
# value here lets us hand it to a client immediately when they connect,
# instead of only broadcasting it once at the moment it's calculated.
_current_boundary_price: float | None = None


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
        _record_chainlink_tick(msg)
        _check_boundary(msg)


def _record_chainlink_tick(msg: dict) -> None:
    # source_ts is when Chainlink itself recorded this price — not when
    # Polymarket's relay forwarded it to us (rtds_ts). The relay delay
    # between those two isn't constant tick to tick, so weighting by
    # rtds_ts distorts how long each price is treated as having been in
    # effect, which directly throws off the TWAP. source_ts is what
    # Polymarket's own TWAP stream is presumably built from, so that's
    # what we weight by too.
    source_ts = msg.get("source_ts")
    ts = (source_ts / 1000) if source_ts else msg["recv_ts"] / 1000
    _chainlink_ticks.append((ts, msg["price"]))
    cutoff = ts - _TICK_BUFFER_MAX_AGE
    while _chainlink_ticks and _chainlink_ticks[0][0] < cutoff:
        _chainlink_ticks.popleft()


def _check_boundary(msg: dict) -> None:
    global _current_window_start, _current_boundary_price
    # rtds_ts (Polymarket's relay send time) drives crossing detection —
    # it reliably keeps advancing in real time, unlike source_ts
    # (Chainlink's own clock), which can stall for stretches and caused
    # P0 to sometimes not get captured at all when tried. The TWAP
    # calculation itself still weights by source_ts below — that's a
    # separate, more accuracy-sensitive use of the timestamp.
    rtds_ts = msg.get("rtds_ts")
    check_ts = (rtds_ts / 1000) if rtds_ts else msg["recv_ts"] / 1000
    window_start = int(check_ts // 300) * 300

    if _current_window_start is None:
        _current_window_start = window_start
        return

    if window_start != _current_window_start:
        # We're now in a different window than last time we checked —
        # this only runs once per real window no matter how many messages
        # arrive after the crossing, since window_start won't change
        # again until the next real boundary.
        _current_window_start = window_start

        # The TWAP window is anchored to this message's own source_ts
        # (Chainlink's clock), matching how _chainlink_ticks is
        # timestamped — not check_ts (rtds_ts) above, which is only used
        # to robustly detect the crossing. Right after a server restart,
        # the tick buffer may hold less than a full 60s of history —
        # compute_twap() just averages over whatever it has, which
        # degrades gracefully rather than failing outright.
        source_ts = msg.get("source_ts")
        twap_now = (source_ts / 1000) if source_ts else check_ts
        twap_price = compute_twap(list(_chainlink_ticks), twap_now - 60, twap_now)
        _current_boundary_price = twap_price if twap_price is not None else msg["price"]
        print(f"[P0] New 5m window opened, boundary price (60s TWAP): ${_current_boundary_price:,.2f}")
        broadcast_boundary(_current_boundary_price)


SILENCE_TIMEOUT = 30.0  # seconds with no message before we treat a feed as stuck


async def run_forever(name: str, stream_fn, on_message) -> None:
    """Any of these exchange connections can drop (network blip, laptop
    sleep, etc). Most failures raise an exception, which this catches and
    retries after a short pause. But a connection can also go silently
    stale — still "open" from our side, receiving nothing, without ever
    raising anything (observed once with the rtds/Chainlink feed during
    testing: it just stopped producing messages, no error, no drop
    message). To catch that case too, we track when the last message
    arrived and force a reconnect if it's been quiet too long."""
    while True:
        last_message_at = time.monotonic()

        def wrapped_on_message(msg):
            nonlocal last_message_at
            last_message_at = time.monotonic()
            on_message(msg)

        stream_task = asyncio.create_task(stream_fn(on_message=wrapped_on_message))
        try:
            while True:
                done, _ = await asyncio.wait({stream_task}, timeout=5)
                if stream_task in done:
                    stream_task.result()  # re-raises whatever killed it
                    break
                if time.monotonic() - last_message_at > SILENCE_TIMEOUT:
                    print(f"{name} feed silent for {SILENCE_TIMEOUT:.0f}s with no messages, forcing reconnect...")
                    stream_task.cancel()
                    break
        except Exception as exc:
            print(f"{name} feed dropped ({exc!r}), reconnecting in 2s...")
        finally:
            if not stream_task.done():
                stream_task.cancel()
            try:
                await stream_task
            except (asyncio.CancelledError, Exception):
                pass
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start every source's feed once, in the background, when the server
    # starts — not per browser tab that connects. All run at once,
    # regardless of which lines are currently visible in the browser.
    feeds = [
        ("binance", stream_binance, on_binance),
        ("coinbase", stream_coinbase, on_coinbase),
        ("rtds", stream_rtds, on_rtds),
        ("clob", stream_current_market, on_clob),
    ]
    for name, stream_fn, on_message in feeds:
        asyncio.create_task(run_forever(name, stream_fn, on_message))
    asyncio.create_task(broadcast_indicators_forever())
    yield


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/price")
async def price_socket(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)

    if _current_boundary_price is not None:
        # Catch this client up immediately, rather than making it wait
        # up to 5 minutes for the next boundary broadcast.
        await _safe_send(websocket, {"type": "boundary", "value": _current_boundary_price})
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
