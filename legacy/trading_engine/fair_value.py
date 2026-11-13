import asyncio
import math
import threading
import time
import requests
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime, timezone
from collections import deque

from Binance_ws import stream_binance
from rtds_ws    import stream_rtds
from clob_ws    import stream_market
from get_market import get_current_market

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

MAX_POINTS           = 10000
VIEW_WINDOW          = 30    # seconds of history shown on chart
SPREAD_WINDOW        = 30    # number of Chainlink ticks used for rolling spread mean
PRICE_EMA_ALPHA      = 0.05  # EMA smoothing on the spread-adjusted price input
                              # lower = smoother but more lag (0.05 ≈ 20-tick average)

# ── Klines-based σ config ────────────────────
KLINES_SYMBOL        = "BTCUSDT"
KLINES_INTERVAL      = "5m"
KLINES_NUM_BARS      = 3     # number of completed 5-min bars to use (~15 min of history)
KLINES_SIGMA_FALLBACK = 5.0  # $/√s fallback if the fetch fails or is blocked

# ─────────────────────────────────────────────
# SHARED DATA STORE
# ─────────────────────────────────────────────

binance_times    = deque(maxlen=MAX_POINTS)
binance_prices   = deque(maxlen=MAX_POINTS)

chainlink_times  = deque(maxlen=MAX_POINTS)
chainlink_prices = deque(maxlen=MAX_POINTS)

up_times    = deque(maxlen=MAX_POINTS)
up_prices   = deque(maxlen=MAX_POINTS)
down_times  = deque(maxlen=MAX_POINTS)
down_prices = deque(maxlen=MAX_POINTS)

fair_up_times    = deque(maxlen=MAX_POINTS)
fair_up_prices   = deque(maxlen=MAX_POINTS)
fair_down_times  = deque(maxlen=MAX_POINTS)
fair_down_prices = deque(maxlen=MAX_POINTS)

oracle_price       = None  # P0: Chainlink BTC captured at the 5-min boundary
_oracle_boundary   = None  # which boundary timestamp set the current oracle_price
_chainlink_current = None  # most recent Chainlink tick
_binance_current   = None  # most recent Binance mid price
_price_ema         = None  # EMA of (binance - spread): the smoothed price fed to fair value
_next_boundary     = None  # unix timestamp of the next 5-min boundary
spread_history     = deque(maxlen=SPREAD_WINDOW)  # rolling (Binance - Chainlink) at each CL tick; only last sample is used for correction
_sigma_5m          = KLINES_SIGMA_FALLBACK  # $/√s — refreshed each window from Binance klines

data_lock = threading.Lock()


def _compute_next_boundary():
    now_ts = datetime.now(timezone.utc).timestamp()
    return (int(now_ts // 300) + 1) * 300


# ─────────────────────────────────────────────
# MESSAGE HANDLERS (price feeds)
# ─────────────────────────────────────────────

def handle_binance(msg):
    global _binance_current, _price_ema
    if msg.get("stream") != "bookTicker":
        return
    try:
        bid = float(msg["best_bid"])
        ask = float(msg["best_ask"])
        mid = (bid + ask) / 2
        now = datetime.now(timezone.utc).timestamp()
        _binance_current = mid
        with data_lock:
            binance_times.append(now)
            binance_prices.append(mid)

        # Update the EMA of the spread-adjusted price
        raw_estimate = get_spread_adjusted_price()
        if raw_estimate is not None:
            if _price_ema is None:
                _price_ema = raw_estimate  # cold start: seed with first value
            else:
                _price_ema = PRICE_EMA_ALPHA * raw_estimate + (1.0 - PRICE_EMA_ALPHA) * _price_ema

        # Recompute fair value at Binance frequency using the smoothed price
        fv_up   = get_fair_value("UP")
        fv_down = get_fair_value("DOWN")
        if fv_up is not None and fv_down is not None:
            with data_lock:
                fair_up_times.append(now)
                fair_up_prices.append(fv_up)
                fair_down_times.append(now)
                fair_down_prices.append(fv_down)
    except Exception:
        pass


# ─────────────────────────────────────────────
# FAIR VALUE MODEL
# ─────────────────────────────────────────────

def norm_cdf(x):
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def seconds_remaining():
    now_dt = datetime.now(timezone.utc)
    return 300 - ((now_dt.minute % 5) * 60 + now_dt.second)


def get_realized_vol_per_sec():
    """Returns the klines-calibrated σ in $/√sec. Refreshed each window."""
    return _sigma_5m


def fetch_sigma_from_klines():
    """
    Fetch the last KLINES_NUM_BARS completed 5-minute candles from Binance and
    compute realized vol in $/√sec.

    Method: std dev of (close - open) across completed bars, scaled to $/√s.
      σ_per_bar  = std( close_i - open_i )   [dollar move per 5-min bar]
      σ_per_sec  = σ_per_bar / √300           [scale to per-second units]

    Falls back to KLINES_SIGMA_FALLBACK if the request fails or is geo-blocked.
    """
    global _sigma_5m
    try:
        resp = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol":   KLINES_SYMBOL,
                "interval": KLINES_INTERVAL,
                "limit":    KLINES_NUM_BARS + 1,  # +1 so we can drop the current incomplete bar
            },
            timeout=5,
        )
        resp.raise_for_status()
        candles = resp.json()

        # Drop the last candle — it's the currently open (incomplete) bar
        completed = candles[:-1]
        if len(completed) < 2:
            print(f"[fair_value] ⚠️  Klines: not enough completed bars ({len(completed)}), keeping σ={_sigma_5m:.3f}")
            return

        n             = len(completed)
        hl_sq         = [(float(c[2]) - float(c[3])) ** 2 for c in completed]
        sigma_per_bar = math.sqrt(sum(hl_sq) / (4 * n * math.log(2)))
        sigma_per_sec = sigma_per_bar / math.sqrt(300)

        _sigma_5m = sigma_per_sec
        print(f"[fair_value] σ refreshed from {n} klines (Parkinson): "
              f"σ_bar=${sigma_per_bar:.2f}  σ_sec=${sigma_per_sec:.4f}/√s")

    except Exception as e:
        print(f"[fair_value] ⚠️  Klines fetch failed ({e}), keeping σ={_sigma_5m:.3f}")



def get_spread_adjusted_price():
    """
    Raw spread-adjusted estimate: current Binance minus rolling mean spread.
    Falls back to the last raw Chainlink tick when spread history is empty.
    """
    if _binance_current is None:
        return _chainlink_current
    if not spread_history:
        return _chainlink_current
    mean_spread = sum(spread_history) / len(spread_history)
    return _binance_current - mean_spread


def get_fair_value(side, smoothed=False):
    """
    Brownian-motion fair probability: Φ( D / (σ × √T) )

      D  = current − P0
      σ  = realized vol in $/√sec  (from recent Binance klines)
      T  = seconds remaining in the 5-min window

    smoothed=True  → uses _price_ema (20-tick average, ~2s lag, good for charting)
    smoothed=False → uses raw spread-adjusted Binance price (instant, good for trading signals)

    Returns a probability in [0, 1] or None if inputs are not yet available.
    """
    p0 = oracle_price
    if smoothed:
        current = _price_ema if _price_ema is not None else get_spread_adjusted_price()
    else:
        current = get_spread_adjusted_price()
    if p0 is None or current is None:
        return None

    T = seconds_remaining()
    if T <= 1:
        return None

    sigma = get_realized_vol_per_sec()
    if sigma <= 0:
        return None

    z       = (current - p0) / (sigma * math.sqrt(T))
    fair_up = norm_cdf(z)
    return fair_up if side == "UP" else (1.0 - fair_up)


# ─────────────────────────────────────────────
# MESSAGE HANDLERS
# ─────────────────────────────────────────────

def handle_rtds(msg):
    global _chainlink_current, _next_boundary, oracle_price, _oracle_boundary

    if msg.get("source") != "CHAINLINK" or msg.get("symbol") != "btc/usd":
        return
    price = msg.get("price")
    if not price:
        return

    chainlink_price = float(price)
    now = datetime.now(timezone.utc).timestamp()

    # Use Chainlink's own source timestamp for boundary comparison — this is
    # what Polymarket's resolver uses, not our local receive time.
    src_ts = msg.get("source_ts")
    chainlink_ts = (src_ts / 1000) if src_ts else now

    with data_lock:
        chainlink_times.append(chainlink_ts)
        chainlink_prices.append(chainlink_price)

    # Initialise boundary tracker on first tick
    if _next_boundary is None:
        _next_boundary = _compute_next_boundary()

    # First Chainlink tick whose source timestamp crosses the 5-min boundary
    if chainlink_ts >= _next_boundary:
        oracle_price    = chainlink_price
        _oracle_boundary = _next_boundary
        print(f"[fair_value] P0 set: ${oracle_price:,.2f} (Chainlink src_ts={chainlink_ts:.3f})")
        _next_boundary = _compute_next_boundary()
    elif oracle_price is None:
        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts > _next_boundary + 15:
            oracle_price    = chainlink_price
            _oracle_boundary = _next_boundary
            print(f"[fair_value] ⚠️  P0 fallback (boundary tick missed): ${oracle_price:,.2f}")
            _next_boundary = _compute_next_boundary()

    _chainlink_current = chainlink_price

    # Record spread at this Chainlink tick so get_spread_adjusted_price() stays calibrated
    if _binance_current is not None:
        spread_history.append(_binance_current - chainlink_price)


def handle_clob(msg):
    event_type = msg.get("event_type")
    side       = msg.get("side")
    if side not in ("UP", "DOWN"):
        return

    bid = ask = None
    if event_type == "book":
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        if bids: bid = float(bids[0]["price"])
        if asks: ask = float(asks[0]["price"])
    elif event_type in ("price_change", "best_bid_ask"):
        b = msg.get("best_bid")
        a = msg.get("best_ask")
        if b is not None: bid = float(b)
        if a is not None: ask = float(a)

    if bid is None and ask is None:
        return

    price = (bid + ask) / 2 if bid is not None and ask is not None else (bid or ask)
    now   = datetime.now(timezone.utc).timestamp()

    with data_lock:
        if side == "UP":
            up_times.append(now)
            up_prices.append(price)
        else:
            down_times.append(now)
            down_prices.append(price)


# ─────────────────────────────────────────────
# BACKGROUND FEEDS
# ─────────────────────────────────────────────

def start_feeds():
    async def run_all():
        # Persistent feeds — never restart between windows
        asyncio.create_task(stream_binance(on_message=handle_binance))
        asyncio.create_task(stream_rtds(on_message=handle_rtds))

        # CLOB — restarts every 5 minutes with fresh token IDs
        current_slug = None
        while True:
            market = get_current_market()
            if not market:
                print("[fair_value] No market found — retrying in 5s...")
                await asyncio.sleep(5)
                continue

            new_window = market["slug"] != current_slug
            if new_window:
                current_slug = market["slug"]
                print(f"[fair_value] Window: {market['event_title']}")

                # Refresh σ from Binance klines — runs in executor so it doesn't block the loop
                await asyncio.get_event_loop().run_in_executor(None, fetch_sigma_from_klines)

                # Only clear P0 if it was set at a previous window's boundary.
                # The boundary tick arrives ~2s before the window rolls, so oracle_price
                # often already holds the correct P0 for the new window — keep it.
                global oracle_price, _oracle_boundary
                current_window_start = int(datetime.now(timezone.utc).timestamp() // 300) * 300
                if _oracle_boundary != current_window_start:
                    oracle_price = None
                    print(f"[fair_value] P0 cleared — waiting for boundary tick")
                else:
                    print(f"[fair_value] P0 retained: ${oracle_price:,.2f} (set at this window's boundary)")

                # Clear token prices and fair value history for the new window
                # (Chainlink history is kept so vol estimate stays warm)
                with data_lock:
                    up_times.clear();       up_prices.clear()
                    down_times.clear();     down_prices.clear()
                    fair_up_times.clear();  fair_up_prices.clear()
                    fair_down_times.clear(); fair_down_prices.clear()

            now_dt    = datetime.now(timezone.utc)
            remaining = 300 - ((now_dt.minute % 5) * 60 + now_dt.second)

            try:
                await asyncio.wait_for(
                    stream_market(market["token_up"], market["token_down"],
                                  on_message=handle_clob),
                    timeout=remaining + 2,
                )
            except asyncio.TimeoutError:
                print("[fair_value] Window ended — rolling to next market...")
            except Exception as e:
                print(f"[fair_value] CLOB error: {e} — reconnecting...")
                await asyncio.sleep(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_all())


# ─────────────────────────────────────────────
# LIVE CHART
# ─────────────────────────────────────────────

fig, ax1 = plt.subplots(figsize=(14, 7))
ax2 = ax1.twinx()

ax1.set_zorder(1)
ax2.set_zorder(2)
ax1.patch.set_visible(False)

fig.patch.set_facecolor("#0f0f0f")
ax2.set_facecolor("#0f0f0f")

# Left axis — BTC prices
line_binance,   = ax1.plot([], [], color="#f0b429", linewidth=1.5,
                            label="Binance direct", drawstyle="steps-post", zorder=3)
line_chainlink, = ax1.plot([], [], color="#7c5cbf", linewidth=2,
                            label="Chainlink oracle", drawstyle="steps-post", zorder=3)
line_oracle,    = ax1.plot([], [], color="white", linewidth=1.2, linestyle="--",
                            label="Polymarket P0", zorder=4, alpha=0.65)
text_oracle = ax1.text(4.5, 0, "", color="white", fontsize=8, va="center",
                        ha="right", fontfamily="monospace", alpha=0.75,
                        bbox=dict(boxstyle="round,pad=0.2", facecolor="#1a1a1a",
                                  edgecolor="#555555", alpha=0.7))

# Right axis — token mid prices and fair value
line_up,   = ax2.plot([], [], color="#00ff88", linewidth=2.5,
                       label="UP price (Mid)", drawstyle="steps-post", zorder=5)
line_down, = ax2.plot([], [], color="#ff4d4d", linewidth=2.5,
                       label="DOWN price (Mid)", linestyle="--", drawstyle="steps-post", zorder=5)
line_fair_up,   = ax2.plot([], [], color="#00ff88", linewidth=1.2, linestyle=":",
                            label="Fair value UP", drawstyle="steps-post", zorder=4, alpha=0.8)
line_fair_down, = ax2.plot([], [], color="#ff4d4d", linewidth=1.2, linestyle=":",
                            label="Fair value DOWN", drawstyle="steps-post", zorder=4, alpha=0.8)

# ── Axis styling ──
ax1.set_title("Fair Value Model — Chainlink BTC vs Polymarket Token Prices",
              color="white", fontsize=14, fontweight="bold", pad=15)
ax1.set_xlabel("Seconds ago", color="#aaaaaa")
ax1.set_ylabel("BTC Price (USD)", color="#7c5cbf")
ax1.tick_params(colors="#aaaaaa")
ax1.tick_params(axis="y", labelcolor="#7c5cbf")
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
for spine in ax1.spines.values():
    spine.set_color("#333333")

ax2.set_ylabel("Token Price (Mid)", color="#aaaaaa")
ax2.tick_params(axis="y", labelcolor="#aaaaaa")
ax2.set_ylim(0, 1)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}"))
for spine in ax2.spines.values():
    spine.set_color("#333333")

ax2.axhline(y=0.5, color="#444444", linewidth=0.8, linestyle="--")

lines  = [line_binance, line_chainlink, line_oracle, line_up, line_down, line_fair_up, line_fair_down]
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="lower left", facecolor="#1a1a1a",
           edgecolor="#333333", labelcolor="white", fontsize=9)

# ── Price annotations ──
text_binance   = ax1.text(0.01, 0.97, "", transform=ax1.transAxes,
                           color="#f0b429", fontsize=9, va="top", fontfamily="monospace")
text_chainlink = ax1.text(0.01, 0.91, "", transform=ax1.transAxes,
                           color="#7c5cbf", fontsize=9, va="top", fontfamily="monospace")
text_up        = ax1.text(0.01, 0.85, "", transform=ax1.transAxes,
                           color="#00ff88", fontsize=9, va="top", fontfamily="monospace")
text_down      = ax1.text(0.01, 0.79, "", transform=ax1.transAxes,
                           color="#ff4d4d", fontsize=9, va="top", fontfamily="monospace")
text_fair_up   = ax1.text(0.01, 0.73, "", transform=ax1.transAxes,
                           color="#00cc66", fontsize=9, va="top",
                           fontfamily="monospace", alpha=0.85)
text_fair_down = ax1.text(0.01, 0.67, "", transform=ax1.transAxes,
                           color="#cc3333", fontsize=9, va="top",
                           fontfamily="monospace", alpha=0.85)
text_vol       = ax1.text(0.01, 0.61, "", transform=ax1.transAxes,
                           color="#aaaaaa", fontsize=9, va="top", fontfamily="monospace")
text_p0        = ax1.text(0.99, 0.97, "", transform=ax1.transAxes,
                           color="#aaaaaa", fontsize=9, va="top",
                           ha="right", fontfamily="monospace")


def animate(_):
    p0 = oracle_price  # single writer — no lock needed

    with data_lock:
        b_times   = list(binance_times)
        b_prices  = list(binance_prices)
        c_times   = list(chainlink_times)
        c_prices  = list(chainlink_prices)
        u_times   = list(up_times)
        u_prices  = list(up_prices)
        d_times   = list(down_times)
        d_prices  = list(down_prices)
        fu_times  = list(fair_up_times)
        fu_prices = list(fair_up_prices)
        fd_times  = list(fair_down_times)
        fd_prices = list(fair_down_prices)

    now = datetime.now(timezone.utc).timestamp()

    def to_plot(ts_list, ps_list):
        if not ts_list:
            return [], []
        xs = [t - now for t in ts_list]
        ys = list(ps_list)
        xs.append(0); ys.append(ys[-1])  # extend to 'now'
        return xs, ys

    bx,  by  = to_plot(b_times,  b_prices)
    cx,  cy  = to_plot(c_times,  c_prices)
    ux,  uy  = to_plot(u_times,  u_prices)
    dx,  dy  = to_plot(d_times,  d_prices)
    fux, fuy = to_plot(fu_times, fu_prices)
    fdx, fdy = to_plot(fd_times, fd_prices)

    line_binance.set_data(bx, by)
    line_chainlink.set_data(cx, cy)
    line_up.set_data(ux, uy)
    line_down.set_data(dx, dy)
    line_fair_up.set_data(fux, fuy)
    line_fair_down.set_data(fdx, fdy)

    if p0 is not None:
        line_oracle.set_data([-VIEW_WINDOW, 5], [p0, p0])
        text_oracle.set_position((4.5, p0))
        text_oracle.set_text(f"P0 ${p0:,.2f}")
    else:
        line_oracle.set_data([], [])
        text_oracle.set_text("")

    ax1.set_xlim(-VIEW_WINDOW, 5)

    all_btc = b_prices + c_prices
    if all_btc:
        pmin    = min(all_btc)
        pmax    = max(all_btc)
        padding = max((pmax - pmin) * 0.4, 10)
        ax1.set_ylim(pmin - padding, pmax + padding)

    ax2.set_ylim(0, 1)

    if b_prices:
        text_binance.set_text(f"Binance:   ${b_prices[-1]:,.2f}")
    if c_prices:
        text_chainlink.set_text(f"Chainlink: ${c_prices[-1]:,.2f}")
    if u_prices:
        text_up.set_text(f"UP price:  {u_prices[-1]:.3f}  ({u_prices[-1]*100:.1f}%)")
    if d_prices:
        text_down.set_text(f"DOWN price:{d_prices[-1]:.3f}  ({d_prices[-1]*100:.1f}%)")
    if fu_prices:
        edge = fu_prices[-1] - u_prices[-1] if u_prices else 0
        text_fair_up.set_text(f"Fair UP:   {fu_prices[-1]:.3f}  (edge {edge:+.3f})")
    if fd_prices:
        edge = fd_prices[-1] - d_prices[-1] if d_prices else 0
        text_fair_down.set_text(f"Fair DOWN: {fd_prices[-1]:.3f}  (edge {edge:+.3f})")

    sigma = get_realized_vol_per_sec()
    T     = seconds_remaining()
    mean_spread = (sum(spread_history) / len(spread_history)) if spread_history else 0
    text_vol.set_text(f"σ={sigma:.3f}$/√s (5m klines)   T={T:.0f}s   spread={mean_spread:+.2f}")
    text_p0.set_text(f"P0: {'$'+f'{p0:,.2f}' if p0 else 'waiting for boundary...'}")

    fig.canvas.draw_idle()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting feeds...")
    feed_thread = threading.Thread(target=start_feeds, daemon=True)
    feed_thread.start()

    time.sleep(3)

    print("Starting chart...")
    ani = animation.FuncAnimation(fig, animate, interval=100, blit=False)
    plt.tight_layout()
    plt.show()
