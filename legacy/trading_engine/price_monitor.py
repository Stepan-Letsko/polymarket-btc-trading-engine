import asyncio
import json
import threading
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from datetime import datetime, timezone
from collections import deque

from Binance_ws  import stream_binance
from rtds_ws     import stream_rtds
from clob_ws     import stream_market
from kraken_ws   import stream_kraken
from Coinbase_ws import stream_coinbase
from Bitstamp_ws import stream_bitstamp
from get_market  import get_current_market

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

MAX_POINTS  = 10000  # Increase to keep more history for high-freq feeds
VIEW_WINDOW = 10     # How many seconds of history to show on the X-axis

# ── Data source toggles ──────────────────────
# Set any to False to disable that feed entirely
# (won't connect, won't plot, won't affect y-axis range)
SHOW_BINANCE   = True
SHOW_RTDS      = True
SHOW_CHAINLINK = True
SHOW_KRAKEN    = False
SHOW_COINBASE  = True
SHOW_BITSTAMP  = False
SHOW_UP        = True
SHOW_DOWN      = True


# ─────────────────────────────────────────────
# SHARED DATA STORE
# ─────────────────────────────────────────────

binance_times    = deque(maxlen=MAX_POINTS)
binance_prices   = deque(maxlen=MAX_POINTS)

rtds_times       = deque(maxlen=MAX_POINTS)
rtds_prices      = deque(maxlen=MAX_POINTS)

chainlink_times  = deque(maxlen=MAX_POINTS)
chainlink_prices = deque(maxlen=MAX_POINTS)

up_times         = deque(maxlen=MAX_POINTS)
up_prices        = deque(maxlen=MAX_POINTS)

down_times       = deque(maxlen=MAX_POINTS)
down_prices      = deque(maxlen=MAX_POINTS)

kraken_times     = deque(maxlen=MAX_POINTS)
kraken_prices    = deque(maxlen=MAX_POINTS)

coinbase_times   = deque(maxlen=MAX_POINTS)
coinbase_prices  = deque(maxlen=MAX_POINTS)

bitstamp_times   = deque(maxlen=MAX_POINTS)
bitstamp_prices  = deque(maxlen=MAX_POINTS)

trade_markers    = deque(maxlen=100) # Stores (timestamp, side)
exit_markers     = deque(maxlen=100) # Stores (timestamp, side)

fair_up_times    = deque(maxlen=MAX_POINTS)
fair_up_prices   = deque(maxlen=MAX_POINTS)
fair_down_times  = deque(maxlen=MAX_POINTS)
fair_down_prices = deque(maxlen=MAX_POINTS)

oracle_price = None  # BTC window open price set by Polymarket (P0); updated each new 5-min window

# ── Oracle boundary tracking ──────────────────
# Chainlink price captured at each 5-min boundary becomes the P0 reference.
# _last_chainlink_price holds the most recent Chainlink tick so that when
# the boundary is crossed we can use the price that was in effect AT that
# exact moment (not the new tick arriving just after it).
_last_chainlink_price = None
_next_boundary        = None   # unix timestamp of the next 5-min boundary


def _compute_next_boundary():
    now_ts = datetime.now(timezone.utc).timestamp()
    return (int(now_ts // 300) + 1) * 300


data_lock = threading.Lock()


# ─────────────────────────────────────────────
# MESSAGE HANDLERS
# ─────────────────────────────────────────────

def handle_binance(msg):
    if msg.get("stream") != "bookTicker":
        return
    try:
        bid = float(msg["best_bid"])
        ask = float(msg["best_ask"])
        mid = (bid + ask) / 2
        now = datetime.now(timezone.utc).timestamp()
        with data_lock:
            binance_times.append(now)
            binance_prices.append(mid)
    except Exception:
        pass


def handle_rtds(msg):
    global _last_chainlink_price, _next_boundary, oracle_price

    source = msg.get("source")
    symbol = msg.get("symbol", "")
    price  = msg.get("price")
    if not price:
        return
    now = datetime.now(timezone.utc).timestamp()
    if source == "BINANCE" and symbol == "btcusdt":
        with data_lock:
            rtds_times.append(now)
            rtds_prices.append(float(price))
    elif source == "CHAINLINK" and symbol == "btc/usd":
        chainlink_price = float(price)

        # Use Chainlink's own source timestamp for boundary comparison — this is
        # what Polymarket's resolver uses, not our local receive time.
        src_ts = msg.get("source_ts")
        chainlink_ts = (src_ts / 1000) if src_ts else now

        with data_lock:
            chainlink_times.append(chainlink_ts)
            chainlink_prices.append(chainlink_price)

        # Initialise boundary on first Chainlink tick
        if _next_boundary is None:
            _next_boundary = _compute_next_boundary()

        # First Chainlink tick whose source timestamp crosses the 5-min boundary
        if chainlink_ts >= _next_boundary:
            oracle_price = chainlink_price
            print(f"[price_monitor] P0 set: ${oracle_price:,.2f} (Chainlink src_ts={chainlink_ts:.3f})")
            _next_boundary = _compute_next_boundary()

        _last_chainlink_price = chainlink_price


def handle_kraken(msg):
    if msg.get("channel") != "ticker":
        return
    mid = msg.get("mid_price")
    if mid is None:
        return
    now = datetime.now(timezone.utc).timestamp()
    with data_lock:
        kraken_times.append(now)
        kraken_prices.append(float(mid))


def handle_coinbase(msg):
    if msg.get("channel") != "ticker":
        return
    mid = msg.get("mid_price")
    if mid is None:
        return
    now = datetime.now(timezone.utc).timestamp()
    with data_lock:
        coinbase_times.append(now)
        coinbase_prices.append(float(mid))


def handle_bitstamp(msg):
    if msg.get("channel") != "book":
        return
    mid = msg.get("mid_price")
    if mid is None:
        return
    now = datetime.now(timezone.utc).timestamp()
    with data_lock:
        bitstamp_times.append(now)
        bitstamp_prices.append(float(mid))


def handle_clob(msg):
    """
    Extract the 'Best Price' (Mid Price) for UP and DOWN tokens.
    Using Mid Price (Avg of Bid and Ask) provides a much more accurate 
    representation of the market value than just the Ask.
    """
    event_type = msg.get("event_type")
    side       = msg.get("side")

    if side not in ("UP", "DOWN"):
        return

    bid = None
    ask = None

    if event_type == "book":
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        if bids:
            bid = float(bids[0]["price"])
        if asks:
            ask = float(asks[0]["price"])

    elif event_type in ("price_change", "best_bid_ask"):
        b = msg.get("best_bid")
        a = msg.get("best_ask")
        if b is not None: bid = float(b)
        if a is not None: ask = float(a)

    # Calculate Mid Price as the "Best Price"
    price = None
    if bid is not None and ask is not None:
        price = (bid + ask) / 2
    elif bid is not None:
        price = bid  # Fallback to bid if no ask
    elif ask is not None:
        price = ask  # Fallback to ask if no bid

    if price is None:
        return

    now = datetime.now(timezone.utc).timestamp()
    with data_lock:
        if side == "UP":
            up_times.append(now)
            up_prices.append(price)
        else:
            down_times.append(now)
            down_prices.append(price)


# ─────────────────────────────────────────────
# BACKGROUND THREAD
# ─────────────────────────────────────────────

current_market = None


def start_feeds():
    async def run_all():
        global current_market

        # ── Persistent feeds — run for the lifetime of the process ──────────
        # These never restart; BTC price history stays continuous across windows.
        if SHOW_BINANCE:
            asyncio.create_task(stream_binance(on_message=handle_binance))
        if SHOW_RTDS or SHOW_CHAINLINK:
            asyncio.create_task(stream_rtds(on_message=handle_rtds))
        if SHOW_KRAKEN:
            asyncio.create_task(stream_kraken(on_message=handle_kraken))
        if SHOW_COINBASE:
            asyncio.create_task(stream_coinbase(on_message=handle_coinbase))
        if SHOW_BITSTAMP:
            asyncio.create_task(stream_bitstamp(on_message=handle_bitstamp))

        # ── CLOB loop — restarts every 5 minutes with fresh token IDs ───────
        while True:
            market = get_current_market()
            if not market:
                print("[price_monitor] No market found — retrying in 5s...")
                await asyncio.sleep(5)
                continue

            current_market = market
            print(f"[price_monitor] Window: {market['event_title']}")

            # Clear stale UP/DOWN prices from the previous window
            with data_lock:
                up_times.clear()
                up_prices.clear()
                down_times.clear()
                down_prices.clear()

            now_dt    = datetime.now(timezone.utc)
            remaining = 300 - ((now_dt.minute % 5) * 60 + now_dt.second)

            if SHOW_UP or SHOW_DOWN:
                try:
                    await asyncio.wait_for(
                        stream_market(
                            market["token_up"],
                            market["token_down"],
                            on_message=handle_clob,
                        ),
                        timeout=remaining + 2,  # +2s buffer past the boundary
                    )
                except asyncio.TimeoutError:
                    print("[price_monitor] Window ended — rolling to next market...")
                except Exception as e:
                    print(f"[price_monitor] CLOB stream error: {e} — reconnecting...")
                    await asyncio.sleep(1)
            else:
                # No CLOB needed — just sleep until the window boundary
                await asyncio.sleep(remaining + 2)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_all())


# ─────────────────────────────────────────────
# MATPLOTLIB LIVE CHART
# ─────────────────────────────────────────────

fig, ax1 = plt.subplots(figsize=(15, 7))
ax2 = ax1.twinx()

# Make sure ax2 (token prices) draws on top of ax1 (BTC price)
ax1.set_zorder(1)
ax2.set_zorder(2)
# ax1 background must be transparent so ax2 lines show through
ax1.patch.set_visible(False)

fig.patch.set_facecolor("#0f0f0f")
ax2.set_facecolor("#0f0f0f")

# BTC price lines on ax1 (left axis)
line_binance,   = ax1.plot([], [], color="#f0b429", linewidth=1.5,
                           label="Binance direct", drawstyle="steps-post", zorder=3)
line_rtds,      = ax1.plot([], [], color="#00d4ff", linewidth=1.5,
                           label="Binance via RTDS", linestyle="--", drawstyle="steps-post", zorder=3)
line_chainlink, = ax1.plot([], [], color="#7c5cbf", linewidth=2,
                           label="Chainlink oracle", linestyle=":", drawstyle="steps-post", zorder=3)
line_kraken,    = ax1.plot([], [], color="#e84142", linewidth=1.5,
                           label="Kraken direct", linestyle="-.", drawstyle="steps-post", zorder=3)
line_coinbase,  = ax1.plot([], [], color="#0052ff", linewidth=1.5,
                           label="Coinbase direct", linestyle=(0, (3, 1, 1, 1, 1, 1)), drawstyle="steps-post", zorder=3)
line_bitstamp,  = ax1.plot([], [], color="#ff9f43", linewidth=1.5,
                           label="Bitstamp direct", linestyle=(0, (5, 2)), drawstyle="steps-post", zorder=3)

# Token ask lines on ax2 (right axis) — drawn on top
line_up,   = ax2.plot([], [], color="#00ff88", linewidth=2.5,
                      label="UP price (Mid)", drawstyle="steps-post", zorder=5)
line_down, = ax2.plot([], [], color="#ff4d4d", linewidth=2.5,
                      label="DOWN price (Mid)", linestyle="--", drawstyle="steps-post", zorder=5)

# Fair value lines — same colours, thinner and dotted so they don't clutter
line_fair_up,   = ax2.plot([], [], color="#00ff88", linewidth=1.2, linestyle=":",
                            label="Fair value UP", drawstyle="steps-post", zorder=4, alpha=0.8)
line_fair_down, = ax2.plot([], [], color="#ff4d4d", linewidth=1.2, linestyle=":",
                            label="Fair value DOWN", drawstyle="steps-post", zorder=4, alpha=0.8)

# Polymarket P0 reference — horizontal line on ax1 showing the BTC price that resolves the market
line_oracle, = ax1.plot([], [], color="white", linewidth=1.2, linestyle="--",
                         label="Polymarket P0", zorder=4, alpha=0.65)
text_oracle  = ax1.text(4.5, 0, "", color="white", fontsize=8, va="center",
                         ha="right", fontfamily="monospace", alpha=0.75,
                         bbox=dict(boxstyle="round,pad=0.2", facecolor="#1a1a1a",
                                   edgecolor="#555555", alpha=0.7))

trade_vlines = [] # Stores vertical line artists for cleanup

# ── Left axis styling ──
ax1.set_title("BTC/USD Live — Price vs Polymarket Token Prices",
              color="white", fontsize=14, fontweight="bold", pad=15)
ax1.set_xlabel("Seconds ago", color="#aaaaaa")
ax1.set_ylabel("BTC Price (USD)", color="#f0b429")
ax1.tick_params(colors="#aaaaaa")
ax1.tick_params(axis="y", labelcolor="#f0b429")
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
for spine in ax1.spines.values():
    spine.set_color("#333333")

# ── Right axis styling ──
ax2.set_ylabel("Token Price (Mid)", color="#aaaaaa")
ax2.tick_params(axis="y", labelcolor="#aaaaaa")
ax2.set_ylim(0, 1)
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}"))
for spine in ax2.spines.values():
    spine.set_color("#333333")

# 0.5 reference line
ax2.axhline(y=0.5, color="#444444", linewidth=0.8, linestyle="--")

# Combined legend — bottom left so it doesn't block the price data
_legend_pairs = [
    (SHOW_BINANCE,   line_binance),
    (SHOW_RTDS,      line_rtds),
    (SHOW_CHAINLINK, line_chainlink),
    (SHOW_KRAKEN,    line_kraken),
    (SHOW_COINBASE,  line_coinbase),
    (SHOW_BITSTAMP,  line_bitstamp),
    (SHOW_UP,        line_up),
    (SHOW_DOWN,      line_down),
    (True,           line_fair_up),
    (True,           line_fair_down),
    (True,           line_oracle),
]
lines  = [l for enabled, l in _legend_pairs if enabled]
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="lower left", facecolor="#1a1a1a",
           edgecolor="#333333", labelcolor="white", fontsize=9)

# ── Price annotations ──
text_binance   = ax1.text(0.01, 0.97, "", transform=ax1.transAxes,
                          color="#f0b429", fontsize=9, va="top",
                          fontfamily="monospace")
text_rtds      = ax1.text(0.01, 0.91, "", transform=ax1.transAxes,
                          color="#00d4ff", fontsize=9, va="top",
                          fontfamily="monospace")
text_chainlink = ax1.text(0.01, 0.85, "", transform=ax1.transAxes,
                          color="#7c5cbf", fontsize=9, va="top",
                          fontfamily="monospace")
text_kraken    = ax1.text(0.01, 0.79, "", transform=ax1.transAxes,
                          color="#e84142", fontsize=9, va="top",
                          fontfamily="monospace")
text_coinbase  = ax1.text(0.01, 0.73, "", transform=ax1.transAxes,
                          color="#0052ff", fontsize=9, va="top",
                          fontfamily="monospace")
text_bitstamp  = ax1.text(0.01, 0.67, "", transform=ax1.transAxes,
                          color="#ff9f43", fontsize=9, va="top",
                          fontfamily="monospace")
text_up        = ax1.text(0.01, 0.61, "", transform=ax1.transAxes,
                          color="#00ff88", fontsize=9, va="top",
                          fontfamily="monospace")
text_down      = ax1.text(0.01, 0.55, "", transform=ax1.transAxes,
                          color="#ff4d4d", fontsize=9, va="top",
                          fontfamily="monospace")
text_fair_up   = ax1.text(0.01, 0.49, "", transform=ax1.transAxes,
                          color="#00cc66", fontsize=9, va="top",
                          fontfamily="monospace", alpha=0.85)
text_fair_down = ax1.text(0.01, 0.43, "", transform=ax1.transAxes,
                          color="#cc3333", fontsize=9, va="top",
                          fontfamily="monospace", alpha=0.85)
text_lag       = ax1.text(0.99, 0.97, "", transform=ax1.transAxes,
                          color="#ff6b6b", fontsize=9, va="top",
                          ha="right", fontfamily="monospace")


def animate(_):
    global trade_vlines
    # Clear previous trade markers
    for line in trade_vlines:
        line.remove()
    trade_vlines = []

    p0 = oracle_price  # snapshot module-level var (no lock needed — single writer)

    with data_lock:
        b_times  = list(binance_times)
        b_prices = list(binance_prices)
        r_times  = list(rtds_times)
        r_prices = list(rtds_prices)
        c_times  = list(chainlink_times)
        c_prices = list(chainlink_prices)
        k_times  = list(kraken_times)
        k_prices = list(kraken_prices)
        cb_times  = list(coinbase_times)
        cb_prices = list(coinbase_prices)
        bs_times  = list(bitstamp_times)
        bs_prices = list(bitstamp_prices)
        u_times  = list(up_times)
        u_prices = list(up_prices)
        d_times  = list(down_times)
        d_prices = list(down_prices)
        fu_times  = list(fair_up_times)
        fu_prices = list(fair_up_prices)
        fd_times  = list(fair_down_times)
        fd_prices = list(fair_down_prices)
        m_data   = list(trade_markers)
        e_data   = list(exit_markers)

    now = datetime.now(timezone.utc).timestamp()

    def get_plot_vectors(ts_list, ps_list):
        """Converts timestamps to 'seconds ago' and pads the last price to 'now'."""
        if not ts_list:
            return [], []
        xs = [t - now for t in ts_list]
        ys = list(ps_list)
        # Append a point at the current time (0) using the last known price
        xs.append(0)
        ys.append(ys[-1])
        return xs, ys

    bx, by = get_plot_vectors(b_times, b_prices)
    rx, ry = get_plot_vectors(r_times, r_prices)
    cx, cy = get_plot_vectors(c_times, c_prices)
    kx, ky   = get_plot_vectors(k_times, k_prices)
    cbx, cby = get_plot_vectors(cb_times, cb_prices)
    bsx, bsy = get_plot_vectors(bs_times, bs_prices)
    ux, uy = get_plot_vectors(u_times, u_prices)
    dx, dy = get_plot_vectors(d_times, d_prices)

    fux, fuy = get_plot_vectors(fu_times, fu_prices)
    fdx, fdy = get_plot_vectors(fd_times, fd_prices)

    line_binance.set_data(bx, by)
    line_rtds.set_data(rx, ry)
    line_chainlink.set_data(cx, cy)
    line_kraken.set_data(kx, ky)
    line_coinbase.set_data(cbx, cby)
    line_bitstamp.set_data(bsx, bsy)
    line_up.set_data(ux, uy)
    line_down.set_data(dx, dy)
    line_fair_up.set_data(fux, fuy)
    line_fair_down.set_data(fdx, fdy)

    # Oracle P0 horizontal reference line
    if p0 is not None:
        line_oracle.set_data([-VIEW_WINDOW, 5], [p0, p0])
        text_oracle.set_position((4.5, p0))
        text_oracle.set_text(f"P0 ${p0:,.2f}")
    else:
        line_oracle.set_data([], [])
        text_oracle.set_text("")

    # Draw entry trade markers (Dashed)
    for ts, side in m_data:
        x = ts - now
        # Only draw if within the visible window
        if x > -VIEW_WINDOW - 5:
            color = "#00ff88" if side == "UP" else "#ff4d4d"
            vl = ax1.axvline(x=x, color=color, linestyle="--", alpha=0.5, linewidth=1, zorder=1)
            trade_vlines.append(vl)

    # Draw exit trade markers (Solid)
    for ts, side in e_data:
        x = ts - now
        if x > -VIEW_WINDOW - 5:
            color = "#00ff88" if side == "UP" else "#ff4d4d"
            # Solid line with higher opacity for the exit
            vl = ax1.axvline(x=x, color=color, linestyle="-", alpha=0.8, linewidth=1.5, zorder=1)
            trade_vlines.append(vl)

    ax1.set_xlim(-VIEW_WINDOW, 5)

    all_prices = (
        (list(b_prices) if SHOW_BINANCE   else []) +
        (list(r_prices) if SHOW_RTDS      else []) +
        (list(c_prices) if SHOW_CHAINLINK else []) +
        (list(k_prices)  if SHOW_KRAKEN   else []) +
        (list(cb_prices) if SHOW_COINBASE  else []) +
        (list(bs_prices) if SHOW_BITSTAMP  else [])
    )
    if all_prices:
        pmin    = min(all_prices)
        pmax    = max(all_prices)
        padding = max((pmax - pmin) * 0.4, 10)
        ax1.set_ylim(pmin - padding, pmax + padding)

    ax2.set_ylim(0, 1)

    if b_prices:
        text_binance.set_text(f"Binance:    ${b_prices[-1]:,.2f}")
    if r_prices:
        text_rtds.set_text(f"RTDS relay: ${r_prices[-1]:,.2f}")
    if c_prices:
        text_chainlink.set_text(f"Chainlink:  ${c_prices[-1]:,.2f}")
    if k_prices:
        text_kraken.set_text(f"Kraken:     ${k_prices[-1]:,.2f}")
    if cb_prices:
        text_coinbase.set_text(f"Coinbase:   ${cb_prices[-1]:,.2f}")
    if bs_prices:
        text_bitstamp.set_text(f"Bitstamp:   ${bs_prices[-1]:,.2f}")
    if u_prices:
        text_up.set_text(f"UP price:   {u_prices[-1]:.3f}  ({u_prices[-1]*100:.1f}%)")
    if d_prices:
        text_down.set_text(f"DOWN price: {d_prices[-1]:.3f}  ({d_prices[-1]*100:.1f}%)")
    if fu_prices:
        edge = fu_prices[-1] - u_prices[-1] if u_prices else 0
        text_fair_up.set_text(f"Fair UP:    {fu_prices[-1]:.3f}  (edge {edge:+.3f})")
    if fd_prices:
        edge = fd_prices[-1] - d_prices[-1] if d_prices else 0
        text_fair_down.set_text(f"Fair DOWN:  {fd_prices[-1]:.3f}  (edge {edge:+.3f})")

    if b_times and r_times and b_prices and r_prices:
        time_lag   = round((b_times[-1] - r_times[-1]) * 1000, 1)
        price_diff = round(b_prices[-1] - r_prices[-1], 2)
        spread_sum = ""
        if u_prices and d_prices:
            total      = round(u_prices[-1] + d_prices[-1], 3)
            spread_sum = f"\nUP+DOWN:    {total:.3f}"
        text_lag.set_text(
            f"RTDS lag:   {time_lag}ms\n"
            f"Price diff: ${price_diff:+.2f}"
            f"{spread_sum}"
        )

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