"""
Shared price-reference math for Polymarket's BTC Up/Down 5-minute markets.

Each market resolves against a specific reference price ("P0" — the price
to beat) captured at the moment its 5-minute window opens. Polymarket's own
resolution source is a Chainlink 60-second TWAP (time-weighted average
price), not a single instantaneous tick — see the market rules at
https://data.chain.link/streams/btc-usd-twap-60s-streams

This module is the one place that logic lives, so both the dashboard and
the actual trading scripts can rely on the same, correct calculation
instead of each reimplementing their own approximation.
"""

WINDOW_SECONDS = 300  # a Polymarket BTC Up/Down window is 5 minutes


def compute_next_boundary(now_ts: float) -> float:
    """The unix timestamp (seconds) of the next 5-minute window boundary
    strictly after now_ts. Boundaries land on exact multiples of 300
    (i.e. real UTC clock marks :00, :05, :10, ...) since Unix epoch
    seconds divide evenly into 5-minute chunks with no drift."""
    return (int(now_ts // WINDOW_SECONDS) + 1) * WINDOW_SECONDS


def compute_twap(ticks: list[tuple[float, float]], window_start: float, window_end: float) -> float | None:
    """Time-weighted average price over [window_start, window_end].

    ticks: a list of (timestamp_seconds, price) pairs, in any order,
    covering at least [window_start, window_end] — extra ticks outside
    that range are fine and ignored.

    Each price is weighted by how long it was the "current" price before
    the next tick replaced it — NOT weighted equally per observation.
    A price that held for 4 real seconds counts 4x more than one that
    held for 1 second, regardless of how many ticks arrived in each
    stretch. See the worked example in the docstring below.

    Returns None if there isn't at least one tick at or before
    window_end to anchor the calculation.

    Example: ticks = [(0, 100), (3, 102), (7, 101)], window = [0, 10]
        100 holds from t=0 to t=3   -> 3s at $100
        102 holds from t=3 to t=7   -> 4s at $102
        101 holds from t=7 to t=10  -> 3s at $101
        TWAP = (100*3 + 102*4 + 101*3) / (3+4+3) = 1011/10 = $101.10
    """
    relevant = sorted((t, p) for t, p in ticks if t <= window_end)
    if not relevant:
        return None

    before_window = [tp for tp in relevant if tp[0] <= window_start]
    in_window = [tp for tp in relevant if window_start < tp[0] <= window_end]

    if before_window:
        # The price already in effect at window_start carries in as the
        # first segment, running from window_start (not its own, earlier
        # timestamp) up to whenever the next tick replaces it.
        segments = [(window_start, before_window[-1][1])] + in_window
    elif in_window:
        # No data before window_start at all — start from the first tick
        # we actually have, rather than assuming a price before it existed.
        segments = in_window
    else:
        return None

    total_weighted = 0.0
    total_duration = 0.0
    for i, (t, price) in enumerate(segments):
        seg_end = segments[i + 1][0] if i + 1 < len(segments) else window_end
        duration = seg_end - t
        if duration > 0:
            total_weighted += price * duration
            total_duration += duration

    if total_duration == 0:
        # All segments collapsed to zero width (e.g. a single tick right
        # at window_end) — fall back to the last known price.
        return segments[-1][1]

    return total_weighted / total_duration
