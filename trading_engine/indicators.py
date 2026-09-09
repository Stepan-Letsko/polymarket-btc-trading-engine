"""
Leading-indicator calculations derived from Binance's BTC order book and
trade flow. Pure functions with no state of their own, so both the
dashboard and the trading scripts can feed them whatever data they're
already collecting, rather than each reimplementing this math.

Same core logic as calc_obi_binance() / get_recent_delta() in
live_trading.py, refactored to take data as arguments instead of reading
from that file's own global `state` dict.
"""

import time


def calc_obi(bids, asks):
    """
    Order Book Imbalance: bid volume as a fraction of total volume across
    the given book levels. Above 0.5 means more resting buy interest than
    sell interest at this depth (bullish lean); below 0.5 means the
    opposite. Typical usage: the top-5 levels from Binance's depth5
    stream.

    bids/asks: lists of [price, quantity] pairs — the format
    Binance_ws.py's depth5 messages already use.

    Returns 0.5 (neutral) if there's no book to measure yet.
    """
    bid_vol = sum(float(level[1]) for level in bids)
    ask_vol = sum(float(level[1]) for level in asks)
    total = bid_vol + ask_vol
    return bid_vol / total if total > 0 else 0.5


def calc_trade_delta(trades, window_ms, now_ms=None):
    """
    Net trade flow: total buy volume minus total sell volume over the
    trailing window_ms milliseconds. Positive means more aggressive
    buying than selling recently; negative means the opposite.

    trades: an iterable of (timestamp_ms, quantity, direction) tuples,
    where direction is "BUY" or "SELL" — matches the fields
    Binance_ws.py already parses out of each aggTrade message.

    now_ms: the timestamp to measure "trailing" from — defaults to the
    current wall-clock time, but can be passed explicitly (useful for
    testing, or for staying consistent with a specific message's own
    timestamp rather than whenever this function happens to run).
    """
    if now_ms is None:
        now_ms = time.time() * 1000
    cutoff = now_ms - window_ms

    buy_vol = sell_vol = 0.0
    for ts, qty, direction in trades:
        if ts >= cutoff:
            if direction == "BUY":
                buy_vol += qty
            else:
                sell_vol += qty
    return buy_vol - sell_vol
