"""
Inspect the real recorded Chainlink ticks around a specific 5-minute
market's opening boundary, and compute what our compute_twap() formula
would have said for that exact window — using the same code path
backend.py uses live, just run against recorded history instead.

Usage:
    python3 inspect_window.py <window_start_unix_seconds> [options]

Example (the window_start is the number at the end of a Polymarket
market's URL, e.g. .../btc-updown-5m-1790623200):
    python3 inspect_window.py 1790623200
    python3 inspect_window.py 1790623200 --expected 83670.94
"""

import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone

# Reuse the exact same TWAP math backend.py uses live, rather than
# re-implementing it here — if this script and production ever disagree,
# we want that to be a real bug we catch, not two different formulas.
TRADING_ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, TRADING_ENGINE_DIR)
from twap import compute_twap  # noqa: E402


def load_chainlink_ticks(data_dir, window_start, lookback, lookahead):
    """
    Loads Chainlink (source_ts, price) ticks within
    [window_start - lookback, window_start + lookahead] from the
    recorded rtds_<date>.jsonl.gz file(s) covering that range.

    Checks both the target UTC date's file and the day before, in case
    the window falls close to midnight UTC and the recorder's own
    day-rollover (based on the machine's local clock) doesn't line up
    exactly with the UTC date we'd naively expect.
    """
    range_start = window_start - lookback
    range_end = window_start + lookahead

    candidate_dates = {
        datetime.fromtimestamp(range_start, tz=timezone.utc).strftime("%Y%m%d"),
        datetime.fromtimestamp(range_end, tz=timezone.utc).strftime("%Y%m%d"),
    }

    ticks = []
    for date_str in sorted(candidate_dates):
        path = os.path.join(data_dir, f"rtds_{date_str}.jsonl.gz")
        if not os.path.exists(path):
            continue
        with gzip.open(path, "rt") as f:
            for line in f:
                msg = json.loads(line)
                source_ts = msg.get("source_ts")
                if source_ts is None:
                    continue
                ts = source_ts / 1000
                if range_start <= ts <= range_end:
                    ticks.append((ts, msg["price"]))

    ticks.sort(key=lambda tp: tp[0])
    return ticks


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("window_start", type=float, help="Market's window_start, unix seconds (e.g. 1790623200)")
    parser.add_argument("--lookback", type=float, default=70, help="Seconds before window_start to load (default 70)")
    parser.add_argument("--lookahead", type=float, default=10, help="Seconds after window_start to load (default 10)")
    parser.add_argument("--expected", type=float, default=None, help="Polymarket's real displayed Price To Beat, to compare against")
    parser.add_argument(
        "--data-dir",
        default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "recorder", "Data")),
        help="Folder containing rtds_<date>.jsonl.gz files (default: recorder/Data)",
    )
    args = parser.parse_args()

    ticks = load_chainlink_ticks(args.data_dir, args.window_start, args.lookback, args.lookahead)

    if not ticks:
        print(f"No Chainlink ticks found in {args.data_dir} for this window. "
              f"Wrong machine/data-dir, or this window wasn't recorded.")
        return

    print(f"Loaded {len(ticks)} Chainlink ticks "
          f"[{args.window_start - args.lookback:.0f} .. {args.window_start + args.lookahead:.0f}]\n")

    for ts, price in ticks:
        clock = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        marker = "  <-- window_start" if abs(ts - args.window_start) < 0.5 else ""
        print(f"  {clock}  (ts={ts:.3f})  ${price:,.2f}{marker}")

    twap_price = compute_twap(ticks, args.window_start - 60, args.window_start)
    print(f"\ncompute_twap() result for [window_start-60, window_start]: ${twap_price:,.2f}" if twap_price is not None
          else "\ncompute_twap() could not compute a result (no ticks before window_start).")

    if args.expected is not None and twap_price is not None:
        diff = twap_price - args.expected
        print(f"Polymarket's real Price To Beat:                          ${args.expected:,.2f}")
        print(f"Difference:                                               ${diff:+,.2f}")


if __name__ == "__main__":
    main()
