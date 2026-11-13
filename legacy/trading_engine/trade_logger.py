import json
import os
import threading
from datetime import datetime, timezone

LOG_DIR  = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE = os.path.join(LOG_DIR, "trades.jsonl")

_lock = threading.Lock()


def _ensure_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _write(event: dict):
    now = datetime.now(timezone.utc)
    event["ts"]       = now.strftime("%H:%M:%S.%f")[:-3]
    event["date"]     = now.strftime("%Y-%m-%d")
    event["datetime"] = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with _lock:
        _ensure_dir()
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(event) + "\n")


# ── Public logging calls ─────────────────────────────────────────────────────

def log_signal(side, score, obi, delta, btc_price, up_ask, down_ask):
    _write({
        "event":     "SIGNAL",
        "side":      side,
        "score":     score,
        "obi":       round(obi, 3),
        "delta_btc": round(delta, 4),
        "btc":       btc_price,
        "up_ask":    up_ask,
        "down_ask":  down_ask,
    })


def log_entry_posted(side, limit_price, market_ask, token_id, order_id=None):
    _write({
        "event":       "ENTRY_POSTED",
        "side":        side,
        "limit_price": limit_price,
        "market_ask":  market_ask,
        "token_id":    token_id[:16] + "...",
        "order_id":    order_id,
    })


def log_entry_matched(side, size, actual_price, latency_ms):
    _write({
        "event":        "ENTRY_MATCHED",
        "side":         side,
        "size":         size,
        "actual_price": actual_price,
        "latency_ms":   round(latency_ms, 1),
    })


def log_entry_confirmed(side, tx_hash):
    _write({
        "event":   "ENTRY_CONFIRMED",
        "side":    side,
        "tx_hash": tx_hash,
    })


def log_exit_trigger(side, reason, btc_price, up_bid, down_bid):
    _write({
        "event":    "EXIT_TRIGGER",
        "side":     side,
        "reason":   reason,
        "btc":      btc_price,
        "up_bid":   up_bid,
        "down_bid": down_bid,
    })


def log_exit_attempt(side, attempt, limit_price, bid, markdown, success, order_id=None):
    _write({
        "event":       "EXIT_ATTEMPT",
        "side":        side,
        "attempt":     attempt,
        "limit_price": limit_price,
        "bid_state":   bid,
        "markdown":    round(markdown, 2),
        "success":     success,
        "order_id":    order_id,
    })


def log_exit_confirmed(side, size, actual_price, entry_price, tx_hash):
    pnl = round((actual_price - entry_price) * size, 4)
    _write({
        "event":        "EXIT_CONFIRMED",
        "side":         side,
        "size":         size,
        "actual_price": actual_price,
        "entry_price":  entry_price,
        "pnl":          pnl,
        "tx_hash":      tx_hash,
    })


def log_exit_stranded(side, shares, up_bid, down_bid, btc_price):
    _write({
        "event":    "EXIT_STRANDED",
        "side":     side,
        "shares":   shares,
        "up_bid":   up_bid,
        "down_bid": down_bid,
        "btc":      btc_price,
    })
