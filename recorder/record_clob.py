import os
import sys
import json
from datetime import datetime
import gzip
import asyncio

ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, ENGINE_DIR)

DATA_DIRECTORY = os.path.dirname(__file__) + "/Data"

# stream_current_market() (not stream_market()) — it already handles
# fetching the currently-open BTC Up/Down market and automatically
# reconnecting with fresh token IDs every 5 minutes, the same way
# dashboard/backend.py uses it. We don't have to track the current
# market ourselves.
from clob_ws import stream_current_market

def open_log_file_for(date_str):
    file_name = "clob_" + date_str + ".jsonl.gz"
    return gzip.open(DATA_DIRECTORY + "/" + file_name, "at")

# current_date tracks which day log_file was opened for. Both get
# reassigned from inside save_to_file() below, which is why that
# function needs "global" — without it, an assignment to either name
# inside a function would just create new local variables instead of
# updating these two.
current_date = datetime.now().strftime("%Y%m%d")
log_file = open_log_file_for(current_date)

def save_to_file(msg):
    global log_file, current_date

    today = datetime.now().strftime("%Y%m%d")
    if today != current_date:
        # A new day has started since we opened the current file —
        # close it out properly (writes gzip's end-of-stream marker,
        # same reasoning as the shutdown handling below) and start a
        # fresh one for today.
        log_file.close()
        current_date = today
        log_file = open_log_file_for(current_date)

    # No filtering here, unlike record_rtds.py — every event type this
    # feed produces (book snapshots, individual order changes, trades,
    # best_bid_ask, new_market, market_resolved) is potentially useful
    # for backtesting later, so we record all of it as-is.
    data_json = json.dumps(msg)
    log_file.write(data_json + "\n")
    log_file.flush()

try:
    asyncio.run(stream_current_market(on_message=save_to_file))
except KeyboardInterrupt:
    print("Stopped.")
finally:
    # .flush() (above) pushes compressed data out, but gzip's final
    # "this stream is properly finished" marker only gets written by
    # .close() — without it, the file can end up unreadable if the
    # process stops mid-write. This runs whether we exit normally,
    # via Ctrl+C, or from an unexpected error.
    log_file.close()
