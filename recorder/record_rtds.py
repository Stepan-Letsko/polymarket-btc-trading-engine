import os
import sys
import json
from datetime import datetime
import gzip
import asyncio

ENGINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "trading_engine"))
sys.path.insert(0, ENGINE_DIR)

DATA_DIRECTORY = os.path.dirname(__file__) + "/Data"

from rtds_ws import stream_rtds

def open_log_file_for(date_str):
    file_name = "rtds_" + date_str + ".jsonl.gz"
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

    if msg["source"] == "BINANCE":
        return
    # rtds_ws.py carries every coin Polymarket tracks on this topic, not
    # just BTC — without this check we'd also record Chainlink prices for
    # ETH, SOL, etc. mixed in under the same "CHAINLINK" source.
    if str(msg.get("symbol", "")).lower() != "btc/usd":
        return

    today = datetime.now().strftime("%Y%m%d")
    if today != current_date:
        # A new day has started since we opened the current file —
        # close it out properly (writes gzip's end-of-stream marker,
        # same reasoning as the shutdown handling below) and start a
        # fresh one for today.
        log_file.close()
        current_date = today
        log_file = open_log_file_for(current_date)

    data_json = json.dumps(msg)
    log_file.write(data_json + "\n")
    log_file.flush()

try:
    asyncio.run(stream_rtds(on_message=save_to_file))
except KeyboardInterrupt:
    print("Stopped.")
finally:
    # .flush() (above) pushes compressed data out, but gzip's final
    # "this stream is properly finished" marker only gets written by
    # .close() — without it, the file can end up unreadable if the
    # process stops mid-write. This runs whether we exit normally,
    # via Ctrl+C, or from an unexpected error.
    log_file.close()
        
        