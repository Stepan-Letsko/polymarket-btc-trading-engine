import time

import duckdb

# Architecture: each raw recorded file (one source, one day) gets
# converted into Parquet partitioned by date AND hour, using Hive-style
# folder naming (source=X/date=Y/hour=Z/data.parquet). At query time, we
# point DuckDB at the whole partitioned tree and filter by a recv_ts
# range — DuckDB uses the folder names to skip irrelevant files entirely
# (never even opens them) before reading anything, so a query for one
# 5-minute backtest window only ever touches a couple of small
# hour-sized files, no matter how much total history exists. This
# replaces the earlier "union everything for one day, then sort"
# approach, which forced DuckDB to hold a whole day's fully-expanded
# data in memory at once just to guarantee sort order — that's what ran
# out of RAM.

READ_OPTS = "sample_size=-1, union_by_name=true"
# read_json_auto() only looks at a SAMPLE of each file to guess its
# schema by default, for speed. CLOB messages have several different
# event types with different optional fields (e.g. only tick_size_change
# rows have "old_tick_size"), so a field that doesn't happen to appear
# anywhere in the sampled rows breaks things once the reader actually
# hits it later in the file. sample_size=-1 scans the WHOLE file to
# build a complete schema first, and union_by_name=true tolerates rows
# that don't all share the exact same set of keys.


def convert_to_partitioned_parquet(path, output_dir, source_name):
    # date/hour are computed from recv_ts (epoch milliseconds -> seconds
    # -> timestamp) and handed to PARTITION_BY, which makes DuckDB write
    # the Hive-style date=.../hour=.../ folder structure itself — we
    # don't construct those paths by hand.
    if source_name == "binance":
        # Binance's depth5 stream also happens to have fields called
        # "bids"/"asks" — but they're Binance's own spot order book
        # (pairs of [price_string, qty_string]), a totally different
        # shape from CLOB's bids/asks (lists of {"price", "size"} dicts).
        # Renaming here avoids a later name collision if these ever get
        # queried together.
        columns = "* EXCLUDE (bids, asks), bids AS binance_bids, asks AS binance_asks"
    else:
        columns = "*"

    select = f"""
        SELECT {columns},
               to_timestamp(recv_ts / 1000)::DATE AS date,
               extract(hour FROM to_timestamp(recv_ts / 1000)) AS hour
        FROM read_json_auto('{path}', {READ_OPTS})
    """
    duckdb.sql(f"""
        COPY ({select}) TO '{output_dir}'
        (FORMAT PARQUET, PARTITION_BY (date, hour), OVERWRITE_OR_IGNORE true)
    """)
    print(f"Converted {source_name} file {path} -> {output_dir} (partitioned by date, hour)")


def convert_day_to_partitioned_parquet(date_str, data_dir="Data", parquet_dir="Data/parquet"):
    for source_name in ("binance", "rtds", "clob"):
        input_path = f"{data_dir}/{source_name}_{date_str}.jsonl.gz"
        output_dir = f"{parquet_dir}/source={source_name}"
        convert_to_partitioned_parquet(input_path, output_dir, source_name)


def load_window(start_ts, end_ts, parquet_dir="Data/parquet"):
    # start_ts/end_ts are recv_ts values (epoch milliseconds). Pointing
    # read_parquet() at the whole partitioned tree with
    # hive_partitioning=true lets DuckDB use the date=/hour= folder
    # names to skip files that can't possibly match before it even
    # opens them ("partition pruning") — only a handful of small,
    # hour-sized files actually get read for a typical backtest window,
    # regardless of how many days of history exist on disk. The
    # recv_ts >= / <= filter then narrows precisely within whatever
    # files survive that pruning, since an hour-sized file still spans
    # more than one 5-minute window.
    query = f"""
        SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet', hive_partitioning=true, union_by_name=true)
        WHERE recv_ts >= {start_ts} AND recv_ts <= {end_ts}
        ORDER BY recv_ts
    """
    return duckdb.sql(query).df()


if __name__ == "__main__":
    start = time.time()

    convert_day_to_partitioned_parquet("20260924")

    elapsed = time.time() - start
    print(f"Conversion took {elapsed:.2f} seconds")

    # Quick test: pull back one 5-minute window and confirm it merges +
    # sorts correctly across all three sources.
    window_start = 1790270400000.0  # ms
    window_end = window_start + 5 * 60 * 1000
    merged = load_window(window_start, window_end)
    print(f"Window query returned {len(merged)} rows")
    print("First 10 recv_ts values (should be increasing):")
    with_full_precision = merged["recv_ts"].head(10).apply(lambda x: f"{x:.3f}")
    print(with_full_precision.to_string(index=False))
