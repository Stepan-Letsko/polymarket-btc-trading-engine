# Research data design

How the recorded market data is turned into clean tables for analysis,
backtesting and machine learning, and why it's designed the way it is.

## The layers

| Layer | What it is | Where |
|---|---|---|
| **Raw** (bronze) | the recorders' hourly `.jsonl.zst` files, exactly as received; never changed | the bucket, `<feed>/date=YYYY-MM-DD/` (see [deployment.md](../../engine/docs/deployment.md#62-bucket)) |
| **Clean tables** (silver) | typed Parquet tables, one per message type, built from the raw files and rebuildable from them at any time | to be decided |
| **Research** (gold) | derived data: rebuilt order books, market outcomes, ML features | later |

## The documents

| File | What it holds |
|---|---|
| [decisions.md](decisions.md) | **why**: each design decision, with the options considered and what was given up |
| [conventions.md](conventions.md) | **what**: the rules every clean table follows |
| `tables/` | one file per clean table: grain, columns, how raw becomes rows, checks (added as each table is designed) |

The raw message formats these tables are built from are documented with
the recorders:
[clob](../../engine/docs/clob_message_types.md),
[binance](../../engine/docs/binance_message_types.md),
[polybolt](../../engine/docs/polybolt_message_types.md).

## How a decision is made

1. Discuss the options.
2. Decide.
3. Record it in `decisions.md`, and update `conventions.md` or the table
   doc it affects.

Decisions are never rewritten. If one changes, a new decision replaces it,
and the old one is marked "superseded by N".
