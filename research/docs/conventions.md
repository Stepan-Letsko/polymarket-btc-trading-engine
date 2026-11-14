# Conventions for the clean tables

The rules every clean (silver) table follows. Why each rule exists is in
[decisions.md](decisions.md); each rule links to its decision. Each table's
own doc in `tables/` only adds what's specific to that table.

## Tables

- **One table per message type**, per feed, each with one grain: one row
  is one clearly defined thing (one trade, one book level, one change).
  Messages that contain lists become one row per list entry.
  ([decision 1](decisions.md#1-one-table-per-message-type-with-a-shared-envelope))

## Envelope

Like the outside of a letter (postmark, sender, tracking number), the
envelope is a small set of columns that is **identical in every table**:
same names, same types, same meaning, whatever the table's own columns
are. It's what lets any tables be merged into one stream in arrival
order, filtered by market, and traced back to the raw message
([decision 1](decisions.md#1-one-table-per-message-type-with-a-shared-envelope)).

Every table starts with these columns, in this order:

| Holds | Column name and type |
|---|---|
| our receive time (`recv_ts`) | to be decided |
| the source's own timestamp | to be decided |
| the feed | to be decided |
| the market window | to be decided |
| the raw file and line the row came from | to be decided |
| a sequence number: the exact original order within the feed | to be decided |

Order of rows: `recv_ts` first (comparable across feeds: all recorders
use the same system clock). Within one feed, the sequence number gives
the true original order. Between feeds with the same `recv_ts`, the true
order is unknown, and a fixed rule decides (to be decided).

## To be decided

- Timestamps and ordering: which timestamps, unit, type, time zone; the
  sequence number; the fixed rule for ties between feeds.
- How prices and sizes are stored: exact decimals or floating point.
- Table and column naming.
- Where the tables live, file layout and sort order inside files.
- What an empty (null) value means, and where it's allowed.
- Rows the conversion can't understand: the rejects table.
