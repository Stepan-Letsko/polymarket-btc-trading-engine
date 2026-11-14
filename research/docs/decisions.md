# Design decisions

Each decision: the context, the options considered, what was chosen, and
what follows from it. Never rewritten: a changed decision gets a new
entry, and the old one is marked "superseded by N".

---

## 1. One table per message type, with a shared envelope

**Status:** decided, 2026-11-14

### Context

The clean tables will be used for two things with different needs:

- **Backtesting** replays the past as the bot would have seen it: every
  event, from every feed, in exactly the order it arrived. Two events in
  the wrong order can let a backtest act on information the real bot
  wouldn't have had yet (look-ahead bias), the most common source of fake
  backtest profits.
- **Machine learning** uses fixed sets of numbers at chosen moments (e.g.
  spread, book imbalance and recent trade flow 30 seconds before a
  market closes), built from clean data.

Every table needs a clear **grain**: what one row represents. The raw
messages have very different natural grains:

| Message | One message contains | Natural row |
|---|---|---|
| clob `book` | a whole order book, up to dozens of levels per side | one price level |
| clob `price_change` | one or several changes to the book | one change |
| clob `last_trade_price` | one trade | one trade |
| clob `best_bid_ask` | the top of the book | one update |
| clob `tick_size_change`, `new_market`, `market_resolved` | a rare event about a market | one event |
| Binance `aggTrade`, `bookTicker`, `depth5` | a trade, top of book, 5 levels | one trade, one update, one level |
| PolyBolt spot price, TWAP | one price | one price |

So one message can become 1 row or 60, depending on its type.

### Options considered

**A. One wide table for everything.** One row per message, a column for
every field any type has, plus a `type` column.
- For: already one stream in time order.
- Against: mixed grains (a whole book in one cell next to single
  trades); most cells empty; columns like "price" mean different things
  for different rows; every new field changes the table everything
  depends on.

**B. One table per message type.** Each with one grain and exactly its
own typed columns.
- For: each table means one thing; lists become rows at their natural
  grain; queries read only the message types they need (`price_change`
  is the bulk of the clob data and stays out of everything else); a
  change in one message type touches one table; the usual shape for
  research and ML data.
- Against: replaying everything in order means merging several tables.


### Decision

**B**, with every row in every table carrying the same **envelope**
columns:

- our receive time (`recv_ts`) and the source's own timestamp;
- the feed and the market window;
- the raw file and line the row came from;
- a sequence number: the exact original order **within the feed**.

The sequence number is needed because time alone can't give the order:
one `price_change` message with three changes gives three rows with the
same `recv_ts`, and an initial book dump arrives all at once. The raw line
number plus the position inside the message restores the order.

It only works within one feed. Each recorder is a separate program
writing its own file, so nothing records the order between feeds except
`recv_ts`, which all recorders take from the same system clock. When
events from two feeds share the same microsecond, their true order is
unknown. That's rare, and at that scale meaningless anyway: network, OS
and thread scheduling each add a few microseconds of variation before the
stamp, and the bot can't react within a microsecond. Such ties get a fixed,
deliberately arbitrary rule, so that replay is always repeatable.

Messages that contain lists (a `book`'s levels, a `price_change`'s
changes) become **one row per entry**, not one row holding the list.
Questions are about prices and sizes, which are then plain columns, and
book snapshots and changes share one grain (one price level), so
rebuilding the book fits naturally. Parquet's min/max skipping works on
them, and ML tools expect flat columns. The repeated envelope costs almost
nothing in Parquet. Each row keeps its raw file and line, so a whole
snapshot can be grouped back together. Whole-book views shaped for a
particular model belong in the research layer.

The exact names and types of the envelope columns, and the ordering
rule, are decided separately.

### Consequences

- The replay stream for backtesting is a merge of the tables, sorted by
  `recv_ts`, then the fixed rule between feeds, then the sequence number
  within a feed: one query. If replay is used heavily, it can be saved as
  its own table in the research layer.
- The envelope is mandatory in every table, so tables from different
  feeds can always be merged in the right order.
- More tables to define and maintain, including small ones for rare
  message types.
- ML features are not stored in these tables; they're built from them in
  the research layer.
