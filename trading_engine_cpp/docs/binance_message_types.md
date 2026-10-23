# Binance combined-stream message types

Notes on the raw JSON messages received from Binance's combined websocket
stream (`btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth5@100ms`), based
on real captured output from `binance_ws.cpp`.

## Shared wrapper (all three types)

```json
{"stream": "btcusdt@depth5@100ms", "data": { ... }}
```
- `stream` — which specific subscription this message came from. Needed
  because we're subscribed to three streams over one combined connection.
- `data` — the actual payload; shape differs completely per stream type
  (see below).

---

## 1. `depth5@100ms` — Partial Book Depth

```json
{"lastUpdateId":101643047202,
 "bids":[["81858.00000000","8.69570000"],["81857.99000000","1.56912000"],...],
 "asks":[["81858.01000000","0.28726000"],["81858.02000000","0.00076000"],...]}
```
- `lastUpdateId` — sequence number for this order book snapshot, lets you
  track whether updates arrived in order.
- `bids` — top 5 buy-side price levels, sorted **best (highest) first**.
  Each entry is `[price, quantity]` — e.g. someone willing to buy at
  $81,858.00, for 8.6957 BTC at that exact level.
- `asks` — top 5 sell-side price levels, sorted **best (lowest) first**,
  same `[price, quantity]` shape.

---

## 2. `bookTicker` — Best Bid/Ask Only

```json
{"u":101643047228,"s":"BTCUSDT","b":"81858.00000000","B":"8.69570000","a":"81858.01000000","A":"0.29704000"}
```
- `u` — update ID for this specific book-ticker event.
- `s` — symbol (`BTCUSDT`).
- `b` — best bid price. `B` — quantity available at that best bid.
- `a` — best ask price. `A` — quantity available at that best ask.
  (lowercase = price, uppercase = quantity, same pairing for both sides)

Fires the instant *either* the best bid or best ask changes — even just a
quantity change, not necessarily a price change — which is why it updates
far more frequently than `depth5`.

---

## 3. `aggTrade` — Aggregate Trade

```json
{"e":"aggTrade","E":1792425993275,"s":"BTCUSDT","a":4093315342,"p":"81858.01000000","q":"0.01306000","f":6775272958,"l":6775273003,"T":1792425993275,"m":false,"M":true}
```
- `e` — event type, always `"aggTrade"` for this stream.
- `E` — **event time**: when Binance's server generated this event
  (epoch ms). The one genuine "true origin timestamp" field among these
  three stream types — `depth5` and `bookTicker` have no equivalent.
- `s` — symbol.
- `a` — the aggregate trade's own ID.
- `p` — price the trade executed at.
- `q` — quantity traded.
- `f` / `l` — first and last individual trade IDs folded into this one
  aggregated event. "Aggregate trade" means Binance combines several
  individual fills at the same price, from the same taker order, into
  one event — `f`/`l` give the range of raw trade IDs bundled together.
- `T` — **trade time**: when the trade executed on Binance's matching
  engine (epoch ms) — subtly different from `E` (when the event was
  generated), though usually very close or identical.
- `m` — "is the buyer the market maker?" If `true`, the buy order was
  already resting on the book, and the *sell* side triggered the trade
  (the "taker"). If `false`, the buyer was the taker.
- `M` — per Binance's own docs, labeled "ignore" — legacy field, no real
  meaning anymore.
