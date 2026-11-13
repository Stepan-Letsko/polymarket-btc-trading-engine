# CLOB Market Channel — message types

Every message received from `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
Subscription format is documented separately in `clob_subscription.md`.

There are **7 JSON event types** plus **one plain-text response** (`PONG`).

## Important things that apply to all of them

- **Prices, sizes and timestamps are all JSON *strings*, not numbers.**
  `"price": "0.48"`, `"timestamp": "1757908892351"`. This differs from
  Binance (where `E` is a real integer), and matters when parsing: read
  them as `std::string_view`, not `int64_t`/`double`, then convert if
  needed.
- **Three event types are gated** behind `custom_feature_enabled: true`
  in the subscription: `best_bid_ask`, `new_market`, `market_resolved`.
  Without that flag they are never sent at all.
- **`new_market` and `market_resolved` are broadcast globally**, NOT
  filtered to your subscribed `assets_ids` — you receive them for every
  market on Polymarket (weather, sports, everything). Filter by `slug`
  (e.g. prefix `btc-updown-5m-`) or you will be processing the entire
  platform's market lifecycle.
- `event_type` is the field to branch on for all JSON messages.

---

## 1. `book` — full orderbook snapshot

Sent on subscribe (when `initial_dump` is true) and again after trades.

```json
{
  "event_type": "book",
  "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
  "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
  "bids": [{"price": "0.48", "size": "30"}, {"price": "0.49", "size": "20"}],
  "asks": [{"price": "0.52", "size": "25"}, {"price": "0.53", "size": "60"}],
  "timestamp": "1757908892351",
  "hash": "0xabc123..."
}
```

| Field | Meaning |
|---|---|
| `asset_id` | Which outcome token this book is for (UP or DOWN). |
| `market` | Condition ID of the market, `0x`-prefixed. |
| `bids` / `asks` | Arrays of `{price, size}` objects. Note: objects, unlike Binance's `[price, qty]` pairs. |
| `timestamp` | Orderbook time, Unix ms, as a string. |
| `hash` | Hash of the orderbook contents at this point. |

---

## 2. `price_change` — orderbook delta

The most frequent event by far. **One event can bundle several individual
level changes** in the `price_changes` array.

```json
{
  "event_type": "price_change",
  "market": "0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1",
  "price_changes": [
    {
      "asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
      "price": "0.5",
      "size": "200",
      "side": "BUY",
      "hash": "56621a121a47ed9333273e21c83b660cff37ae50",
      "best_bid": "0.5",
      "best_ask": "1"
    }
  ],
  "timestamp": "1757908892351"
}
```

| Field | Meaning |
|---|---|
| `price_changes[].asset_id` | Which token this change applies to — note it's *inside* each change, not at event level. |
| `price_changes[].price` | The price level affected. |
| `price_changes[].size` | New **aggregate** size at that level. **`"0"` means the level was removed entirely**, not a zero-size order. |
| `price_changes[].side` | `BUY` or `SELL`. |
| `price_changes[].hash` | Hash of the order that caused the change. |
| `price_changes[].best_bid` / `best_ask` | Optional — best bid/ask after this change. |
| `timestamp` | Event time, Unix ms, as a string. |

---

## 3. `last_trade_price` — a trade executed

```json
{
  "event_type": "last_trade_price",
  "asset_id": "114122071509644379678018727908709560226618148003371446110114509806601493071694",
  "market": "0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347",
  "price": "0.456",
  "size": "219.217767",
  "fee_rate_bps": "0",
  "side": "BUY",
  "timestamp": "1750428146322",
  "transaction_hash": "0xeeefffggghhh"
}
```

| Field | Meaning |
|---|---|
| `price` / `size` | Execution price and trade size. |
| `side` | **From the taker's perspective** (the aggressor). Same concept as Binance's `m` flag, but a direct string rather than a boolean. |
| `fee_rate_bps` | Fee rate in basis points. Optional. |
| `transaction_hash` | On-chain transaction hash. Optional. |

---

## 4. `tick_size_change` — minimum price increment changed

Fires when a market's price approaches 0 or 1 and Polymarket tightens the
allowed tick size.

```json
{
  "event_type": "tick_size_change",
  "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
  "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
  "old_tick_size": "0.01",
  "new_tick_size": "0.001",
  "timestamp": "1757908892351"
}
```

Relevant if you're placing orders — order prices must be valid multiples
of the current tick size.

---

## 5. `best_bid_ask` — top of book only *(requires `custom_feature_enabled`)*

A lightweight update carrying just the top-of-book numbers, without a full
snapshot. Analogous to Binance's `bookTicker` versus `depth5`.

```json
{
  "event_type": "best_bid_ask",
  "market": "0x0005c0d312de0be897668695bae9f32b624b4a1ae8b140c49f08447fcc74f442",
  "asset_id": "85354956062430465315924116860125388538595433819574542752031640332592237464430",
  "best_bid": "0.73",
  "best_ask": "0.77",
  "spread": "0.04",
  "timestamp": "1766789469958"
}
```

---

## 6. `new_market` — a market was created *(requires `custom_feature_enabled`)*

**Broadcast globally — you receive these for every market on Polymarket,
not just your subscribed assets.** Filter on `slug`.

```json
{
  "event_type": "new_market",
  "id": "1031769",
  "question": "Will NVIDIA (NVDA) close above $240 end of January?",
  "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
  "slug": "nvda-above-240-on-january-30-2026",
  "description": "...",
  "assets_ids": ["76043...", "31690..."],
  "outcomes": ["Yes", "No"],
  "timestamp": "1766790415550",
  "condition_id": "0x311d0c...",
  "active": true,
  "clob_token_ids": ["76043...", "31690..."],
  "order_price_min_tick_size": "0.01",
  "group_item_title": "NVDA above $240"
}
```

| Field | Meaning |
|---|---|
| `slug` | How to identify *your* markets — BTC 5m windows use `btc-updown-5m-{unix_ts}`. |
| `clob_token_ids` / `assets_ids` | The two outcome token IDs — same values `get_market.cpp` fetches from the Gamma API. |
| `outcomes` | Human-readable outcome names, e.g. `["Up", "Down"]`. |
| `order_price_min_tick_size` | Starting tick size for this market. |

**Why this matters for us:** this event carries everything needed to
subscribe to the next 5-minute window, delivered the instant Polymarket
creates it — no Gamma API poll required. Combined with the
`updateSubscription` operation, markets can be rotated without ever
dropping the connection.

---

## 7. `market_resolved` — outcome finalized *(requires `custom_feature_enabled`)*

Also broadcast globally.

```json
{
  "event_type": "market_resolved",
  "id": "1031769",
  "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
  "assets_ids": ["76043...", "31690..."],
  "winning_asset_id": "76043073756653678226373981964075571318267289248134717369284518995922789326425",
  "winning_outcome": "Yes",
  "timestamp": "1766790415550"
}
```

| Field | Meaning |
|---|---|
| `winning_asset_id` | Which token paid out. |
| `winning_outcome` | Human-readable result — `"Up"`/`"Down"` for BTC markets. |

This is the **ground truth** for backtesting: the actual settled outcome,
straight from Polymarket, rather than anything we compute ourselves.

---

## 8. `PONG` — heartbeat response

Not JSON. The server replies with the literal plain text `PONG` after
receiving our plain text `PING`. Parsing code must handle this before
attempting to parse a message as JSON, or it will fail on it.
