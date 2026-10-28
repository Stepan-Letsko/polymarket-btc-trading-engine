# CLOB Market Channel — subscription reference

How to subscribe to Polymarket's CLOB order book feed (the "Market Channel").

**Endpoint:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`

Unlike Binance (where the subscription is encoded in the URL query string),
this URL is generic — the same for every user and every market. The server
has no idea which of Polymarket's thousands of markets you want until you
explicitly tell it, so a subscription message must be sent after the
connection opens.

## Subscription message

Sent as a JSON text frame immediately after the connection opens:

```json
{
  "assets_ids": [
    "<token_up_id>",
    "<token_down_id>"
  ],
  "type": "market",
  "initial_dump": true,
  "custom_feature_enabled": true
}
```

### Fields

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `assets_ids` | array of strings | **yes** | — | Token IDs (from `clobTokenIds` in the Gamma API) to stream data for. For a BTC Up/Down market: `[token_up, token_down]`. |
| `type` | string | **yes** | — | Must be `"market"` for this channel. |
| `initial_dump` | boolean | no | `true` | Send a full orderbook snapshot immediately on subscribe, rather than waiting for the first change. |
| `level` | integer (1, 2, or 3) | no | `2` | Subscription detail level. |
| `custom_feature_enabled` | boolean | no | `false` | **Must be `true`** to receive `best_bid_ask`, `new_market`, and `market_resolved` events. Without it, those event types are never sent at all. |

## Heartbeat

Send the literal plain text `PING` (not JSON) every ~10 seconds. The server
replies with plain text `PONG`. Without it, the connection gets dropped for
inactivity.

## Updating subscriptions without reconnecting

Instead of tearing down and rebuilding the connection when the 5-minute
market rotates, subscriptions can be changed on the open connection:

```json
{
  "operation": "subscribe",
  "assets_ids": ["<new_token_id>"]
}
```

`operation` accepts `"subscribe"` or `"unsubscribe"`. Both also accept the
`level` and `custom_feature_enabled` fields.

Note: the Python implementation (`trading_engine/clob_ws.py`) currently
rotates markets by fully reconnecting every 5 minutes rather than using
this operation.

## Where the token IDs come from

`assets_ids` values are fetched from the Gamma API before connecting —
see `src/get_market.cpp`:

1. Round the current time down to the nearest 5-minute mark and build the
   slug: `btc-updown-5m-{unix_timestamp}`.
2. `GET https://gamma-api.polymarket.com/events?slug={slug}`
3. Read `markets[0].clobTokenIds` — note this is a **string containing
   JSON**, so it needs a second parse pass.
4. `clobTokenIds[0]` is `token_up`, `clobTokenIds[1]` is `token_down`.

## Event types received

See `clob_message_types.md` for every message shape with field-by-field
notes — `book`, `price_change`, `last_trade_price`, `tick_size_change`,
the three gated behind `custom_feature_enabled` (`best_bid_ask`,
`new_market`, `market_resolved`), and the plain-text `PONG`.

Official reference: https://docs.polymarket.com/api-reference/wss/market
