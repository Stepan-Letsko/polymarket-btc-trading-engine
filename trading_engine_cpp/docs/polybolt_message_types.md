# PolyBolt message types

Notes on the JSON messages received from PolyBolt for the two channels we
record: `price.crypto.twap` (btcusd, 60 s window) and `price.crypto`
(btcusd, Chainlink). Connection, login and subscription are covered in
`polybolt_subscription.md`.

> Written from Polymarket's documentation and the official contract
> (`https://ws-live-v2.polymarket.com/asyncapi.json`, PolyBolt 2.0.0), then
> checked against real traffic captured by `polybolt_ws.cpp` on 2026-11-02.
> All examples below are real captured messages unless marked otherwise.
>
> **Observed:** both channels send exactly **one live update per second**,
> stamped on whole seconds (`ts` ends in `000`). Snapshots held 119 points,
> one per second, covering the previous ~2 minutes.

## Time unit: milliseconds

Every PolyBolt timestamp (`ts`, `timestamp`) is in **Unix milliseconds** (13
digits). Unlike Binance, there is no option to ask for microseconds. Our own
`recv_ts` stays in microseconds (16 digits).

## How to tell messages apart

Two families of message arrive on the same connection:

| Has an `op` field | Has a `channel` + `payload` field |
|---|---|
| **Acknowledgement** — the server's reply to something we sent (`authed`, `subscribed`, `error`, ...) | **Data envelope** — a price, either a snapshot or a live update |

Within data envelopes, `channel` says which subscription it belongs to, and
`"snapshot": true` marks the one snapshot sent after each new subscription.

---

## 1. Acknowledgements

Replies to our requests. `rid` is echoed whenever our request carried one.

### `authed` — login succeeded

```json
{"op": "authed", "rid": "a1"}
```

### `subscribed` — a subscription was accepted

```json
{"op":"subscribed","channel":"price.crypto.twap","rid":"s1","provider":"chainlink"}
```

One per subscription, so a batch of two produces two `subscribed` acks.
- `provider` — the provider actually being served. The contract says this
  only appears while the server's provider selector is enabled, and that
  it's disabled by default — but **in practice it is present** (observed
  2026-11-02), confirming both channels are served by Chainlink.

### `unsubscribed` (documented, not yet observed)

```json
{"op": "unsubscribed", "channel": "price.crypto"}
```

### `pong` — reply to our optional `{"op":"ping"}` (documented, not yet observed)

```json
{"op": "pong", "rid": "p1"}
```

### `error` — a request failed (documented, not yet observed)

```json
{"op": "error", "code": "auth_required", "channel": "price.crypto", "rid": "s1"}
```

- `code` — why (table below).
- `channel` — present when the error is about a specific channel.

Most errors leave the connection **open**. The ones marked "then close" are
followed by close `4008`.

| `code` | Meaning |
|---|---|
| `bad_op` | Malformed frame, or an incomplete / mixed set of credentials |
| `bad_channel` | Unknown channel |
| `bad_filter` | Missing or invalid filter (e.g. a `provider` that isn't `chainlink`/`pyth`) |
| `auth_required` | Subscribed to a gated channel before authenticating — authenticate, then resend |
| `auth_invalid` | Credentials refused — fix them before retrying |
| `auth_unavailable` | The server can't check credentials right now — wait, then retry |
| `auth_expired` | Web-app ticket login only; not used with CLOB credentials |
| `sub_limit` | 65th subscription — then close |
| `rate_limited` | More than 20 subscribe/unsubscribe frames per second — then close |
| `auth_attempts` | 9th auth frame on this connection — then close |

---

## 2. Data envelopes — shared fields

Every price message, snapshot or live, on either channel, is wrapped like
this:

```json
{"v":1,"channel":"price.crypto","seq":2,"ts":1793638131000,"payload":{ ... }}
```

- `v` — envelope version, always `1`.
- `channel` — `"price.crypto"` or `"price.crypto.twap"`.
- `seq` — sequence number, counting up by exactly 1 per message **per
  channel, per connection** (snapshots use one too). **Resets on every
  reconnect**, so only compare `seq` values from the same connection and
  channel. A jump (e.g. 41 → 43) means a message is missing.
- `ts` — the producer's event time (epoch ms). On live updates it equals
  the payload's `timestamp`; on a snapshot it equals the newest point's
  `timestamp`.
- `snapshot` — present and `true` only on the snapshot after a new
  subscription. Absent on live updates.
- `dropped` — present only when non-zero: how many messages the server
  skipped on this channel since the last one it delivered, because we were
  reading too slowly ("slow consumer"). Any appearance of this is a data gap.
- `payload` — the price itself. Its shape differs between snapshot and live
  update (below).

---

## 3. `price.crypto` — Chainlink spot BTC/USD

### Live update

```json
{"v":1,"channel":"price.crypto","seq":2,"ts":1793638131000,"payload":{"full_accuracy_value":"77400.71885192938","source":"chainlink","symbol":"btcusd","timestamp":1793638131000,"value":77400.71885192937}}
```

- `symbol` — `"btcusd"`.
- `value` — the price as a floating-point number. **Prefer
  `full_accuracy_value`**: a float can't represent every decimal exactly.
  You can see it in the example: `...92938` as text, `...92937` as a float.
- `full_accuracy_value` — the exact price as a decimal **string**, as
  published by the source. Parse this one.
- `timestamp` — the source's event time for this price (epoch ms).
- `source` — which vendor produced the price: `"chainlink"` or `"pyth"`.
  Present on every live update. During a server-side changeover,
  consecutive messages can legitimately carry different `source` values —
  check it rather than assuming Chainlink.

### Snapshot

Sent once after subscribing: the previous **two minutes** of prices in one
batch.

Real capture, shortened to the first 2 of its 119 points:

```json
{
  "v": 1,
  "channel": "price.crypto",
  "seq": 1,
  "ts": 1793638130000,
  "snapshot": true,
  "payload": {
    "data": [
      {"full_accuracy_value": "77452.06611521449",  "timestamp": 1793638012000, "value": 77452.06611521449},
      {"full_accuracy_value": "77445.867502385225", "timestamp": 1793638013000, "value": 77445.86750238523}
    ],
    "source": "chainlink",
    "symbol": "btcusd"
  }
}
```

- `symbol` and `source` appear **once**, at batch level. The points in
  `data` carry neither.
- `data` — the previous two minutes of points, **oldest first**, one per
  second, each with `timestamp`, `value`, `full_accuracy_value`. **Empty
  array** when there's no recent data.
- The first live update follows straight on: its `timestamp` is one second
  after the snapshot's newest point.

---

## 4. `price.crypto.twap` — Chainlink BTC/USD TWAP 60 s (the resolution feed)

Same envelope and the same two payload shapes as `price.crypto`, plus
`window_seconds` in the payload.

### Live update

```json
{"v":1,"channel":"price.crypto.twap","seq":2,"ts":1793638131000,"payload":{"full_accuracy_value":"77388.384727849645375488","source":"chainlink","symbol":"btcusd","timestamp":1793638131000,"value":77388.38472784964,"window_seconds":60}}
```

- `value` / `full_accuracy_value` — the time-weighted average price over
  the trailing 60 seconds. **Always use `full_accuracy_value` here**: the
  TWAP is published with 18 decimal places (`77388.384727849645375488`),
  far more than a float (`value`, about 16 significant digits) can hold.
- `window_seconds` — the averaging window, always `60`.
- `source` — always `"chainlink"`; this channel has a single provider.

### Snapshot

Real capture, shortened to the first 2 of its 119 points:

```json
{
  "v": 1,
  "channel": "price.crypto.twap",
  "seq": 1,
  "ts": 1793638130000,
  "snapshot": true,
  "payload": {
    "data": [
      {"full_accuracy_value": "77464.536862721692401664", "timestamp": 1793638012000, "value": 77464.5368627217},
      {"full_accuracy_value": "77464.389361843612680192", "timestamp": 1793638013000, "value": 77464.38936184361}
    ],
    "source": "chainlink",
    "symbol": "btcusd",
    "window_seconds": 60
  }
}
```

---

## Things to watch out for

- **A repeated `timestamp` is not necessarily a duplicate.** The docs
  describe this for the equity channels: when a market opens or closes, the
  server repeats the last price and `timestamp` with
  `is_carried_forward: true`, and warns never to discard an update only
  because its timestamp repeats. It isn't documented for `price.crypto`, but
  the same rule is the safe default.
- **Snapshots overlap data we already have.** After a reconnect, the new
  snapshot covers the last two minutes, which can include points already
  recorded on the old connection. That's fine for recording (we keep
  everything raw); whoever processes the data later must expect the
  overlap. Don't use `seq` to match points across connections — it
  restarted.
- **`seq` and `dropped` are free gap detection.** We record the raw
  messages, so both are kept in the recording for the backtest and a future
  watchdog to use.
- **Every reconnect is visible in the data.** It produces a fresh `authed`,
  two `subscribed` acks and two snapshots with `seq` back at 1 — so
  recording acks too (not just prices) marks exactly where gaps are.
- **Field order inside `payload` is alphabetical** in real traffic
  (`full_accuracy_value`, `source`, `symbol`, ...), not the order the docs
  show. JSON field order carries no meaning, so code must look fields up by
  name — never rely on position.
