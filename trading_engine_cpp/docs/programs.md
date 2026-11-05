# Programs

Every program `cmake --build` produces, what it's for, and how to run it.
Build instructions are in the [README](../README.md).

All commands below are run from `trading_engine_cpp/`, so the programs
are at `./build/<name>` and recordings land in `data/` (git-ignored).

| Program | What it's for |
|---|---|
| [`binance_ws`](#binance_ws) | records Binance BTCUSDT trades and order book |
| [`clob_ws`](#clob_ws) | records the current Polymarket BTC 5m market's order book |
| [`polybolt_ws`](#polybolt_ws) | records the Chainlink BTC price and the TWAP the markets resolve on |
| [`compress_hours`](#compress_hours) | compresses finished hourly recordings to `.zst` |
| [`get_market`](#get_market) | looks up a BTC 5m market's token ids (debugging) |
| [`tools/latency_test.sh`](#toolslatency_testsh) | measures the round trip from this machine to a server |
| [Tests](#tests) | five test programs, run after every build |

---

## The three recorders: what they share

`binance_ws`, `clob_ws` and `polybolt_ws` work the same way, so this is
described once here.

### Options (all three)

| Option | Meaning | Default |
|---|---|---|
| `--out-dir <folder>` | write one file per UTC hour to `<folder>/<feed>/<feed>_YYYY-MM-DDTHH.jsonl` | none: records go to stdout |
| `--verbose` | also print every message, decoded, to stderr. **Slow**: each print delays the receiving thread, and so the timestamps. For debugging only. | off |
| `--url <ws-url>` | connect somewhere other than the real feed, e.g. a local test server for testing reconnects | the real feed |
| `--help`, `-h` | print the options and exit | |

An unknown option (e.g. a typo like `--verbos`) is an error, not ignored.

### Where output goes

- **Data** goes to the hourly files with `--out-dir`, or to **stdout**
  without it (e.g. `./build/binance_ws > test.jsonl`).
- **Logs** always go to **stderr**, so they can never end up inside the
  data. They include connection events, reconnects, errors, and a
  `[stats]` line every 5 seconds:
  ```
  [stats] 752 msgs in last 5s, 0 dropped
  ```
  `dropped` counts every record that didn't get saved, whether the queue
  overflowed or the disk failed. It should always be 0.

### The record format

One JSON object per line. `recv_ts` is when our machine received the
message, in **microseconds** since 1970 (UTC). `data` is the message
exactly as the feed sent it.

```json
{"recv_ts":1793799953016819,"data":{...the original message...}}
```

`clob_ws` adds the market's 5-minute window start (seconds since 1970):

```json
{"recv_ts":1793802312345678,"window":1793802300,"data":{...}}
```

A new file starts at each UTC hour, chosen by each record's own `recv_ts`.
Restarting within the same hour adds to the existing file.

### Reconnecting

All three reconnect by themselves:

- **When they reconnect:** after a dropped connection, and also when the
  feed goes **silent** (no messages for 10s for Binance and PolyBolt, 30s
  for clob).
- **How long they wait:** a random time between 0 and a limit. The limit
  starts at 1 second and doubles after each failed attempt, up to 30
  seconds. The randomness stops many clients from all retrying at the
  same moment.
- **When the wait resets:** once a connection has stayed healthy for 30
  seconds.

### Stopping

Press **Ctrl+C** (or send SIGTERM, which is what the server's service
manager does). The recorder closes the connection, writes everything
still queued, and exits. No data is lost.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | stopped normally (Ctrl+C / SIGTERM), or `--help` |
| 1 | bad option; `--out-dir` folder can't be created or written to; `polybolt_ws` only: missing credentials, or the server rejected them (close code 4001 or 4008) |

---

## binance_ws

Records Binance's BTCUSDT `aggTrade` (trades), `bookTicker` (best bid
and ask) and `depth5@100ms` (top 5 levels, every 100 ms) streams. The
trade timestamps `E` and `T` arrive in microseconds. Message details are
in [binance_message_types.md](binance_message_types.md).

```bash
./build/binance_ws --out-dir data          # -> data/binance/binance_2026-11-04T14.jsonl
./build/binance_ws > test.jsonl            # quick test to a single file
./build/binance_ws --verbose > /dev/null   # watch decoded messages, discard the data
```

## clob_ws

Records Polymarket's order book feed for the BTC 5-minute Up/Down market
that's running right now:

1. **At startup** it looks up the market's Up and Down tokens (from
   Polymarket's Gamma API), retrying every 3 seconds until it gets them.
2. **Every 5 minutes** it switches to the next market by itself. It
   subscribes 30 seconds before the boundary, so the new market's data
   starts arriving right on time.
3. **Every record** carries the `window` it belongs to.

It sends `PING` every 9 seconds to keep the connection open. The server's
`PONG` replies are not recorded and don't count as feed activity.
Subscription and message details are in
[clob_subscription.md](clob_subscription.md) and
[clob_message_types.md](clob_message_types.md).

```bash
./build/clob_ws --out-dir data             # -> data/clob/clob_2026-11-04T14.jsonl
```

Each hourly file holds 12 five-minute markets, told apart by `window`.

## polybolt_ws

Records two Polymarket PolyBolt channels:

- `price.crypto.twap` (btcusd, 60-second window): **the price the BTC 5m
  markets resolve on**;
- `price.crypto` (btcusd): the Chainlink spot price that TWAP averages.

On connecting, it logs in, waits for the server to confirm, and then
subscribes. Every message is recorded, including the server's
replies. Details are in
[polybolt_subscription.md](polybolt_subscription.md) and
[polybolt_message_types.md](polybolt_message_types.md).

**Needs credentials** in three environment variables: `POLY_API_KEY`,
`POLY_SECRET` and `POLY_PASSPHRASE`. Keep them in `trading_engine_cpp/.env`
(git-ignored, never commit it) and load them into the shell first:

```bash
set -a; source .env; set +a
./build/polybolt_ws --out-dir data         # -> data/polybolt/polybolt_2026-11-04T14.jsonl
```

If any are missing, it names which ones and exits. It never prints their
values.

**How it reacts to the server closing the connection:**

| Close code | Meaning | What happens |
|---|---|---|
| 4001 | credentials rejected | exit 1 (retrying can't help) |
| 4008 | a limit was broken | exit 1 |
| 4003 | server restarting | reconnect after a random 0–10 s |
| anything else | network drop etc. | normal reconnect |

---

## compress_hours

Compresses every **finished** hourly recording to zstd, making one pass
and then exiting. On the server it runs every 10 minutes at the lowest
priority, so it never slows the recorders.

```
./build/compress_hours --data-dir <folder> [--level <1-22>] [--dry-run]
```

| Option | Meaning | Default |
|---|---|---|
| `--data-dir <folder>` | the folder the recorders write to (their `--out-dir`). **Required.** Must already exist. | |
| `--level <1-22>` | zstd compression level: higher = smaller files, slower to compress. Opening them stays fast at every level. | 19 (until it's chosen from a full real hour of data) |
| `--dry-run` | only list what would be compressed; change nothing | off |
| `--help`, `-h` | print the options and exit | |

```bash
./build/compress_hours --data-dir data --dry-run   # see what it would do
nice -n 19 ./build/compress_hours --data-dir data  # do it, at low priority
```

**Which files count as finished.** A file is compressed only when *both*
are true:

- the hour in its name ended at least 5 minutes ago (UTC);
- it hasn't been modified for at least 5 minutes.

Only exact `<feed>_YYYY-MM-DDTHH.jsonl` names inside `<feed>/` folders
are touched. Anything else is left alone.

**What happens to each file:**

1. `X.jsonl` is compressed to `X.jsonl.zst.tmp`.
2. The `.tmp` is checked byte for byte against the original.
3. It's renamed to `X.jsonl.zst`.
4. Only then is `X.jsonl` deleted.

So **a `.zst` file is always complete**: ignore any `.tmp`. Each `.zst`
also contains a checksum, so damage is detected when it's read. To read
one by hand: `zstd -dc data/clob/clob_2026-11-04T13.jsonl.zst | head`.

**Output** goes to stdout, one line per file plus a summary:

```
compressed clob_2026-11-04T13.jsonl: 7.64 MB -> 0.60 MB (12.8x) in 8.4s
1 compressed, 0 failed
```

Only one copy runs per data folder at a time, enforced by a lock on
`<folder>/.compress_hours.lock`. A second copy prints `another
compress_hours is already running` and exits 1.

| Exit code | Meaning |
|---|---|
| 0 | everything worked, or nothing to do |
| 1 | at least one file failed (details in the output, other files still done); bad option; folder missing; already running |

---

## get_market

Looks up a BTC 5-minute market's Up token, Down token and condition id
from Polymarket's Gamma API. This is the same lookup `clob_ws` does at
startup and at each switch. Useful to check that the lookup works, or to
see the ids for a market.

```bash
./build/get_market                 # the market running right now
./build/get_market 1793802300      # the market whose window starts at that time (seconds since 1970)
```

```
token up: 1234...
token down: 5678...
condition id: 0xabc...
```

Exits 1 with `no valid market returned` if the market doesn't exist (yet)
or the lookup failed.

---

## tools/latency_test.sh

A bash script (not built by CMake) that measures how long a request takes
to get an answer back from a server, over one reused connection, the way
a trading client works. It's for comparing machines: run it on each one
**at the same time**.

```
./tools/latency_test.sh [url] [count]
```

| Argument | Meaning | Default |
|---|---|---|
| `url` | what to ask. The default is an answer Cloudflare can't serve from memory, so every request reaches Polymarket's own server. | `https://clob.polymarket.com/time` |
| `count` | how many requests to time, sent 5 per second (at least 10) | 1000 (~3.5 minutes) |

```bash
./tools/latency_test.sh                                       # Polymarket CLOB
./tools/latency_test.sh https://api.binance.com/api/v3/time   # Binance
```

```
front door only (TCP connect):    21.18 ms
full round trip, 50 samples:
  min 32.66   median 40.87   p90 101.25   p99 142.73   max 142.73 ms
```

- **Front door only:** one round trip to the nearest Cloudflare or
  CloudFront location, which is what `ping` would show.
- **Full round trip:** to the real server and back. Picture the samples
  sorted from fastest to slowest: `median` is the middle one, and `p99`
  is the one only 1% of requests were slower than, which shows how
  consistent the connection is.

The first request (which opens the connection) isn't counted, and
neither are requests that had to reopen the connection or got an error
status. These are reported as notes. Exits 1 if fewer than 95% of
requests could be used. Needs only bash and curl 7.84 or newer.

---

## Tests

Run all five after every build, and always on a new machine:

```bash
cd build
./recorder_test && ./backoff_test && ./hourly_file_sink_test && ./compress_file_test && ./compress_hours_test
```

Each prints a `pass` / `FAIL` line per check, ends with `all checks
passed`, and exits 0, or exits 1 if any check failed. They work in the
system's temp folder and need no network or credentials.

| Test | Checks |
|---|---|
| `recorder_test` | the queue between receiving and writing: nothing lost or duplicated with several threads, shutdown writes everything, every dropped record is counted |
| `backoff_test` | reconnect waits: random up to a limit that starts at 1s and doubles up to 30s, and the limit never overflows |
| `hourly_file_sink_test` | hourly files: the exact hour boundary, appending after a restart, repairing a half-written line, a clock going backwards, a full disk counted record by record, error lines not flooding the log |
| `compress_file_test` | compressing one file safely: the result is identical to the original, crash leftovers are finished, a mismatching `.zst` is left alone, bad levels are refused |
| `compress_hours_test` | which files count as finished, strict file names (matching what the recorders write), carrying on past a failed file, `--dry-run`, the single-copy lock |
