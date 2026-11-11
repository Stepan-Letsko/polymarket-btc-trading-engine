# trading_engine_cpp

The C++ data recorders for Polymarket's BTC 5-minute Up/Down markets, and
the tool that compresses their recordings.

## Requirements

Two kinds of dependencies:

- **Installed on the machine** (below): the compiler, CMake, git, and the
  system libraries IXWebSocket builds against - OpenSSL (encrypted
  connections) and zlib (optional websocket compression).
- **Fetched automatically by CMake** at pinned versions, nothing to
  install: [IXWebSocket](https://github.com/machinezone/IXWebSocket)
  v12.1.0 and [zstd](https://github.com/facebook/zstd) v1.5.7.
  [simdjson](https://github.com/simdjson/simdjson) is included in
  `third_party/`.

### Debian / Ubuntu (the server)

```bash
sudo apt update
sudo apt install -y build-essential cmake git pkg-config libssl-dev zlib1g-dev
```

The `-dev` packages are what *building against* a library needs; the
plain ones only cover running programs.

### macOS

```bash
xcode-select --install   # compiler; zlib and TLS come with macOS
brew install cmake
```

## Building

```bash
mkdir -p build && cd build
cmake ..                  # Release (optimised) is the default
cmake --build . -j2       # -j = files compiled at once; up to your vCPU count
```

The first `cmake ..` downloads IXWebSocket and zstd, so it needs internet
access.

## Tests

Run these after building, and always on a new machine:

```bash
./recorder_test && ./backoff_test && ./hourly_file_sink_test && ./compress_file_test && ./compress_hours_test
```

Each prints `all checks passed` and exits 0, or names the failing check
and exits 1.

On Linux, also test the upload script (against a fake Google, no network
needed): `../deploy/upload_hours_test.sh`.

## Programs

| Program | What it does |
|---|---|
| `binance_ws` | records Binance BTCUSDT aggTrade, bookTicker and depth5 |
| `clob_ws` | records the current Polymarket BTC 5m market's order book, switching markets every 5 minutes |
| `polybolt_ws` | records Polymarket's Chainlink BTC/USD price and the TWAP-60s the markets resolve on (needs the `POLY_*` credentials in `.env`) |
| `compress_hours` | compresses finished hourly files to `.zst` |
| `get_market` | prints a BTC 5m market's Up/Down token ids and condition id (debugging) |

Run from `trading_engine_cpp/`, so `data/` lands there (it is git-ignored):

```bash
./build/binance_ws --out-dir data
./build/clob_ws    --out-dir data
set -a; source .env; set +a; ./build/polybolt_ws --out-dir data
./build/compress_hours --data-dir data
```

Without `--out-dir`, the recorders write to stdout instead. Every option,
example, output and exit code is in [docs/programs.md](docs/programs.md). The `docs/` folder describes each
feed's connection and message format.

Running them permanently on a server (systemd, compression, the upload
to Google Cloud Storage) is in [docs/deployment.md](docs/deployment.md).
