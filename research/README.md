# research

Research on the recorded Polymarket BTC 5-minute market data: converting
the raw recordings into clean Parquet tables, analysis, backtesting and
machine learning.

The data design (layers, decisions, table definitions) is in
[docs/](docs/README.md).

## Setup

Needs [uv](https://docs.astral.sh/uv/). It installs the right Python (see
`.python-version`) and the exact package versions from `uv.lock` into a
private `.venv`:

```bash
cd research
uv sync
```

## Checks

```bash
uv run pytest               # tests
uv run ruff check           # likely mistakes
uv run ruff format          # consistent layout (--check to only report)
```
