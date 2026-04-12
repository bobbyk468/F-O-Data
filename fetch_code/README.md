# Fetch toolkit (`fetch_code/`)

Zerodha **historical OHLCV fetch and incremental update** scripts for this repository. Output goes to **`../data/`** (repository root). The **`jugaad_trader`** package must live at the repository root (clone layout matches [F-O-Data](https://github.com/bobbyk468/F-O-Data)).

## Setup

```bash
cd /path/to/F-O-Data   # repo root (contains data/, fetch_code/, jugaad_trader/)
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# Optional: pip install -e .  if you use pyproject local install
jtrader zerodha startsession   # or: .venv/bin/python fetch_code/test_login.py
```

## Common commands

| Task | Command (from repo root) |
|------|---------------------------|
| Login / session check | `.venv/bin/python fetch_code/test_login.py` |
| Incremental 15m (indices + F&O) | `./fetch_code/update_15m_all.sh` or `.venv/bin/python fetch_code/update_incremental.py --only all --workers 4` |
| Full refresh runner | `.venv/bin/python fetch_code/run_update_all.py` |
| Indices 15m (full / default range) | `.venv/bin/python fetch_code/fetch_all_indices_15min.py --workers 4` |
| Indices 1m | `.venv/bin/python fetch_code/fetch_all_indices_1min.py --resume ...` |
| Nifty 50 index 15m | `.venv/bin/python fetch_code/fetch_nifty50_15min.py` |
| All F&O underlyings 15m | `.venv/bin/python fetch_code/fetch_fo_stocks_15min.py --workers 4` |
| EOD (indices + F&O) | `.venv/bin/python fetch_code/fetch_eod_90d.py --only all --days 90 --workers 4` |

Shell scripts assume the virtualenv is **`.venv` at repo root** and set `ROOT` to the parent of `fetch_code/`.

## Layout

- **`repo_paths.py`** — `REPO_ROOT` and `DATA_DIR` (always the parent of `fetch_code/`).
- **`config/nifty50_symbols.txt`** — at **repository root** (not inside `fetch_code/`), used to route F&O files into `data/nifty50/` vs `data/other/`.

## Root wrappers

For backwards compatibility, short scripts at the **repository root** delegate to `fetch_code/` (same names as before). Prefer running the `fetch_code/` entrypoints directly.
