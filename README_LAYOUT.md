# Jugaad-trader — Layout

All fetched data and logs are under fixed directories so the repo root stays clean.

## Structure

```
jugaad-trader/
├── data/                      # All fetched data (same pattern: 15min + eod per folder)
│   ├── indices/                # Nifty 50 index, Bank, sector indices
│   │   ├── 15min/              # nifty_*.csv, india_vix_15min.csv
│   │   ├── 5min/               # nifty_*_5min.csv (plain OHLCV; fetch_all_indices_5min.py)
│   │   ├── 1min/               # nifty_*_1min.csv (optional; see fetch_all_indices_1min.py)
│   │   └── eod/                # *_eod.csv
│   ├── nifty50/                # Nifty 50 constituent stocks
│   │   ├── 15min/
│   │   ├── 5min/
│   │   └── eod/
│   └── other/                  # Other F&O stocks
│       ├── 15min/
│       ├── 5min/
│       └── eod/
├── logs/                      # Fetch run logs
│   ├── eod_fo_full_log.txt
│   ├── fetch_fo_log.txt
│   └── fetch_indices_log.txt
├── docs/                      # README_indices_fetch.md, USAGE.md, etc.
├── fetch_all_indices_15min.py # → data/indices/15min/
├── fetch_all_indices_5min.py  # → data/indices/5min/
├── fetch_all_indices_1min.py  # → data/indices/1min/
├── fetch_fo_stocks_5min.py     # → data/nifty50/5min & data/other/5min/
├── fetch_nifty50_15min.py     # → data/indices_15min/nifty_50_15min.csv
├── fetch_fo_stocks_15min.py   # → data/fo_stocks/
├── fetch_eod_90d.py           # → data/eod_data/
├── align_eod_fo.py            # reads data/fo_stocks & data/eod_data/fo_stocks
└── ...
```

## Default paths (no flags)

- **Indices 15min:** `python fetch_all_indices_15min.py` → `data/indices/15min/`
- **Indices 5min:** `python fetch_all_indices_5min.py` → `data/indices/5min/` (plain OHLCV; same symbol set as 15min; `--workers 4` optional). Refreshes session via **`test_login`** if the access token expires mid-run. `fetch_code/fetch_nifty50_5min.py` is Nifty-50-only to the same folder.
- **F&O 5min:** `python fetch_fo_stocks_5min.py` → `data/nifty50/5min/` & `data/other/5min/` (same auto re-login on token errors)
- **Indices 1min:** `python fetch_all_indices_1min.py` → `data/indices/1min/`  
  - Default range: **2015-01-01 → today** (very large; run in a persistent terminal or `nohup`).  
  - Daily updates: `--from-date YYYY-MM-DD`. Zerodha may only return minute bars from ~**Sep 2015** for some indices; earlier days may be empty.
  - Example full backfill: `./run_fetch_indices_1min_from_2015.sh` (logs under `logs/`; uses `--resume` so restarts continue from last row in each CSV). Outer batches default to **30 calendar days** (`--period-days 30`); use `--period-days 7` or `14` for more frequent checkpoints.
  - **Coverage check:** `python verify_1min_indices.py` (missing files, stale series, gaps excluding lunch break).
  - **Foreground (see progress in Terminal):** `./run_1min_indices_foreground.sh` — prints live lines + saves `logs/1min_foreground_*.log`. In Cursor: **Terminal → New Terminal**, `cd` to `jugaad-trader`, run the script.
- **Nifty 50 15min:** `python fetch_nifty50_15min.py` → `data/indices/15min/nifty_50_15min.csv`
- **F&O 15min:** `python fetch_fo_stocks_15min.py` → `data/nifty50/15min/` & `data/other/15min/`
- **EOD (FO):** `python fetch_eod_90d.py --only fo` → `data/nifty50/eod/` & `data/other/eod/`
- **EOD (indices):** `python fetch_eod_90d.py --only indices` → `data/indices/eod/`
- **Align EOD with 15min:** `python align_eod_fo.py --dry-run` uses `data/nifty50/15min`, `data/other/15min`, and same for eod

You can still override with `--output-dir` / `--fo-stocks-dir` / `--eod-fo-dir` where supported.
