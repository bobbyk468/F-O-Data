# Fetching 15-min spot data for NSE indices

## Script: `fetch_all_indices_15min.py`

Fetches 15-minute OHLC data for NSE indices (spot) for **max possible range** by default: **2015-09-01** to **today**, in **60-day periods** (was ~30-day months). You can override with `--from-date` / `--to-date`. Saves one CSV per index in the repo directory.

### Default list (main + sector indices)

- **Main:** NIFTY 50, NIFTY BANK, NIFTY FIN SERVICE (Finnifty), NIFTY MIDCAP 100, NIFTY NEXT 50, INDIA VIX  
- **Sectors:** NIFTY IT, NIFTY AUTO, NIFTY PHARMA, NIFTY FMCG, NIFTY METAL, NIFTY ENERGY, NIFTY REALTY, NIFTY PSU BANK, NIFTY MEDIA, NIFTY HEALTHCARE, NIFTY CONSR DURBL, NIFTY OIL AND GAS, NIFTY PVT BANK, NIFTY INFRA, NIFTY MNC, NIFTY PSE, NIFTY SERV SECTOR, NIFTY COMMODITIES, NIFTY CONSUMPTION  

### Usage

```bash
# Activate venv (or use .venv/bin/python)
cd /Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader

# Default: fetch main + sector indices (in 60-day periods)
.venv/bin/python fetch_all_indices_15min.py

# Fetch only specific symbols (faster for testing)
.venv/bin/python fetch_all_indices_15min.py --symbols "NIFTY 50,NIFTY BANK,NIFTY FIN SERVICE"

# Use 4 parallel workers (faster; respects API rate limit)
.venv/bin/python fetch_all_indices_15min.py --workers 4
.venv/bin/python fetch_all_indices_15min.py --symbols "NIFTY BANK,NIFTY FIN SERVICE,NIFTY IT" --workers 3

# Fetch all 136 NSE indices
.venv/bin/python fetch_all_indices_15min.py --all

# Save CSVs to a different directory
.venv/bin/python fetch_all_indices_15min.py --output-dir /path/to/csvs

# Custom date range (default is max possible: 2015-09-01 to today)
.venv/bin/python fetch_all_indices_15min.py --from-date 2020-01-01 --to-date 2024-12-31
```

### Output files

- `nifty_50_15min.csv`, `nifty_bank_15min.csv`, `nifty_fin_service_15min.csv`, `nifty_it_15min.csv`, etc.  
- Columns: `date`, `open`, `high`, `low`, `close`, `volume`  
- Index data has `volume=0`.

### Prerequisites

- Zerodha session must be valid: run `jtrader zerodha startsession` or `python test_login.py` if needed.  
- Network: ensure no proxy blocks `kite.zerodha.com` (or set `http_proxy=` / `https_proxy=` when running).

### Runtime

- Default list (~25 indices): about **1–2 hours** (60-day periods, rate-limited).  
- Use `--symbols "NIFTY 50,NIFTY BANK"` for a quick test.

---

## F&O stocks: `fetch_fo_stocks_15min.py`

After indices/sectors are done, fetch **15-minute spot (equity) data for all F&O stocks** (~207 symbols). Uses the same 60-day periods and 4-day chunks; saves one CSV per stock under `fo_stocks/` by default.

### Usage

```bash
# All F&O stocks (default 4 workers), output in fo_stocks/
.venv/bin/python fetch_fo_stocks_15min.py

# Custom output dir and workers
.venv/bin/python fetch_fo_stocks_15min.py --output-dir /path/to/csvs --workers 6

# Only a few symbols (for testing)
.venv/bin/python fetch_fo_stocks_15min.py --symbols "RELIANCE,TCS,INFY"

# Custom date range
.venv/bin/python fetch_fo_stocks_15min.py --from-date 2023-01-01 --to-date 2024-12-31
```

### Output

- **Directory:** `fo_stocks/` (or `--output-dir`)
- **Files:** `reliance_15min.csv`, `tcs_15min.csv`, etc. (same columns: date, open, high, low, close, volume)
- **Symbols:** All NSE equity underlyings that have F&O (indices like NIFTY/BANKNIFTY are excluded; fetch those with the indices script).

---

## EOD (daily) 90-day data: `fetch_eod_90d.py`

Fetches **end-of-day (daily)** data for the **last 90 days** for:
- **Indices & sectors** (same 25 as 15min) → `eod_data/indices/`
- **F&O stocks** (all ~207) → `eod_data/fo_stocks/`

Uses multiprocessing (default 4 workers) and delay 0.0035s. One API call per instrument (90 days &lt; 100 candles).

### Usage

```bash
# All: indices + F&O stocks (90 days, 4 workers)
.venv/bin/python fetch_eod_90d.py

# Only indices or only F&O
.venv/bin/python fetch_eod_90d.py --only indices
.venv/bin/python fetch_eod_90d.py --only fo

# Custom days, workers, output dir
.venv/bin/python fetch_eod_90d.py --days 60 --workers 6 --output-dir /path/to/eod_data
```

### Output

- `eod_data/indices/<symbol>_eod_90d.csv` (e.g. `nifty_50_eod_90d.csv`)
- `eod_data/fo_stocks/<symbol>_eod_90d.csv` (e.g. `reliance_eod_90d.csv`)
- Columns: `date`, `open`, `high`, `low`, `close`, `volume`
