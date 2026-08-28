# Data Refresh Runbook

Complete guide for refreshing all Zerodha market data. This document is intended for both human operators and AI agents picking up the task.

---

## Overview

The pipeline runs in four sequential stages:

| Stage | Script | Updates | Count |
|-------|--------|---------|-------|
| 1 | `fetch_code/test_login.py` | Zerodha session token | — |
| 2 | `fetch_code/update_incremental.py` | 15min + EOD for indices and all F&O stocks | 473 files |
| 3 | `fetch_code/fetch_all_indices_5min.py` | 5min for 25 NSE indices | 25 files |
| 4 | `fetch_code/fetch_fo_stocks_5min.py` | 5min for all F&O stocks | ~211 files |

**All commands must be run from the repo root:**
```
cd /Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader
```

The Python virtualenv is at `.venv/`. Always prefix commands with `.venv/bin/python`.

---

## Credentials

Credentials are stored in `.env` (git-ignored) at the repo root. The login script reads them automatically:

```
ZERODHA_USER_ID=MEX578
ZERODHA_PASSWORD=...
ZERODHA_TOTP_SECRET=...   # authenticator setup key for auto-TOTP
```

The session is saved as a pickle file at:
```
~/Library/Application Support/jtrader/.zsession
```

---

## Stage 1 — Login

```bash
.venv/bin/python fetch_code/test_login.py
```

Expected output ends with:
```
Logged in successfully.
Profile: Brahmaji Katragadda | Email: brahmaji.bobby@gmail.com
Session saved to: .../jtrader/.zsession
```

**If this fails:** check that `.env` exists and contains all three keys. The TOTP secret must be the raw base32 setup key (not a time-based OTP code).

Note: `update_incremental.py` can auto-re-login if the session expires mid-run (it calls `test_login.py` internally on `TokenException`). For the 5min scripts, a fresh session from Stage 1 is required before running.

---

## Stage 2 — Incremental 15min + EOD update

```bash
.venv/bin/python fetch_code/update_incremental.py --only all --workers 4
```

**What it does:**
- Reads the last timestamp in each existing CSV
- Fetches only the missing range from Kite API
- Merges new candles and rewrites the full file
- After merge, recomputes CPR (prior session), SuperTrend(25,7), and Bollinger(25,2) on the full series

**Files updated (473 total):**

| Path | Stocks | Timeframe |
|------|--------|-----------|
| `data/indices/15min/` | 25 NSE indices | 15min |
| `data/indices/eod/` | 25 NSE indices | EOD |
| `data/nifty50/15min/` | 50 Nifty50 F&O stocks | 15min |
| `data/nifty50/eod/` | 50 Nifty50 F&O stocks | EOD |
| `data/other/15min/` | ~174 other F&O stocks | 15min |
| `data/other/eod/` | ~174 other F&O stocks | EOD |

**Options:**
```bash
--only all          # all 473 files (default for a full refresh)
--only indices15    # only indices 15min
--only fo15         # only F&O 15min (nifty50 + other)
--only indiceseod   # only indices EOD
--only foeod        # only F&O EOD
--workers N         # parallel workers (4 is safe; don't exceed 8)
```

**Expected output:** one `[done] SYMBOL: N rows` line per file. Ends with:
```
Done. Updated 473/473 files.
```

---

## Stage 3 — 5min Indices

```bash
.venv/bin/python fetch_code/fetch_all_indices_5min.py
```

**What it does:**
- Fetches 5min OHLCV for 25 NSE indices (spot prices)
- Resumes from last saved bar date; fetches only missing range
- Requests ~90 calendar days per API call (Kite limit)
- Output is plain OHLCV (no indicator columns)

**Files updated (25 total):**
```
data/indices/5min/nifty_50_5min.csv
data/indices/5min/nifty_bank_5min.csv
data/indices/5min/nifty_fin_service_5min.csv
... (all 25 indices)
```

**Expected output per index:**
```
NIFTY 50 (token 256265)
  Resume: 202223 bars on disk, fetch from 2026-08-10...
  2026-08-10..2026-08-22... 750
  -> data/indices/5min/nifty_50_5min.csv (202898 candles)
```

**Connection errors:** If a `ConnectionResetError` appears for one index (Kite occasionally drops), re-run the full script — it resumes from the last saved bar and only re-fetches the missing chunk. No data is lost.

---

## Stage 4 — 5min F&O Stocks

```bash
.venv/bin/python fetch_code/fetch_fo_stocks_5min.py
```

**What it does:**
- Fetches 5min OHLCV for all F&O equity underlyings
- Same resume/merge logic as Stage 3
- Nifty50 stocks → `data/nifty50/5min/`
- Other F&O stocks → `data/other/5min/`

**Files updated (~211 total):**
```
data/nifty50/5min/<slug>_5min.csv   (49 stocks)
data/other/5min/<slug>_5min.csv     (162 stocks)
```

Ends with:
```
Done.
```

---

## Data Directory Layout

```
data/
├── indices/
│   ├── 15min/      25 files — NSE index 15min OHLCV + CPR + SuperTrend + BB
│   ├── eod/        25 files — NSE index daily OHLCV + CPR + SuperTrend + BB
│   └── 5min/       25 files — NSE index 5min plain OHLCV
├── nifty50/
│   ├── 15min/      50 files — Nifty50 stock 15min + indicators
│   ├── eod/        50 files — Nifty50 stock daily + indicators
│   └── 5min/       49 files — Nifty50 stock 5min plain OHLCV
└── other/
    ├── 15min/     ~174 files — other F&O stock 15min + indicators
    ├── eod/       ~174 files — other F&O stock daily + indicators
    └── 5min/      ~162 files — other F&O stock 5min plain OHLCV
```

Resampled timeframes (30/45/60min) derived from 15min data are in `data/indices/` subdirectories.

---

## When Local Data Has Been Deleted

If the `data/` directory was removed from the local filesystem (e.g. to free disk space after pushing to git), restore it before running the incremental pipeline. Without local files the scripts fall back to fetching full history from 2015, which takes hours.

**Step 1 — Restore from git:**
```bash
git checkout HEAD -- data/
```

This restores all 757 tracked files from the latest commit (~5.9 GB, takes ~30–60 seconds).

**Step 2 — Then run the normal pipeline** (Stages 1–4 above). The incremental scripts will resume from the last saved timestamp in each restored file and only fetch what's new.

---

## Git Push

After all four stages complete:

```bash
git add data/
git commit -m "Refresh all data to YYYY-MM-DD (15min, EOD, 5min indices + F&O)"
git push origin HEAD:main
```

The push is large (~4–5M insertions on a full weekly refresh). It typically takes 3–5 minutes. Run in the background if needed:

```bash
git push origin HEAD:main &
```

The branch is `fo-data-main` locally; it tracks `origin/main` on the remote.

---

## Full Refresh — One-liner Sequence

```bash
cd /Users/brahmajikatragadda/Desktop/Zerodha_Data/jugaad-trader

.venv/bin/python fetch_code/test_login.py \
  && .venv/bin/python fetch_code/update_incremental.py --only all --workers 4 \
  && .venv/bin/python fetch_code/fetch_all_indices_5min.py \
  && .venv/bin/python fetch_code/fetch_fo_stocks_5min.py \
  && git add data/ \
  && git commit -m "Refresh all data to $(date +%Y-%m-%d) (15min, EOD, 5min indices + F&O)" \
  && git push origin HEAD:main
```

---

## Timing

| Stage | Typical Duration |
|-------|-----------------|
| Login | ~5 seconds |
| 15min + EOD (473 files, 4 workers) | 5–10 minutes |
| 5min indices (25 files, sequential) | 2–4 minutes |
| 5min F&O (~211 files, sequential) | 10–20 minutes |
| Git commit + push | 3–6 minutes |
| **Total** | **~25–40 minutes** |

Duration depends on how many trading days have elapsed since the last refresh. A daily refresh is fast (seconds per stage); a weekly refresh is at the longer end.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `TokenException` mid-run | Session expired | `update_incremental.py` handles this automatically; for 5min scripts, re-run Stage 1 then the failing stage |
| `ConnectionResetError` on one file | Kite API dropped connection | Re-run the failing stage; resume logic skips already-fetched data |
| `[done] SYMBOL: same N rows` (no increment) | No new trading days since last refresh | Normal; market was closed |
| `fatal: upstream branch does not match` on push | Branch name mismatch | Use `git push origin HEAD:main` explicitly |
| Push times out (>5 min) | Large pack size | Run `git push origin HEAD:main &` in background |
