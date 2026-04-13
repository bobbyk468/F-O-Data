#!/usr/bin/env python3
"""
Fetch EOD (daily) data for:
  - Last N days only (default 90), or
  - Full history in 90-day API chunks (--full-history), from --full-history-start
    (default 2000-01-01). Zerodha only returns bars that exist (listing / exchange availability).

For: F&O stocks + indexes/sectors. Multiprocessing, delay 0.0035.
"""
# --- fetch_code/: data/ and jugaad_trader/ live at repository root ---
import sys
from pathlib import Path as _Path
_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import REPO_ROOT  # noqa: E402

import os
import re
import sys
import csv
import time
import argparse
from datetime import datetime, timedelta, date
from multiprocessing import Pool

# paths: bootstrap above

EOD_DAYS = 90
DELAY_SEC = 0.0035
# First calendar day to *request* for --full-history; API returns only existing daily bars.
# 2000-01-01 is safely before NSE/Zerodha coverage for most symbols (avoids guessing 2015 only).
DEFAULT_FULL_HISTORY_START = date(2000, 1, 1)
FULL_HISTORY_START = DEFAULT_FULL_HISTORY_START  # back-compat for imports

# Same main + sector list as 15min indices script
MAIN_AND_SECTOR_SYMBOLS = [
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "NIFTY MIDCAP 100",
    "NIFTY NEXT 50",
    "INDIA VIX",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY PHARMA",
    "NIFTY FMCG",
    "NIFTY METAL",
    "NIFTY ENERGY",
    "NIFTY REALTY",
    "NIFTY PSU BANK",
    "NIFTY MEDIA",
    "NIFTY HEALTHCARE",
    "NIFTY CONSR DURBL",
    "NIFTY OIL AND GAS",
    "NIFTY PVT BANK",
    "NIFTY INFRA",
    "NIFTY MNC",
    "NIFTY PSE",
    "NIFTY SERV SECTOR",
    "NIFTY COMMODITIES",
    "NIFTY CONSUMPTION",
]

FO_INDEX_NAMES = {
    "NIFTY",
    "BANK NIFTY",
    "BANKNIFTY",
    "FIN NIFTY",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "MIDCPNIFTY",
    "NIFTY MIDCAP SELECT",
    "FINNIFTY",
    "NIFTYNXT50",
}


def slug(symbol: str) -> str:
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_\-]", "", s)
    return s.lower() or "eq"


def _load_nifty50_symbols():
    """Load Nifty 50 constituent symbols from parent repo config (for fo_stocks nifty50/ vs other/)."""
    base = str(REPO_ROOT)
    repo_root = str(REPO_ROOT)
    path = os.path.join(repo_root, "config", "nifty50_symbols.txt")
    if not os.path.isfile(path):
        return set()
    out = set()
    with open(path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                out.add(line)
    return out


def get_index_symbol_to_token(kite):
    instruments = kite.instruments("NSE")
    return {
        i["tradingsymbol"]: i["instrument_token"]
        for i in instruments
        if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"
    }


def get_fo_symbol_to_token(kite):
    nfo = kite.instruments("NFO")
    nse = kite.instruments("NSE")
    futs = [
        i
        for i in nfo
        if i.get("instrument_type") == "FUT" and i.get("segment") == "NFO-FUT"
    ]
    fo_names = sorted(
        set(i.get("name") for i in futs if i.get("name")) - FO_INDEX_NAMES
    )
    nse_eq = {
        i["tradingsymbol"]: i["instrument_token"]
        for i in nse
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    }
    return {name: nse_eq[name] for name in fo_names if name in nse_eq}


def fetch_eod_one(kite, instrument_token, from_date, to_date, delay_sec=0.0035):
    try:
        data = kite.historical_data(
            instrument_token,
            from_date,
            to_date,
            interval="day",
        )
    except Exception as e:
        data = []
        if "TokenException" in str(type(e).__name__) or "Invalid" in str(e):
            raise
    time.sleep(delay_sec)
    return data or []


def save_eod_csv(candles, out_path):
    if not candles:
        return 0
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for c in candles:
            w.writerow([
                c.get("date"),
                c.get("open"),
                c.get("high"),
                c.get("low"),
                c.get("close"),
                c.get("volume", 0),
            ])
    return len(candles)


def _worker_fetch(args):
    """(token, symbol, from_date, to_date, out_path, delay_sec). Returns count."""
    instrument_token, symbol, from_date, to_date, out_path, delay_sec = args
    if isinstance(from_date, (list, tuple)):
        from_date = date(from_date[0], from_date[1], from_date[2])
    if isinstance(to_date, (list, tuple)):
        to_date = date(to_date[0], to_date[1], to_date[2])
    from jugaad_trader import Zerodha
    kite = Zerodha()
    kite.set_access_token()
    candles = fetch_eod_one(kite, instrument_token, from_date, to_date, delay_sec)
    return save_eod_csv(candles, out_path)


def _worker_fetch_full(args):
    """Fetch from history_start to today in 90-day chunks.
    args = (token, symbol, out_path, delay_sec, (y,m,d) history_start)."""
    instrument_token, symbol, out_path, delay_sec, start_tup = args
    from_date = date(start_tup[0], start_tup[1], start_tup[2])
    from jugaad_trader import Zerodha
    kite = Zerodha()
    kite.set_access_token()
    to_date = datetime.now().date()
    by_ts = {}
    period_start = from_date
    while period_start <= to_date:
        period_end = min(period_start + timedelta(days=EOD_DAYS - 1), to_date)
        chunk = fetch_eod_one(kite, instrument_token, period_start, period_end, delay_sec)
        for c in chunk:
            ts = c.get("date")
            if ts is not None:
                by_ts[ts] = c
        period_start = period_end + timedelta(days=1)
    sorted_candles = [by_ts[k] for k in sorted(by_ts)]
    return save_eod_csv(sorted_candles, out_path)


def run_batch(kite, symbol_to_token, from_date, to_date, out_dir, workers, delay_sec, label="", nifty50_symbols=None):
    to_date_t = (to_date.year, to_date.month, to_date.day)
    from_date_t = (from_date.year, from_date.month, from_date.day)
    tasks = []
    for sym in sorted(symbol_to_token.keys()):
        token = symbol_to_token[sym]
        name = slug(sym)
        sub = "nifty50" if (nifty50_symbols and sym in nifty50_symbols) else "other"
        if nifty50_symbols is not None:
            os.makedirs(os.path.join(out_dir, sub, "eod"), exist_ok=True)
        out_path = os.path.join(out_dir, sub, "eod", f"{name}_eod_90d.csv") if nifty50_symbols is not None else os.path.join(out_dir, f"{name}_eod_90d.csv")
        tasks.append((token, sym, from_date_t, to_date_t, out_path, delay_sec))
    if not tasks:
        print(f"  No symbols for {label}")
        return
    if workers <= 1:
        for (token, sym, fd, td, out_path, d) in tasks:
            fd = date(fd[0], fd[1], fd[2])
            td = date(td[0], td[1], td[2])
            candles = fetch_eod_one(kite, token, fd, td, d)
            n = save_eod_csv(candles, out_path)
            print(f"  {sym}: {n} days")
    else:
        with Pool(workers) as pool:
            counts = pool.map(_worker_fetch, tasks)
        for sym, n in zip(sorted(symbol_to_token.keys()), counts):
            print(f"  {sym}: {n} days")


def run_batch_full(
    kite,
    symbol_to_token,
    out_dir,
    workers,
    delay_sec,
    label="",
    nifty50_symbols=None,
    history_start: date | None = None,
):
    """Fetch from history_start (default DEFAULT_FULL_HISTORY_START) to today in 90-day intervals per symbol."""
    start = history_start or DEFAULT_FULL_HISTORY_START
    start_tup = (start.year, start.month, start.day)
    tasks = []
    for sym in sorted(symbol_to_token.keys()):
        token = symbol_to_token[sym]
        name = slug(sym)
        if nifty50_symbols is not None:
            sub = "nifty50" if sym in nifty50_symbols else "other"
            os.makedirs(os.path.join(out_dir, sub, "eod"), exist_ok=True)
            out_path = os.path.join(out_dir, sub, "eod", f"{name}_eod.csv")
        else:
            out_path = os.path.join(out_dir, f"{name}_eod.csv")
        tasks.append((token, sym, out_path, delay_sec, start_tup))
    if not tasks:
        print(f"  No symbols for {label}")
        return
    if workers <= 1:
        to_date = datetime.now().date()
        from_date = start
        for (token, sym, out_path, d, _st) in tasks:
            by_ts = {}
            period_start = from_date
            while period_start <= to_date:
                period_end = min(period_start + timedelta(days=EOD_DAYS - 1), to_date)
                chunk = fetch_eod_one(kite, token, period_start, period_end, d)
                for c in chunk:
                    ts = c.get("date")
                    if ts is not None:
                        by_ts[ts] = c
                period_start = period_end + timedelta(days=1)
            n = save_eod_csv([by_ts[ts] for ts in sorted(by_ts)], out_path)
            print(f"  {sym}: {n} days")
    else:
        with Pool(workers) as pool:
            counts = pool.map(_worker_fetch_full, tasks)
        for sym, n in zip(sorted(symbol_to_token.keys()), counts):
            print(f"  {sym}: {n} days")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch EOD (daily) data for last 90 days: F&O stocks + indexes/sectors"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Base dir for output (default: eod_data/ in repo). Creates eod_data/indices, eod_data/fo_stocks.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        metavar="N",
        help="Number of calendar days to fetch (default 90).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers (default 4). With --full-history, minimum 2 (multiprocessing).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0035,
        help="Sleep between API calls in sec (default 0.0035).",
    )
    parser.add_argument(
        "--only",
        choices=("indices", "fo", "all"),
        default="all",
        help="Fetch only indices, only F&O stocks, or all (default all).",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Fetch from --full-history-start to today in 90-day intervals (default: last --days only).",
    )
    parser.add_argument(
        "--full-history-start",
        default=None,
        metavar="YYYY-MM-DD",
        help=f"First calendar date to request with --full-history (default: {DEFAULT_FULL_HISTORY_START.isoformat()}). "
        "Earlier requests still only return data Zerodha has.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated F&O symbols (e.g. RELIANCE,BAJAJ-AUTO). Only used when --only fo. Default: all F&O.",
    )
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    base = str(REPO_ROOT)
    data_dir = os.path.join(base, "data")
    os.makedirs(data_dir, exist_ok=True)
    indices_dir = os.path.join(data_dir, "indices", "eod")  # index EOD -> data/indices/eod
    fo_dir = data_dir  # F&O EOD -> data/nifty50/eod & data/other/eod
    os.makedirs(indices_dir, exist_ok=True)
    for sub in ("nifty50", "other"):
        os.makedirs(os.path.join(fo_dir, sub, "eod"), exist_ok=True)

    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=args.days)
    workers = max(1, min(args.workers, 8))
    if args.full_history:
        workers = max(2, workers)  # force multiprocessing for full-history (faster)
    delay_sec = max(0.001, args.delay)

    history_start = DEFAULT_FULL_HISTORY_START
    if args.full_history_start:
        history_start = datetime.strptime(args.full_history_start, "%Y-%m-%d").date()

    if args.full_history:
        print(
            f"EOD fetch: full history from {history_start} to {to_date} "
            f"({EOD_DAYS}-day API chunks; Zerodha returns only existing bars)"
        )
    else:
        print(f"EOD fetch: last {args.days} days ({from_date} to {to_date})")
    print(f"Output: data/indices/eod, data/nifty50/eod, data/other/eod (workers={workers}, delay={delay_sec}s)\n")

    if args.only in ("indices", "all"):
        index_map = get_index_symbol_to_token(kite)
        indices_symbols = {
            s: index_map[s]
            for s in MAIN_AND_SECTOR_SYMBOLS
            if s in index_map
        }
        print(f"Indices & sectors ({len(indices_symbols)} symbols) -> {indices_dir}")
        if args.full_history:
            run_batch_full(
                kite,
                indices_symbols,
                indices_dir,
                workers,
                delay_sec,
                "indices",
                history_start=history_start,
            )
        else:
            run_batch(
                kite,
                indices_symbols,
                from_date,
                to_date,
                indices_dir,
                workers,
                delay_sec,
                "indices",
            )
        print()

    if args.only in ("fo", "all"):
        fo_map = get_fo_symbol_to_token(kite)
        if args.symbols:
            requested = [s.strip() for s in args.symbols.split(",") if s.strip()]
            fo_map = {s: fo_map[s] for s in requested if s in fo_map}
            missing = set(requested) - set(fo_map.keys())
            if missing:
                print("Note: symbols not in F&O list (skipped):", sorted(missing))
        fo_nifty50 = _load_nifty50_symbols()
        if fo_nifty50:
            print(f"F&O stocks ({len(fo_map)} symbols) -> {fo_dir}/nifty50/eod & .../other/eod")
        else:
            print(f"F&O stocks ({len(fo_map)} symbols) -> {fo_dir}")
        if args.full_history:
            run_batch_full(
                kite,
                fo_map,
                fo_dir,
                workers,
                delay_sec,
                "fo",
                nifty50_symbols=fo_nifty50 or None,
                history_start=history_start,
            )
        else:
            run_batch(
                kite,
                fo_map,
                from_date,
                to_date,
                fo_dir,
                workers,
                delay_sec,
                "fo",
                nifty50_symbols=fo_nifty50 or None,
            )

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
