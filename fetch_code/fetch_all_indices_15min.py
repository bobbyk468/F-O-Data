#!/usr/bin/env python3
"""
Fetch 15-minute spot (index) data for NSE indices: main indices + sector indices.
Iterates in 60-day periods; within each period uses 4-day sub-chunks (API limit 100 candles/call).
Saves one CSV per index in the repo directory.
Supports --workers N for parallel fetch (each worker fetches one index; delay scales to respect API rate).
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

# Max possible range: Zerodha 15min index data available from 2015-09-01 (per Kite API)
# 100 bars per request ≈ 4 trading days for 15min
DEFAULT_START_DATE = date(2015, 9, 1)
CHUNK_DAYS = 4
# Outer loop: fetch in 60-day periods (instead of ~30-day months) for fewer iterations
PERIOD_DAYS = 60

# Main indices + sector indices (tradingsymbol as in NSE)
MAIN_AND_SECTOR_SYMBOLS = [
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",  # Finnifty
    "NIFTY MIDCAP 100",
    "NIFTY NEXT 50",
    "INDIA VIX",
    # Sectors
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


def slug(symbol: str) -> str:
    """Sanitize tradingsymbol for filename: NIFTY BANK -> nifty_bank."""
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    return s.lower() or "index"


def get_index_instruments(kite):
    """Return list of {instrument_token, tradingsymbol} for NSE indices."""
    instruments = kite.instruments("NSE")
    return [
        {"instrument_token": i["instrument_token"], "tradingsymbol": i["tradingsymbol"]}
        for i in instruments
        if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"
    ]


def fetch_15min_for_instrument(kite, instrument_token, from_date, to_date, delay_sec=0.0035):
    """Fetch 15min data in 4-day chunks. Returns list of candle dicts. delay_sec between requests."""
    all_candles = []
    current_start = from_date
    while current_start <= to_date:
        current_end = min(current_start + timedelta(days=CHUNK_DAYS), to_date)
        try:
            chunk = kite.historical_data(
                instrument_token,
                current_start,
                current_end,
                interval="15minute",
            )
        except Exception as e:
            chunk = []
            if "TokenException" in str(type(e).__name__) or "Invalid" in str(e):
                raise
        if chunk:
            all_candles.extend(chunk)
        current_start = current_end + timedelta(days=1)
        time.sleep(delay_sec)
    return all_candles


def fetch_one_index(kite, instrument_token, tradingsymbol, from_date, to_date, out_dir, delay_sec=0.0035):
    """Fetch full history for one index in 60-day periods; save CSV (after each period for resume)."""
    by_ts = {}
    # Build 60-day periods (instead of ~30-day months)
    period_list = []
    d = from_date
    while d <= to_date:
        period_end = min(d + timedelta(days=PERIOD_DAYS - 1), to_date)
        period_list.append((d, period_end))
        d = period_end + timedelta(days=1)

    name = slug(tradingsymbol)
    out_path = os.path.join(out_dir, f"{name}_15min.csv")

    for period_start, period_end in period_list:
        print(f"  {period_start}..{period_end}...", end=" ", flush=True)
        try:
            candles = fetch_15min_for_instrument(
                kite, instrument_token, period_start, period_end, delay_sec
            )
        except Exception as e:
            print(f"Error: {e}")
            break
        for c in candles:
            ts = c.get("date")
            if ts is not None:
                by_ts[ts] = c
        print(f"{len(candles)}", flush=True)
        # Write CSV after each period so we keep data if interrupted
        if by_ts:
            sorted_candles = [by_ts[k] for k in sorted(by_ts)]
            with open(out_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["date", "open", "high", "low", "close", "volume"])
                for c in sorted_candles:
                    w.writerow([
                        c.get("date"), c.get("open"), c.get("high"),
                        c.get("low"), c.get("close"), c.get("volume", 0),
                    ])

    if not by_ts:
        return 0
    sorted_candles = [by_ts[k] for k in sorted(by_ts)]
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for c in sorted_candles:
            w.writerow([
                c.get("date"),
                c.get("open"),
                c.get("high"),
                c.get("low"),
                c.get("close"),
                c.get("volume", 0),
            ])
    print(f"  -> {out_path} ({len(sorted_candles)} candles)")
    return len(sorted_candles)


def _worker_fetch_one(args):
    """Run in child process: load session, fetch one index. args = (token, symbol, from_date, to_date, out_dir, delay_sec)."""
    instrument_token, tradingsymbol, from_date, to_date, out_dir, delay_sec = args
    if isinstance(from_date, (list, tuple)):
        from_date = date(from_date[0], from_date[1], from_date[2])
    if isinstance(to_date, (list, tuple)):
        to_date = date(to_date[0], to_date[1], to_date[2])
    from jugaad_trader import Zerodha
    kite = Zerodha()
    kite.set_access_token()
    return fetch_one_index(kite, instrument_token, tradingsymbol, from_date, to_date, out_dir, delay_sec)


def main():
    parser = argparse.ArgumentParser(description="Fetch 15min spot data for NSE indices")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all NSE indices (136). Default: main + sector list only.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to save CSVs (default: repo directory)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated tradingsymbols (e.g. 'NIFTY 50,NIFTY BANK'). Default: main+sector list.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel workers (each fetches one index). Default 1. Use 3-4 to speed up; delay scales to respect API rate.",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date (default: max possible = 2015-09-01).",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    base = str(REPO_ROOT)
    out_dir = args.output_dir or os.path.join(base, "data", "indices", "15min")
    os.makedirs(out_dir, exist_ok=True)
    to_date = datetime.now().date()
    if args.to_date:
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
    from_date = DEFAULT_START_DATE
    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()

    index_list = get_index_instruments(kite)
    symbol_to_token = {i["tradingsymbol"]: i["instrument_token"] for i in index_list}

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        symbols = [s for s in symbols if s in symbol_to_token]
        missing = set(s.strip() for s in args.symbols.split(",")) - set(symbol_to_token.keys())
        if missing:
            print("Note: symbols not found (skipped):", missing)
    elif args.all:
        symbols = sorted(symbol_to_token.keys())
    else:
        symbols = [s for s in MAIN_AND_SECTOR_SYMBOLS if s in symbol_to_token]
        missing = set(MAIN_AND_SECTOR_SYMBOLS) - set(symbol_to_token.keys())
        if missing:
            print("Note: symbols not in NSE indices list (skipped):", missing)

    workers = max(1, min(args.workers, 8))
    delay_sec = round(0.0035 * workers, 4)

    print(f"Fetching 15min data for {len(symbols)} indices from {from_date} to {to_date} (workers={workers}, delay={delay_sec}s).\n")

    from_date_tuple = (from_date.year, from_date.month, from_date.day)
    to_date_tuple = (to_date.year, to_date.month, to_date.day)

    if workers == 1:
        for sym in symbols:
            token = symbol_to_token[sym]
            print(f"{sym} (token {token})")
            fetch_one_index(kite, token, sym, from_date, to_date, out_dir, delay_sec)
    else:
        task_args = [
            (symbol_to_token[sym], sym, from_date_tuple, to_date_tuple, out_dir, delay_sec)
            for sym in symbols
        ]
        with Pool(workers) as pool:
            results = pool.map(_worker_fetch_one, task_args)
        for sym, count in zip(symbols, results):
            print(f"  {sym}: {count} candles")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
