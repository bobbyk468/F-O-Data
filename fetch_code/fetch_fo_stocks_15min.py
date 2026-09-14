#!/usr/bin/env python3
"""
Fetch 15-minute spot (equity) data for all F&O (Futures & Options) stocks.
Uses same 60-day periods and 4-day sub-chunks as index fetch. Saves one CSV per stock.
Run after sector indices are done. Requires valid Zerodha session.
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
import sys
import argparse
from datetime import datetime, date
from multiprocessing import Pool

# paths: bootstrap above

# Reuse fetch logic from indices script
from fetch_code.fetch_all_indices_15min import (
    fetch_one_index,
    _worker_fetch_one,
    DEFAULT_START_DATE,
)

def _load_nifty50_symbols():
    """Load Nifty 50 constituent symbols from parent repo config (for fo_stocks nifty50/ vs other/)."""
    base_dir = str(REPO_ROOT)
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


# F&O index underlyings to exclude (we fetch indices separately)
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


def get_fo_equity_symbol_to_token(kite):
    """Return dict of NSE equity tradingsymbol -> instrument_token for all F&O stocks."""
    nfo = kite.instruments("NFO")
    nse = kite.instruments("NSE")
    # Unique underlyings from stock futures (exclude indices)
    futs = [
        i
        for i in nfo
        if i.get("instrument_type") == "FUT"
        and i.get("segment") == "NFO-FUT"
    ]
    fo_names = sorted(
        set(i.get("name") for i in futs if i.get("name")) - FO_INDEX_NAMES
    )
    # NSE equity: tradingsymbol -> instrument_token
    nse_eq = {
        i["tradingsymbol"]: i["instrument_token"]
        for i in nse
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    }
    # Only include F&O underlyings that have NSE equity
    return {name: nse_eq[name] for name in fo_names if name in nse_eq}


def main():
    parser = argparse.ArgumentParser(
        description="Fetch 15min spot data for all F&O stocks (NSE equity)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to save CSVs (default: fo_stocks/ in repo)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols (e.g. RELIANCE,TCS). Default: all F&O stocks.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Parallel workers (default 4).",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date (default: 2015-09-01).",
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

    base_dir = str(REPO_ROOT)
    data_dir = args.output_dir or os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    nifty50 = _load_nifty50_symbols()
    for sub in ("nifty50", "other"):
        os.makedirs(os.path.join(data_dir, sub, "15min"), exist_ok=True)

    to_date = datetime.now().date()
    if args.to_date:
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
    from_date = DEFAULT_START_DATE
    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()

    symbol_to_token = get_fo_equity_symbol_to_token(kite)
    if args.symbols:
        requested = [s.strip() for s in args.symbols.split(",") if s.strip()]
        symbols = [s for s in requested if s in symbol_to_token]
        missing = set(requested) - set(symbol_to_token.keys())
        if missing:
            print("Note: symbols not in F&O list (skipped):", missing)
    else:
        symbols = sorted(symbol_to_token.keys())

    workers = max(1, min(args.workers, 8))
    delay_sec = round(0.0035 * workers, 4)

    print(
        f"Fetching 15min equity data for {len(symbols)} F&O stocks from {from_date} to {to_date}"
    )
    print(f"Output: {data_dir}/nifty50/15min & .../other/15min (workers={workers}, delay={delay_sec}s).\n")

    from_date_tuple = (from_date.year, from_date.month, from_date.day)
    to_date_tuple = (to_date.year, to_date.month, to_date.day)

    def out_dir_for(sym):
        sub = "nifty50" if sym in nifty50 else "other"
        return os.path.join(data_dir, sub, "15min")

    if workers == 1:
        for sym in symbols:
            token = symbol_to_token[sym]
            print(f"{sym} (token {token})")
            fetch_one_index(
                kite, token, sym, from_date, to_date, out_dir_for(sym), delay_sec
            )
    else:
        task_args = [
            (
                symbol_to_token[sym],
                sym,
                from_date_tuple,
                to_date_tuple,
                out_dir_for(sym),
                delay_sec,
            )
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
