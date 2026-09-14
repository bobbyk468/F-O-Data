#!/usr/bin/env python3
"""
Fetch 5-minute spot (equity) data for all F&O underlyings (same universe as fetch_fo_stocks_15min).
Uses ~90-day (~3 month) API chunks, sequential only (same as fetch_all_indices_5min).
Output: data/nifty50/5min & data/other/5min (plain OHLCV).
"""
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
import argparse
from datetime import datetime, date
from fetch_code.fetch_all_indices_5min import fetch_one_index, DEFAULT_START_DATE


def _load_nifty50_symbols():
    base_dir = str(REPO_ROOT)
    path = os.path.join(base_dir, "config", "nifty50_symbols.txt")
    if not os.path.isfile(path):
        return set()
    out = set()
    with open(path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                out.add(line)
    return out


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
    nfo = kite.instruments("NFO")
    nse = kite.instruments("NSE")
    futs = [i for i in nfo if i.get("instrument_type") == "FUT" and i.get("segment") == "NFO-FUT"]
    fo_names = sorted(set(i.get("name") for i in futs if i.get("name")) - FO_INDEX_NAMES)
    nse_eq = {
        i["tradingsymbol"]: i["instrument_token"]
        for i in nse
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    }
    return {name: nse_eq[name] for name in fo_names if name in nse_eq}


def main():
    parser = argparse.ArgumentParser(description="Fetch 5min spot data for all F&O stocks (NSE equity)")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Data root (default: repo data/)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols. Default: all F&O stocks.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Ignored; fetches run sequentially.",
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
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore existing CSVs; refetch the full [from-date, to-date] window.",
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
        os.makedirs(os.path.join(data_dir, sub, "5min"), exist_ok=True)

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

    if args.workers != 1:
        print("Note: --workers is ignored; 5min fetch runs sequentially.\n")

    print(
        f"Fetching 5min equity data for {len(symbols)} F&O stocks from {from_date} to {to_date}"
    )
    resume = not args.no_resume
    print(
        f"Output: {data_dir}/nifty50/5min & .../other/5min (sequential, ~90d; "
        f"{'resume/merge' if resume else 'full refetch'}).\n"
    )

    def out_dir_for(sym):
        sub = "nifty50" if sym in nifty50 else "other"
        return os.path.join(data_dir, sub, "5min")

    for sym in symbols:
        token = symbol_to_token[sym]
        print(f"{sym} (token {token})")
        fetch_one_index(kite, token, sym, from_date, to_date, out_dir_for(sym), resume=resume)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
