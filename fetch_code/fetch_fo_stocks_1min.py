#!/usr/bin/env python3
"""
Fetch 1-minute NSE equity (cash) data for all F&O underlyings (same universe as fetch_fo_stocks_15min).

Writes:
  data/nifty50/1min/<slug>_1min.csv   — symbols listed in config/nifty50_symbols.txt
  data/other/1min/<slug>_1min.csv     — remaining F&O names

Uses the same chunked 1m logic as fetch_all_indices_1min (100 bars/request, IST session).
Parallelism: one symbol per worker (--workers N, max 8).

Tip: full history for 200+ names is very large; prefer --resume and/or a recent --from-date,
then widen the window in stages.
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

import argparse
import os
import sys
from datetime import datetime, date
from multiprocessing import Pool

from fetch_code.fetch_all_indices_15min import DEFAULT_START_DATE
from fetch_code.fetch_all_indices_1min import (
    DEFAULT_PERIOD_DAYS,
    _worker_fetch_one,
    fetch_one_index,
)


def _load_nifty50_symbols():
    """Nifty 50 constituents from repo config (routing nifty50/ vs other/)."""
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
    """NSE equity tradingsymbol -> instrument_token for every F&O stock underlying."""
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch 1min NSE equity data for all F&O underlyings (parallel workers)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Data root (default: <repo>/data)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols (e.g. RELIANCE,TCS). Default: all F&O underlyings.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="Parallel symbols (default 6, max 8).",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD",
        help=f"Start date (default: {DEFAULT_START_DATE.isoformat()}, same as 15m F&O fetch).",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.03,
        help="Sleep between API calls inside each worker (default 0.03).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue each CSV from last timestamp (recommended).",
    )
    parser.add_argument(
        "--period-days",
        type=int,
        default=DEFAULT_PERIOD_DAYS,
        metavar="N",
        help=f"Outer batch size in calendar days (default {DEFAULT_PERIOD_DAYS}).",
    )
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    data_dir = args.output_dir or os.path.join(str(REPO_ROOT), "data")
    os.makedirs(data_dir, exist_ok=True)
    nifty50 = _load_nifty50_symbols()
    for sub in ("nifty50", "other"):
        os.makedirs(os.path.join(data_dir, sub, "1min"), exist_ok=True)

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

    workers = max(1, min(int(args.workers), 8))
    delay_sec = float(args.delay)
    period_days = max(1, min(int(args.period_days), 366))

    print(
        f"Fetching 1min equity data for {len(symbols)} F&O names from {from_date} to {to_date} "
        f"(workers={workers}, delay={delay_sec}s, resume={args.resume}, period_days={period_days})."
    )
    print(f"Output: {data_dir}/nifty50/1min & .../other/1min\n")

    from_date_tuple = (from_date.year, from_date.month, from_date.day)
    to_date_tuple = (to_date.year, to_date.month, to_date.day)

    def out_dir_for(sym: str) -> str:
        sub = "nifty50" if sym in nifty50 else "other"
        return os.path.join(data_dir, sub, "1min")

    if workers == 1:
        for sym in symbols:
            token = symbol_to_token[sym]
            print(f"{sym} (token {token})", flush=True)
            fetch_one_index(
                kite,
                token,
                sym,
                from_date,
                to_date,
                out_dir_for(sym),
                delay_sec,
                resume=args.resume,
                period_days=period_days,
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
                args.resume,
                period_days,
            )
            for sym in symbols
        ]
        with Pool(workers) as pool:
            results = pool.map(_worker_fetch_one, task_args)
        for sym, count in zip(symbols, results):
            print(f"  {sym}: {count} candles", flush=True)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
