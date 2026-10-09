#!/usr/bin/env python3
"""Update 5min stock files that fetch_fo_stocks_5min.py no longer touches.

fetch_fo_stocks_5min.py only walks the *current* F&O list, so a stock that dropped out of it silently stops
updating. This finds 5min stock files whose last bar is older than the freshest file's and updates them via
their NSE equity token (same resume logic, price re-adjustment check and line-ending handling).
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import DATA_DIR  # noqa: E402

import argparse
from datetime import datetime

from fetch_code.fetch_all_indices_5min import DEFAULT_START_DATE, fetch_one_index, last_csv_datetime, slug


def main() -> int:
    ap = argparse.ArgumentParser(description="Update stale 5min stock files outside the F&O list")
    ap.add_argument("--to-date", default=None, metavar="YYYY-MM-DD", help="Default: today.")
    args = ap.parse_args()
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else datetime.now().date()

    files = sorted(list(DATA_DIR.glob("nifty50/5min/*_5min.csv")) + list(DATA_DIR.glob("other/5min/*_5min.csv")))
    last = {f: last_csv_datetime(str(f)) for f in files}
    newest = max((d for d in last.values() if d), default=None)
    stale = [f for f in files if last[f] is not None and newest is not None and last[f].date() < newest.date()]
    if not stale:
        print("No stale 5min stock files.")
        return 0

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()
    eq = {
        slug(i["tradingsymbol"]): (i["tradingsymbol"], i["instrument_token"])
        for i in kite.instruments("NSE")
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    }
    failed = []
    for f in stale:
        key = f.name[: -len("_5min.csv")]
        hit = eq.get(key)
        if hit is None:
            print(f"[skip] {key}: not a listed NSE equity any more (last bar {last[f]:%Y-%m-%d}); left unchanged")
            continue
        sym, token = hit
        print(f"[straggler] {sym}: last bar {last[f]:%Y-%m-%d}, newest file {newest:%Y-%m-%d}", flush=True)
        try:
            fetch_one_index(kite, int(token), sym, DEFAULT_START_DATE, to_date, str(f.parent), resume=True)
        except Exception as e:
            print(f"FAILED {sym}: {type(e).__name__}: {str(e)[:160]}", flush=True)
            failed.append(sym)
    print(f"Done. {len(stale) - len(failed)}/{len(stale)} stale files processed.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
