#!/usr/bin/env python3
"""Incrementally extend the existing 1min files (indices + the stocks that have one) to --to-date.

Re-downloads from each file's last date, merges, and runs the price re-adjustment check
(see adjustment_check.py): if Kite re-adjusted history, the file is refetched in full from its first date.
A file whose fetch fails is left unchanged and reported; exit code is 1 if any file failed.
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
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from adjustment_check import frames_readjusted, rebase_merge
from kite_retry import FetchError, historical_with_retry
from ohlc_indicators import coerce_datetime_ist

CHUNK_DAYS = 55  # Kite allows 60 days per 1minute request
COLS = ["date", "open", "high", "low", "close", "volume"]


def slug(symbol: str) -> str:
    s = re.sub(r"\s+", "_", symbol.strip().upper())
    return re.sub(r"[^A-Z0-9_]", "", s).lower() or "index"


def fetch_range(kite, token, start: date, end: date, delay: float) -> pd.DataFrame:
    rows, cur = [], start
    while cur <= end:
        ce = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
        for c in historical_with_retry(kite, token, cur, ce, "minute", retries=6, backoff=2.0):
            rows.append([c.get("date"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume", 0)])
        cur = ce + timedelta(days=1)
        time.sleep(delay)
    df = pd.DataFrame(rows, columns=COLS)
    df["date"] = coerce_datetime_ist(df["date"])
    return df.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


def update_file(kite, path: Path, token: int, to_date: date, delay: float) -> tuple[str, int]:
    old = pd.read_csv(path)
    old["date"] = coerce_datetime_ist(old["date"])
    last, first = old["date"].max().date(), old["date"].min().date()
    new = fetch_range(kite, token, last, to_date, delay)
    flagged, ratio, n_ov = frames_readjusted(old, new)
    if flagged:
        print(f"[ADJUSTED] {path.stem}: prices re-adjusted (new/old close = {ratio:.4f} over {n_ov} bars) -> refetching full history", flush=True)
        full = rebase_merge(old, fetch_range(kite, token, first, to_date, delay))
    else:
        full = pd.concat([old, new], ignore_index=True)
        full = full.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    if len(full) < len(old):
        raise FetchError(f"{path.name}: result has fewer rows ({len(full)}) than existing ({len(old)})")
    tmp = path.with_suffix(".csv.tmp")
    full[COLS].to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path.name, len(full) - len(old)


def main() -> int:
    ap = argparse.ArgumentParser(description="Extend existing 1min CSVs to --to-date")
    ap.add_argument("--to-date", default=None, metavar="YYYY-MM-DD", help="Default: today (IST).")
    ap.add_argument("--delay", type=float, default=0.3)
    args = ap.parse_args()
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else datetime.now().date()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()
    nse = kite.instruments("NSE")
    idx = {slug(i["tradingsymbol"]): i["instrument_token"] for i in nse if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"}
    eq = {slug(i["tradingsymbol"]): i["instrument_token"] for i in nse if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"}

    files = sorted(DATA_DIR.glob("*/1min/*_1min.csv"))
    print(f"Updating {len(files)} 1min files to {to_date}", flush=True)
    done, failed = 0, []
    for i, p in enumerate(files, 1):
        key = p.name[: -len("_1min.csv")]
        token = (idx if p.parent.parent.name == "indices" else eq).get(key)
        if token is None:
            print(f"[{i}/{len(files)}] FAILED {key}: no instrument token (file left unchanged)", flush=True)
            failed.append(key)
            continue
        try:
            name, added = update_file(kite, p, int(token), to_date, args.delay)
            done += 1
            print(f"[{i}/{len(files)}] [done] {key}: +{added} rows", flush=True)
        except Exception as e:  # leave the file untouched, keep going
            print(f"[{i}/{len(files)}] FAILED {key}: {type(e).__name__}: {str(e)[:160]} (file left unchanged)", flush=True)
            failed.append(key)
    print(f"Done. Updated {done}/{len(files)} files.")
    if failed:
        print(f"FAILED ({len(failed)}): {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
