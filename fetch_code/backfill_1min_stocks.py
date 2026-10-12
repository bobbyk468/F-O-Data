#!/usr/bin/env python3
"""Build full-history 1min files for stocks that do not have one yet.

Start date per stock = first bar of its 5min file (so no calls are wasted before listing). Fetches 55-day
chunks with retries, writes each file atomically only after a complete fetch, and skips files that already
exist, so it can be stopped and restarted. After it finishes, scripts/refresh_data.sh keeps the files current
via update_1min.py.

Usage: .venv/bin/python -u fetch_code/backfill_1min_stocks.py --to-date YYYY-MM-DD [--symbols A,B] [--limit N]
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
import shutil
import time
from multiprocessing import Pool
from datetime import date, datetime

import pandas as pd

from kite_retry import FetchError
from ohlc_indicators import coerce_datetime_ist
from update_1min import COLS, fetch_range, slug


def first_day(path: _Path) -> date:
    with open(path) as f:
        next(f)
        return datetime.fromisoformat(next(f).split(",")[0]).date()


_KITE = None


def _work(job):
    """One stock in a worker process. Returns (key, status, detail, secs)."""
    global _KITE
    key, p5, out, token, to_date, delay, min_free = job
    t0 = time.time()
    try:
        if _KITE is None:
            from jugaad_trader import Zerodha

            _KITE = Zerodha()
            _KITE.set_access_token()
        if shutil.disk_usage(_REPO).free < min_free * 1024**3:
            return key, "STOP", "low disk", 0
        df = fetch_range(_KITE, token, first_day(_Path(p5)), to_date, delay)
        if df.empty:
            return key, "EMPTY", "API returned nothing", time.time() - t0
        out = _Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".csv.tmp")
        df[COLS].to_csv(tmp, index=False)
        os.replace(tmp, out)
        return key, "OK", f"{len(df):,} rows {df.date.min():%Y-%m-%d}->{df.date.max():%Y-%m-%d}", time.time() - t0
    except Exception as e:
        return key, "FAILED", f"{type(e).__name__}: {str(e)[:150]} (no file written)", time.time() - t0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--to-date", required=True, metavar="YYYY-MM-DD")
    ap.add_argument("--symbols", default=None, help="Comma-separated file slugs (e.g. infy,hdfcbank).")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--delay", type=float, default=0.2, help="Sleep per request inside each worker.")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers (Kite allows ~3 requests/s; 4 is safe).")
    ap.add_argument("--min-free-gb", type=float, default=4.0)
    args = ap.parse_args()
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()
    eq = {
        slug(i["tradingsymbol"]): i["instrument_token"]
        for i in kite.instruments("NSE")
        if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
    }

    want = {s.strip().lower() for s in args.symbols.split(",")} if args.symbols else None
    todo = []
    for p5 in sorted(list(DATA_DIR.glob("nifty50/5min/*_5min.csv")) + list(DATA_DIR.glob("other/5min/*_5min.csv"))):
        key = p5.name[: -len("_5min.csv")]
        out = p5.parent.parent / "1min" / f"{key}_1min.csv"
        if out.exists() or (want and key not in want):
            continue
        todo.append((key, p5, out))
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(todo)} stocks need a 1min file (to {to_date})", flush=True)

    jobs, skipped = [], []
    for key, p5, out in todo:
        token = eq.get(key)
        if token is None:
            print(f"SKIP {key}: no NSE equity token (delisted or renamed)", flush=True)
            skipped.append(key)
            continue
        jobs.append((key, str(p5), str(out), int(token), to_date, args.delay, args.min_free_gb))

    ok, failed = 0, []
    with Pool(max(1, min(args.workers, 8))) as pool:
        for n, (key, status, detail, secs) in enumerate(pool.imap_unordered(_work, jobs), 1):
            print(f"[{n}/{len(jobs)}] {status} {key}: {detail} ({secs:.0f}s)", flush=True)
            if status == "OK":
                ok += 1
            elif status == "FAILED":
                failed.append(key)
            elif status in ("EMPTY", "STOP"):
                skipped.append(key)
    print(f"DONE ok={ok} failed={len(failed)} skipped={len(skipped)}", flush=True)
    if failed:
        print("FAILED:", ",".join(failed))
    if skipped:
        print("SKIPPED:", ",".join(skipped))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
