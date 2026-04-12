#!/usr/bin/env python3
"""
Update *all symbols and all timeframes* that currently exist under ./data.

What it does:
1) Refresh session (test_login.py)
2) Incrementally update:
   - indices 15min
   - indices EOD
   - F&O stocks 15min (nifty50 + other)
   - F&O stocks EOD  (nifty50 + other)
3) Regenerate derived NIFTY 50 index timeframes (20/30/35/45/50/1hr) if those folders exist
   using data/indices/resample_15min_to_30min.py (requires pandas).
4) Print a freshness report.

Run:
  .venv/bin/python -u update_all_present.py --workers 4 --delay 0.05
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent


def run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=str(BASE))


def main() -> int:
    ap = argparse.ArgumentParser(description="Update all present data/timeframes under data/")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.05)
    ap.add_argument("--skip-login", action="store_true")
    args = ap.parse_args()

    py = sys.executable

    if not args.skip_login:
        run([py, "-u", "test_login.py"])

    # Incremental updates for the files that exist
    run([py, "-u", "update_incremental.py", "--only", "indices15", "--workers", str(args.workers), "--delay", str(args.delay)])
    run([py, "-u", "update_incremental.py", "--only", "fo15", "--workers", str(args.workers), "--delay", str(args.delay)])
    run([py, "-u", "update_incremental.py", "--only", "indices_eod", "--workers", str(args.workers), "--delay", str(args.delay)])
    run([py, "-u", "update_incremental.py", "--only", "fo_eod", "--workers", str(args.workers), "--delay", str(args.delay)])

    # Derived timeframes for NIFTY 50 index (if folders exist)
    # Derived timeframes regenerated from 15-min sources (indices + nifty50 + other, if such folders exist)
    resample_py = BASE / "resample_all_timeframes.py"
    if resample_py.is_file():
        run([py, "-u", str(resample_py)])
    else:
        print("\n(skip) Missing resample_all_timeframes.py", flush=True)

    # Verification summary
    run([py, "-u", "verify_data_freshness.py", "--min-age-days", "2"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

