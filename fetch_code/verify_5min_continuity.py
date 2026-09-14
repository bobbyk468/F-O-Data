#!/usr/bin/env python3
"""
Check 5-minute OHLCV CSVs for basic continuity: monotonic time, duplicates,
and intra-day gaps larger than one bar (5 minutes).

NSE regular session bars are 5 minutes apart; overnight and weekends are ignored.
Does not know exchange holidays — missing whole days are normal.

Usage:
  .venv/bin/python -u fetch_code/verify_5min_continuity.py data/indices/5min
  .venv/bin/python -u fetch_code/verify_5min_continuity.py data/nifty50/5min data/other/5min --max-gaps 20
  .venv/bin/python -u fetch_code/verify_5min_continuity.py data/indices/5min --strict

Exit 0 unless duplicate timestamps / unsorted rows, or unless --strict and uneven 5m steps exist.
"""
from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

EXPECTED = timedelta(minutes=5)
TOL = timedelta(seconds=90)


def _check_one_csv(path: Path, max_gaps_report: int) -> dict:
    out: dict = {
        "path": str(path),
        "rows": 0,
        "first": None,
        "last": None,
        "integrity_ok": True,
        "integrity_issues": [],
        "intra_day_gaps": [],
    }
    if not path.is_file():
        out["integrity_ok"] = False
        out["integrity_issues"].append("missing file")
        return out

    df = pd.read_csv(path, parse_dates=["date"])
    if df.empty:
        out["integrity_issues"].append("empty (no data rows)")
        return out

    out["rows"] = len(df)
    out["first"] = df["date"].iloc[0]
    out["last"] = df["date"].iloc[-1]

    if not df["date"].is_monotonic_increasing:
        out["integrity_ok"] = False
        out["integrity_issues"].append("dates not strictly sorted")

    dup = int(df["date"].duplicated().sum())
    if dup:
        out["integrity_ok"] = False
        out["integrity_issues"].append(f"duplicate timestamps: {dup}")

    ts = pd.to_datetime(df["date"], utc=True)
    day = ts.dt.date
    df2 = pd.DataFrame({"_ts": ts, "_day": day})

    diffs = df2["_ts"].diff()
    same_prev_day = day == day.shift(1)
    # First row: diff NaN; skip. Consecutive same calendar day: expect ~5m
    for i in range(1, len(df2)):
        if not bool(same_prev_day.iloc[i]):
            continue
        d = diffs.iloc[i]
        if pd.isna(d):
            continue
        delta = d.to_pytimedelta()
        if delta > EXPECTED + TOL or delta < EXPECTED - TOL:
            t0 = df2["_ts"].iloc[i - 1]
            t1 = df2["_ts"].iloc[i]
            out["intra_day_gaps"].append(
                (str(day.iloc[i]), t0.isoformat(), t1.isoformat(), str(delta))
            )
            if len(out["intra_day_gaps"]) >= max_gaps_report:
                break

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify 5min CSV time continuity")
    ap.add_argument(
        "paths",
        nargs="+",
        help="CSV files or directories (recursive *.csv)",
    )
    ap.add_argument(
        "--max-gaps",
        type=int,
        default=15,
        metavar="N",
        help="Max intra-day gap examples per file (default 15)",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Treat uneven 5m spacing as failure (exit 2). Default: only duplicates/unsorted fail.",
    )
    args = ap.parse_args()

    files: list[Path] = []
    for p in args.paths:
        path = Path(p)
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(sorted(path.rglob("*.csv")))
        else:
            print(f"Skip (not found): {path}", file=sys.stderr)

    if not files:
        print("No CSV files to check.", file=sys.stderr)
        return 1

    integrity_bad = 0
    spacing_bad = 0
    files_with_spacing = 0
    for f in files:
        r = _check_one_csv(f, args.max_gaps)
        gaps = r.get("intra_day_gaps", [])
        int_ok = r.get("integrity_ok", True)
        if not int_ok:
            integrity_bad += 1
        if gaps:
            files_with_spacing += 1
        if gaps and args.strict:
            spacing_bad += 1

        if not int_ok:
            status = "FAIL"
        elif gaps:
            status = "WARN" if not args.strict else "FAIL"
        else:
            status = "OK"

        fl = ""
        if r["rows"] > 0 and r["first"] is not None:
            fl = f" | {r['first']} .. {r['last']}"
        print(f"[{status}] {f.name} rows={r['rows']}{fl}")
        for iss in r.get("integrity_issues", []):
            print(f"       {iss}")
        if gaps:
            print(
                f"       uneven 5m spacing: {len(gaps)} spot(s) shown "
                f"(often halts / special sessions / API holes; use --strict to fail)"
            )
        for day, t0, t1, delta in gaps:
            print(f"       gap {day}: {t0} -> {t1} ({delta})")

    print(
        f"\nChecked {len(files)} file(s). Integrity errors: {integrity_bad}. "
        f"Files with uneven 5m spacing (non-fatal by default): {files_with_spacing}."
    )
    if integrity_bad:
        return 2
    if spacing_bad:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
