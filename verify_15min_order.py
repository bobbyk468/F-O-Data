#!/usr/bin/env python3
"""
Verify 15m OHLCV CSVs are in chronological row order (as stored) and optionally OHLC rules.

Checks (per file, optional --since filter in IST):
  * date column strictly increases row-by-row (no duplicates, no time going backwards)
  * unless --skip-ohlc: high >= low, high >= open & close, low <= open & close; volume >= 0

Strict OHLC can fail on real feeds (e.g. India VIX tick rounding; auction open after
corporate actions). Use --skip-ohlc when you only care that rows are time-sorted.

Does not sort rows — order is exactly as in the file.

Usage:
  .venv/bin/python verify_15min_order.py
  .venv/bin/python verify_15min_order.py --only nifty50 --since 2023-01-01
  .venv/bin/python verify_15min_order.py --skip-ohlc
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import pandas as pd

IST = "Asia/Kolkata"


def iter_15min_csvs(data_root: Path, only: str) -> Iterator[Path]:
    if only == "all":
        subs = ["indices/15min", "nifty50/15min", "other/15min"]
    elif only == "indices":
        subs = ["indices/15min"]
    elif only == "nifty50":
        subs = ["nifty50/15min"]
    elif only == "other":
        subs = ["other/15min"]
    else:
        raise ValueError(only)
    for sub in subs:
        d = data_root / sub
        if d.is_dir():
            yield from sorted(d.glob("*_15min.csv"))


@dataclass
class FileReport:
    path: str
    rows: int = 0
    order_violations: int = 0
    order_examples: list[str] = field(default_factory=list)
    ohlc_violations: int = 0
    ohlc_examples: list[str] = field(default_factory=list)
    nan_rows: int = 0
    read_error: str | None = None


def verify_file(path: Path, since: pd.Timestamp | None, check_ohlc: bool) -> FileReport:
    rep = FileReport(path=str(path))
    try:
        df = pd.read_csv(
            path,
            usecols=["date", "open", "high", "low", "close", "volume"],
            parse_dates=["date"],
        )
    except Exception as e:
        rep.read_error = str(e)
        return rep

    if df.empty:
        return rep

    t = pd.to_datetime(df["date"], utc=True)
    if since is not None:
        t_ist = t.dt.tz_convert(IST)
        keep = t_ist >= since
        df = df.loc[keep].reset_index(drop=True)
        t = pd.to_datetime(df["date"], utc=True)

    rep.rows = len(df)
    if rep.rows == 0:
        return rep

    key = ["open", "high", "low", "close", "volume"]
    rep.nan_rows = int(df[key].isna().any(axis=1).sum())

    if rep.rows >= 2:
        td = t.diff()
        bad_mask = td <= pd.Timedelta(0)
        bad_mask.iloc[0] = False
        rep.order_violations = int(bad_mask.sum())
        if rep.order_violations:
            idx = bad_mask[bad_mask].index[:5].tolist()
            for i in idx:
                i0 = max(0, i - 1)
                rep.order_examples.append(f"row {i}: {df['date'].iloc[i0]} -> {df['date'].iloc[i]}")

    if check_ohlc:
        o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
        bad_ohlc = (
            (h < l)
            | (h < o)
            | (h < c)
            | (l > o)
            | (l > c)
            | (v < 0)
        )
        rep.ohlc_violations = int(bad_ohlc.sum())
        if rep.ohlc_violations:
            for j in bad_ohlc[bad_ohlc].index[:5].tolist():
                rep.ohlc_examples.append(
                    f"row {j} {df['date'].iloc[j]} "
                    f"O={o.iloc[j]} H={h.iloc[j]} L={l.iloc[j]} C={c.iloc[j]} V={v.iloc[j]}"
                )

    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify 15m CSV row order and OHLC consistency")
    ap.add_argument("--only", choices=("all", "indices", "nifty50", "other"), default="all")
    ap.add_argument("--max-files", type=int, default=0, help="Limit files (0 = all)")
    ap.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        default=None,
        help="Only consider rows on or after this date (IST).",
    )
    ap.add_argument("--warn-only", action="store_true", help="Always exit 0; still print issues.")
    ap.add_argument(
        "--skip-ohlc",
        action="store_true",
        help="Only check chronological row order (ignore OHLC/volume rules).",
    )
    args = ap.parse_args()

    since_ts: pd.Timestamp | None = None
    if args.since:
        since_ts = pd.Timestamp(args.since, tz=IST)

    root = Path(__file__).resolve().parent
    data = root / "data"
    if not data.is_dir():
        print("No data/ directory found.", file=sys.stderr)
        return 1

    paths = list(iter_15min_csvs(data, args.only))
    if args.max_files and args.max_files > 0:
        paths = paths[: int(args.max_files)]

    check_ohlc = not args.skip_ohlc
    reports = [verify_file(p, since_ts, check_ohlc) for p in paths]
    read_errs = [r for r in reports if r.read_error]
    bad_order = [r for r in reports if r.order_violations > 0]
    bad_ohlc = [r for r in reports if r.ohlc_violations > 0]
    bad_nan = [r for r in reports if r.nan_rows > 0]

    print(f"Scanned {len(reports)} files | only={args.only}")
    if args.since:
        print(f"  Filter: rows with date >= {args.since} (IST)")
    if args.skip_ohlc:
        print("  OHLC checks: skipped (--skip-ohlc)")
    print(f"  Total rows considered: {sum(r.rows for r in reports):,}")
    print()
    print("--- Summary ---")
    print(
        f"Chronological row order (date strictly increasing as stored): "
        f"{'OK' if not bad_order else f'FAIL ({len(bad_order)} files)'}"
    )
    if check_ohlc:
        print(
            f"OHLC/volume (high>=O,C,L; low<=O,C; high>=low; vol>=0): "
            f"{'OK' if not bad_ohlc else f'FAIL ({len(bad_ohlc)} files)'}"
        )
    print()
    print(f"Files with read errors: {len(read_errs)}")
    print(f"Files with NaN in OHLCV (any column): {len(bad_nan)}")
    print(f"Files with non-increasing date (row order): {len(bad_order)}")
    if check_ohlc:
        print(f"Files with OHLC/volume logic violations: {len(bad_ohlc)}")
    print()

    if read_errs:
        print("--- Read errors ---")
        for r in read_errs[:20]:
            print(f"  {r.path}: {r.read_error}")
        if len(read_errs) > 20:
            print(f"  ... +{len(read_errs) - 20} more")
        print()

    if bad_nan:
        print("--- NaN rows (sample) ---")
        for r in bad_nan[:15]:
            print(f"  {r.path}  nan_rows={r.nan_rows}")
        if len(bad_nan) > 15:
            print(f"  ... +{len(bad_nan) - 15} more files")
        print()

    if bad_order:
        print("--- Date not strictly increasing (file row order) ---")
        for r in bad_order[:25]:
            print(f"  {r.path}  violations={r.order_violations}")
            for ex in r.order_examples:
                print(f"      {ex}")
        if len(bad_order) > 25:
            print(f"  ... +{len(bad_order) - 25} more files")
        print()

    if check_ohlc and bad_ohlc:
        print("--- OHLC / volume violations ---")
        for r in bad_ohlc[:25]:
            print(f"  {r.path}  violations={r.ohlc_violations}")
            for ex in r.ohlc_examples:
                print(f"      {ex}")
        if len(bad_ohlc) > 25:
            print(f"  ... +{len(bad_ohlc) - 25} more files")
        print(
            "Note: common causes are India VIX tick rounding vs open, and "
            "auction / CA opens where printed open is outside the bar range."
        )
        print()

    if args.warn_only:
        return 0
    if read_errs or bad_order or (check_ohlc and bad_ohlc):
        return 1
    if check_ohlc:
        print("OK: rows are strictly chronological; OHLC/volume checks pass.")
    else:
        print("OK: rows are strictly chronological (--skip-ohlc).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
