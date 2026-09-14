#!/usr/bin/env python3
"""
Verify 15-minute OHLCV CSV timestamp continuity.

Two modes:
  * regular_session (default): Mon–Fri only, bar open times 09:15–15:30 IST. Within each
    calendar day, consecutive bars must be exactly 15 minutes apart. Skips weekends and
    special Saturday / Muhurat evenings so normal cash-session grids are what we check.
  * calendar_day: stricter — any two bars on the same IST calendar day must step by 15m
    (flags short special sessions).

Also reports duplicate timestamps. Long gaps (>= 12h) are assumed session/weekend breaks.

Usage:
  .venv/bin/python verify_15min_continuity.py
  .venv/bin/python verify_15min_continuity.py --only nifty50
  .venv/bin/python verify_15min_continuity.py --mode calendar_day
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import pandas as pd

IST = "Asia/Kolkata"
BAR = pd.Timedelta(minutes=15)
MIN_LONG_BREAK = pd.Timedelta(hours=12)
# NSE regular cash session (bar open times; last full session bar is typically 15:15).
_REG_START_SEC = 9 * 3600 + 15 * 60
_REG_END_SEC = 15 * 3600 + 30 * 60


def _regular_nse_cash_mask(ts: pd.Series) -> pd.Series:
    """Mon–Fri, 09:15:00–15:30:00 local (IST) bar opens."""
    loc = ts.dt.tz_convert(IST)
    wd = loc.dt.dayofweek
    sec = loc.dt.hour * 3600 + loc.dt.minute * 60 + loc.dt.second
    return (wd < 5) & (sec >= _REG_START_SEC) & (sec <= _REG_END_SEC)


@dataclass
class FileReport:
    path: str
    rows: int = 0
    duplicate_timestamps: int = 0
    intraday_bad: int = 0
    intraday_examples: list[tuple[str, str, str]] = field(default_factory=list)


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


def _intraday_gaps_for_series(t: pd.Series) -> tuple[int, list[tuple[str, str, str]]]:
    """Within each IST calendar day, count gaps between sorted times that are not exactly 15m."""
    t = t.sort_values()
    day = t.dt.tz_convert(IST).dt.normalize()
    bad = 0
    examples: list[tuple[str, str, str]] = []
    for d0 in day.unique():
        sub = t[day == d0]
        if len(sub) < 2:
            continue
        dd = sub.diff()
        for i in range(1, len(sub)):
            g = dd.iloc[i]
            if pd.isna(g) or g == BAR:
                continue
            bad += 1
            if len(examples) < 4:
                examples.append(
                    (str(pd.Timestamp(d0).date()), str(g), f"{sub.iloc[i - 1]} -> {sub.iloc[i]}")
                )
    return bad, examples


def verify_file(path: Path, mode: str, since: pd.Timestamp | None) -> FileReport:
    rep = FileReport(path=str(path))
    try:
        df = pd.read_csv(path, usecols=["date"], parse_dates=["date"])
    except Exception as e:
        rep.intraday_examples.append(("READ_ERROR", str(e), ""))
        rep.intraday_bad = 1
        return rep

    if df.empty:
        return rep

    t = pd.to_datetime(df["date"], utc=True).dt.tz_convert(IST)
    t = t.sort_values().reset_index(drop=True)
    if since is not None:
        t = t[t >= since]
    if t.empty:
        return rep
    rep.rows = len(t)
    rep.duplicate_timestamps = int(t.duplicated().sum())
    t = t.drop_duplicates()

    if mode == "regular_session":
        t = t[_regular_nse_cash_mask(t)].sort_values().reset_index(drop=True)

    rep.intraday_bad, rep.intraday_examples = _intraday_gaps_for_series(t)
    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify 15m CSV time continuity")
    ap.add_argument("--only", choices=("all", "indices", "nifty50", "other"), default="all")
    ap.add_argument(
        "--mode",
        choices=("regular_session", "calendar_day"),
        default="regular_session",
        help="regular_session = Mon–Fri 09:15–15:30 IST only (recommended). "
        "calendar_day = any same-calendar-day chain (flags Muhurat Saturdays etc.).",
    )
    ap.add_argument("--max-files", type=int, default=0, help="Limit files (0 = all)")
    ap.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        default=None,
        help="Only bars on or after this date (IST) are checked (useful to skip old halts).",
    )
    ap.add_argument(
        "--warn-only",
        action="store_true",
        help="Always exit 0; still print all issues (for logs / CI soft check).",
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

    reports = [verify_file(p, args.mode, since_ts) for p in paths]
    bad_intraday = [r for r in reports if r.intraday_bad > 0]
    bad_dupes = [r for r in reports if r.duplicate_timestamps > 0]

    print(f"Scanned {len(reports)} files | mode={args.mode} | only={args.only}")
    print(f"  Total rows (before dedupe): {sum(r.rows for r in reports):,}")
    print()
    print(f"Files with duplicate timestamps: {len(bad_dupes)}")
    print(f"Files with intra-day continuity breaks: {len(bad_intraday)}")
    print()

    if bad_dupes:
        print("--- Duplicate timestamps ---")
        for r in bad_dupes[:25]:
            print(f"  {r.path}  dupes={r.duplicate_timestamps}")
        if len(bad_dupes) > 25:
            print(f"  ... +{len(bad_dupes) - 25} more")
        print()

    if bad_intraday:
        print("--- Intra-day gaps (not exactly +15m between consecutive bar opens) ---")
        for r in bad_intraday[:25]:
            print(f"  {r.path}  bad_gaps={r.intraday_bad}")
            for ex in r.intraday_examples:
                print(f"      day={ex[0]} gap={ex[1]}  {ex[2]}")
        if len(bad_intraday) > 25:
            print(f"  ... +{len(bad_intraday) - 25} more files")
        print()
        print(
            "Note: remaining gaps are often exchange halts, delayed opens, or "
            "special-session days (e.g. budget). Use --mode calendar_day to see "
            "weekend/special-session effects."
        )
        print()

    total_bad = sum(r.intraday_bad for r in reports)
    if total_bad and (bad_intraday or bad_dupes):
        dates: set[str] = set()
        for r in bad_intraday:
            for ex in r.intraday_examples:
                if ex[0] and ex[0] != "READ_ERROR":
                    dates.add(ex[0])
        if dates:
            print(f"Unique calendar dates with ≥1 sample gap (first examples only): {len(dates)}")
            print(f"  (e.g. {', '.join(sorted(dates)[:12])}{'...' if len(dates) > 12 else ''})")
            print()

    if args.warn_only:
        return 0
    if bad_dupes or bad_intraday:
        return 1
    print("OK: no duplicate timestamps; regular Mon–Fri session chains are strictly 15m.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
