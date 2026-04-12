#!/usr/bin/env python3
"""
Verify NSE index 1-minute CSVs under data/indices/1min/:
  - Expected files (from MAIN_AND_SECTOR_SYMBOLS)
  - Per-file: date range, row count, last bar
  - Trading days with fewer than 375 bars (9:15–15:29 IST) — possible missing minutes
  - Large intra-day gaps (>1 minute) within a session

Does not modify data. NSE holidays are not modeled (weekdays with no/partial data may be holidays).
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from fetch_code.fetch_all_indices_1min import MAIN_AND_SECTOR_SYMBOLS, slug  # noqa: E402

IST = "Asia/Kolkata"
# Full cash session 1-min bars: 09:15 .. 15:29 inclusive = 375 bars
# Full NSE cash index session 09:15–15:29 = 375 one-minute bars (continuous session).
FULL_SESSION_BARS = 375


def _is_expected_lunch_gap(prev: datetime, delta_min: float) -> bool:
    """Older NSE data has ~75–95 min gap around lunch (morning vs afternoon block)."""
    if not (70 <= delta_min <= 120):
        return False
    h, m = prev.hour, prev.minute
    # Typical: last bar before lunch ~12:15–12:30; gap ~90+ min to ~13:45
    if (11 <= h <= 12) or (h == 13 and m <= 50):
        return True
    return False


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.is_file() or path.stat().st_size < 20:
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"])
    except Exception as e:
        print(f"  ERROR reading {path}: {e}")
        return None
    if df.empty or "date" not in df.columns:
        return None
    ts = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.assign(_ts=ts.dt.tz_convert(IST))
    df = df.dropna(subset=["_ts"])
    return df


def analyze_file(path: Path, max_partial_days: int, max_gap_samples: int) -> dict:
    df = _load_csv(path)
    if df is None:
        return {"error": "empty or unreadable"}

    out: dict = {
        "rows": len(df),
        "first": df["_ts"].min(),
        "last": df["_ts"].max(),
        "partial_days": [],  # (date, count, expected ~375)
        "intra_gaps": [],  # (day, gap_minutes, after_ts)
        "dup_ts": int(df["_ts"].duplicated().sum()),
    }

    df["_d"] = df["_ts"].dt.date
    counts = df.groupby("_d").size()
    d0, d1 = df["_ts"].min().date(), df["_ts"].max().date()
    for d, cnt in counts.items():
        wd = datetime.combine(d, datetime.min.time()).weekday()
        if wd >= 5:
            continue
        if cnt < FULL_SESSION_BARS:
            out["partial_days"].append((d, int(cnt)))
    out["partial_days"].sort(key=lambda x: x[0])
    if len(out["partial_days"]) > max_partial_days:
        out["partial_days_truncated"] = len(out["partial_days"]) - max_partial_days
        out["partial_days"] = out["partial_days"][:max_partial_days]

    # Weekdays in [d0,d1] with no rows at all (holiday or missing data)
    missing_weekdays: list[date] = []
    cur = d0
    while cur <= d1:
        if cur.weekday() < 5 and cur not in counts.index:
            missing_weekdays.append(cur)
        cur += timedelta(days=1)
    out["missing_weekdays_count"] = len(missing_weekdays)
    out["missing_weekdays_sample"] = missing_weekdays[:5]

    # Intra-day gaps (same IST date, consecutive bars > 1 min apart)
    df_sorted = df.sort_values("_ts")
    prev = None
    for _, row in df_sorted.iterrows():
        t = row["_ts"]
        if prev is not None and prev.date() == t.date():
            delta = (t - prev).total_seconds() / 60.0
            if delta > 1.5 and not _is_expected_lunch_gap(prev, delta):
                out["intra_gaps"].append((t.date(), int(delta), prev))
                if len(out["intra_gaps"]) >= max_gap_samples:
                    break
        prev = t

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify index 1-minute CSVs")
    ap.add_argument(
        "--dir",
        type=Path,
        default=BASE / "data" / "indices" / "1min",
        help="Directory with *_1min.csv",
    )
    ap.add_argument("--max-partial-days", type=int, default=25, help="List at most N partial weekdays")
    ap.add_argument("--max-gap-samples", type=int, default=15, help="Report at most N intra-day gaps per file")
    args = ap.parse_args()

    expected = {slug(s): s for s in MAIN_AND_SECTOR_SYMBOLS}
    present = sorted(args.dir.glob("*_1min.csv"))
    by_slug = {p.stem.replace("_1min", ""): p for p in present}

    print(f"Directory: {args.dir}\n")
    print("=== Expected vs present ===")
    missing_files = []
    for sl, sym in expected.items():
        fn = f"{sl}_1min.csv"
        p = args.dir / fn
        if not p.is_file():
            missing_files.append((sym, fn))
            print(f"  MISSING file: {fn}  ({sym})")
        else:
            print(f"  OK {fn}")

    extra = set(by_slug.keys()) - set(expected.keys())
    if extra:
        print("\n  Extra files (not in default 25-symbol list):")
        for e in sorted(extra):
            print(f"    {e}_1min.csv")

    print("\n=== Per-file summary ===")
    issues = 0
    for sl, sym in sorted(expected.items(), key=lambda x: x[1]):
        p = args.dir / f"{sl}_1min.csv"
        if not p.is_file():
            continue
        r = analyze_file(p, args.max_partial_days, args.max_gap_samples)
        if "error" in r:
            print(f"\n{sym}: {r['error']}")
            issues += 1
            continue
        print(f"\n{sym} ({p.name})")
        print(f"  rows={r['rows']:,}  first={r['first']}  last={r['last']}")
        if r["dup_ts"]:
            print(f"  WARNING: duplicate timestamps: {r['dup_ts']}")
            issues += 1
        if r["partial_days"]:
            print(f"  weekdays with <{FULL_SESSION_BARS} bars (sample up to {args.max_partial_days}):")
            for d, cnt in r["partial_days"]:
                print(f"    {d}  {cnt} bars")
            if r.get("partial_days_truncated"):
                print(f"    ... and {r['partial_days_truncated']} more partial days")
            issues += 1
        else:
            print(f"  no weekday with <{FULL_SESSION_BARS} bars (partial session)")

        mwc = r.get("missing_weekdays_count", 0)
        if mwc:
            print(
                f"  weekdays with no rows in range (holidays / no trading / not listed yet): {mwc} "
                f"(sample: {r.get('missing_weekdays_sample', [])})"
            )

        if r["intra_gaps"]:
            print(f"  intra-day gaps >1 min (sample):")
            for d, gap_m, after in r["intra_gaps"][: args.max_gap_samples]:
                print(f"    {d}  gap ~{gap_m} min after {after}")
            issues += 1

    # Cross-file last-date alignment (optional)
    print("\n=== Last bar (IST) across files ===")
    last_dates = []
    for sl, sym in expected.items():
        p = args.dir / f"{sl}_1min.csv"
        if not p.is_file():
            continue
        df = _load_csv(p)
        if df is None or df.empty:
            continue
        last_dates.append((sym, df["_ts"].max()))
    last_dates.sort(key=lambda x: x[1])
    if last_dates:
        newest = last_dates[-1][1]
        print(f"  newest last bar among present files: {newest}")
        for sym, ts in last_dates:
            delta = (newest - ts).total_seconds()
            if delta > 24 * 3600:
                print(f"  STALE vs newest: {sym}  last={ts}  ({delta/3600:.1f} h behind)")

    print("\nDone.")
    if missing_files:
        print(f"\nNote: {len(missing_files)} expected symbol file(s) are missing — fetch may still be running.")
    if issues:
        print("\nReview WARNING lines above for gaps, duplicates, or partial sessions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
