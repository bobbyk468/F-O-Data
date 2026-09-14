#!/usr/bin/env python3
"""
Check all CSV files in data/nifty50/eod for missing data:
- Missing or unexpected columns
- Empty / very short files
- Blank or invalid values in OHLCV
- Gaps in the time series (consecutive trading-day gaps)
Uses only stdlib (no pandas).
"""
import os
import sys
import csv
import argparse
from datetime import datetime, date
from collections import defaultdict

REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]
MIN_ROWS_WARN = 500  # flag file if total rows < this


def parse_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if " " in s:
            s = s.split(" ")[0]
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def check_file(path: str, symbol: str) -> dict:
    out = {
        "symbol": symbol,
        "path": path,
        "rows": 0,
        "min_date": None,
        "max_date": None,
        "missing_cols": [],
        "nan_counts": {},
        "gap_days": 0,
        "empty": False,
        "error": None,
    }
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        out["error"] = str(e)
        return out

    if not rows:
        out["empty"] = True
        return out

    out["rows"] = len(rows)
    if not reader.fieldnames:
        out["error"] = "no header"
        return out

    missing = [c for c in REQUIRED_COLS if c not in reader.fieldnames]
    if missing:
        out["missing_cols"] = missing
        return out

    dates = []
    for row in rows:
        d = parse_date(row.get("date"))
        if d:
            dates.append(d)
        for c in ["open", "high", "low", "close", "volume"]:
            v = (row.get(c) or "").strip()
            if v == "" or v.lower() in ("nan", "null", "none"):
                out["nan_counts"][c] = out["nan_counts"].get(c, 0) + 1

    if dates:
        out["min_date"] = min(dates)
        out["max_date"] = max(dates)
        days_sorted = sorted(set(dates))
        gaps = sum(1 for i in range(1, len(days_sorted)) if (days_sorted[i] - days_sorted[i - 1]).days > 1)
        out["gap_days"] = gaps

    return out


def main():
    parser = argparse.ArgumentParser(description="Check nifty50/eod CSVs for missing data and gaps.")
    parser.add_argument("--data-dir", default=None, help="Path to data/nifty50/eod.")
    parser.add_argument("-o", "--output", default=None, help="Write report to this file.")
    args = parser.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    default_dir = os.path.join(base, "data", "nifty50", "eod")
    data_dir = args.data_dir or default_dir

    if not os.path.isdir(data_dir):
        print(f"Error: not a directory: {data_dir}", file=sys.stderr)
        return 1

    files = [f for f in os.listdir(data_dir) if f.endswith("_eod.csv")]
    if not files:
        print(f"No *_eod.csv files in {data_dir}")
        return 0

    results = [check_file(os.path.join(data_dir, f), f.replace("_eod.csv", "")) for f in sorted(files)]

    lines = [
        "=" * 60,
        "Nifty50 EOD data check: " + data_dir,
        "=" * 60,
        f"Files scanned: {len(files)}",
        "",
        "--- Summary ---",
    ]
    ok = [r for r in results if not r["error"] and not r["empty"] and not r["missing_cols"] and not r["nan_counts"]]
    with_issues = [r for r in results if r["error"] or r["empty"] or r["missing_cols"] or r["nan_counts"]]
    short = [r for r in ok if r["rows"] < MIN_ROWS_WARN]
    gaps = [r for r in ok if r["gap_days"] > 0]

    lines.append(f"  OK (no critical issues): {len(ok)}")
    lines.append(f"  With issues:             {len(with_issues)} (error/empty/missing cols/NaN)")
    lines.append(f"  Short (<{MIN_ROWS_WARN} rows):  {len(short)}")
    lines.append(f"  Gap days:                {len(gaps)} (weekends/holidays — expected)")
    lines.append("")
    if not with_issues and not short:
        lines.append("  Conclusion: No critical missing data.")
    lines.append("")

    if with_issues:
        lines.append("--- Files with errors / empty / missing cols / NaN ---")
        for r in with_issues:
            if r["error"]:
                lines.append(f"  {r['symbol']}: ERROR {r['error']}")
            elif r["empty"]:
                lines.append(f"  {r['symbol']}: empty file")
            elif r["missing_cols"]:
                lines.append(f"  {r['symbol']}: missing columns {r['missing_cols']}")
            elif r["nan_counts"]:
                lines.append(f"  {r['symbol']}: NaN/blank counts {r['nan_counts']}")
        lines.append("")

    if short:
        lines.append("--- Short files ---")
        for r in short:
            min_d = r["min_date"].strftime("%Y-%m-%d") if r["min_date"] else "?"
            max_d = r["max_date"].strftime("%Y-%m-%d") if r["max_date"] else "?"
            lines.append(f"  {r['symbol']}: {r['rows']} rows (min={min_d}, max={max_d})")
        lines.append("")

    lines.append("--- All files: rows, date range, gap_days ---")
    for r in results:
        if r["error"] or r["empty"]:
            lines.append(f"  {r['symbol']}: error/empty")
            continue
        min_d = r["min_date"].strftime("%Y-%m-%d") if r["min_date"] else "?"
        max_d = r["max_date"].strftime("%Y-%m-%d") if r["max_date"] else "?"
        nan_str = f" NaN={r['nan_counts']}" if r["nan_counts"] else ""
        lines.append(f"  {r['symbol']}: rows={r['rows']:>6}  {min_d} .. {max_d}  gap_days={r['gap_days']}{nan_str}")
    lines.append("")
    lines.append("Done.")

    report = "\n".join(lines)
    print(report)
    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
