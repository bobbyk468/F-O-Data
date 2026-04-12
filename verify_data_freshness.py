#!/usr/bin/env python3
"""
Report "freshness" (last timestamp) of all CSVs under ./data.

This is purely a verification tool: it does not modify any files.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


BASE = Path(__file__).resolve().parent
DATA = BASE / "data"


def _parse_dt(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def last_csv_datetime(path: Path) -> Optional[datetime]:
    if not path.is_file():
        return None
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        block = 8192
        data = b""
        pos = size
        while pos > 0 and b"\n" not in data:
            read_size = block if pos >= block else pos
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size) + data
            if len(data) > 200_000:
                break
    text = data.decode("utf-8", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ln.lower().startswith("date,"):
            continue
        first = ln.split(",", 1)[0]
        dt = _parse_dt(first)
        if dt is not None:
            return dt
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify last timestamps in data CSVs")
    ap.add_argument("--min-age-days", type=int, default=2, help="Report files older than N days (default 2)")
    ap.add_argument(
        "--write-stale",
        default=None,
        metavar="FILE",
        help="Write stale file paths (relative to repo root) to FILE.",
    )
    ap.add_argument(
        "--include-no-date",
        action="store_true",
        help="Include files with no parseable date as stale (default: excluded from --write-stale).",
    )
    args = ap.parse_args()

    now = datetime.now().astimezone()
    rows = []
    for p in sorted(DATA.rglob("*.csv")):
        dt = last_csv_datetime(p)
        age_days = None
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=now.tzinfo)
            age_days = (now - dt).total_seconds() / 86400.0
        rows.append((p, dt, age_days))

    stale = [r for r in rows if r[2] is None or r[2] >= float(args.min_age_days)]
    print(f"Scanned {len(rows)} CSVs under {DATA}")
    print(f"Stale threshold: {args.min_age_days} days\n")

    if not stale:
        print("All files are fresh within threshold.")
        return 0

    for p, dt, age_days in stale:
        if dt is None:
            print(f"STALE  (no date)   | {p.relative_to(BASE)}")
        else:
            print(f"STALE  ({age_days:6.2f}d) | {dt.isoformat()} | {p.relative_to(BASE)}")

    if args.write_stale:
        out_path = Path(args.write_stale)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            for p, dt, age_days in stale:
                if dt is None and not args.include_no_date:
                    continue
                f.write(str(p.relative_to(BASE)) + "\n")
        print(f"\nWrote stale list to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

