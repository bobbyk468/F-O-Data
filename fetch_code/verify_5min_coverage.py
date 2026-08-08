#!/usr/bin/env python3
"""
Check that 5-minute CSVs exist and cover the expected date range per symbol.

For one or more directories (or individual CSV paths), reports first/last bar date and
row count. Optionally compares every file's *last* date to the newest last-date among
the scanned set and flags symbols that end too early (``STALE``).

For indices, ``--indices-manifest`` checks that each main+sector index has a
``<slug>_5min.csv`` under the given directory (typically data/indices/5min).

Usage:
  .venv/bin/python -u fetch_code/verify_5min_coverage.py data/indices/5min --indices-manifest
  .venv/bin/python -u fetch_code/verify_5min_coverage.py data/nifty50/5min data/other/5min
  .venv/bin/python -u fetch_code/verify_5min_coverage.py data/indices/5min --grace-days 5
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

# --- repo root on path (same pattern as other fetch_code tools) ---
_FC = Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)


def _parse_dt(s: Any) -> Optional[datetime]:
    if isinstance(s, datetime):
        return s
    if isinstance(s, date) and not isinstance(s, datetime):
        return datetime(s.year, s.month, s.day)
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt, n in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(s[:n], fmt)
        except Exception:
            continue
    return None


def _csv_first_data_line(path: Path) -> Optional[str]:
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline()
        if not header:
            return None
        return f.readline()


def _csv_last_data_line(path: Path) -> Optional[str]:
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
        return ln
    return None


def _line_first_field(line: Optional[str]) -> Optional[datetime]:
    if not line:
        return None
    first = line.split(",", 1)[0].strip()
    return _parse_dt(first)


def _count_data_rows(path: Path) -> int:
    n = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            n += chunk.count(b"\n")
    return max(0, n - 1)


@dataclass
class FileCoverage:
    path: Path
    rows: int
    first_dt: Optional[datetime]
    last_dt: Optional[datetime]
    status: str  # OK | EMPTY | STALE | MISSING


def _scan_csv(path: Path) -> FileCoverage:
    if not path.is_file():
        return FileCoverage(path, 0, None, None, "MISSING")
    rows = _count_data_rows(path)
    if rows <= 0:
        return FileCoverage(path, 0, None, None, "EMPTY")
    first_dt = _line_first_field(_csv_first_data_line(path))
    last_dt = _line_first_field(_csv_last_data_line(path))
    if first_dt is None or last_dt is None:
        return FileCoverage(path, rows, first_dt, last_dt, "EMPTY")
    return FileCoverage(path, rows, first_dt, last_dt, "OK")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Verify 5min CSVs exist and end near the same date as peers."
    )
    ap.add_argument(
        "paths",
        nargs="+",
        help="CSV files or directories (recursive *.csv)",
    )
    ap.add_argument(
        "--indices-manifest",
        action="store_true",
        help="Expect data/indices/5min layout: require each main+sector index CSV.",
    )
    ap.add_argument(
        "--grace-days",
        type=int,
        default=3,
        metavar="N",
        help="STALE if last bar date is more than N calendar days before the newest "
        "last-date in the scan (default 3).",
    )
    ap.add_argument(
        "--expect-last",
        default=None,
        metavar="YYYY-MM-DD",
        help="Also flag STALE if last bar date is before this date (optional).",
    )
    args = ap.parse_args()

    expect_last: Optional[date] = None
    if args.expect_last:
        expect_last = datetime.strptime(args.expect_last, "%Y-%m-%d").date()

    files: list[Path] = []
    manifest_missing: list[str] = []

    if args.indices_manifest:
        from fetch_code.fetch_all_indices_5min import MAIN_AND_SECTOR_SYMBOLS, slug

        if len(args.paths) != 1:
            print("--indices-manifest expects exactly one directory.", file=sys.stderr)
            return 2
        root = Path(args.paths[0])
        if not root.is_dir():
            print(f"Not a directory: {root}", file=sys.stderr)
            return 2
        for sym in MAIN_AND_SECTOR_SYMBOLS:
            name = f"{slug(sym)}_5min.csv"
            p = root / name
            files.append(p)
            if not p.is_file():
                manifest_missing.append(f"{sym} -> {name}")
    else:
        for p in args.paths:
            path = Path(p)
            if path.is_file():
                files.append(path)
            elif path.is_dir():
                files.extend(sorted(path.rglob("*.csv")))
            else:
                print(f"Skip (not found): {path}", file=sys.stderr)

    if not files:
        print("No paths to check.", file=sys.stderr)
        return 2

    rows: list[FileCoverage] = [_scan_csv(p) for p in files]

    last_dates = [r.last_dt.date() for r in rows if r.last_dt is not None]
    peer_last = max(last_dates) if last_dates else None

    stale = 0
    problems = 0

    for r in rows:
        if r.status == "MISSING":
            problems += 1
            continue
        if r.rows <= 0 or r.last_dt is None:
            r.status = "EMPTY"
            problems += 1
            continue
        ld = r.last_dt.date()
        is_stale = False
        if peer_last is not None and ld < peer_last - timedelta(days=args.grace_days):
            is_stale = True
        if expect_last is not None and ld < expect_last:
            is_stale = True
        if is_stale:
            r.status = "STALE"
            stale += 1
            problems += 1

    # Print table
    print(
        f"{'STATUS':<8} {'ROWS':>8} {'FIRST':<12} {'LAST':<12} PATH"
    )
    for r in sorted(rows, key=lambda x: str(x.path)):
        fd = r.first_dt.date().isoformat() if r.first_dt else "-"
        ld = r.last_dt.date().isoformat() if r.last_dt else "-"
        print(f"{r.status:<8} {r.rows:>8} {fd:<12} {ld:<12} {r.path}")

    print()
    if peer_last:
        print(f"Newest last-bar date in this scan: {peer_last.isoformat()} (--grace-days={args.grace_days})")
    if expect_last:
        print(f"Also requiring last >= {expect_last.isoformat()} (--expect-last)")
    if args.indices_manifest and manifest_missing:
        print(f"\nMissing index CSVs ({len(manifest_missing)}):")
        for m in manifest_missing:
            print(f"  {m}")

    summary_missing = sum(1 for r in rows if r.status == "MISSING")
    summary_empty = sum(1 for r in rows if r.status == "EMPTY")
    print(
        f"\nSummary: {len(rows)} path(s). "
        f"MISSING={summary_missing}, EMPTY={summary_empty}, STALE={stale}, OK="
        f"{len(rows) - problems}."
    )

    if manifest_missing:
        return 2
    if problems > 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
