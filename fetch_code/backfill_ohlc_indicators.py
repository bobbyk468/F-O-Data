#!/usr/bin/env python3
"""Recompute CPR, SuperTrend(25,7), Bollinger(25,2) on existing OHLCV CSVs (no API)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_FC = Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

from repo_paths import REPO_ROOT  # noqa: E402

from ohlc_indicators import rewrite_15m_csv_with_indicators, rewrite_eod_csv_with_indicators  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill indicators on data/**/*.csv")
    ap.add_argument("--only", choices=("15m", "eod", "all"), default="all")
    ap.add_argument("--max-files", type=int, default=0, help="Limit (0 = no limit)")
    args = ap.parse_args()
    data = REPO_ROOT / "data"
    paths: list[Path] = []
    if args.only in ("15m", "all"):
        paths.extend(sorted(data.glob("**/15min/*_15min.csv")))
    if args.only in ("eod", "all"):
        eod_paths = sorted(data.glob("**/eod/*_eod.csv"))
        paths.extend(p for p in eod_paths if not p.name.endswith("_eod_90d.csv"))
    paths.sort()
    if args.max_files and args.max_files > 0:
        paths = paths[: int(args.max_files)]
    for i, p in enumerate(paths, 1):
        try:
            if "15min" in p.parts:
                rewrite_15m_csv_with_indicators(p)
            else:
                rewrite_eod_csv_with_indicators(p)
        except Exception as e:
            print(f"FAIL {p}: {e}", file=sys.stderr)
            return 1
        print(f"[{i}/{len(paths)}] {p}")
    print(f"Done. {len(paths)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
