#!/usr/bin/env python3
"""
Merge NIFTY 50 index 5min OHLC with aggregate volume = sum of volumes of all
constituent stocks (from data/nifty50/5min/*_5min.csv) at each timestamp.

Index CSV from Kite often has volume=0; this overwrites ``volume`` with the sum.
Constituents are read from config/nifty50_symbols.txt (same slug rules as fetch).

Usage:
  .venv/bin/python -u fetch_code/build_nifty50_index_volume.py
  .venv/bin/python -u fetch_code/build_nifty50_index_volume.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_FC = Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)

from repo_paths import REPO_ROOT  # noqa: E402

import pandas as pd

from fetch_code.fetch_all_indices_5min import slug  # noqa: E402


def _load_symbols(path: Path) -> list[str]:
    out: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                out.append(line)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Set NIFTY 50 5min volume = sum of constituents")
    ap.add_argument(
        "--index-csv",
        type=Path,
        default=None,
        help="Index OHLCV CSV (default: data/indices/5min/nifty_50_5min.csv)",
    )
    ap.add_argument(
        "--nifty50-dir",
        type=Path,
        default=None,
        help="Constituent 5min folder (default: data/nifty50/5min)",
    )
    ap.add_argument(
        "--symbols-file",
        type=Path,
        default=None,
        help="default: config/nifty50_symbols.txt",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV (default: overwrite --index-csv)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute stats only; do not write",
    )
    args = ap.parse_args()

    base = Path(REPO_ROOT)
    index_csv = args.index_csv or (base / "data" / "indices" / "5min" / "nifty_50_5min.csv")
    nifty_dir = args.nifty50_dir or (base / "data" / "nifty50" / "5min")
    sym_file = args.symbols_file or (base / "config" / "nifty50_symbols.txt")
    output = args.output or index_csv

    if not index_csv.is_file():
        print(f"Missing index file: {index_csv}", file=sys.stderr)
        return 2

    symbols = _load_symbols(sym_file)
    series_list: list[pd.Series] = []
    missing_files: list[str] = []

    for sym in symbols:
        fn = f"{slug(sym)}_5min.csv"
        p = nifty_dir / fn
        if not p.is_file():
            missing_files.append(f"{sym} -> {fn}")
            continue
        df = pd.read_csv(p, usecols=["date", "volume"])
        df["date"] = pd.to_datetime(df["date"], utc=True)
        series_list.append(df.set_index("date")["volume"].rename(sym))

    if not series_list:
        print("No constituent CSVs loaded.", file=sys.stderr)
        return 2

    if missing_files:
        print(f"Warning: {len(missing_files)} constituent file(s) missing (skipped):", file=sys.stderr)
        for m in missing_files[:15]:
            print(f"  {m}", file=sys.stderr)
        if len(missing_files) > 15:
            print(f"  ... and {len(missing_files) - 15} more", file=sys.stderr)

    vol_wide = pd.concat(series_list, axis=1, sort=True)
    vol_sum = vol_wide.sum(axis=1, skipna=True)

    idx = pd.read_csv(index_csv)
    idx["_ts"] = pd.to_datetime(idx["date"], utc=True)
    aligned = vol_sum.reindex(idx["_ts"])
    n_matched = int(aligned.notna().sum())
    n_idx = len(idx)
    # Use numpy array — assigning a DatetimeIndex-aligned Series misaligns on RangeIndex.
    idx["volume"] = aligned.fillna(0).round().to_numpy(dtype="int64")
    idx = idx.drop(columns=["_ts"])

    print(
        f"Constituents used: {len(series_list)}/{len(symbols)}. "
        f"Index rows: {n_idx}. Bars with at least one stock volume: {n_matched}."
    )
    print(f"Sample total volume (last row): {idx['volume'].iloc[-1]}")

    if args.dry_run:
        print("Dry run; not writing.")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    idx.to_csv(output, index=False)
    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
