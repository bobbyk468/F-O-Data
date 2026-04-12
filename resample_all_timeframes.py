#!/usr/bin/env python3
"""
Regenerate derived OHLCV timeframes from 15-min source data for any universe folder.

Universes:
  data/indices/
  data/nifty50/
  data/other/

Source:
  <universe>/15min/*.csv   (e.g. nifty_50_15min.csv, reliance_15min.csv)

Derived timeframes (only if the folder exists; only touches existing files):
  <universe>/20min,30min,35min,45min,50min,1hr

For each file in a derived timeframe folder, rebuild it from the matching 15-min file.
Example:
  data/indices/30min/nifty_50_30min.csv  <- data/indices/15min/nifty_50_15min.csv
  data/nifty50/1hr/reliance_1hr.csv      <- data/nifty50/15min/reliance_15min.csv

Requires: pandas
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


BASE = Path(__file__).resolve().parent
DATA = BASE / "data"

UNIVERSES = ("indices", "nifty50", "other")

OFFSETS_MIN = {
    "20min": 5,
    "30min": 15,
    "35min": 15,
    "45min": 45,
    "50min": 25,
    "1hr": 45,
    "1h": 45,
}


def _rule_and_offset(tf: str) -> tuple[str, pd.Timedelta]:
    tf = tf.lower()
    if tf in ("1hr", "1h"):
        return ("1h", pd.Timedelta(minutes=OFFSETS_MIN[tf]))
    if tf.endswith("min"):
        mins = int(tf.replace("min", ""))
        return (f"{mins}min", pd.Timedelta(minutes=OFFSETS_MIN[tf]))
    raise ValueError(f"Unsupported timeframe folder: {tf}")


def _resample_ohlcv(df: pd.DataFrame, rule: str, offset: pd.Timedelta) -> pd.DataFrame:
    resampled = df.resample(rule, offset=offset).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(how="all")
    resampled = resampled.dropna(subset=["open", "high", "low", "close"])
    resampled = resampled.reset_index()
    resampled.rename(columns={"index": "date"}, inplace=True)
    resampled["date"] = resampled["date"].dt.tz_convert("Asia/Kolkata").astype(str)
    return resampled


def resample_file(src_15min: Path, out_path: Path, tf: str) -> int:
    rule, offset = _rule_and_offset(tf)
    df = pd.read_csv(src_15min)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    out = _resample_ohlcv(df, rule, offset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    return len(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate derived timeframes from 15-min sources (indices/nifty50/other)")
    ap.add_argument(
        "--timeframes",
        default="20min,30min,35min,45min,50min,1hr",
        help="Comma-separated derived timeframe folders to process (default: 20min,30min,35min,45min,50min,1hr)",
    )
    args = ap.parse_args()

    tfs = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    did_any = False

    for u in UNIVERSES:
        udir = DATA / u
        src_dir = udir / "15min"
        if not src_dir.is_dir():
            continue
        existing_tfs = [t for t in tfs if (udir / t).is_dir()]
        if not existing_tfs:
            continue

        print(f"\nUniverse: {u}")
        for tf in existing_tfs:
            tf_dir = udir / tf
            csvs = sorted(p for p in tf_dir.glob("*.csv") if p.is_file())
            if not csvs:
                continue
            did_any = True
            print(f"  Timeframe {tf}: {len(csvs)} files")
            for outp in csvs:
                name = outp.name
                suffix = f"_{tf}.csv"
                if name.endswith(suffix):
                    base = name[: -len(suffix)]
                else:
                    base = name.rsplit(".", 1)[0]
                src = src_dir / f"{base}_15min.csv"
                if not src.is_file():
                    print(f"    (skip) missing 15min source for {name}: expected {src}")
                    continue
                n = resample_file(src, outp, tf)
                print(f"    wrote {name} ({n} rows)")

    if not did_any:
        print("No derived timeframe folders found under data/(indices|nifty50|other). Nothing to do.")
        return 0

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

