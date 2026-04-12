#!/usr/bin/env python3
"""
Regenerate derived index timeframes from 15-min source data.

This scans:
  data/indices/<timeframe>/*.csv

For each derived timeframe folder that exists (e.g. 20min, 30min, 1hr),
it rebuilds each CSV from its matching 15-min file:
  data/indices/15min/<symbol>_15min.csv

It only touches files that already exist in the derived timeframe folders
(so it won't create new symbols/timeframes unless you already have them).

Requires: pandas
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


BASE = Path(__file__).resolve().parent
INDICES = BASE / "data" / "indices"
SRC_15 = INDICES / "15min"


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
    # Keep existing format: timezone +05:30 string
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
    ap = argparse.ArgumentParser(description="Regenerate derived index timeframes from 15-min CSVs")
    ap.add_argument(
        "--timeframes",
        default="20min,30min,35min,45min,50min,1hr",
        help="Comma-separated timeframe folders under data/indices (default: 20min,30min,35min,45min,50min,1hr)",
    )
    args = ap.parse_args()

    tfs = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    existing_tfs = [t for t in tfs if (INDICES / t).is_dir()]
    if not existing_tfs:
        print("No derived timeframe folders exist under data/indices/. Nothing to do.")
        return 0

    for tf in existing_tfs:
        tf_dir = INDICES / tf
        csvs = sorted(p for p in tf_dir.glob("*.csv") if p.is_file())
        if not csvs:
            continue
        print(f"\nTimeframe {tf}: {len(csvs)} files")
        for p in csvs:
            name = p.name
            # Expect output like nifty_50_30min.csv -> source nifty_50_15min.csv
            if name.endswith(f"_{tf}.csv"):
                base = name[: -len(f"_{tf}.csv")]
            else:
                base = name.rsplit(".", 1)[0]
            src = SRC_15 / f"{base}_15min.csv"
            if not src.is_file():
                print(f"  (skip) missing 15min source for {name}: expected {src}")
                continue
            n = resample_file(src, p, tf)
            print(f"  wrote {name} ({n} rows)")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

