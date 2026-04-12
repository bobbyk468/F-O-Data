#!/usr/bin/env python3
"""
Build daily OHLC from 15m CSVs, then for each session date compute:

  * CPR (Central Pivot Range) from the *previous* session's H/L/C:
      PP = (H + L + C) / 3,  BC = (H + L) / 2,  TC = 2*PP - BC
    CPR width = max(TC, BC) - min(TC, BC)  (always >= 0)

  * SuperTrend(25, 2) on daily high/low/close (Wilder ATR, band logic aligned with pandas_ta).

Writes one CSV per input symbol under --out-dir.

Usage:
  .venv/bin/python compute_daily_cpr_supertrend.py --only nifty50
  .venv/bin/python compute_daily_cpr_supertrend.py --only all --out-dir output/daily_cpr_st
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
ST_PERIOD = 25
ST_MULT = 2.0


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


def load_15m(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(IST)
    df["day"] = df["date"].dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


def daily_ohlc_from_15m(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby("day", sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
    )
    return daily


def atr_wilder(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Wilder / RMA smoothed ATR; first ATR at index period-1 is mean(TR[0:period])."""
    h = high.astype(float)
    l = low.astype(float)
    c = close.astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    tr = tr.copy()
    tr.iloc[0] = h.iloc[0] - l.iloc[0]

    n = len(tr)
    atr = pd.Series(np.nan, index=tr.index, dtype=float)
    if n < period:
        return atr
    atr.iloc[period - 1] = tr.iloc[:period].mean()
    for i in range(period, n):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period
    return atr


def supertrend(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    length: int = ST_PERIOD,
    multiplier: float = ST_MULT,
) -> tuple[pd.Series, pd.Series]:
    """
    SuperTrend line and direction (+1 bullish / support line, -1 bearish / resistance line).
    Band update rules match pandas_ta.overlap.supertrend (mutating upper/lower bands).
    """
    h = high.astype(float).copy()
    l = low.astype(float).copy()
    c = close.astype(float).copy()
    atr_s = atr_wilder(h, l, c, length)
    hl2 = (h + l) / 2
    upperband = hl2 + multiplier * atr_s
    lowerband = hl2 - multiplier * atr_s

    m = len(c)
    dir_ = np.ones(m, dtype=np.int8)
    trend = np.full(m, np.nan, dtype=float)

    for i in range(1, m):
        if pd.isna(upperband.iloc[i - 1]) or pd.isna(lowerband.iloc[i - 1]):
            continue
        if c.iloc[i] > upperband.iloc[i - 1]:
            dir_[i] = 1
        elif c.iloc[i] < lowerband.iloc[i - 1]:
            dir_[i] = -1
        else:
            dir_[i] = dir_[i - 1]
            if dir_[i] > 0 and lowerband.iloc[i] < lowerband.iloc[i - 1]:
                lowerband.iloc[i] = lowerband.iloc[i - 1]
            if dir_[i] < 0 and upperband.iloc[i] > upperband.iloc[i - 1]:
                upperband.iloc[i] = upperband.iloc[i - 1]

        if dir_[i] > 0:
            trend[i] = lowerband.iloc[i]
        else:
            trend[i] = upperband.iloc[i]

    idx = close.index
    return pd.Series(trend, index=idx), pd.Series(dir_, index=idx, dtype=np.int8)


def add_cpr_width(daily: pd.DataFrame) -> pd.DataFrame:
    """CPR for session `day` uses previous row's H/L/C (prior session)."""
    out = daily.copy()
    ph = out["high"].shift(1)
    pl = out["low"].shift(1)
    pc = out["close"].shift(1)
    pp = (ph + pl + pc) / 3
    bc = (ph + pl) / 2
    tc = 2 * pp - bc
    out["cpr_pp"] = pp
    out["cpr_bc"] = bc
    out["cpr_tc"] = tc
    top = np.maximum(tc, bc)
    bot = np.minimum(tc, bc)
    out["cpr_width"] = top - bot
    return out


def process_one(path: Path, out_dir: Path) -> tuple[str, int, float]:
    df = load_15m(path)
    daily = daily_ohlc_from_15m(df)
    daily = add_cpr_width(daily)
    w = daily["cpr_width"].dropna()
    if len(w) and float(w.min()) < 0:
        raise ValueError(f"Negative CPR width in {path}")
    st_line, st_dir = supertrend(daily["high"], daily["low"], daily["close"], ST_PERIOD, ST_MULT)
    daily["supertrend"] = st_line
    daily["supertrend_dir"] = st_dir

    stem = path.stem.replace("_15min", "")
    out_path = out_dir / f"{stem}_daily_cpr_st.csv"
    daily.to_csv(out_path, index=False)
    return str(out_path), len(daily), float(w.min()) if len(w) else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily CPR width + SuperTrend(25,2) from 15m CSVs")
    ap.add_argument("--only", choices=("all", "indices", "nifty50", "other"), default="all")
    ap.add_argument("--max-files", type=int, default=0, help="Limit files (0 = all)")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default: jugaad-trader/output/daily_cpr_st)",
    )
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    data = root / "data"
    out_dir = args.out_dir or (root / "output" / "daily_cpr_st")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not data.is_dir():
        print("No data/ directory found.", file=sys.stderr)
        return 1

    paths = list(iter_15min_csvs(data, args.only))
    if args.max_files and args.max_files > 0:
        paths = paths[: int(args.max_files)]

    if not paths:
        print("No *_15min.csv files found.", file=sys.stderr)
        return 1

    written = 0
    rows = 0
    min_width_global = float("inf")
    for p in paths:
        outp, n, min_w = process_one(p, out_dir)
        written += 1
        rows += n
        if not np.isnan(min_w):
            min_width_global = min(min_width_global, min_w)
        print(outp, n)

    print(f"\nDone: {written} files, {rows:,} total daily rows -> {out_dir}")
    if min_width_global < float("inf"):
        print(f"Global min CPR width (finite rows): {min_width_global:.6g} (expect >= 0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
