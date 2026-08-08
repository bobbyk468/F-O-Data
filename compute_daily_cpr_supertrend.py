#!/usr/bin/env python3
"""
Compute CPR (PP, BC, TC, width as band/PP) and SuperTrend on daily and 15-minute series.

CPR for session D uses prior session H/L/C (shift-1 on daily OHLC).
CPR width (cpr_width) = (max(TC, BC) - min(TC, BC)) / PP — band width as a decimal of pivot (e.g. 0.015 ≡ 1.5%; NaN when PP is 0 or missing).

SuperTrend: Wilder ATR, default period 25, multiplier 7 (pandas_ta-style bands).

Modes (--mode):
  * daily-from-15m — aggregate 15m → daily, then CPR + ST; one CSV per *_15min.csv
  * eod — read *_eod.csv (indices, nifty50, other); one enriched CSV per file
  * 15min — each 15m bar gets that session's CPR (from prior calendar day) + ST on 15m OHLC;
    writes under output/15min_cpr_st/{indices,nifty50,other}/

Usage:
  .venv/bin/python compute_daily_cpr_supertrend.py --mode daily-from-15m --only nifty50
  .venv/bin/python compute_daily_cpr_supertrend.py --mode eod --only all
  .venv/bin/python compute_daily_cpr_supertrend.py --mode 15min --only indices --st-mult 7
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
ST_PERIOD_DEFAULT = 25
ST_MULT_DEFAULT = 7.0


def iter_15min_csvs(data_root: Path, only: str) -> Iterator[tuple[Path, str]]:
    """Yield (path, category) where category is indices | nifty50 | other."""
    if only == "all":
        subs = [
            ("indices/15min", "indices"),
            ("nifty50/15min", "nifty50"),
            ("other/15min", "other"),
        ]
    elif only == "indices":
        subs = [("indices/15min", "indices")]
    elif only == "nifty50":
        subs = [("nifty50/15min", "nifty50")]
    elif only == "other":
        subs = [("other/15min", "other")]
    else:
        raise ValueError(only)
    for rel, cat in subs:
        d = data_root / rel
        if d.is_dir():
            for p in sorted(d.glob("*_15min.csv")):
                yield p, cat


def iter_eod_csvs(data_root: Path, only: str) -> Iterator[tuple[Path, str]]:
    """Yield (path, category) where category is indices|nifty50|other."""
    if only == "all":
        subs = [("indices/eod", "indices"), ("nifty50/eod", "nifty50"), ("other/eod", "other")]
    elif only == "indices":
        subs = [("indices/eod", "indices")]
    elif only == "nifty50":
        subs = [("nifty50/eod", "nifty50")]
    elif only == "other":
        subs = [("other/eod", "other")]
    else:
        raise ValueError(only)
    for rel, cat in subs:
        d = data_root / rel
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*_eod.csv")):
            if p.name.endswith("_eod_90d.csv"):
                continue
            yield p, cat


def load_15m(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(IST)
    df["day"] = df["date"].dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


def load_eod(path: Path) -> pd.DataFrame:
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
    length: int,
    multiplier: float,
) -> tuple[pd.Series, pd.Series]:
    """
    SuperTrend line and direction (+1 bullish, -1 bearish).
    Band update rules match pandas_ta.overlap.supertrend.
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


def add_cpr(daily: pd.DataFrame) -> pd.DataFrame:
    """CPR for row i uses previous row's H/L/C (prior session). Adds cpr_pp, cpr_bc, cpr_tc, cpr_width (band/PP decimal)."""
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
    band = top - bot
    out["cpr_width"] = (band / pp).where(pp.notna() & (pp != 0))
    return out


def apply_st(df: pd.DataFrame, st_period: int, st_mult: float) -> None:
    st_line, st_dir = supertrend(df["high"], df["low"], df["close"], st_period, st_mult)
    df["supertrend"] = st_line
    df["supertrend_dir"] = st_dir


def process_daily_from_15m(path: Path, out_dir: Path, st_period: int, st_mult: float) -> tuple[str, int, float]:
    df = load_15m(path)
    daily = daily_ohlc_from_15m(df)
    daily = add_cpr(daily)
    w = daily["cpr_width"].dropna()
    if len(w) and float(w.min()) < 0:
        raise ValueError(f"Negative CPR width in {path}")
    apply_st(daily, st_period, st_mult)

    stem = path.stem.replace("_15min", "")
    out_path = out_dir / f"{stem}_daily_cpr_st.csv"
    daily.to_csv(out_path, index=False)
    return str(out_path), len(daily), float(w.min()) if len(w) else float("nan")


def process_eod(path: Path, out_dir: Path, category: str, st_period: int, st_mult: float) -> tuple[str, int, float]:
    daily = load_eod(path)
    daily = add_cpr(daily)
    w = daily["cpr_width"].dropna()
    if len(w) and float(w.min()) < 0:
        raise ValueError(f"Negative CPR width in {path}")
    apply_st(daily, st_period, st_mult)

    stem = path.stem
    sub = out_dir / category
    sub.mkdir(parents=True, exist_ok=True)
    out_path = sub / f"{stem}_cpr_st.csv"
    daily.to_csv(out_path, index=False)
    return str(out_path), len(daily), float(w.min()) if len(w) else float("nan")


def process_15min(
    path: Path, out_dir: Path, category: str, st_period: int, st_mult: float
) -> tuple[str, int, float]:
    df = load_15m(path)
    daily = daily_ohlc_from_15m(df)
    daily = add_cpr(daily)
    w = daily["cpr_width"].dropna()
    if len(w) and float(w.min()) < 0:
        raise ValueError(f"Negative CPR width (daily) in {path}")

    cpr_cols = ["cpr_pp", "cpr_bc", "cpr_tc", "cpr_width"]
    mrg = daily.set_index("day")[cpr_cols].reset_index()
    out = df.merge(mrg, on="day", how="left")
    apply_st(out, st_period, st_mult)

    stem = path.stem
    sub = out_dir / category
    sub.mkdir(parents=True, exist_ok=True)
    out_path = sub / f"{stem}_cpr_st.csv"
    out.to_csv(out_path, index=False)
    return str(out_path), len(out), float(w.min()) if len(w) else float("nan")


def default_out_dir(root: Path, mode: str) -> Path:
    if mode == "daily-from-15m":
        return root / "output" / "daily_cpr_st"
    if mode == "eod":
        return root / "output" / "eod_cpr_st"
    if mode == "15min":
        return root / "output" / "15min_cpr_st"
    raise ValueError(mode)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="CPR (PP,BC,TC,width) + SuperTrend on daily-from-15m, EOD, or 15m bars"
    )
    ap.add_argument(
        "--mode",
        choices=("daily-from-15m", "eod", "15min"),
        default="daily-from-15m",
        help="daily-from-15m: aggregate 15m→daily. eod: read *_eod.csv. 15min: tag each bar with session CPR + ST on 15m.",
    )
    ap.add_argument("--only", choices=("all", "indices", "nifty50", "other"), default="all")
    ap.add_argument("--max-files", type=int, default=0, help="Limit files (0 = all)")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory (default: daily flat; eod & 15min use subdirs indices|nifty50|other under out)",
    )
    ap.add_argument("--st-period", type=int, default=ST_PERIOD_DEFAULT, metavar="N", help="ATR period (default 25)")
    ap.add_argument("--st-mult", type=float, default=ST_MULT_DEFAULT, metavar="X", help="ATR multiplier (default 7)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    data = root / "data"
    out_dir = args.out_dir or default_out_dir(root, args.mode)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not data.is_dir():
        print("No data/ directory found.", file=sys.stderr)
        return 1

    st_period = max(2, int(args.st_period))
    st_mult = float(args.st_mult)

    paths_daily: list[tuple[Path, str]] = []
    paths_eod: list[tuple[Path, str]] = []
    paths_15: list[tuple[Path, str]] = []

    if args.mode == "daily-from-15m":
        paths_daily = list(iter_15min_csvs(data, args.only))
    elif args.mode == "eod":
        paths_eod = list(iter_eod_csvs(data, args.only))
    else:
        paths_15 = list(iter_15min_csvs(data, args.only))

    if args.max_files and args.max_files > 0:
        if args.mode == "eod":
            paths_eod = paths_eod[: int(args.max_files)]
        elif args.mode == "daily-from-15m":
            paths_daily = paths_daily[: int(args.max_files)]
        else:
            paths_15 = paths_15[: int(args.max_files)]

    if args.mode == "daily-from-15m" and not paths_daily:
        print("No *_15min.csv files found.", file=sys.stderr)
        return 1
    if args.mode == "eod" and not paths_eod:
        print("No *_eod.csv files found.", file=sys.stderr)
        return 1
    if args.mode == "15min" and not paths_15:
        print("No *_15min.csv files found.", file=sys.stderr)
        return 1

    written = 0
    rows = 0
    min_width_global = float("inf")

    if args.mode == "daily-from-15m":
        for p, _cat in paths_daily:
            outp, n, min_w = process_daily_from_15m(p, out_dir, st_period, st_mult)
            written += 1
            rows += n
            if not np.isnan(min_w):
                min_width_global = min(min_width_global, min_w)
            print(outp, n)
    elif args.mode == "eod":
        for p, cat in paths_eod:
            outp, n, min_w = process_eod(p, out_dir, cat, st_period, st_mult)
            written += 1
            rows += n
            if not np.isnan(min_w):
                min_width_global = min(min_width_global, min_w)
            print(outp, n)
    else:
        for p, cat in paths_15:
            outp, n, min_w = process_15min(p, out_dir, cat, st_period, st_mult)
            written += 1
            rows += n
            if not np.isnan(min_w):
                min_width_global = min(min_width_global, min_w)
            print(outp, n)

    print(f"\nDone: mode={args.mode}, {written} files, {rows:,} total rows -> {out_dir}")
    print(f"SuperTrend params: period={st_period}, mult={st_mult}")
    if min_width_global < float("inf"):
        print(f"Global min CPR width (finite rows): {min_width_global:.6g} (expect >= 0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
