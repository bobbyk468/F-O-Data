#!/usr/bin/env python3
"""
Scan 15min CPR+ST outputs: keep sessions where
  - cpr_width >= 0.01 (band/PP decimal) on **either** the current session (09:15 bar)
    **or** the previous session's last bar (keep if at least one side meets the floor), and
  - (09:15 close > ST and prior session last close < ST) OR
    (09:15 close < ST and prior session last close > ST)

Writes one CSV: symbol + category + date (09:15 IST) + prev_session_date (calendar date only)
+ cpr_width_prev_session + remaining 09:15 bar fields (no duplicate day/session_day columns).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

IST = "Asia/Kolkata"
SUFFIX = "_15min_cpr_st.csv"
CPR_WIDTH_MIN = 0.01


def symbol_from_path(p: Path) -> str:
    if not p.name.endswith(SUFFIX):
        return p.stem
    return p.name[: -len(SUFFIX)]


def iter_cpr_st_files(root: Path) -> list[tuple[Path, str, str]]:
    """(path, category, symbol)"""
    out: list[tuple[Path, str, str]] = []
    for cat in ("indices", "nifty50", "other"):
        d = root / cat
        if not d.is_dir():
            continue
        for p in sorted(d.glob(f"*{SUFFIX}")):
            out.append((p, cat, symbol_from_path(p)))
    return out


def first_bar_of_session(df: pd.DataFrame, day: pd.Timestamp) -> pd.Series | None:
    sub = df.loc[df["day"] == day].sort_values("date")
    if sub.empty:
        return None
    return sub.iloc[0]


def last_bar_of_session(df: pd.DataFrame, day: pd.Timestamp) -> pd.Series | None:
    sub = df.loc[df["day"] == day].sort_values("date")
    if sub.empty:
        return None
    return sub.iloc[-1]


def st_cross_filter(first: pd.Series, last_prev: pd.Series) -> bool:
    c1, st1 = first["close"], first["supertrend"]
    c0, st0 = last_prev["close"], last_prev["supertrend"]
    if any(pd.isna(x) for x in (c1, st1, c0, st0)):
        return False
    a = float(c1) > float(st1) and float(c0) < float(st0)
    b = float(c1) < float(st1) and float(c0) > float(st0)
    return a or b


def scan_file(path: Path, category: str, symbol: str) -> list[dict]:
    df = pd.read_csv(path, parse_dates=["date", "day"])
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(IST)
    df["day"] = pd.to_datetime(df["day"], utc=True).dt.tz_convert(IST)
    days = sorted(df["day"].dropna().unique())
    hits: list[dict] = []
    for i in range(1, len(days)):
        prev_day, today = days[i - 1], days[i]
        last_prev = last_bar_of_session(df, prev_day)
        first_today = first_bar_of_session(df, today)
        if last_prev is None or first_today is None:
            continue
        cw_today = first_today.get("cpr_width")
        cw_prev = last_prev.get("cpr_width")
        ok_today = not pd.isna(cw_today) and float(cw_today) >= CPR_WIDTH_MIN
        ok_prev = not pd.isna(cw_prev) and float(cw_prev) >= CPR_WIDTH_MIN
        if not (ok_today or ok_prev):
            continue
        if not st_cross_filter(first_today, last_prev):
            continue
        row = first_today.to_dict()
        row["symbol"] = symbol
        row["category"] = category
        row["_prev_session_day"] = prev_day  # internal; converted to prev_session_date in main
        row["cpr_width_prev_session"] = (
            float(cw_prev) if not pd.isna(cw_prev) else float("nan")
        )
        hits.append(row)
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description="Filter 15min CPR+ST for 09:15 ST cross + CPR width")
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Folder containing indices/, nifty50/, other/ (default: <script>/output/15min_cpr_st)",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: <input-dir>/filtered_915_cpr_st_cross.csv)",
    )
    ap.add_argument(
        "--stocks-only",
        action="store_true",
        help="Only nifty50/ and other/ (skip indices/)",
    )
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    input_dir = args.input_dir or (root / "output" / "15min_cpr_st")
    if not input_dir.is_dir():
        raise SystemExit(f"Missing input dir: {input_dir}")

    files = iter_cpr_st_files(input_dir)
    if args.stocks_only:
        files = [t for t in files if t[1] != "indices"]

    all_rows: list[dict] = []
    for path, cat, sym in files:
        all_rows.extend(scan_file(path, cat, sym))

    if not all_rows:
        out = args.output or (input_dir / "filtered_915_cpr_st_cross.csv")
        pd.DataFrame().to_csv(out, index=False)
        print(f"No matches. Wrote empty: {out}")
        return 0

    wide = pd.DataFrame(all_rows)
    bar_ts = pd.to_datetime(wide["date"], utc=True).dt.tz_convert(IST)
    wide["date"] = bar_ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    prev_d = pd.to_datetime(wide["_prev_session_day"], utc=True).dt.tz_convert(IST)
    wide["prev_session_date"] = prev_d.dt.strftime("%Y-%m-%d")
    wide = wide.drop(columns=["_prev_session_day", "day"], errors="ignore")
    lead = ["symbol", "category", "date", "prev_session_date", "cpr_width_prev_session"]
    rest = [c for c in wide.columns if c not in lead]
    wide = wide[lead + rest]

    out_path = args.output or (input_dir / "filtered_915_cpr_st_cross.csv")
    wide.to_csv(out_path, index=False)
    print(f"Matches: {len(wide)} rows from {len(files)} symbols -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
