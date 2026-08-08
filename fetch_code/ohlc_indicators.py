"""
Enrich OHLCV DataFrames with CPR (prior session), SuperTrend(25,7), Bollinger(25,2).
Used when writing15m and EOD CSVs during fetch / incremental update.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_FC = Path(__file__).resolve().parent
_REPO = _FC.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import compute_daily_cpr_supertrend as cst  # noqa: E402

IST = "Asia/Kolkata"
ST_PERIOD = 25
ST_MULT = 7.0
BB_LENGTH = 25
BB_STD = 2.0


def _to_ist_timestamp(x) -> pd.Timestamp:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return pd.NaT
    t = pd.Timestamp(x)
    if pd.isna(t):
        return t
    if t.tzinfo is None:
        return t.tz_localize(IST, ambiguous="infer", nonexistent="shift_forward")
    return t.tz_convert(IST)


def coerce_datetime_ist(s: pd.Series) -> pd.Series:
    """Parse CSV/API datetimes and normalize to Asia/Kolkata (handles naive legacy rows)."""
    # Vectorized to_datetime fails on mixed naive + tz-aware in one series (append from API).
    return s.map(_to_ist_timestamp)

COLS_15M = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "day",
    "cpr_pp",
    "cpr_bc",
    "cpr_tc",
    "cpr_width",
    "supertrend",
    "supertrend_dir",
    "bb_middle",
    "bb_upper",
    "bb_lower",
]

COLS_EOD = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "cpr_pp",
    "cpr_bc",
    "cpr_tc",
    "cpr_width",
    "supertrend",
    "supertrend_dir",
    "bb_middle",
    "bb_upper",
    "bb_lower",
]


def bollinger_bands(close: pd.Series, length: int, num_std: float) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Middle = SMA(length); bands = middle ± num_std * stdev (ddof=0)."""
    c = close.astype(float)
    mid = c.rolling(length, min_periods=length).mean()
    std = c.rolling(length, min_periods=length).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return mid, upper, lower


def enrich_15min_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = out["volume"].fillna(0)
    out["date"] = coerce_datetime_ist(out["date"])
    out = out.sort_values("date").reset_index(drop=True)
    out["day"] = out["date"].dt.normalize()

    daily = cst.daily_ohlc_from_15m(out)
    daily = cst.add_cpr(daily)
    cpr_cols = ["cpr_pp", "cpr_bc", "cpr_tc", "cpr_width"]
    out = out.merge(daily.set_index("day")[cpr_cols].reset_index(), on="day", how="left")

    st_line, st_dir = cst.supertrend(out["high"], out["low"], out["close"], ST_PERIOD, ST_MULT)
    out["supertrend"] = st_line
    out["supertrend_dir"] = st_dir

    mid, up, lo = bollinger_bands(out["close"], BB_LENGTH, BB_STD)
    out["bb_middle"] = mid
    out["bb_upper"] = up
    out["bb_lower"] = lo

    out["day"] = out["day"].dt.strftime("%Y-%m-%d")
    return out


def enrich_eod_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = out["volume"].fillna(0)
    out["date"] = coerce_datetime_ist(out["date"])
    out = out.sort_values("date").reset_index(drop=True)

    out = cst.add_cpr(out)
    st_line, st_dir = cst.supertrend(out["high"], out["low"], out["close"], ST_PERIOD, ST_MULT)
    out["supertrend"] = st_line
    out["supertrend_dir"] = st_dir
    mid, up, lo = bollinger_bands(out["close"], BB_LENGTH, BB_STD)
    out["bb_middle"] = mid
    out["bb_upper"] = up
    out["bb_lower"] = lo
    return out


def candles_to_enriched_15m_df(candles: list[dict]) -> pd.DataFrame:
    rows = []
    for c in candles or []:
        rows.append(
            {
                "date": c.get("date"),
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume", 0),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=COLS_15M)
    return enrich_15min_df(df)


def candles_to_enriched_eod_df(candles: list[dict]) -> pd.DataFrame:
    rows = []
    for c in candles or []:
        rows.append(
            {
                "date": c.get("date"),
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume", 0),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=COLS_EOD)
    return enrich_eod_df(df)


def write_15min_enriched_csv(out_path: str | Path, candles: list[dict]) -> int:
    path = Path(out_path)
    df = candles_to_enriched_15m_df(candles)
    if df.empty:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=COLS_15M).to_csv(path, index=False)
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df[COLS_15M].to_csv(path, index=False)
    return len(df)


def write_eod_enriched_csv(out_path: str | Path, candles: list[dict]) -> int:
    path = Path(out_path)
    df = candles_to_enriched_eod_df(candles)
    if df.empty:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=COLS_EOD).to_csv(path, index=False)
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df[COLS_EOD].to_csv(path, index=False)
    return len(df)


def read_base_ohlcv_15m(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    base = ["date", "open", "high", "low", "close", "volume"]
    missing = [c for c in base if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")
    return df[base].copy()


def read_base_ohlcv_eod(path: Path) -> pd.DataFrame:
    return read_base_ohlcv_15m(path)


def rewrite_15m_csv_with_indicators(path: Path) -> int:
    """Re-read OHLCV only, recompute indicators, overwrite file."""
    df = read_base_ohlcv_15m(path)
    if df.empty:
        pd.DataFrame(columns=COLS_15M).to_csv(path, index=False)
        return 0
    out = enrich_15min_df(df)
    out[COLS_15M].to_csv(path, index=False)
    return len(out)


def rewrite_eod_csv_with_indicators(path: Path) -> int:
    df = read_base_ohlcv_eod(path)
    if df.empty:
        pd.DataFrame(columns=COLS_EOD).to_csv(path, index=False)
        return 0
    out = enrich_eod_df(df)
    out[COLS_EOD].to_csv(path, index=False)
    return len(out)
