"""
Resample 15-min OHLCV CSV to 20/30/35/45/50-min or 1-hr.
Reads from 15min/nifty_50_15min.csv.
Writes to 20min/, 30min/, 35min/, 45min/, 50min/, or 1hr/.
Bars aligned so first bar of session starts at 09:15 IST.
Usage: run from this directory or from repo root with proper path.
"""
import pandas as pd
from pathlib import Path

# Paths relative to this script
SCRIPT_DIR = Path(__file__).resolve().parent
DIR_15MIN = SCRIPT_DIR / "15min"
DIR_20MIN = SCRIPT_DIR / "20min"
DIR_30MIN = SCRIPT_DIR / "30min"
DIR_35MIN = SCRIPT_DIR / "35min"
DIR_45MIN = SCRIPT_DIR / "45min"
DIR_50MIN = SCRIPT_DIR / "50min"
DIR_1HR = SCRIPT_DIR / "1hr"

INPUT_FILE = DIR_15MIN / "nifty_50_15min.csv"
OUTPUT_FILE_20MIN = DIR_20MIN / "nifty_50_20min.csv"
OUTPUT_FILE_30MIN = DIR_30MIN / "nifty_50_30min.csv"
OUTPUT_FILE_35MIN = DIR_35MIN / "nifty_50_35min.csv"
OUTPUT_FILE_45MIN = DIR_45MIN / "nifty_50_45min.csv"
OUTPUT_FILE_50MIN = DIR_50MIN / "nifty_50_50min.csv"
OUTPUT_FILE_1HR = DIR_1HR / "nifty_50_1hr.csv"
OUTPUT_FILE = OUTPUT_FILE_30MIN  # default


def _resample_ohlcv(
    df: pd.DataFrame,
    rule: str,
    offset: pd.Timedelta,
) -> pd.DataFrame:
    """Resample OHLCV with given rule and offset. df must have datetime index."""
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


def resample_15min_to_20min(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_20MIN,
) -> pd.DataFrame:
    """Resample to 20-min OHLCV (09:15, 09:35, 09:55, ...). offset=5min for 09:15 bar start."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    resampled = _resample_ohlcv(df, "20min", pd.Timedelta(minutes=5))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 20-min bars")
    print(f"Written: {output_path}")
    return resampled


def resample_15min_to_30min(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_30MIN,
) -> pd.DataFrame:
    """Load 15-min CSV, resample to 30-min OHLCV, save and return the 30-min DataFrame."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    # offset=15min so bars align to 09:15, 09:45, 10:15... (NSE open 09:15)
    resampled = _resample_ohlcv(df, "30min", pd.Timedelta(minutes=15))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 30-min bars")
    print(f"Written: {output_path}")
    return resampled


def resample_15min_to_35min(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_35MIN,
) -> pd.DataFrame:
    """Resample to 35-min OHLCV (09:15, 09:50, 10:25, ...). offset=15min for 09:15 bar start."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    resampled = _resample_ohlcv(df, "35min", pd.Timedelta(minutes=15))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 35-min bars")
    print(f"Written: {output_path}")
    return resampled


def resample_15min_to_45min(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_45MIN,
) -> pd.DataFrame:
    """Load 15-min CSV, resample to 45-min OHLCV (09:15, 10:00, 10:45, ...), save and return."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    # offset=45min so 45min bars align to 09:15, 10:00, 10:45 IST
    resampled = _resample_ohlcv(df, "45min", pd.Timedelta(minutes=45))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 45-min bars")
    print(f"Written: {output_path}")
    return resampled


def resample_15min_to_50min(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_50MIN,
) -> pd.DataFrame:
    """Resample to 50-min OHLCV (09:15, 10:05, 10:55, ...). offset=25min for 09:15 bar start."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    resampled = _resample_ohlcv(df, "50min", pd.Timedelta(minutes=25))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 50-min bars")
    print(f"Written: {output_path}")
    return resampled


def resample_15min_to_1hr(
    input_path: Path = INPUT_FILE,
    output_path: Path = OUTPUT_FILE_1HR,
) -> pd.DataFrame:
    """Load 15-min CSV, resample to 1-hr OHLCV (09:15, 10:15, ...), save and return."""
    df = pd.read_csv(input_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.set_index("date").sort_index()
    # offset=45min so 1h bars align to 09:15, 10:15, 11:15 IST (03:45 UTC = 09:15 IST)
    resampled = _resample_ohlcv(df, "1h", pd.Timedelta(minutes=45))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(output_path, index=False)
    print(f"Resampled {len(df):,} 15-min bars → {len(resampled):,} 1-hr bars")
    print(f"Written: {output_path}")
    return resampled


if __name__ == "__main__":
    resample_15min_to_20min()
    resample_15min_to_30min()
    resample_15min_to_35min()
    resample_15min_to_45min()
    resample_15min_to_50min()
    resample_15min_to_1hr()
