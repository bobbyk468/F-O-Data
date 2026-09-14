#!/usr/bin/env python3
"""
Fetch 5-minute spot (index) data for NSE indices: main indices + sector indices.

Kite allows up to ~100 calendar days of 5-minute data per historical_data call (see Kite docs).
We request ~90 days (~3 months) per call. Sequential only; no sleeps (including on errors/retries).

Output: data/indices/5min/<slug>_5min.csv (plain OHLCV; no 15m-style indicator columns).

On expired session (TokenException / incorrect access_token), calls test_login once per
failed chunk and retries (same idea as update_incremental.py).

By default, existing ``*_5min.csv`` files are loaded and only the range from the last
saved bar's calendar date through ``--to-date`` is fetched (merged by bar timestamp).
Use ``--no-resume`` for a full refetch of the requested window.
"""
import sys
from pathlib import Path as _Path

_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import REPO_ROOT  # noqa: E402

import csv
import os
import re
import argparse
from datetime import datetime, timedelta, date
from typing import Any, Dict, Optional

DEFAULT_START_DATE = date(2015, 9, 1)
# ~3 months per API call (under Kite 5minute max span ~100 days).
CHUNK_DAYS = 90
PERIOD_DAYS = 90

MAIN_AND_SECTOR_SYMBOLS = [
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "NIFTY MIDCAP 100",
    "NIFTY NEXT 50",
    "INDIA VIX",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY PHARMA",
    "NIFTY FMCG",
    "NIFTY METAL",
    "NIFTY ENERGY",
    "NIFTY REALTY",
    "NIFTY PSU BANK",
    "NIFTY MEDIA",
    "NIFTY HEALTHCARE",
    "NIFTY CONSR DURBL",
    "NIFTY OIL AND GAS",
    "NIFTY PVT BANK",
    "NIFTY INFRA",
    "NIFTY MNC",
    "NIFTY PSE",
    "NIFTY SERV SECTOR",
    "NIFTY COMMODITIES",
    "NIFTY CONSUMPTION",
]


def slug(symbol: str) -> str:
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    return s.lower() or "index"


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


def _bar_key(dt: Any) -> str:
    """Stable key for 5m bars so CSV reload and API candles dedupe."""
    d = dt if isinstance(dt, datetime) else _parse_dt(str(dt))
    if d is None:
        return ""
    if d.tzinfo is not None:
        d = d.replace(tzinfo=None)
    return d.strftime("%Y-%m-%d %H:%M")


def last_csv_datetime(path: str) -> Optional[datetime]:
    """Last data-row datetime without reading the whole file."""
    if not os.path.isfile(path):
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
        first = ln.split(",", 1)[0]
        dt = _parse_dt(first)
        if dt is not None:
            return dt
    return None


def load_5min_csv_bars(path: str) -> Dict[str, dict]:
    """Load existing OHLCV rows keyed by _bar_key for merge."""
    out: Dict[str, dict] = {}
    if not os.path.isfile(path):
        return out
    try:
        with open(path, newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                return out
            colmap = {name.lower(): name for name in r.fieldnames}
            date_col = colmap.get("date")
            if not date_col:
                return out
            o_col = colmap.get("open")
            h_col = colmap.get("high")
            l_col = colmap.get("low")
            c_col = colmap.get("close")
            v_col = colmap.get("volume")
            if not all([o_col, h_col, l_col, c_col]):
                return out
            for row in r:
                dt = _parse_dt(row.get(date_col, ""))
                if dt is None:
                    continue
                try:
                    o = float(row[o_col])
                    h = float(row[h_col])
                    lo = float(row[l_col])
                    cl = float(row[c_col])
                    vol = float(row.get(v_col, 0) or 0) if v_col else 0.0
                except (KeyError, ValueError):
                    continue
                k = _bar_key(dt)
                if k:
                    out[k] = {"date": dt, "open": o, "high": h, "low": lo, "close": cl, "volume": int(vol)}
    except OSError:
        return out
    return out


def get_index_instruments(kite):
    instruments = kite.instruments("NSE")
    return [
        {"instrument_token": i["instrument_token"], "tradingsymbol": i["tradingsymbol"]}
        for i in instruments
        if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"
    ]


def _is_token_error(e: Exception) -> bool:
    name = type(e).__name__
    msg = str(e)
    return (
        "TokenException" in name
        or "Incorrect `api_key` or `access_token`" in msg
        or "Incorrect api_key or access_token" in msg
        or "access_token" in msg
    )


def _is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "too many" in msg or "429" in msg or "rate limit" in msg


def relogin_once() -> bool:
    """Refresh Zerodha session (writes jtrader .zsession)."""
    try:
        import test_login

        return test_login.main() == 0
    except Exception:
        try:
            from fetch_code import test_login as tl

            return tl.main() == 0
        except Exception:
            return False


def _write_5min_csv(out_path: str, sorted_candles: list) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for c in sorted_candles:
            w.writerow(
                [
                    c.get("date"),
                    c.get("open"),
                    c.get("high"),
                    c.get("low"),
                    c.get("close"),
                    c.get("volume", 0),
                ]
            )


def _historical_5min_chunk(kite, instrument_token, current_start, current_end, max_relogin=2):
    """One historical_data call; on token error re-login; on rate limit retry immediately."""
    token_retries = 0
    rate_strikes = 0
    max_rate_strikes = 25
    while True:
        try:
            return kite.historical_data(
                instrument_token,
                current_start,
                current_end,
                interval="5minute",
            )
        except Exception as e:
            if _is_token_error(e) and token_retries < max_relogin:
                print("\n  Token/session expired. Re-logging and retrying chunk...", flush=True)
                if not relogin_once():
                    raise RuntimeError(
                        "Re-login failed. Run: .venv/bin/python -u test_login.py"
                    ) from e
                kite.set_access_token()
                token_retries += 1
                continue
            if _is_rate_limit_error(e) and rate_strikes < max_rate_strikes:
                print("\n  Rate limited; retrying chunk immediately...", flush=True)
                rate_strikes += 1
                continue
            raise


def fetch_5min_for_instrument(kite, instrument_token, from_date, to_date):
    all_candles = []
    current_start = from_date
    while current_start <= to_date:
        current_end = min(current_start + timedelta(days=CHUNK_DAYS), to_date)
        chunk = _historical_5min_chunk(kite, instrument_token, current_start, current_end)
        if chunk:
            all_candles.extend(chunk)
        current_start = current_end + timedelta(days=1)
    return all_candles


def fetch_one_index(
    kite,
    instrument_token,
    tradingsymbol,
    from_date,
    to_date,
    out_dir,
    resume: bool = True,
):
    name = slug(tradingsymbol)
    out_path = os.path.join(out_dir, f"{name}_5min.csv")

    by_bar: Dict[str, dict] = {}
    effective_from = from_date
    if resume and os.path.isfile(out_path):
        by_bar = load_5min_csv_bars(out_path)
        last_dt = last_csv_datetime(out_path)
        if last_dt is not None:
            effective_from = max(from_date, last_dt.date())
        if by_bar:
            print(
                f"  Resume: {len(by_bar)} bars on disk, fetch from {effective_from}...",
                flush=True,
            )

    if effective_from > to_date:
        print(
            f"  Up to date — skip ({len(by_bar)} bars, nothing to add before {to_date}).",
            flush=True,
        )
        return len(by_bar)

    period_list = []
    d = effective_from
    while d <= to_date:
        period_end = min(d + timedelta(days=PERIOD_DAYS - 1), to_date)
        period_list.append((d, period_end))
        d = period_end + timedelta(days=1)

    for period_start, period_end in period_list:
        print(f"  {period_start}..{period_end}...", end=" ", flush=True)
        try:
            candles = fetch_5min_for_instrument(kite, instrument_token, period_start, period_end)
        except Exception as e:
            print(f"Error: {e}")
            break
        for c in candles:
            ts = c.get("date")
            if ts is None:
                continue
            k = _bar_key(ts)
            if k:
                by_bar[k] = c
        print(f"{len(candles)}", flush=True)
        if by_bar:
            sorted_candles = [by_bar[k] for k in sorted(by_bar.keys())]
            _write_5min_csv(out_path, sorted_candles)

    if not by_bar:
        return 0
    sorted_candles = [by_bar[k] for k in sorted(by_bar.keys())]
    _write_5min_csv(out_path, sorted_candles)
    print(f"  -> {out_path} ({len(sorted_candles)} candles)")
    return len(sorted_candles)


def main():
    parser = argparse.ArgumentParser(description="Fetch 5min spot data for NSE indices")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all NSE indices. Default: main + sector list only.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for CSVs (default: data/indices/5min)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated tradingsymbols. Default: main+sector list.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Ignored; fetches run sequentially (single process).",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date (default: 2015-09-01).",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore existing CSVs; refetch the full [from-date, to-date] window.",
    )
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    base = str(REPO_ROOT)
    out_dir = args.output_dir or os.path.join(base, "data", "indices", "5min")
    os.makedirs(out_dir, exist_ok=True)
    to_date = datetime.now().date()
    if args.to_date:
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
    from_date = DEFAULT_START_DATE
    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()

    index_list = get_index_instruments(kite)
    symbol_to_token = {i["tradingsymbol"]: i["instrument_token"] for i in index_list}

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        symbols = [s for s in symbols if s in symbol_to_token]
        missing = set(s.strip() for s in args.symbols.split(",")) - set(symbol_to_token.keys())
        if missing:
            print("Note: symbols not found (skipped):", missing)
    elif args.all:
        symbols = sorted(symbol_to_token.keys())
    else:
        symbols = [s for s in MAIN_AND_SECTOR_SYMBOLS if s in symbol_to_token]
        missing = set(MAIN_AND_SECTOR_SYMBOLS) - set(symbol_to_token.keys())
        if missing:
            print("Note: symbols not in NSE indices list (skipped):", missing)

    if args.workers != 1:
        print("Note: --workers is ignored; 5min fetch runs sequentially.\n")

    resume = not args.no_resume
    print(
        f"Fetching 5min data for {len(symbols)} indices from {from_date} to {to_date} "
        f"(sequential, ~{CHUNK_DAYS}d per request; "
        f"{'resume/merge existing files' if resume else 'full refetch (--no-resume)'}).\n"
    )

    for sym in symbols:
        token = symbol_to_token[sym]
        print(f"{sym} (token {token})")
        fetch_one_index(kite, token, sym, from_date, to_date, out_dir, resume=resume)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
