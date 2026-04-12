#!/usr/bin/env python3
"""
Fetch 1-minute spot (index) data for NSE indices: main indices + sector indices.

Kite API returns at most 100 candles per historical_data call. For 1-minute bars,
we use ~99 minutes per request; we step in IST within each trading day.

Default output: data/indices/1min/<slug>_1min.csv

Note: Full history (2015 -> today) for 25 indices is a very large number of API
calls. Data is requested in outer batches of --period-days (default 30 = ~1 month),
then merged. Use --resume to continue from existing CSVs. Weekends are skipped.
Default --delay is per worker; use --delay-scale workers if you hit limits.

Supports --workers N for parallel fetch (one index per worker).
"""
import os
import re
import sys
import csv
import time as time_module
import argparse
from datetime import datetime, timedelta, date, time as dt_time
from multiprocessing import Pool
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

IST = ZoneInfo("Asia/Kolkata")

# Earliest calendar start for backfills; Zerodha often has index minute data from ~Sep 2015.
DEFAULT_START_DATE = date(2015, 1, 1)
# One-minute bars: Kite allows at most 100 candles per request.
CHUNK_MINUTES = 99
SESSION_OPEN = dt_time(9, 15)
SESSION_CLOSE = dt_time(15, 30)
# Default outer batch size (calendar days per progress chunk). 30 days = fewer checkpoints, faster bulk backfill.
DEFAULT_PERIOD_DAYS = 30

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


def get_index_instruments(kite):
    instruments = kite.instruments("NSE")
    return [
        {"instrument_token": i["instrument_token"], "tradingsymbol": i["tradingsymbol"]}
        for i in instruments
        if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"
    ]


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    start = datetime.combine(d, SESSION_OPEN, tzinfo=IST)
    end = datetime.combine(d, SESSION_CLOSE, tzinfo=IST)
    return start, end


def _ensure_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def _parse_dt_cell(raw: str) -> datetime:
    raw = (raw or "").strip().strip('"')
    if not raw:
        raise ValueError("empty date cell")
    if "T" in raw and raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=IST)
    else:
        dt = _ensure_ist(dt)
    return dt


def _next_fetch_start_after(last_ts: datetime) -> datetime:
    """First 1-minute bar to request after last_ts (IST session bounds)."""
    last_ts = _ensure_ist(last_ts)
    nxt = last_ts + timedelta(minutes=1)
    day_end = datetime.combine(last_ts.date(), SESSION_CLOSE, tzinfo=IST)
    if nxt > day_end:
        d = last_ts.date() + timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return datetime.combine(d, SESSION_OPEN, tzinfo=IST)
    day_start = datetime.combine(nxt.date(), SESSION_OPEN, tzinfo=IST)
    if nxt < day_start:
        return day_start
    return nxt


def load_existing_1min_csv(path: str) -> dict:
    """Load CSV into {datetime: row dict} keyed by candle time (IST)."""
    by_ts = {}
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return by_ts
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "date" not in r.fieldnames:
            return by_ts
        for row in r:
            cell = row.get("date")
            if not cell:
                continue
            try:
                dt = _parse_dt_cell(cell)
            except (ValueError, TypeError):
                continue
            dt = _ensure_ist(dt)
            by_ts[dt] = {
                "date": dt,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume", 0),
            }
    return by_ts


def fetch_1min_for_instrument(kite, instrument_token, start_dt: datetime, end_date: date, delay_sec=0.05):
    """Fetch 1-minute data from first bar at/after start_dt through end_date (inclusive, IST)."""
    all_candles = []
    start_dt = _ensure_ist(start_dt)
    d = start_dt.date()
    while d <= end_date:
        # Skip Sat/Sun — no NSE session; saves ~28% of calendar iterations.
        if d.weekday() >= 5:
            d += timedelta(days=1)
            continue
        day_start, day_end = _day_bounds(d)
        t = day_start if d > start_dt.date() else max(day_start, start_dt)
        while t <= day_end:
            t_end = min(t + timedelta(minutes=CHUNK_MINUTES), day_end)
            try:
                chunk = kite.historical_data(
                    instrument_token,
                    t,
                    t_end,
                    interval="minute",
                )
            except Exception as e:
                chunk = []
                if "TokenException" in str(type(e).__name__) or "Invalid" in str(e):
                    raise
            if chunk:
                all_candles.extend(chunk)
            t = t_end + timedelta(minutes=1)
            time_module.sleep(delay_sec)
        d += timedelta(days=1)
    return all_candles


def _merge_candles_into_by_ts(by_ts: dict, candles: list) -> None:
    for c in candles:
        ts = c.get("date")
        if ts is None:
            continue
        if isinstance(ts, datetime):
            by_ts[_ensure_ist(ts)] = c


def _write_1min_csv(out_path: str, by_ts: dict) -> None:
    sorted_candles = [by_ts[k] for k in sorted(by_ts)]
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for c in sorted_candles:
            w.writerow([
                c.get("date"),
                c.get("open"),
                c.get("high"),
                c.get("low"),
                c.get("close"),
                c.get("volume", 0),
            ])


def fetch_one_index(
    kite,
    instrument_token,
    tradingsymbol,
    from_date,
    to_date,
    out_dir,
    delay_sec=0.05,
    resume=False,
    period_days: int = DEFAULT_PERIOD_DAYS,
):
    name = slug(tradingsymbol)
    out_path = os.path.join(out_dir, f"{name}_1min.csv")

    by_ts = {}
    if resume and os.path.isfile(out_path):
        by_ts = load_existing_1min_csv(out_path)
        if by_ts:
            print(f"  {tradingsymbol}: resume, loaded {len(by_ts)} existing rows", flush=True)

    if resume and by_ts:
        start_dt = _next_fetch_start_after(max(by_ts.keys()))
    else:
        start_dt = datetime.combine(from_date, SESSION_OPEN, tzinfo=IST)

    floor = datetime.combine(from_date, SESSION_OPEN, tzinfo=IST)
    if start_dt < floor:
        start_dt = floor

    if start_dt.date() > to_date:
        print(f"  {tradingsymbol}: already up to date through {to_date}", flush=True)
        if by_ts:
            _write_1min_csv(out_path, by_ts)
            print(f"  -> {out_path} ({len(by_ts)} candles)", flush=True)
        return len(by_ts)

    period_list = []
    d = start_dt.date()
    while d <= to_date:
        period_end = min(d + timedelta(days=period_days - 1), to_date)
        period_list.append((d, period_end))
        d = period_end + timedelta(days=1)

    for period_start, period_end in period_list:
        print(f"  {period_start}..{period_end}...", end=" ", flush=True)
        chunk_start = (
            start_dt
            if period_start == start_dt.date()
            else datetime.combine(period_start, SESSION_OPEN, tzinfo=IST)
        )
        try:
            candles = fetch_1min_for_instrument(
                kite, instrument_token, chunk_start, period_end, delay_sec
            )
        except Exception as e:
            print(f"Error: {e}")
            break
        _merge_candles_into_by_ts(by_ts, candles)
        print(f"{len(candles)} new", flush=True)
        if by_ts:
            _write_1min_csv(out_path, by_ts)

    if not by_ts:
        return 0
    print(f"  -> {out_path} ({len(by_ts)} candles)")
    return len(by_ts)


def _worker_fetch_one(args):
    instrument_token, tradingsymbol, from_date, to_date, out_dir, delay_sec, resume, period_days = args
    if isinstance(from_date, (list, tuple)):
        from_date = date(from_date[0], from_date[1], from_date[2])
    if isinstance(to_date, (list, tuple)):
        to_date = date(to_date[0], to_date[1], to_date[2])
    from jugaad_trader import Zerodha
    kite = Zerodha()
    kite.set_access_token()
    return fetch_one_index(
        kite,
        instrument_token,
        tradingsymbol,
        from_date,
        to_date,
        out_dir,
        delay_sec,
        resume=resume,
        period_days=period_days,
    )


def main():
    parser = argparse.ArgumentParser(description="Fetch 1-minute spot data for NSE indices")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all NSE indices. Default: main + sector list only.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for CSVs (default: data/indices/1min)",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated tradingsymbols. Default: main+sector list.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Parallel indices at once (default 4, max 8).",
    )
    parser.add_argument(
        "--from-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date (default: 2015-01-01).",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.03,
        help="Sleep between API calls per worker in seconds (default 0.03).",
    )
    parser.add_argument(
        "--delay-scale",
        choices=("none", "workers"),
        default="none",
        help="none: use --delay as-is (faster). workers: multiply delay by worker count "
        "(older conservative throttle; slower).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Load existing *_1min.csv files and fetch only after the last timestamp (merge).",
    )
    parser.add_argument(
        "--period-days",
        type=int,
        default=DEFAULT_PERIOD_DAYS,
        metavar="N",
        help="Calendar days per outer batch (default: 30). Use 7 for weekly or 14 for biweekly checkpoints.",
    )
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    base = os.path.dirname(os.path.abspath(__file__))
    out_dir = args.output_dir or os.path.join(base, "data", "indices", "1min")
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

    workers = max(1, min(args.workers, 8))
    if args.delay_scale == "workers":
        delay_sec = round(float(args.delay) * workers, 4)
    else:
        delay_sec = float(args.delay)

    period_days = max(1, min(int(args.period_days), 366))

    print(
        f"Fetching 1min data for {len(symbols)} indices from {from_date} to {to_date} "
        f"(workers={workers}, delay={delay_sec}s per call, delay_scale={args.delay_scale}, "
        f"resume={args.resume}, period_days={period_days}).\n"
    )
    if not args.resume:
        print("Tip: use --resume to continue from existing CSVs without re-downloading from --from-date.\n")

    from_date_tuple = (from_date.year, from_date.month, from_date.day)
    to_date_tuple = (to_date.year, to_date.month, to_date.day)

    if workers == 1:
        for sym in symbols:
            token = symbol_to_token[sym]
            print(f"{sym} (token {token})")
            fetch_one_index(
                kite,
                token,
                sym,
                from_date,
                to_date,
                out_dir,
                delay_sec,
                resume=args.resume,
                period_days=period_days,
            )
    else:
        task_args = [
            (
                symbol_to_token[sym],
                sym,
                from_date_tuple,
                to_date_tuple,
                out_dir,
                delay_sec,
                args.resume,
                period_days,
            )
            for sym in symbols
        ]
        with Pool(workers) as pool:
            results = pool.map(_worker_fetch_one, task_args)
        for sym, count in zip(symbols, results):
            print(f"  {sym}: {count} candles")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
