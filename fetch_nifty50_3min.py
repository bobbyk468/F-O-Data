#!/usr/bin/env python3
"""
Fetch 3-minute OHLC data for Nifty 50 for max possible range (2015-09-01 to today).
Uses chunked requests (1-day chunks). 3min = ~125 bars/day.
Override with --from-date / --to-date if needed.
"""
import os
import sys
import csv
import time
import argparse
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Nifty 50 index instrument token (NSE)
NIFTY50_TOKEN = 840269

# Max possible range: Zerodha index data from 2015-09-01
DEFAULT_START_DATE = date(2015, 9, 1)
# ~125 three-min bars per trading day; use 1-day chunks
CHUNK_DAYS = 1


def main():
    parser = argparse.ArgumentParser(description="Fetch Nifty 50 3min data (max range by default)")
    parser.add_argument("--from-date", metavar="YYYY-MM-DD", help="Start date (default: 2015-09-01)")
    parser.add_argument("--to-date",   metavar="YYYY-MM-DD", help="End date (default: today)")
    args = parser.parse_args()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    to_date   = datetime.now().date()
    from_date = DEFAULT_START_DATE
    if args.to_date:
        to_date   = datetime.strptime(args.to_date,   "%Y-%m-%d").date()
    if args.from_date:
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()

    all_candles = []

    current_start = from_date
    while current_start <= to_date:
        current_end = min(current_start + timedelta(days=CHUNK_DAYS), to_date)
        if current_end < current_start:
            break
        print(f"Fetching {current_start} to {current_end}...", end=" ", flush=True)
        try:
            chunk = kite.historical_data(
                NIFTY50_TOKEN,
                current_start,
                current_end,
                interval="3minute",
            )
        except Exception as e:
            print(f"Error: {e}")
            chunk = []
        if chunk:
            all_candles.extend(chunk)
            print(f"got {len(chunk)} candles (total {len(all_candles)})")
        else:
            print("no data")
        current_start = current_end + timedelta(days=1)
        time.sleep(0.4)  # stay under ~3 req/s

    if not all_candles:
        print("No data returned. Check date range and session.")
        return 1

    # Deduplicate by timestamp, sort ascending
    by_ts = {}
    for c in all_candles:
        ts = c.get("date")
        if ts is not None:
            by_ts[ts] = c
    sorted_candles = [by_ts[k] for k in sorted(by_ts)]

    base    = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base, "data", "indices", "3min")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "nifty_50_3min.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for candle in sorted_candles:
            w.writerow([
                candle.get("date"),
                candle.get("open"),
                candle.get("high"),
                candle.get("low"),
                candle.get("close"),
                candle.get("volume", 0),
            ])

    first_ts = sorted_candles[0].get("date")  if sorted_candles else None
    last_ts  = sorted_candles[-1].get("date") if sorted_candles else None
    print(f"\nSaved {len(sorted_candles)} candles to {out_path}")
    print(f"Date range: {first_ts} to {last_ts}  (requested: {from_date} to {to_date})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
