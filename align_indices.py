#!/usr/bin/env python3
"""
Align indices 15min and EOD under data/indices/: ensure every index that has
data/indices/15min/*_15min.csv also has data/indices/eod/*_eod.csv (and optionally
vice versa). Reports mismatches and can fetch missing EOD or 15min.
"""
import os
import re
import sys
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def slug(symbol: str) -> str:
    """Same as fetch_all_indices_15min: NIFTY 50 -> nifty_50."""
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    return s.lower() or "index"


def main():
    parser = argparse.ArgumentParser(
        description="Align data/indices/15min and data/indices/eod; report or fetch missing."
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Base data dir (default: repo data/).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing; do not fetch.",
    )
    parser.add_argument(
        "--fetch-missing-eod",
        action="store_true",
        help="Fetch EOD for indices that have 15min but no EOD.",
    )
    parser.add_argument(
        "--fetch-missing-15min",
        action="store_true",
        help="Fetch 15min for indices that have EOD but no 15min.",
    )
    parser.add_argument(
        "--full-history-eod",
        action="store_true",
        help="With --fetch-missing-eod, fetch full EOD history (default: last 90 days).",
    )
    args = parser.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    data_dir = args.data_dir or os.path.join(base, "data")
    dir_15min = os.path.join(data_dir, "indices", "15min")
    dir_eod = os.path.join(data_dir, "indices", "eod")

    # Known index symbols -> slug (for mapping file base back to symbol)
    from fetch_eod_90d import MAIN_AND_SECTOR_SYMBOLS

    symbol_to_slug = {s: slug(s) for s in MAIN_AND_SECTOR_SYMBOLS}
    slug_to_symbol = {v: k for k, v in symbol_to_slug.items()}

    # Bases present in each folder
    has_15min = set()
    if os.path.isdir(dir_15min):
        for f in os.listdir(dir_15min):
            if f.endswith("_15min.csv"):
                has_15min.add(f.replace("_15min.csv", ""))

    has_eod = set()
    if os.path.isdir(dir_eod):
        for f in os.listdir(dir_eod):
            if f.endswith("_eod.csv"):
                has_eod.add(f.replace("_eod.csv", ""))

    missing_eod = has_15min - has_eod
    missing_15min = has_eod - has_15min
    aligned = has_15min & has_eod

    print(f"data/indices/15min: {len(has_15min)} files")
    print(f"data/indices/eod:  {len(has_eod)} files")
    print(f"Aligned (have both): {len(aligned)}")
    print(f"Missing EOD (have 15min, no EOD): {len(missing_eod)}")
    if missing_eod:
        print("  ", sorted(missing_eod))
    print(f"Missing 15min (have EOD, no 15min): {len(missing_15min)}")
    if missing_15min:
        print("  ", sorted(missing_15min))

    if not missing_eod and not missing_15min:
        print("\nIndices are aligned: every file in 15min has a matching EOD and vice versa.")
        return 0

    # Map slugs back to NSE symbols for fetch
    missing_eod_symbols = sorted(slug_to_symbol.get(s, s.upper().replace("_", " ")) for s in missing_eod)
    missing_15min_symbols = sorted(slug_to_symbol.get(s, s.upper().replace("_", " ")) for s in missing_15min)

    if args.dry_run:
        print("\nDry-run: not fetching. Use --fetch-missing-eod or --fetch-missing-15min to fix.")
        return 0

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    if args.fetch_missing_eod and missing_eod_symbols:
        # Resolve to symbols that exist in NSE indices
        from fetch_eod_90d import get_index_symbol_to_token, run_batch_full, run_batch

        index_map = get_index_symbol_to_token(kite)
        to_fetch = [s for s in missing_eod_symbols if s in index_map]
        if to_fetch:
            sub_map = {s: index_map[s] for s in to_fetch}
            if args.full_history_eod:
                run_batch_full(kite, sub_map, dir_eod, 2, 0.0035, "indices")
            else:
                to_date = datetime.now().date()
                from_date = to_date - timedelta(days=90)
                run_batch(kite, sub_map, from_date, to_date, dir_eod, 2, 0.0035, "indices")
            print(f"Fetched EOD for {len(to_fetch)} indices.")
        else:
            print("No missing EOD symbols found in NSE index list.")

    if args.fetch_missing_15min and missing_15min_symbols:
        from fetch_all_indices_15min import (
            get_index_instruments,
            fetch_one_index,
            _worker_fetch_one,
            DEFAULT_START_DATE,
        )
        from multiprocessing import Pool

        index_list = get_index_instruments(kite)
        symbol_to_token = {i["tradingsymbol"]: i["instrument_token"] for i in index_list}
        to_fetch = [s for s in missing_15min_symbols if s in symbol_to_token]
        if to_fetch:
            os.makedirs(dir_15min, exist_ok=True)
            to_date = datetime.now().date()
            from_date = DEFAULT_START_DATE
            for sym in to_fetch:
                token = symbol_to_token[sym]
                fetch_one_index(kite, token, sym, from_date, to_date, dir_15min, 0.0035)
            print(f"Fetched 15min for {len(to_fetch)} indices.")
        else:
            print("No missing 15min symbols found in NSE index list.")

    print("Done. Re-run without --dry-run to verify alignment.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
