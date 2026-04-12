#!/usr/bin/env python3
"""
Align EOD F&O data with 15min F&O data: find symbols that have fo_stocks/*_15min.csv
but lack eod_data/fo_stocks/*_eod.csv, then optionally fetch EOD for them only.

15min filenames use slug that strips hyphens (e.g. bajajauto_15min.csv).
EOD filenames use slug that keeps hyphens (e.g. bajaj-auto_eod.csv).
So we resolve by symbol via the F&O list from the API.
"""
import os
import re
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def slug_15min(symbol: str) -> str:
    """Same as fetch_all_indices_15min: removes hyphens -> bajajauto."""
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    return s.lower() or "eq"


def slug_eod(symbol: str) -> str:
    """Same as fetch_eod_90d: keeps hyphens -> bajaj-auto."""
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_\-]", "", s)
    return s.lower() or "eq"


def main():
    parser = argparse.ArgumentParser(
        description="Align EOD fo_stocks with 15min fo_stocks: list or fetch missing EOD."
    )
    parser.add_argument(
        "--fo-stocks-dir",
        default=None,
        help="Base data dir; 15min in <dir>/nifty50/15min & <dir>/other/15min (default: data/).",
    )
    parser.add_argument(
        "--eod-fo-dir",
        default=None,
        help="Base dir for EOD; eod in <dir>/nifty50/eod & <dir>/other/eod (default: same as fo-stocks-dir).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print missing symbols and counts; do not fetch.",
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Fetch full EOD history for missing symbols (default: last 90 days only).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Workers for EOD fetch (default 4).",
    )
    parser.add_argument(
        "--write-symbols",
        metavar="FILE",
        default=None,
        help="Write missing symbols (one per line) to FILE. Use with --dry-run and then: "
        "fetch_eod_90d.py --only fo --full-history --symbols $(cat FILE | paste -sd,).",
    )
    args = parser.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    data_dir = args.fo_stocks_dir or os.path.join(base, "data")  # 15min under data/nifty50/15min & data/other/15min
    eod_fo_dir = args.eod_fo_dir or data_dir  # EOD under data/nifty50/eod & data/other/eod

    from fetch_code.fetch_eod_90d import get_fo_symbol_to_token, run_batch_full, run_batch, _load_nifty50_symbols
    from datetime import datetime, timedelta

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()

    fo_map = get_fo_symbol_to_token(kite)
    if not fo_map:
        print("No F&O symbols from API.")
        return 1

    # Which symbols have 15min file? (check data/nifty50/15min & data/other/15min)
    has_15min = set()
    for sym in fo_map:
        name = slug_15min(sym)
        for sub in ("nifty50", "other"):
            path = os.path.join(data_dir, sub, "15min", f"{name}_15min.csv")
            if os.path.isfile(path):
                has_15min.add(sym)
                break

    # Which symbols have EOD file? (check data/nifty50/eod & data/other/eod)
    has_eod = set()
    for sym in fo_map:
        name = slug_eod(sym)
        for sub in ("nifty50", "other"):
            path = os.path.join(eod_fo_dir, sub, "eod", f"{name}_eod.csv")
            if os.path.isfile(path):
                has_eod.add(sym)
                break

    missing_eod = has_15min - has_eod
    only_eod = has_eod - has_15min  # have EOD but no 15min (e.g. old run with different set)

    print(f"fo_stocks (15min): {len(has_15min)} symbols")
    print(f"eod_data/fo_stocks: {len(has_eod)} symbols")
    print(f"Missing EOD (have 15min, no EOD): {len(missing_eod)}")
    if only_eod:
        print(f"Only in EOD (no 15min): {len(only_eod)}")

    if not missing_eod:
        print("\nAlready aligned: every symbol with 15min data has EOD.")
        return 0

    print("\nMissing EOD symbols:", sorted(missing_eod))

    if args.write_symbols:
        with open(args.write_symbols, "w") as f:
            for s in sorted(missing_eod):
                f.write(s + "\n")
        print(f"Wrote {len(missing_eod)} symbols to {args.write_symbols}")

    if args.dry_run:
        print("\nDry-run: not fetching. Run without --dry-run to fetch EOD for these.")
        return 0

    # Fetch EOD for missing only (into nifty50/ or other/ to match fo_stocks layout)
    sub_map = {s: fo_map[s] for s in sorted(missing_eod)}
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=90)
    delay_sec = 0.0035
    fo_nifty50 = _load_nifty50_symbols()

    if args.full_history:
        print(f"\nFetching full EOD history for {len(sub_map)} symbols...")
        run_batch_full(kite, sub_map, eod_fo_dir, args.workers, delay_sec, "fo", nifty50_symbols=fo_nifty50 or None)
    else:
        print(f"\nFetching last 90 days EOD for {len(sub_map)} symbols...")
        run_batch(kite, sub_map, from_date, to_date, eod_fo_dir, args.workers, delay_sec, "fo", nifty50_symbols=fo_nifty50 or None)

    print("\nDone. Re-run with --dry-run to verify alignment.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
