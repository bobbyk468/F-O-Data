#!/usr/bin/env python3
"""
Ensure data/nifty50/15min and data/nifty50/eod each have 50 files (one per Nifty 50 constituent).
If any are missing, fetch 15min and/or EOD for those symbols only.
"""
import os
import re
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
NIFTY50_FILE = os.path.join(REPO_ROOT, "config", "nifty50_symbols.txt")


def slug_15min(symbol: str) -> str:
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    return s.lower() or "eq"


def slug_eod(symbol: str) -> str:
    s = symbol.strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Z0-9_\-]", "", s)
    return s.lower() or "eq"


def load_nifty50():
    if not os.path.isfile(NIFTY50_FILE):
        raise SystemExit(f"Missing {NIFTY50_FILE}")
    out = []
    with open(NIFTY50_FILE) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                out.append(line)
    return out


def main():
    parser = argparse.ArgumentParser(description="Ensure Nifty50 folder has 50 constituents; fetch missing.")
    parser.add_argument("--dry-run", action="store_true", help="Only report missing, do not fetch.")
    parser.add_argument("--fetch-15min", action="store_true", help="Fetch 15min for missing symbols.")
    parser.add_argument("--fetch-eod", action="store_true", help="Fetch EOD for missing symbols (full history).")
    args = parser.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    dir_15min = os.path.join(base, "data", "nifty50", "15min")
    dir_eod = os.path.join(base, "data", "nifty50", "eod")

    symbols = load_nifty50()
    print(f"Nifty 50 list: {len(symbols)} symbols")

    # Which have 15min file?
    have_15min = set()
    if os.path.isdir(dir_15min):
        for f in os.listdir(dir_15min):
            if f.endswith("_15min.csv"):
                have_15min.add(f.replace("_15min.csv", ""))

    # Which have EOD file?
    have_eod = set()
    if os.path.isdir(dir_eod):
        for f in os.listdir(dir_eod):
            if f.endswith("_eod.csv"):
                have_eod.add(f.replace("_eod.csv", ""))

    missing_15min = [s for s in symbols if slug_15min(s) not in have_15min]
    missing_eod = [s for s in symbols if slug_eod(s) not in have_eod]

    print(f"  Have 15min: {len(have_15min)}  (need 50)")
    print(f"  Have EOD:   {len(have_eod)}  (need 50)")
    if missing_15min:
        print(f"  Missing 15min: {missing_15min}")
    if missing_eod:
        print(f"  Missing EOD:   {missing_eod}")

    if not missing_15min and not missing_eod:
        print("\nNifty50 folder has data for all 50 constituents.")
        return 0

    if args.dry_run:
        print("\nDry-run: not fetching. Use --fetch-15min and/or --fetch-eod to fetch missing.")
        return 0

    if args.fetch_15min and missing_15min:
        import subprocess
        cmd = [
            sys.executable,
            os.path.join(base, "fetch_code", "fetch_fo_stocks_15min.py"),
            "--symbols",
            ",".join(missing_15min),
        ]
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, check=True)
        print("15min fetch done for:", missing_15min)

    if args.fetch_eod and missing_eod:
        from fetch_code.fetch_eod_90d import get_fo_symbol_to_token, run_batch_full, _load_nifty50_symbols
        from jugaad_trader import Zerodha
        kite = Zerodha()
        kite.set_access_token()
        fo_map = get_fo_symbol_to_token(kite)
        sub_map = {s: fo_map[s] for s in missing_eod if s in fo_map}
        if sub_map:
            run_batch_full(kite, sub_map, os.path.join(base, "data"), 2, 0.0035, "fo", nifty50_symbols=set(symbols))
            print("EOD fetch completed for:", list(sub_map.keys()))
        else:
            print("None of the missing EOD symbols are in F&O list:", missing_eod)

    return 0


if __name__ == "__main__":
    sys.exit(main())
