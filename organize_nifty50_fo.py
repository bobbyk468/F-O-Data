#!/usr/bin/env python3
"""
Organize F&O data into Nifty50 and other folders, each with 15min/ and eod/ inside.

Target layout:
  data/nifty50/15min/   — Nifty 50 constituent 15min CSVs
  data/nifty50/eod/    — Nifty 50 constituent EOD CSVs
  data/other/15min/    — Other F&O 15min
  data/other/eod/      — Other F&O EOD

If you have legacy data in data/fo_stocks/ or data/eod_data/fo_stocks/ (with or
without nifty50/other subdirs), run this once to move files into the above layout.
Uses config/nifty50_symbols.txt from repo root.
"""
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(REPO_ROOT, "config")
NIFTY50_FILE = os.path.join(CONFIG_DIR, "nifty50_symbols.txt")


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


def load_nifty50_symbols():
    path = NIFTY50_FILE
    if not os.path.isfile(path):
        raise SystemExit(f"Nifty 50 list not found: {path}")
    symbols = []
    with open(path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                symbols.append(line)
    return symbols


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base, "data")
    nifty50 = set(load_nifty50_symbols())
    slugs_15min = {slug_15min(s) for s in nifty50}
    slugs_eod = {slug_eod(s) for s in nifty50}

    # Source dirs (legacy or current)
    fo_stocks = os.path.join(data_dir, "fo_stocks")
    eod_fo = os.path.join(data_dir, "eod_data", "fo_stocks")

    # 15min: move from fo_stocks[/nifty50|/other] -> data/nifty50/15min & data/other/15min
    for parent in [fo_stocks, os.path.join(fo_stocks, "nifty50"), os.path.join(fo_stocks, "other")]:
        if not os.path.isdir(parent):
            continue
        suffix = "_15min.csv"
        for f in os.listdir(parent):
            if not f.endswith(suffix):
                continue
            base_name = f[: -len(suffix)]
            src = os.path.join(parent, f)
            if not os.path.isfile(src):
                continue
            sub = "nifty50" if base_name in slugs_15min else "other"
            dest_dir = os.path.join(data_dir, sub, "15min")
            os.makedirs(dest_dir, exist_ok=True)
            os.rename(src, os.path.join(dest_dir, f))
            print(f"  15min: {f} -> {sub}/15min/")

    # EOD: move from eod_data/fo_stocks[/nifty50|/other] -> data/nifty50/eod & data/other/eod
    for parent in [eod_fo, os.path.join(eod_fo, "nifty50"), os.path.join(eod_fo, "other")]:
        if not os.path.isdir(parent):
            continue
        for suffix in ("_eod.csv", "_eod_90d.csv"):
            for f in os.listdir(parent):
                if not f.endswith(suffix):
                    continue
                base_name = f[: -len(suffix)]
                src = os.path.join(parent, f)
                if not os.path.isfile(src):
                    continue
                sub = "nifty50" if base_name in slugs_eod else "other"
                dest_dir = os.path.join(data_dir, sub, "eod")
                os.makedirs(dest_dir, exist_ok=True)
                os.rename(src, os.path.join(dest_dir, f))
                print(f"  eod: {f} -> {sub}/eod/")

    print("Done.")


if __name__ == "__main__":
    main()
