#!/usr/bin/env python3
"""
Run data validation (15min + EOD checks) for nifty50, other, and/or indices.
Same checks as check_nifty50_15min_data.py and check_nifty50_eod_data.py;
reports are written to logs/<folder>_15min_data_check.txt and logs/<folder>_eod_data_check.txt.
"""
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")

FOLDERS = ("nifty50", "other", "indices")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Validate 15min and EOD data in data/nifty50, data/other, data/indices.")
    parser.add_argument(
        "--folder",
        choices=FOLDERS + ("all",),
        default="all",
        help="Which folder to validate (default: all).",
    )
    parser.add_argument(
        "--no-15min",
        action="store_true",
        help="Skip 15min validation.",
    )
    parser.add_argument(
        "--no-eod",
        action="store_true",
        help="Skip EOD validation.",
    )
    args = parser.parse_args()

    os.makedirs(LOGS_DIR, exist_ok=True)
    folders = FOLDERS if args.folder == "all" else (args.folder,)
    python = sys.executable
    check_15min = os.path.join(SCRIPT_DIR, "check_nifty50_15min_data.py")
    check_eod = os.path.join(SCRIPT_DIR, "check_nifty50_eod_data.py")

    for folder in folders:
        base = os.path.join(DATA_DIR, folder)
        if not os.path.isdir(base):
            print(f"Skipping {folder}: not a directory: {base}")
            continue
        print(f"\n--- Validating {folder} ---")
        if not args.no_15min:
            dir_15min = os.path.join(base, "15min")
            if os.path.isdir(dir_15min):
                out_15 = os.path.join(LOGS_DIR, f"{folder}_15min_data_check.txt")
                subprocess.run(
                    [python, check_15min, "--data-dir", dir_15min, "-o", out_15],
                    cwd=SCRIPT_DIR,
                )
                print(f"  15min report: {out_15}")
            else:
                print(f"  No 15min dir: {dir_15min}")
        if not args.no_eod:
            dir_eod = os.path.join(base, "eod")
            if os.path.isdir(dir_eod):
                out_eod = os.path.join(LOGS_DIR, f"{folder}_eod_data_check.txt")
                subprocess.run(
                    [python, check_eod, "--data-dir", dir_eod, "-o", out_eod],
                    cwd=SCRIPT_DIR,
                )
                print(f"  EOD report:   {out_eod}")
            else:
                print(f"  No eod dir: {dir_eod}")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
