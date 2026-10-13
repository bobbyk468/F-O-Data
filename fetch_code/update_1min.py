#!/usr/bin/env python3
"""Incrementally extend the existing 1min files (indices + the stocks that have one) to --to-date.

Re-downloads from each file's last date, merges, and runs the price re-adjustment check
(see adjustment_check.py): if Kite re-adjusted history, the file is refetched in full from its first date.
A file whose fetch fails is left unchanged and reported; exit code is 1 if any file failed.
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import DATA_DIR  # noqa: E402

import argparse
import io
import os
import re
import time
from datetime import date, datetime, timedelta
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from adjustment_check import frames_readjusted, rebase_merge
from kite_retry import FetchError, historical_with_retry
from ohlc_indicators import coerce_datetime_ist

CHUNK_DAYS = 55  # Kite allows 60 days per 1minute request
COLS = ["date", "open", "high", "low", "close", "volume"]


def slug(symbol: str) -> str:
    s = re.sub(r"\s+", "_", symbol.strip().upper())
    return re.sub(r"[^A-Z0-9_]", "", s).lower() or "index"


def fetch_range(kite, token, start: date, end: date, delay: float) -> pd.DataFrame:
    rows, cur = [], start
    while cur <= end:
        ce = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
        for c in historical_with_retry(kite, token, cur, ce, "minute", retries=6, backoff=2.0):
            rows.append([c.get("date"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume", 0)])
        cur = ce + timedelta(days=1)
        time.sleep(delay)
    df = pd.DataFrame(rows, columns=COLS)
    df["date"] = coerce_datetime_ist(df["date"])
    return df.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


TAIL_BYTES = 1_500_000  # ~25k rows (~65 sessions): far more than the one-session overlap that is re-downloaded


def _read_tail(path: Path):
    """(DataFrame of the last complete rows, byte offset where they start, line ending), or None if the file is small."""
    size = path.stat().st_size
    if size < TAIL_BYTES * 2:
        return None
    with open(path, "rb") as f:
        f.seek(size - TAIL_BYTES)
        chunk = f.read()
    eol = b"\r\n" if b"\r\n" in chunk[:8192] else b"\n"
    nl = chunk.find(b"\n")
    if nl < 0:
        return None
    body = chunk[nl + 1:]
    df = pd.read_csv(io.BytesIO(b"date,open,high,low,close,volume\n" + body))
    df["date"] = coerce_datetime_ist(df["date"])
    return df, size - len(body), eol


def update_file(kite, path: Path, token: int, to_date: date, delay: float) -> tuple[str, int]:
    """Fast path: re-download from the last session, compare only the file tail, and splice the merged last
    session(s) in place of the old tail rows (copy + atomic replace, no full parse). Anything unusual
    (re-adjusted prices, small file, any error) falls back to the full read/merge/rewrite."""
    try:
        tail = _read_tail(path)
        if tail is not None:
            old_tail, offset, eol = tail
            last_day = old_tail["date"].max().date()
            new = fetch_range(kite, token, last_day, to_date, delay)
            flagged, _, _ = frames_readjusted(old_tail, new)
            first_idx = old_tail.index[old_tail["date"].dt.date >= last_day][0]
            if not flagged and first_idx > 0:
                keep = old_tail.iloc[first_idx:]
                merged = pd.concat([keep, new], ignore_index=True).sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
                if len(merged) >= len(keep):
                    # locate the cut (start of the first kept row) by skipping first_idx lines from the tail start
                    with open(path, "rb") as f:
                        f.seek(offset)
                        for _ in range(first_idx):
                            f.readline()
                        cut = f.tell()
                    tmp = path.with_suffix(".csv.tmp")
                    with open(path, "rb") as src, open(tmp, "wb") as dst:
                        remaining = cut
                        while remaining:
                            buf = src.read(min(8 * 1024 * 1024, remaining))
                            dst.write(buf)
                            remaining -= len(buf)
                        dst.write(merged[COLS].to_csv(index=False, header=False, lineterminator=eol.decode()).encode())
                    os.replace(tmp, path)
                    return path.name, len(merged) - len(keep)
    except Exception as e:  # fall back to the safe full path
        print(f"[fast path skipped for {path.stem}: {type(e).__name__}: {str(e)[:80]}]", flush=True)
    return _update_file_full(kite, path, token, to_date, delay)


def _update_file_full(kite, path: Path, token: int, to_date: date, delay: float) -> tuple[str, int]:
    old = pd.read_csv(path)
    old["date"] = coerce_datetime_ist(old["date"])
    last, first = old["date"].max().date(), old["date"].min().date()
    new = fetch_range(kite, token, last, to_date, delay)
    flagged, ratio, n_ov = frames_readjusted(old, new)
    if flagged:
        print(f"[ADJUSTED] {path.stem}: prices re-adjusted (new/old close = {ratio:.4f} over {n_ov} bars) -> refetching full history", flush=True)
        full = rebase_merge(old, fetch_range(kite, token, first, to_date, delay))
    else:
        full = pd.concat([old, new], ignore_index=True)
        full = full.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    if len(full) < len(old):
        raise FetchError(f"{path.name}: result has fewer rows ({len(full)}) than existing ({len(old)})")
    tmp = path.with_suffix(".csv.tmp")
    full[COLS].to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path.name, len(full) - len(old)


_KITE = None


def _work(job):
    """Worker process: own session, one file. Returns (key, ok, detail)."""
    global _KITE
    key, path, token, to_date, delay = job
    try:
        if _KITE is None:
            from jugaad_trader import Zerodha

            _KITE = Zerodha()
            _KITE.set_access_token()
        _, added = update_file(_KITE, Path(path), token, to_date, delay)
        return key, True, f"+{added} rows"
    except Exception as e:  # leave the file untouched, keep going
        return key, False, f"{type(e).__name__}: {str(e)[:160]} (file left unchanged)"


def main() -> int:
    ap = argparse.ArgumentParser(description="Extend existing 1min CSVs to --to-date")
    ap.add_argument("--to-date", default=None, metavar="YYYY-MM-DD", help="Default: today (IST).")
    ap.add_argument("--delay", type=float, default=0.2)
    ap.add_argument("--workers", type=int, default=1, help="Parallel worker processes (default 1; 4 is safe for Kite's rate limit).")
    args = ap.parse_args()
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else datetime.now().date()

    from jugaad_trader import Zerodha

    kite = Zerodha()
    kite.set_access_token()
    nse = kite.instruments("NSE")
    idx = {slug(i["tradingsymbol"]): i["instrument_token"] for i in nse if i.get("segment") == "INDICES" and i.get("exchange") == "NSE"}
    eq = {slug(i["tradingsymbol"]): i["instrument_token"] for i in nse if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"}

    files = sorted(DATA_DIR.glob("*/1min/*_1min.csv"))
    print(f"Updating {len(files)} 1min files to {to_date} ({args.workers} worker(s))", flush=True)
    jobs, failed = [], []
    for p in files:
        key = p.name[: -len("_1min.csv")]
        token = (idx if p.parent.parent.name == "indices" else eq).get(key)
        if token is None:
            print(f"FAILED {key}: no instrument token (file left unchanged)", flush=True)
            failed.append(key)
        else:
            jobs.append((key, str(p), int(token), to_date, args.delay))

    done = 0

    def report(n, key, ok, detail):
        nonlocal done
        if ok:
            done += 1
            print(f"[{n}/{len(jobs)}] [done] {key}: {detail}", flush=True)
        else:
            failed.append(key)
            print(f"[{n}/{len(jobs)}] FAILED {key}: {detail}", flush=True)

    if args.workers > 1:
        with Pool(max(1, min(args.workers, 8))) as pool:
            for n, (key, ok, detail) in enumerate(pool.imap_unordered(_work, jobs), 1):
                report(n, key, ok, detail)
    else:
        for n, job in enumerate(jobs, 1):
            report(n, *_work(job))
    print(f"Done. Updated {done}/{len(files)} files.")
    if failed:
        print(f"FAILED ({len(failed)}): {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
