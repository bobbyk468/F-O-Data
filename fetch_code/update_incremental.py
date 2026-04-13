#!/usr/bin/env python3
"""
Incrementally update existing CSVs under ./data by fetching only missing range.

Why:
- You already have lots of data files; re-fetching full history is slow.
- This script reads the *last timestamp* in each CSV and appends new candles.
- If TokenException occurs (expired session), it will run test_login.py once and retry.

Supported:
- Indices 15min:   data/indices/15min/*_15min.csv
- Indices EOD:     data/indices/eod/*_eod.csv
- F&O 15min:       data/nifty50/15min/*_15min.csv and data/other/15min/*_15min.csv
- F&O EOD:         data/nifty50/eod/*_eod.csv and data/other/eod/*_eod.csv

Usage:
  .venv/bin/python -u update_incremental.py --only indices15
  .venv/bin/python -u update_incremental.py --only fo15 --workers 4
  .venv/bin/python -u update_incremental.py --only all --workers 4
"""

from __future__ import annotations


# --- fetch_code/: data/ and jugaad_trader/ live at repository root ---
import sys
from pathlib import Path as _Path
_FC = _Path(__file__).resolve().parent
_REPO = _FC.parent
for _d in (_REPO, _FC):
    _s = str(_d)
    if _s not in sys.path:
        sys.path.insert(0, _s)
from repo_paths import REPO_ROOT  # noqa: E402

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from multiprocessing import Pool
from pathlib import Path
from typing import Iterable, Optional

# paths: bootstrap above

from jugaad_trader import Zerodha

from fetch_code.fetch_all_indices_15min import (
    CHUNK_DAYS,
    fetch_15min_for_instrument,
    get_index_instruments,
    slug as slug_15,
)
from fetch_code.fetch_fo_stocks_15min import get_fo_equity_symbol_to_token
from fetch_code.fetch_eod_90d import (
    fetch_eod_one,
    get_fo_symbol_to_token,
    get_index_symbol_to_token,
    slug as slug_eod,
)


BASE = REPO_ROOT
DATA = BASE / "data"


def _parse_dt(s: str) -> Optional[datetime]:
    # Kite/jugaad-trader may already return datetime/date objects.
    if isinstance(s, datetime):
        return s
    if isinstance(s, date):
        return datetime(s.year, s.month, s.day)
    s = (s or "").strip()
    if not s:
        return None
    # Typical CSV contains ISO datetime with timezone: 2015-09-01 09:15:00+05:30
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Fallback: without tz
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def last_csv_datetime(path: Path) -> Optional[datetime]:
    """
    Read last non-empty data row datetime quickly.
    Assumes first column is 'date'.
    """
    if not path.is_file():
        return None
    # Read from the end to avoid loading huge file.
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
    # Walk backwards skipping header or incomplete lines.
    for ln in reversed(lines):
        if ln.lower().startswith("date,"):
            continue
        first = ln.split(",", 1)[0]
        dt = _parse_dt(first)
        if dt is not None:
            return dt
    return None


def _merge_write_csv(path: Path, rows: list[list], header: list[str]) -> int:
    # Build map by datetime for dedupe
    by_ts = {}
    for r in rows:
        dt = _parse_dt(r[0])
        if dt is None:
            continue
        by_ts[dt] = r
    out = [by_ts[k] for k in sorted(by_ts)]
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(out)
    os.replace(tmp, path)
    return len(out)


def _read_existing_rows(path: Path) -> tuple[list[str], list[list]]:
    if not path.is_file():
        return (["date", "open", "high", "low", "close", "volume"], [])
    with open(path, "r", newline="") as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            return (["date", "open", "high", "low", "close", "volume"], [])
        rows = [row for row in r if row and any(cell.strip() for cell in row)]
    if not header:
        header = ["date", "open", "high", "low", "close", "volume"]
    return (header, rows)


def ensure_session():
    kite = Zerodha()
    kite.set_access_token()
    return kite


def relogin_once() -> bool:
    # Run test_login.py in-process to refresh session.
    # We avoid subprocess to keep it simple; import and call main().
    try:
        import test_login
        rc = test_login.main()
        return rc == 0
    except Exception:
        return False


def _is_token_exception(e: Exception) -> bool:
    name = type(e).__name__
    msg = str(e)
    return "TokenException" in name or "Incorrect `api_key` or `access_token`" in msg or "access_token" in msg


@dataclass(frozen=True)
class UpdateJob:
    kind: str  # indices15, fo15, indices_eod, fo_eod
    symbol: str
    token: int
    out_path: str
    delay: float


def _update_indices15_one(job: UpdateJob) -> tuple[str, int]:
    kite = Zerodha()
    kite.set_access_token()
    out_path = Path(job.out_path)
    last_dt = last_csv_datetime(out_path)
    to_date = datetime.now().date()
    if last_dt is None:
        # No file or empty => fetch nothing here; use main full scripts for bootstrap.
        return (job.symbol, 0)
    # Fetch from last_date-1 day to handle partial/trading gaps
    from_date = (last_dt.date() - timedelta(days=1))
    candles = fetch_15min_for_instrument(kite, job.token, from_date, to_date, delay_sec=job.delay)
    header, existing = _read_existing_rows(out_path)
    appended = [
        [c.get("date"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume", 0)]
        for c in (candles or [])
    ]
    n = _merge_write_csv(out_path, existing + appended, header)
    return (job.symbol, n)


def _update_eod_one(job: UpdateJob) -> tuple[str, int]:
    kite = Zerodha()
    kite.set_access_token()
    out_path = Path(job.out_path)
    last_dt = last_csv_datetime(out_path)
    to_date = datetime.now().date()
    if last_dt is None:
        return (job.symbol, 0)
    from_date = (last_dt.date() - timedelta(days=5))
    candles = fetch_eod_one(kite, job.token, from_date, to_date, delay_sec=job.delay)
    header, existing = _read_existing_rows(out_path)
    appended = [
        [c.get("date"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume", 0)]
        for c in (candles or [])
    ]
    n = _merge_write_csv(out_path, existing + appended, header)
    return (job.symbol, n)


def build_jobs(only: str, delay: float) -> list[UpdateJob]:
    kite = ensure_session()

    jobs: list[UpdateJob] = []

    if only in ("indices15", "all"):
        # Map existing files -> symbol token by slug
        idx_dir = DATA / "indices" / "15min"
        existing = [p for p in idx_dir.glob("*_15min.csv") if p.is_file()]
        if existing:
            sym_map = {slug_15(i["tradingsymbol"]): (i["tradingsymbol"], i["instrument_token"]) for i in get_index_instruments(kite)}
            for p in existing:
                key = p.name.replace("_15min.csv", "")
                if key in sym_map:
                    sym, tok = sym_map[key]
                    jobs.append(UpdateJob("indices15", sym, int(tok), str(p), delay))

    if only in ("indices_eod", "all"):
        idx_dir = DATA / "indices" / "eod"
        existing = [p for p in idx_dir.glob("*_eod.csv") if p.is_file()]
        if existing:
            sym_to_tok = get_index_symbol_to_token(kite)
            slug_to_sym = {slug_eod(sym): sym for sym in sym_to_tok.keys()}
            for p in existing:
                key = p.name.replace("_eod.csv", "")
                sym = slug_to_sym.get(key)
                if sym:
                    jobs.append(UpdateJob("indices_eod", sym, int(sym_to_tok[sym]), str(p), delay))

    if only in ("fo15", "all"):
        # Prefer the current F&O-underlying list, but fall back to *any* NSE EQ symbol
        # so we can update existing files even if a symbol is no longer in the current F&O set.
        sym_to_tok = get_fo_equity_symbol_to_token(kite)
        nse = kite.instruments("NSE")
        nse_eq = {
            i["tradingsymbol"]: i["instrument_token"]
            for i in nse
            if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
        }
        for sub in ("nifty50", "other"):
            d = DATA / sub / "15min"
            for p in d.glob("*_15min.csv"):
                key = p.name.replace("_15min.csv", "")
                # 15min slug removes hyphens; first try F&O map, then NSE EQ map.
                matched = False
                for sym, tok in sym_to_tok.items():
                    if slug_15(sym) == key:
                        jobs.append(UpdateJob("fo15", sym, int(tok), str(p), delay))
                        matched = True
                        break
                if matched:
                    continue
                for sym, tok in nse_eq.items():
                    if slug_15(sym) == key:
                        jobs.append(UpdateJob("fo15", sym, int(tok), str(p), delay))
                        break

    if only in ("fo_eod", "all"):
        # Prefer the current F&O-underlying list, but fall back to NSE EQ symbols
        # so existing EOD files stay updateable even if symbol drops from F&O list.
        sym_to_tok = get_fo_symbol_to_token(kite)
        nse = kite.instruments("NSE")
        nse_eq = {
            i["tradingsymbol"]: i["instrument_token"]
            for i in nse
            if i.get("segment") == "NSE" and i.get("instrument_type") == "EQ"
        }
        slug_to_sym = {slug_eod(sym): sym for sym in sym_to_tok.keys()}
        slug_to_sym_eq = {slug_eod(sym): sym for sym in nse_eq.keys()}
        for sub in ("nifty50", "other"):
            d = DATA / sub / "eod"
            for p in d.glob("*_eod.csv"):
                key = p.name.replace("_eod.csv", "")
                sym = slug_to_sym.get(key)
                if sym:
                    jobs.append(UpdateJob("fo_eod", sym, int(sym_to_tok[sym]), str(p), delay))
                    continue
                sym2 = slug_to_sym_eq.get(key)
                if sym2:
                    jobs.append(UpdateJob("fo_eod", sym2, int(nse_eq[sym2]), str(p), delay))

    return jobs


def main() -> int:
    ap = argparse.ArgumentParser(description="Incrementally update existing data CSVs")
    ap.add_argument("--only", choices=("indices15", "indices_eod", "fo15", "fo_eod", "all"), default="all")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.05, help="Delay between requests in seconds (default 0.05)")
    ap.add_argument(
        "--paths-file",
        default=None,
        metavar="FILE",
        help="Optional: only update files whose relative paths (from repo root) are listed in FILE.",
    )
    ap.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="For testing: update only first N files (default 0 = no limit).",
    )
    args = ap.parse_args()

    # Ensure session first (and fail early if not logged in)
    try:
        ensure_session()
    except Exception as e:
        print("No valid session. Run: .venv/bin/python -u test_login.py")
        print("Error:", str(e))
        return 1

    jobs = build_jobs(args.only, args.delay)
    # Prefer NIFTY 50 first for quick verification.
    jobs.sort(key=lambda j: (0 if j.symbol == "NIFTY 50" else 1, j.kind, j.symbol))
    if args.paths_file:
        wanted = set()
        with open(args.paths_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    wanted.add(str((BASE / line).resolve()))
        jobs = [j for j in jobs if str(Path(j.out_path).resolve()) in wanted]
    if args.max_files and args.max_files > 0:
        jobs = jobs[: int(args.max_files)]
    if not jobs:
        print("No matching existing CSVs found to update for:", args.only)
        return 0

    print(f"Updating {len(jobs)} files (mode={args.only}, workers={args.workers}, delay={args.delay}s)")
    sys.stdout.flush()

    def run_all() -> list[tuple[str, int]]:
        workers = max(1, min(int(args.workers), 8))
        if workers == 1:
            out = []
            for i, j in enumerate(jobs, start=1):
                print(f"[{i}/{len(jobs)}] {j.kind} {j.symbol} -> {j.out_path}")
                sys.stdout.flush()
                fn = _update_indices15_one if j.kind in ("indices15", "fo15") else _update_eod_one
                sym, n = fn(j)
                print(f"[done] {sym}: {n} rows")
                sys.stdout.flush()
                out.append((sym, n))
            return out
        else:
            # Dispatch based on kind (two pools to keep function pickle simple)
            idx_jobs = [j for j in jobs if j.kind in ("indices15", "fo15")]
            eod_jobs = [j for j in jobs if j.kind in ("indices_eod", "fo_eod")]
            results: list[tuple[str, int]] = []
            if idx_jobs:
                with Pool(workers) as pool:
                    for sym, n in pool.imap_unordered(_update_indices15_one, idx_jobs):
                        print(f"[done] {sym}: {n} rows")
                        sys.stdout.flush()
                        results.append((sym, n))
            if eod_jobs:
                with Pool(workers) as pool:
                    for sym, n in pool.imap_unordered(_update_eod_one, eod_jobs):
                        print(f"[done] {sym}: {n} rows")
                        sys.stdout.flush()
                        results.append((sym, n))
            return results

    try:
        results = run_all()
    except Exception as e:
        if _is_token_exception(e):
            print("Token/session seems expired. Re-logging once and retrying...")
            ok = relogin_once()
            if not ok:
                print("Re-login failed. Run: .venv/bin/python -u test_login.py")
                return 1
            time.sleep(1)
            results = run_all()
        else:
            raise

    updated = sum(1 for _, n in results if n > 0)
    print(f"Done. Updated {updated}/{len(results)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

