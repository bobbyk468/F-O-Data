#!/usr/bin/env python3
"""
Run all data-update scripts with live + periodic status.

This is a convenience runner:
- Streams subprocess output live to console AND to a log file
- Prints a heartbeat every N seconds with elapsed time and quick file counts

Usage:
  .venv/bin/python run_update_all.py
  .venv/bin/python run_update_all.py --heartbeat 30
  .venv/bin/python run_update_all.py --skip-login
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


JUGAAD_DIR = Path(__file__).resolve().parent
LOGS_DIR = JUGAAD_DIR / "logs"
DATA_DIR = JUGAAD_DIR / "data"


@dataclass(frozen=True)
class Step:
    name: str
    cmd: list[str]


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _count_csvs(p: Path) -> int:
    if not p.exists():
        return 0
    return sum(1 for _ in p.rglob("*.csv"))


def _heartbeat(stop_evt: threading.Event, interval_s: int, state: dict) -> None:
    start = state["start_monotonic"]
    while not stop_evt.wait(interval_s):
        step = state.get("step", "-")
        log_path = state.get("log_path")
        last_line = state.get("last_line", "")
        elapsed = int(time.monotonic() - start)
        indices_15 = _count_csvs(DATA_DIR / "indices" / "15min")
        indices_eod = _count_csvs(DATA_DIR / "indices" / "eod")
        nifty50_15 = _count_csvs(DATA_DIR / "nifty50" / "15min")
        nifty50_eod = _count_csvs(DATA_DIR / "nifty50" / "eod")
        other_15 = _count_csvs(DATA_DIR / "other" / "15min")
        other_eod = _count_csvs(DATA_DIR / "other" / "eod")
        print(
            f"[{_ts()}] HEARTBEAT | step={step} | elapsed={elapsed}s | "
            f"indices(15m={indices_15},eod={indices_eod}) "
            f"nifty50(15m={nifty50_15},eod={nifty50_eod}) "
            f"other(15m={other_15},eod={other_eod})"
        )
        if log_path:
            print(f"[{_ts()}] HEARTBEAT | log={log_path}")
        if last_line:
            print(f"[{_ts()}] HEARTBEAT | last_output={last_line}")
        sys.stdout.flush()


def run_step(step: Step, log_path: Path, heartbeat_state: dict) -> None:
    heartbeat_state["step"] = step.name
    heartbeat_state["log_path"] = str(log_path)
    heartbeat_state["last_line"] = ""

    print(f"[{_ts()}] START {step.name}")
    print(f"[{_ts()}] CMD   {' '.join(step.cmd)}")
    print(f"[{_ts()}] LOG   {log_path}")
    sys.stdout.flush()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as lf:
        lf.write(f"\n[{_ts()}] START {step.name}\n")
        lf.write(f"[{_ts()}] CMD {' '.join(step.cmd)}\n\n")
        lf.flush()

        env = os.environ.copy()
        # Force unbuffered output so progress prints appear immediately.
        env["PYTHONUNBUFFERED"] = "1"

        p = subprocess.Popen(
            step.cmd,
            cwd=str(JUGAAD_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
        )

        assert p.stdout is not None
        for line in p.stdout:
            line = line.rstrip("\n")
            heartbeat_state["last_line"] = line[-300:] if line else ""
            print(line)
            lf.write(line + "\n")
            lf.flush()

        rc = p.wait()
        lf.write(f"\n[{_ts()}] END {step.name} exit_code={rc}\n")
        lf.flush()

        if rc != 0:
            raise RuntimeError(f"Step failed: {step.name} (exit code {rc}). See {log_path}")

    print(f"[{_ts()}] END   {step.name} (ok)")
    sys.stdout.flush()


def main() -> int:
    ap = argparse.ArgumentParser(description="Run all Jugaad-trader data updates with status")
    ap.add_argument("--heartbeat", type=int, default=30, help="Heartbeat interval seconds (default 30)")
    ap.add_argument("--skip-login", action="store_true", help="Skip running test_login.py")
    ap.add_argument("--workers", type=int, default=4, help="Workers for multiprocessing scripts (default 4)")
    ap.add_argument(
        "--eod-full-history",
        action="store_true",
        help="Fetch EOD full history (2015->today in 90d chunks). Default: last 90 days only.",
    )
    args = ap.parse_args()

    py = sys.executable  # should be .venv/bin/python when you run via venv
    py_u = [py, "-u"]
    steps: list[Step] = []

    if not args.skip_login:
        steps.append(Step("login (test_login.py)", [*py_u, "test_login.py"]))

    steps.extend(
        [
            Step("indices 15min", [*py_u, "fetch_all_indices_15min.py", "--workers", str(args.workers)]),
            Step("nifty 50 15min", [*py_u, "fetch_nifty50_15min.py"]),
            Step("F&O stocks 15min", [*py_u, "fetch_fo_stocks_15min.py", "--workers", str(args.workers)]),
        ]
    )

    if args.eod_full_history:
        steps.extend(
            [
                Step("indices EOD (full history)", [py, "fetch_eod_90d.py", "--only", "indices", "--full-history", "--workers", str(args.workers)]),
                Step("indices EOD (full history)", [*py_u, "fetch_eod_90d.py", "--only", "indices", "--full-history", "--workers", str(args.workers)]),
                Step("F&O EOD (full history)", [*py_u, "fetch_eod_90d.py", "--only", "fo", "--full-history", "--workers", str(args.workers)]),
            ]
        )
    else:
        steps.extend(
            [
                Step("indices EOD (last 90d)", [*py_u, "fetch_eod_90d.py", "--only", "indices", "--days", "90", "--workers", str(args.workers)]),
                Step("F&O EOD (last 90d)", [*py_u, "fetch_eod_90d.py", "--only", "fo", "--days", "90", "--workers", str(args.workers)]),
            ]
        )

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    heartbeat_state = {"start_monotonic": time.monotonic(), "step": "-", "log_path": None, "last_line": ""}
    stop_evt = threading.Event()
    hb = threading.Thread(target=_heartbeat, args=(stop_evt, int(args.heartbeat), heartbeat_state), daemon=True)
    hb.start()

    try:
        for i, step in enumerate(steps, start=1):
            log_path = LOGS_DIR / f"run_update_all-{run_id}-{i:02d}-{step.name.replace(' ', '_').replace('/', '_')}.log"
            run_step(step, log_path, heartbeat_state)
    finally:
        stop_evt.set()

    print(f"[{_ts()}] ALL DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

