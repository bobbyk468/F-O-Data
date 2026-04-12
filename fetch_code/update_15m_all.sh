#!/usr/bin/env bash
# Refresh all existing 15m CSVs (indices + nifty50/other stocks) from last bar to today.
# Requires: cd here, .venv present, Zerodha session (auto re-login via update_incremental).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
"$PY" -u fetch_code/update_incremental.py --only indices15 --workers "${WORKERS:-4}"
"$PY" -u fetch_code/update_incremental.py --only fo15 --workers "${WORKERS:-4}"
