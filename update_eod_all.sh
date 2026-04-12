#!/usr/bin/env bash
# Refresh all existing EOD CSVs (indices + nifty50/other) from last bar to today.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python"
"$PY" -u update_incremental.py --only indices_eod --workers "${WORKERS:-4}"
"$PY" -u update_incremental.py --only fo_eod --workers "${WORKERS:-4}"
