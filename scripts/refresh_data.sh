#!/usr/bin/env bash
# Core data refresh: login, 5min fetch, NIFTY50 volume, 15min + EOD incremental.
set -euo pipefail

export TZ=Asia/Kolkata
export PYTHONUNBUFFERED=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${ROOT}/.venv/bin/python"
WORKERS="${SCHEDULE_WORKERS:-4}"
TO_DATE="${TO_DATE:-$(date '+%Y-%m-%d')}"

cd "$ROOT"
if [[ ! -x "$PY" ]]; then
  echo "ERROR: venv python missing: $PY"
  echo "Create with: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

echo "--- session: test_login ---"
"$PY" -u fetch_code/test_login.py

echo "--- 5min indices (to-date=${TO_DATE}) ---"
"$PY" -u fetch_all_indices_5min.py --to-date "$TO_DATE"

echo "--- 5min F&O (to-date=${TO_DATE}) ---"
"$PY" -u fetch_fo_stocks_5min.py --to-date "$TO_DATE"

echo "--- rebuild NIFTY 50 index aggregate volume ---"
"$PY" -u fetch_code/build_nifty50_index_volume.py

echo "--- update_incremental (15min + EOD, to-date=${TO_DATE}, workers=${WORKERS}) ---"
"$PY" -u fetch_code/update_incremental.py --only all --workers "$WORKERS" --delay 0.05 --to-date "$TO_DATE"
