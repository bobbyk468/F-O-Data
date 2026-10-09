#!/usr/bin/env bash
# Core data refresh for ALL timeframes. Each stage runs even if an earlier one failed; the script exits 1
# (and lists the failed stages) if any stage failed. A failed login aborts everything.
#
# Stages: login -> 5min indices -> 5min F&O stocks -> 5min stragglers -> NIFTY 50 aggregate volume
#         -> 15min + EOD -> 1min -> derived NIFTY 50 timeframes (20/30/35/45/50min, 1hr)
set -uo pipefail

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

FAILED_STAGES=()
STAGE_LOG="$(mktemp -t refresh_stage.XXXXXX)"
trap 'rm -f "$STAGE_LOG"' EXIT

# run_stage "<name>" cmd...  -- streams output, marks the stage failed on a non-zero exit or on error markers.
run_stage() {
  local name="$1"; shift
  echo "--- ${name} ---"
  "$@" 2>&1 | tee "$STAGE_LOG"
  local rc=${PIPESTATUS[0]}
  if [[ $rc -ne 0 ]]; then
    echo "!!! STAGE FAILED: ${name} (exit ${rc})"
    FAILED_STAGES+=("$name")
  elif grep -qE '^FAILED|\[FAILED\]|Error:|Traceback|Full refetch incomplete' "$STAGE_LOG"; then
    echo "!!! STAGE HAD ERRORS: ${name}"
    FAILED_STAGES+=("$name (errors in output)")
  fi
}

echo "=== refresh ${TO_DATE} started $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

echo "--- session: test_login ---"
if ! "$PY" -u fetch_code/test_login.py; then
  echo "!!! LOGIN FAILED -- aborting (nothing was fetched)"
  exit 2
fi

run_stage "5min indices"            "$PY" -u fetch_all_indices_5min.py --to-date "$TO_DATE"
run_stage "5min F&O stocks"         "$PY" -u fetch_fo_stocks_5min.py --to-date "$TO_DATE"
run_stage "5min stragglers"         "$PY" -u fetch_code/update_5min_stragglers.py --to-date "$TO_DATE"
run_stage "NIFTY 50 aggregate volume" "$PY" -u fetch_code/build_nifty50_index_volume.py
run_stage "15min + EOD"             "$PY" -u fetch_code/update_incremental.py --only all --workers "$WORKERS" --delay 0.05 --to-date "$TO_DATE"
run_stage "1min"                    "$PY" -u fetch_code/update_1min.py --to-date "$TO_DATE"
run_stage "derived NIFTY 50 timeframes" "$PY" -u data/indices/resample_15min_to_30min.py

echo "=== refresh ${TO_DATE} finished $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
if [[ ${#FAILED_STAGES[@]} -gt 0 ]]; then
  printf 'FAILED STAGES (%d):\n' "${#FAILED_STAGES[@]}"
  printf '  - %s\n' "${FAILED_STAGES[@]}"
  exit 1
fi
echo "ALL STAGES OK"
