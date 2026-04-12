#!/usr/bin/env bash
# Run 1min index fetch in the FOREGROUND: all output prints to this terminal and is also saved under logs/.
# Open Cursor → Terminal (Ctrl+` / Cmd+`), cd to jugaad-trader, then: ./run_1min_indices_foreground.sh
# Extra args are passed through, e.g. ./run_1min_indices_foreground.sh --workers 2
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
LOG="logs/1min_foreground_$(date +%Y%m%d_%H%M%S).log"
export PYTHONUNBUFFERED=1

echo "=============================================="
echo " 1-minute indices fetch — foreground + log"
echo "=============================================="
echo " Log file: $LOG"
echo " Progress: each line shows a date range and 'N new' candles merged."
echo " Stop: Ctrl+C (restart with same script; --resume continues from CSVs)."
echo "=============================================="
echo ""

{
  echo "=== $(date) ==="
  ${ROOT}/.venv/bin/python -u fetch_code/test_login.py
  ${ROOT}/.venv/bin/python -u fetch_code/fetch_all_indices_1min.py \
    --from-date 2015-01-01 \
    --period-days 30 \
    --workers 4 \
    --delay 0.03 \
    --resume \
    "$@"
} 2>&1 | tee -a "$LOG"

echo ""
echo "Finished. Full log: $LOG"
