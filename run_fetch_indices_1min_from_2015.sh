#!/usr/bin/env bash
# Full 1min index backfill (2015-01-01 → today). Takes many hours; logs to logs/.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs
LOG="logs/fetch_indices_1min_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to $LOG" | tee "$LOG"
./.venv/bin/python -u test_login.py 2>&1 | tee -a "$LOG"
# Default: 4 workers, 0.03s delay (see fetch_all_indices_1min.py --help).
# If Kite returns rate-limit errors, add: --delay-scale workers --delay 0.05
./.venv/bin/python -u fetch_all_indices_1min.py \
  --from-date 2015-01-01 \
  --period-days 30 \
  --workers 4 \
  --delay 0.03 \
  --resume \
  "$@" 2>&1 | tee -a "$LOG"
echo "Finished. See $LOG" | tee -a "$LOG"
