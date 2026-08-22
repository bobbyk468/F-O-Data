#!/usr/bin/env bash
# Local cron refresh flow (logs only; no git push unless PUSH_TO_GITHUB=1).
set -euo pipefail

export TZ=Asia/Kolkata

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs"
LOG="${LOG_DIR}/cron_daily_refresh.log"

mkdir -p "$LOG_DIR"
{
  echo "======== $(date '+%Y-%m-%d %H:%M:%S %Z') ========"
  bash "${ROOT}/scripts/refresh_data.sh"
  if [[ "${PUSH_TO_GITHUB:-0}" == "1" ]]; then
    bash "${ROOT}/scripts/push_data_to_github.sh"
  fi
  echo "======== done $(date '+%Y-%m-%d %H:%M:%S %Z') ========"
} >>"$LOG" 2>&1
