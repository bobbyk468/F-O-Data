#!/usr/bin/env bash
# End-to-end refresh + GitHub push for Cursor Automation and manual runs.
set -euo pipefail

export TZ=Asia/Kolkata

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs"
LOG="${LOG_DIR}/automation_refresh.log"

mkdir -p "$LOG_DIR"

{
  echo "======== $(date '+%Y-%m-%d %H:%M:%S %Z') ========"
  bash "${ROOT}/scripts/refresh_data.sh"
  bash "${ROOT}/scripts/push_data_to_github.sh"
  echo "======== done $(date '+%Y-%m-%d %H:%M:%S %Z') ========"
} >>"$LOG" 2>&1

tail -20 "$LOG"
