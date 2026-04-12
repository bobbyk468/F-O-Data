#!/usr/bin/env bash
# Refresh 15m + EOD for all existing CSVs (indices and stocks).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
"$ROOT/update_15m_all.sh"
"$ROOT/update_eod_all.sh"
