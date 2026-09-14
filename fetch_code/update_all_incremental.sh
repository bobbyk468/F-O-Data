#!/usr/bin/env bash
# Refresh 15m + EOD for all existing CSVs (indices and stocks).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
"$ROOT/fetch_code/update_15m_all.sh"
"$ROOT/fetch_code/update_eod_all.sh"
