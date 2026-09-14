#!/usr/bin/env bash
exec "$(cd "$(dirname "$0")" && pwd)/fetch_code/update_all_incremental.sh" "$@"
