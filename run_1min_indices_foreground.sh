#!/usr/bin/env bash
exec "$(cd "$(dirname "$0")" && pwd)/fetch_code/run_1min_indices_foreground.sh" "$@"
