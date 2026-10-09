#!/usr/bin/env bash
# Unattended daily run: safety checks -> refresh all timeframes -> push to GitHub -> notify.
# Log: logs/daily_refresh_YYYYMMDD.log   Status of the last run: logs/last_run.txt
# Exit codes: 0 ok / nothing to do, 1 refresh had failures (not pushed), 2 refused to start, 3 push failed.
set -uo pipefail

export TZ=Asia/Kolkata
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
TODAY="$(date '+%Y-%m-%d')"
LOG="logs/daily_refresh_$(date '+%Y%m%d').log"
exec > >(tee -a "$LOG") 2>&1

notify() {  # title, message
  /usr/bin/osascript -e "display notification \"${2//\"/\'}\" with title \"${1//\"/\'}\"" >/dev/null 2>&1 || true
}
finish() {  # code, status text
  echo "$(date '+%Y-%m-%d %H:%M:%S %Z') | exit=$1 | $2" > logs/last_run.txt
  echo "=== $2 ==="
  exit "$1"
}

echo
echo "=================== daily refresh ${TODAY} $(date '+%H:%M:%S %Z') ==================="

# --- single instance ---
LOCKDIR="logs/.daily_refresh.lock"
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  OLD="$(cat "$LOCKDIR/pid" 2>/dev/null || true)"
  if [[ -n "$OLD" ]] && kill -0 "$OLD" 2>/dev/null; then
    echo "Another refresh is already running (pid $OLD); exiting."
    exit 0
  fi
  echo "Removing stale lock (pid ${OLD:-unknown} not running)."
  rm -rf "$LOCKDIR"; mkdir "$LOCKDIR"
fi
echo $$ > "$LOCKDIR/pid"
trap 'rm -rf "$LOCKDIR"' EXIT

# --- pre-flight guards: refuse to run rather than do something expensive or destructive ---
TRACKED="$(git ls-files -- data | grep -c '\.csv$' || true)"
ONDISK="$(find data -name '*.csv' 2>/dev/null | wc -l | tr -d ' ')"
if (( ONDISK * 100 < TRACKED * 95 )); then
  MSG="data/ not fully restored (${ONDISK} of ${TRACKED} CSVs on disk). Incremental updates would refetch full history. Run: git checkout origin/main -- data/"
  notify "Zerodha refresh NOT run" "data folder incomplete (${ONDISK}/${TRACKED} files)"
  finish 2 "REFUSED: $MSG"
fi

FREE_KB="$(df -k "$ROOT" | awk 'NR==2 {print $4}')"
if (( FREE_KB < 3 * 1024 * 1024 )); then
  notify "Zerodha refresh NOT run" "less than 3GB free disk"
  finish 2 "REFUSED: less than 3GB free disk ($((FREE_KB / 1024))MB)"
fi

if ! git diff --cached --quiet; then
  notify "Zerodha refresh NOT run" "git has staged changes"
  finish 2 "REFUSED: git index has staged changes; commit or reset them first"
fi

if ! git fetch origin -q; then
  echo "WARNING: git fetch failed (offline?); continuing, the push step may fail."
else
  BEHIND="$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)"
  if (( BEHIND > 0 )); then
    echo "Local is ${BEHIND} commit(s) behind origin/main; fast-forwarding."
    if ! git merge --ff-only origin/main -q; then
      notify "Zerodha refresh NOT run" "local and GitHub have diverged"
      finish 2 "REFUSED: cannot fast-forward to origin/main (diverged history)"
    fi
  fi
fi

# --- refresh (caffeinate keeps the Mac awake while it runs) ---
START_TS=$SECONDS
caffeinate -i bash scripts/refresh_data.sh
RC=$?
echo "refresh_data.sh exit code: ${RC} (took $(( (SECONDS - START_TS) / 60 )) min)"
if (( RC == 2 )); then
  notify "Zerodha refresh FAILED" "Login failed - nothing fetched. See ${LOG}"
  finish 1 "FAILED: login failed, nothing fetched"
elif (( RC != 0 )); then
  notify "Zerodha refresh FAILED" "Some stages failed - NOT pushed. See ${LOG}"
  finish 1 "FAILED: refresh had failures; data left local, NOT pushed (next run resumes automatically)"
fi

# --- freshness check: every file should be at the newest date (LTIM is delisted and stays behind) ---
STALE="$(.venv/bin/python - <<'EOF'
import glob, os
from collections import defaultdict
def last_day(p):
    with open(p, "rb") as f:
        f.seek(0, 2); size = f.tell(); f.seek(max(0, size - 400))
        lines = f.read().decode(errors="ignore").splitlines()
    return lines[-1][:10] if lines else ""
stale = []
for tf in ("1min", "5min", "15min", "eod"):
    d = {p: last_day(p) for p in glob.glob(f"data/*/{tf}/*_{tf}.csv")}
    newest = max(d.values())
    stale += [f"{os.path.basename(p)}({v})" for p, v in d.items() if v < newest and not os.path.basename(p).startswith("ltim")]
print(" ".join(stale))
EOF
)"
if [[ -n "$STALE" ]]; then
  echo "WARNING: files behind the newest date: ${STALE}"
  notify "Zerodha refresh: stale files" "$(echo "$STALE" | wc -w | tr -d ' ') files behind - NOT pushed"
  finish 1 "FAILED: stale files after refresh (${STALE}); NOT pushed"
fi

# --- push (set NO_PUSH=1 to test without touching GitHub) ---
if [[ "${NO_PUSH:-0}" == "1" ]]; then
  finish 0 "OK (NO_PUSH=1): refresh passed all checks; push skipped"
fi
HEAD_BEFORE="$(git rev-parse HEAD)"
bash scripts/push_data_to_github.sh
PRC=$?
if (( PRC != 0 )); then
  notify "Zerodha refresh: push FAILED" "Data refreshed locally, GitHub push failed. See ${LOG}"
  finish 3 "PUSH FAILED (data is refreshed locally)"
fi

HEAD_SHORT="$(git rev-parse --short HEAD)"
find logs -name 'daily_refresh_*.log' -mtime +30 -delete 2>/dev/null || true
if [[ "$(git rev-parse HEAD)" == "$HEAD_BEFORE" ]]; then
  notify "Zerodha data refresh" "No new data for ${TODAY} (market holiday?) - nothing to push"
  finish 0 "OK: ran for ${TODAY}; no new data (market holiday?), nothing pushed"
fi
notify "Zerodha data refreshed" "All timeframes updated to ${TODAY} and pushed (${HEAD_SHORT})"
finish 0 "OK: all timeframes refreshed for ${TODAY}; pushed ${HEAD_SHORT}"
