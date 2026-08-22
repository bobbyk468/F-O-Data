#!/usr/bin/env bash
# Commit and push only data/ CSV updates. Skips when there are no changes.
set -euo pipefail

export TZ=Asia/Kolkata

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BRANCH="${GIT_BRANCH:-$(git branch --show-current)}"
REMOTE="${GIT_REMOTE:-origin}"

if [[ -z "$BRANCH" ]]; then
  echo "ERROR: could not determine git branch"
  exit 1
fi

git add data/

if git diff --cached --quiet; then
  echo "No data changes to commit."
  exit 0
fi

MSG="data: daily refresh $(date '+%Y-%m-%d %H:%M %Z')"
git commit -m "$MSG"
git push "$REMOTE" "$BRANCH"
echo "Pushed data updates to ${REMOTE}/${BRANCH}"
