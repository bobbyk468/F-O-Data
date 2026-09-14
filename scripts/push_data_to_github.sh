#!/usr/bin/env bash
# Commit and push only data/ CSV updates. Skips when there are no changes.
set -euo pipefail

export TZ=Asia/Kolkata

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Target branch on the remote (independent of the local branch's name, which
# may differ, e.g. local "fo-data-main" pushing to remote "main").
REMOTE_BRANCH="${GIT_REMOTE_BRANCH:-main}"
REMOTE="${GIT_REMOTE:-origin}"

git add data/

if git diff --cached --quiet; then
  echo "No data changes to commit."
  exit 0
fi

MSG="data: daily refresh $(date '+%Y-%m-%d %H:%M %Z')"
git commit -m "$MSG"
git push "$REMOTE" "HEAD:${REMOTE_BRANCH}"
echo "Pushed data updates to ${REMOTE}/${REMOTE_BRANCH}"
