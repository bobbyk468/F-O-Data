"""Repository root for an F-O-Data clone: parent directory of fetch_code/."""
from __future__ import annotations

from pathlib import Path

_FETCH_CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = _FETCH_CODE_DIR.parent
DATA_DIR = REPO_ROOT / "data"
