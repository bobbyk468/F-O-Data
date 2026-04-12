#!/usr/bin/env python3
"""Shim: implementation in fetch_code/."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
from fetch_code.test_login import main

if __name__ == "__main__":
    rc = main()
    raise SystemExit(rc if rc is not None else 0)
