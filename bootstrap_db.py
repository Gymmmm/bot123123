#!/usr/bin/env python3
"""Compatibility wrapper: run scripts/bootstrap_db.py from repo root."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
runpy.run_path(str(ROOT / "scripts" / "bootstrap_db.py"), run_name="__main__")
