#!/usr/bin/env python3
"""Compatibility wrapper: run scripts/check_workflow.py from repo root."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
runpy.run_path(str(ROOT / "scripts" / "check_workflow.py"), run_name="__main__")
