#!/usr/bin/env python3
"""Compatibility wrapper: run scripts/publish_ready_batch.py from repo root."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
runpy.run_path(str(ROOT / "scripts" / "publish_ready_batch.py"), run_name="__main__")
