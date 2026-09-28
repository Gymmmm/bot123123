"""Keep the test suite isolated from every project or production database."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


_TEST_ROOT = Path(tempfile.mkdtemp(prefix="qiaolian-pytest-"))
os.environ["DB_PATH"] = str(_TEST_ROOT / "suite-default.db")
os.environ["COLLECTOR_DOWNLOAD_DIR"] = str(_TEST_ROOT / "collector-downloads")
os.environ["TELETHON_SESSION_PATH"] = str(_TEST_ROOT / "sessions" / "collector")

