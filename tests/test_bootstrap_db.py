from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def test_bootstrap_creates_pipeline_and_user_bot_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "delivery.db"
    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/bootstrap_db.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert {
        "source_posts",
        "drafts",
        "listings",
        "users",
        "favorites",
        "leads",
        "appointments",
        "tenant_bindings",
        "repair_tickets",
        "subscriptions",
    }.issubset(tables)
