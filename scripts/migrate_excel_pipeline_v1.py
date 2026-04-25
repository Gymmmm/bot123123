#!/usr/bin/env python3
"""Apply Excel-first pipeline tables to existing SQLite DB."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "data" / "qiaolian_dual_bot.db"))).resolve()
SCHEMA_PATH = ROOT / "schema_core.sql"

NEW_TABLES = (
    "excel_intake_batches",
    "excel_listing_rows",
    "cover_render_jobs",
    "publish_queue_v2",
)


def main() -> int:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return 2
    if not SCHEMA_PATH.exists():
        print(f"Schema not found: {SCHEMA_PATH}")
        return 2

    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(sql)
        conn.commit()
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()

    missing = [name for name in NEW_TABLES if name not in existing]
    if missing:
        print("Migration incomplete, missing tables:", ", ".join(missing))
        return 1

    print("Migration OK.")
    print("DB:", DB_PATH)
    print("Tables:", ", ".join(NEW_TABLES))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
