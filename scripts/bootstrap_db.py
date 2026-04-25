#!/usr/bin/env python3
"""确保 SQLite 含采集/解析/发帖所需表结构。若库是旧版空壳，设 QL_REBUILD_DB=1 强制备份重建。"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", override=True)
DB_PATH = Path(os.getenv("DB_PATH", str(ROOT / "data/qiaolian_dual_bot.db"))).resolve()
SCHEMA = ROOT / "schema_core.sql"


def _drafts_has_core(conn: sqlite3.Connection) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='drafts'"
    )
    if not cur.fetchone():
        return False
    cols = {r[1] for r in conn.execute("PRAGMA table_info(drafts)").fetchall()}
    need = {"draft_id", "source_post_id", "title", "review_status", "cover_asset_id"}
    return need.issubset(cols)


def main() -> int:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    rebuild = os.getenv("QL_REBUILD_DB", "").strip().lower() in ("1", "true", "yes")

    if DB_PATH.exists() and rebuild:
        bak = DB_PATH.with_suffix(f".db.bak.{os.getpid()}")
        shutil.move(str(DB_PATH), str(bak))
        print(f"已备份旧库 -> {bak}", file=sys.stderr)

    is_new = not DB_PATH.exists() or DB_PATH.stat().st_size == 0

    if not is_new:
        conn = sqlite3.connect(DB_PATH)
        try:
            ok = _drafts_has_core(conn)
        finally:
            conn.close()
        if not ok and not rebuild:
            print(
                "现有数据库缺少 drafts 核心字段。请备份后执行：\n"
                "  QL_REBUILD_DB=1 python3 scripts/bootstrap_db.py\n"
                f"或手动删除 {DB_PATH} 后重跑本脚本。",
                file=sys.stderr,
            )
            return 2

    sql = SCHEMA.read_text(encoding="utf-8")
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()
    print(f"OK schema 已应用：{DB_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
