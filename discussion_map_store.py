"""
discussion mapping compatibility store.

Canonical source for published Telegram posts is posts.discuss_message_id.
The historical discussion_map table and JSON file remain read/write fallbacks so
old deployments and old posts keep working during migration.

环境变量：
  DISCUSSION_MAP_BACKEND — auto | json | sqlite（默认 auto）
  DISCUSSION_MAP_WRITE_DB — 1/true 时 save 额外镜像到旧 discussion_map 表
  DISCUSSION_MAP_FILE / DB_PATH — 与主工程一致
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _map_file() -> Path:
    return Path(
        os.getenv(
            "DISCUSSION_MAP_FILE",
            str(_repo_root() / "data" / "discussion_map.json"),
        )
    ).resolve()


def _db_file() -> Path:
    raw = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
    p = Path(raw)
    if not p.is_absolute():
        p = (_repo_root() / p).resolve()
    return p


def _backend() -> str:
    return (os.getenv("DISCUSSION_MAP_BACKEND", "auto") or "auto").strip().lower()


def _load_json() -> dict:
    mf = _map_file()
    if not mf.is_file():
        return {}
    try:
        with mf.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.exception("discussion_map JSON read failed: %s", mf)
        return {}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _load_posts_sqlite() -> dict:
    """读取正式 posts 映射；这是已发布房源的唯一优先事实来源。"""
    dbp = _db_file()
    if not dbp.is_file():
        return {}
    conn = sqlite3.connect(str(dbp))
    try:
        if not _table_exists(conn, "posts"):
            return {}
        cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(posts)").fetchall()}
        if not {"channel_message_id", "discuss_message_id"}.issubset(cols):
            return {}
        out: dict[str, int] = {}
        rows = conn.execute(
            """SELECT channel_message_id, discuss_message_id
               FROM posts
               WHERE channel_message_id IS NOT NULL
                 AND discuss_message_id IS NOT NULL
                 AND TRIM(CAST(discuss_message_id AS TEXT))<>''
                 AND platform='telegram'
                 AND publish_status IN ('published','success','ok')"""
        ).fetchall()
        for cid, mid in rows:
            try:
                out[str(int(cid))] = int(mid)
            except (TypeError, ValueError):
                continue
        return out
    finally:
        conn.close()


def _load_legacy_sqlite() -> dict:
    dbp = _db_file()
    if not dbp.is_file():
        return {}
    conn = sqlite3.connect(str(dbp))
    try:
        if not _table_exists(conn, "discussion_map"):
            return {}
        out: dict[str, int] = {}
        for cid, mid in conn.execute(
            "SELECT channel_post_id, discussion_msg_id FROM discussion_map "
            "WHERE discussion_msg_id IS NOT NULL"
        ).fetchall():
            try:
                out[str(int(cid))] = int(mid)
            except (TypeError, ValueError):
                continue
        return out
    finally:
        conn.close()


def _merge_with_priority(*maps: dict) -> dict:
    """左侧优先；后续来源只补缺失键。"""
    merged: dict[str, int] = {}
    for mapping in maps:
        for key, value in (mapping or {}).items():
            key = str(key)
            if key in merged:
                continue
            try:
                merged[key] = int(value)
            except (TypeError, ValueError):
                continue
    return merged


def load_discuss_map() -> dict:
    b = _backend()
    if b == "json":
        return _load_json()

    canonical = _load_posts_sqlite()
    legacy_sqlite = _load_legacy_sqlite()
    if b == "sqlite":
        return _merge_with_priority(canonical, legacy_sqlite)

    # auto: posts is canonical; old table and JSON only fill missing historical rows.
    return _merge_with_priority(canonical, legacy_sqlite, _load_json())


def _save_json(data: dict) -> None:
    mf = _map_file()
    mf.parent.mkdir(parents=True, exist_ok=True)
    with mf.open("w", encoding="utf-8") as f:
        json.dump(data or {}, f, ensure_ascii=False, indent=2)


def _save_sqlite(data: dict) -> None:
    """只维护旧兼容表；正式 posts 映射由发布流程自身写入。"""
    dbp = _db_file()
    dbp.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(dbp))
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS discussion_map (
                channel_post_id INTEGER PRIMARY KEY,
                discussion_msg_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        for k, v in (data or {}).items():
            try:
                cid = int(k)
                mid = int(v) if v is not None else None
            except (TypeError, ValueError):
                continue
            if mid is None:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO discussion_map (channel_post_id, discussion_msg_id) VALUES (?, ?)",
                (cid, mid),
            )
        conn.commit()
    finally:
        conn.close()


def save_discuss_map(data: dict) -> None:
    """保留 JSON/旧表兼容写入；新代码读取时始终优先 posts 映射。"""
    _save_json(data or {})
    if os.getenv("DISCUSSION_MAP_WRITE_DB", "").strip().lower() in ("1", "true", "yes"):
        try:
            _save_sqlite(data or {})
        except Exception:
            logger.exception("discussion_map SQLite mirror failed")
