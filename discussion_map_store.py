"""
discussion_map：JSON 与 SQLite 双源整合。

环境变量：
  DISCUSSION_MAP_BACKEND — auto | json | sqlite（默认 auto：DB 有数据则读 DB，否则读 JSON）
  DISCUSSION_MAP_WRITE_DB — 1/true 时 save 除写 JSON 外同步 UPSERT 到 SQLite discussion_map 表
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


def _load_sqlite() -> dict:
    dbp = _db_file()
    if not dbp.is_file():
        return {}
    conn = sqlite3.connect(str(dbp))
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='discussion_map'"
        ).fetchone()
        if not row:
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


def load_discuss_map() -> dict:
    b = _backend()
    if b == "json":
        return _load_json()
    if b == "sqlite":
        return _load_sqlite()
    d = _load_sqlite()
    if d:
        return d
    return _load_json()


def _save_json(data: dict) -> None:
    mf = _map_file()
    mf.parent.mkdir(parents=True, exist_ok=True)
    with mf.open("w", encoding="utf-8") as f:
        json.dump(data or {}, f, ensure_ascii=False, indent=2)


def _save_sqlite(data: dict) -> None:
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
    """始终写 JSON；DISCUSSION_MAP_WRITE_DB=1 时额外镜像到 SQLite。"""
    _save_json(data or {})
    if (os.getenv("DISCUSSION_MAP_WRITE_DB", "").strip().lower() in ("1", "true", "yes")):
        try:
            _save_sqlite(data or {})
        except Exception:
            logger.exception("discussion_map SQLite mirror failed")
