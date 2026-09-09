"""Source evidence repository extracted from ``collector_db_compat``.

This repository owns only immutable-ish intake evidence and source media. It
has no draft, listing, package, post, or publish-log methods. Construction never
initializes schema; additive DDL is executed only by ``initialize_v3_storage``.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any


SOURCE_DDL = """
CREATE TABLE IF NOT EXISTS source_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_post_id TEXT NOT NULL,
    source_url TEXT,
    source_author TEXT,
    raw_text TEXT NOT NULL DEFAULT '',
    raw_images_json TEXT NOT NULL DEFAULT '[]',
    raw_videos_json TEXT NOT NULL DEFAULT '[]',
    raw_contact TEXT,
    raw_meta_json TEXT NOT NULL DEFAULT '{}',
    dedupe_hash TEXT NOT NULL,
    parse_status TEXT NOT NULL DEFAULT 'pending',
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_source_posts_source_tuple_v3
ON source_posts(source_type, source_name, source_post_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_dedupe_v3
ON source_posts(dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_source_posts_parse_v3
ON source_posts(parse_status, id);

CREATE TABLE IF NOT EXISTS media_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL UNIQUE,
    owner_type TEXT NOT NULL,
    owner_ref_id INTEGER,
    owner_ref_key TEXT,
    asset_type TEXT,
    source_type TEXT,
    source_url TEXT,
    source_file_id TEXT,
    local_path TEXT,
    file_url TEXT,
    file_hash TEXT,
    telegram_file_id TEXT,
    telegram_file_unique_id TEXT,
    media_type TEXT,
    is_watermarked INTEGER NOT NULL DEFAULT 0,
    is_cover INTEGER NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0,
    width INTEGER,
    height INTEGER,
    duration REAL,
    file_size INTEGER,
    mime_type TEXT,
    meta_json TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_media_assets_owner_v3
ON media_assets(owner_type, owner_ref_id, sort_order);
"""


class SourceRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def find_duplicate(
        self,
        *,
        source_type: str,
        source_name: str,
        source_post_id: str,
        dedupe_hash: str,
        source_url: str,
    ) -> tuple[int | None, str]:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT id FROM source_posts
                   WHERE source_type=? AND source_name=? AND source_post_id=?
                   ORDER BY id DESC LIMIT 1""",
                (str(source_type), str(source_name), str(source_post_id)),
            ).fetchone()
            if row:
                return int(row["id"]), "source_tuple"
            row = conn.execute(
                "SELECT id FROM source_posts WHERE dedupe_hash=? ORDER BY id DESC LIMIT 1",
                (str(dedupe_hash),),
            ).fetchone()
            if row:
                return int(row["id"]), "dedupe_hash"
            if str(source_url or "").strip():
                row = conn.execute(
                    "SELECT id FROM source_posts WHERE source_url=? ORDER BY id DESC LIMIT 1",
                    (str(source_url),),
                ).fetchone()
                if row:
                    return int(row["id"]), "source_url"
        return None, ""

    def save_source_post(
        self,
        *,
        source_id: str | None,
        source_type: str,
        source_name: str,
        source_post_id: str,
        source_url: str,
        source_author: str,
        raw_text: str,
        raw_images: list[Any],
        raw_videos: list[Any],
        raw_contact: str,
        raw_meta: dict[str, Any],
        dedupe_hash: str,
        parse_status: str = "pending",
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO source_posts
                   (source_id,source_type,source_name,source_post_id,source_url,
                    source_author,raw_text,raw_images_json,raw_videos_json,
                    raw_contact,raw_meta_json,dedupe_hash,parse_status,
                    fetched_at,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (
                    source_id,
                    str(source_type),
                    str(source_name),
                    str(source_post_id),
                    str(source_url or ""),
                    str(source_author or ""),
                    str(raw_text or ""),
                    json.dumps(raw_images or [], ensure_ascii=False),
                    json.dumps(raw_videos or [], ensure_ascii=False),
                    str(raw_contact or ""),
                    json.dumps(raw_meta or {}, ensure_ascii=False, sort_keys=True),
                    str(dedupe_hash),
                    str(parse_status),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def save_source_images(self, source_post_id: int, images: list[Any]) -> int:
        written = 0
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for index, item in enumerate(images or []):
                if not isinstance(item, dict) or not item.get("local_path"):
                    continue
                asset_id = "AST_" + uuid.uuid4().hex[:16].upper()
                conn.execute(
                    """INSERT INTO media_assets
                       (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,
                        source_type,source_file_id,local_path,file_hash,
                        telegram_file_id,telegram_file_unique_id,media_type,
                        is_watermarked,is_cover,sort_order,status)
                       VALUES (?,'source_post',?,?, 'photo','telegram',?,?,?,?,?,
                               'photo',0,? ,?,'active')""",
                    (
                        asset_id,
                        int(source_post_id),
                        str(source_post_id),
                        str(item.get("telegram_file_id") or ""),
                        str(item.get("local_path") or ""),
                        str(item.get("file_hash") or ""),
                        str(item.get("telegram_file_id") or ""),
                        str(item.get("telegram_file_unique_id") or ""),
                        1 if index == 0 else 0,
                        index,
                    ),
                )
                written += 1
            conn.commit()
        return written

    def get_source_post(self, source_post_pk: int) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM source_posts WHERE id=?", (int(source_post_pk),)
            ).fetchone()

    def update_parse_status(self, source_post_pk: int, status: str) -> None:
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE source_posts SET parse_status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (str(status), int(source_post_pk)),
            )
            if cur.rowcount != 1:
                raise KeyError(source_post_pk)
            conn.commit()


__all__ = ["SOURCE_DDL", "SourceRepository"]
