from __future__ import annotations

import sqlite3

from qiaolian_v3.db.connection import connect
from qiaolian_v3.db.migration_runner import MigrationRunner


LOCKED_BASELINE_SHA = '8e4605cf5cc21dfec3ce30729654b09e39de9abf'


def baseline_connection() -> sqlite3.Connection:
    """Minimal structural fixture for the locked V2.2 tables touched by Phase 1.

    Phase 1 migrations are additive. The fixture intentionally includes legacy
    `source_posts` and `listings` so tests can prove they are not rewritten.
    """
    conn = connect(':memory:')
    conn.executescript(
        '''
        CREATE TABLE source_posts (
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
            raw_contact TEXT NOT NULL DEFAULT '',
            raw_meta_json TEXT NOT NULL DEFAULT '{}',
            dedupe_hash TEXT,
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT NOT NULL DEFAULT '',
            fetched_at TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE listings (
            listing_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            property_type TEXT NOT NULL,
            area TEXT NOT NULL,
            community TEXT NOT NULL,
            price INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            layout TEXT NOT NULL DEFAULT '',
            size_sqm TEXT NOT NULL DEFAULT '',
            tags_json TEXT NOT NULL DEFAULT '[]',
            highlights TEXT NOT NULL DEFAULT '',
            hidden_costs TEXT NOT NULL DEFAULT '',
            drawbacks TEXT NOT NULL DEFAULT '',
            deposit_rule TEXT NOT NULL DEFAULT '',
            available_date TEXT NOT NULL DEFAULT '',
            media_file_id TEXT NOT NULL DEFAULT '',
            media_type TEXT NOT NULL DEFAULT '',
            source_post_url TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        '''
    )
    return conn


def insert_source(conn: sqlite3.Connection, *, external_id: str = '100') -> int:
    cur = conn.execute(
        '''INSERT INTO source_posts(source_type,source_name,source_post_id,raw_text,created_at,updated_at)
           VALUES ('telegram_channel','fixture',?,'raw evidence','2026-01-01','2026-01-01')''',
        (external_id,),
    )
    return int(cur.lastrowid)


def migrated_connection() -> sqlite3.Connection:
    conn = baseline_connection()
    MigrationRunner(conn).migrate()
    return conn
