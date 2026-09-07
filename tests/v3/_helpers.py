from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "001_v3_phase1_core.sql"


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE source_posts (
            id INTEGER PRIMARY KEY,
            source_id TEXT,
            source_type TEXT,
            source_name TEXT,
            source_post_id TEXT,
            raw_text TEXT,
            raw_meta_json TEXT,
            raw_images_json TEXT,
            raw_videos_json TEXT,
            parse_status TEXT DEFAULT 'pending',
            parse_error TEXT DEFAULT '',
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
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
            channel_message_id INTEGER,
            source_post_url TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            canonical_facts_hash TEXT,
            canonical_facts_schema TEXT,
            public_location_key TEXT,
            public_location_display TEXT,
            publication_location_level TEXT,
            canonical_area_key TEXT,
            property_subtype TEXT,
            project_brand TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.executescript(MIGRATION.read_text(encoding="utf-8"))
    return conn
