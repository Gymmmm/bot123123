"""归因存储：复用 leads，并加一张 user_attribution 快照表。不重建库。"""
from __future__ import annotations

import json
from typing import Any

from .common import db
from .db import row_to_dict


def ensure_attribution_schema() -> None:
    with db.connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_attribution (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                first_source_type TEXT NOT NULL DEFAULT '',
                first_source_detail TEXT NOT NULL DEFAULT '',
                first_entry_at TEXT NOT NULL DEFAULT '',
                first_listing_id TEXT NOT NULL DEFAULT '',
                first_deep_link TEXT NOT NULL DEFAULT '',
                first_entry_action TEXT NOT NULL DEFAULT '',
                first_legacy INTEGER NOT NULL DEFAULT 0,
                latest_source_type TEXT NOT NULL DEFAULT '',
                latest_source_detail TEXT NOT NULL DEFAULT '',
                latest_touch_at TEXT NOT NULL DEFAULT '',
                latest_listing_id TEXT NOT NULL DEFAULT '',
                latest_deep_link TEXT NOT NULL DEFAULT '',
                latest_entry_action TEXT NOT NULL DEFAULT '',
                channel_message_id INTEGER,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        for column, ddl in (
            ("source_type", "TEXT NOT NULL DEFAULT ''"),
            ("source_detail", "TEXT NOT NULL DEFAULT ''"),
            ("first_source_type", "TEXT NOT NULL DEFAULT ''"),
            ("first_source_detail", "TEXT NOT NULL DEFAULT ''"),
            ("first_entry_at", "TEXT NOT NULL DEFAULT ''"),
            ("latest_touch_at", "TEXT NOT NULL DEFAULT ''"),
            ("entry_action", "TEXT NOT NULL DEFAULT ''"),
            ("deep_link_payload", "TEXT NOT NULL DEFAULT ''"),
            ("channel_message_id", "INTEGER"),
        ):
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
            if column not in cols:
                conn.execute(f"ALTER TABLE leads ADD COLUMN {column} {ddl}")
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_leads_source_type_created
                ON leads(source_type, created_at);
            CREATE INDEX IF NOT EXISTS idx_leads_status_created
                ON leads(lead_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_attr_latest
                ON user_attribution(latest_touch_at);
            """
        )


def get_user_attribution(user_id: int) -> dict[str, Any] | None:
    ensure_attribution_schema()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM user_attribution WHERE user_id=? LIMIT 1",
            (int(user_id),),
        ).fetchone()
    return row_to_dict(row)
