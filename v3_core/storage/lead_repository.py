"""Additive V3 persistence for User Bot leads.

The columns mirror the locked production ``leads`` contract while writing to a
separate ``leads_v3`` table. Construction performs no schema initialization;
DDL is executed only by the explicit V3 storage bootstrap.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any


DDL = """
CREATE TABLE IF NOT EXISTS leads_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL DEFAULT '',
    listing_id TEXT NOT NULL DEFAULT '',
    area TEXT NOT NULL DEFAULT '',
    property_type TEXT NOT NULL DEFAULT '',
    budget_min INTEGER,
    budget_max INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}',
    message_id INTEGER,
    post_token TEXT NOT NULL DEFAULT '',
    caption_variant TEXT NOT NULL DEFAULT '',
    agent_id TEXT NOT NULL DEFAULT '',
    response_at TEXT NOT NULL DEFAULT '',
    conversion_value REAL NOT NULL DEFAULT 0,
    advisor_id TEXT NOT NULL DEFAULT '',
    advisor_name TEXT NOT NULL DEFAULT '',
    assigned_at TEXT NOT NULL DEFAULT '',
    intent TEXT NOT NULL DEFAULT '',
    lead_status TEXT NOT NULL DEFAULT 'new',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leads_v3_user_created
    ON leads_v3(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_leads_v3_listing_action
    ON leads_v3(listing_id, action);
"""


class SQLiteLeadRepository:
    DDL = DDL

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def create(self, values: dict[str, Any]) -> int:
        payload = dict(values or {})
        user_id = int(payload.get("user_id") or 0)
        if user_id <= 0:
            raise ValueError("lead_user_id_required")
        action = str(payload.get("action") or "").strip()
        if not action:
            raise ValueError("lead_action_required")
        created_at = str(payload.get("created_at") or "").strip()
        if not created_at:
            raise ValueError("lead_created_at_required")

        raw_payload = payload.get("payload")
        if isinstance(raw_payload, str):
            try:
                parsed = json.loads(raw_payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = {"raw": raw_payload}
        elif isinstance(raw_payload, dict):
            parsed = raw_payload
        else:
            parsed = {}
        payload_json = json.dumps(parsed, ensure_ascii=False, sort_keys=True)

        message_id = payload.get("message_id")
        if message_id in (None, "", 0, "0"):
            message_id = None
        else:
            message_id = int(message_id)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """INSERT INTO leads_v3
                   (user_id,username,display_name,source,action,listing_id,area,
                    property_type,budget_min,budget_max,payload_json,message_id,
                    post_token,caption_variant,agent_id,response_at,conversion_value,
                    advisor_id,advisor_name,assigned_at,intent,lead_status,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    user_id,
                    str(payload.get("username") or ""),
                    str(payload.get("display_name") or ""),
                    str(payload.get("source") or ""),
                    action,
                    str(payload.get("listing_id") or ""),
                    str(payload.get("area") or ""),
                    str(payload.get("property_type") or ""),
                    payload.get("budget_min"),
                    payload.get("budget_max"),
                    payload_json,
                    message_id,
                    str(payload.get("post_token") or ""),
                    str(payload.get("caption_variant") or ""),
                    str(payload.get("agent_id") or ""),
                    str(payload.get("response_at") or ""),
                    float(payload.get("conversion_value") or 0),
                    str(payload.get("advisor_id") or ""),
                    str(payload.get("advisor_name") or ""),
                    str(payload.get("assigned_at") or ""),
                    str(payload.get("intent") or ""),
                    str(payload.get("lead_status") or "new"),
                    created_at,
                ),
            )
            lead_id = int(cursor.lastrowid)
            conn.commit()
        return lead_id

    def get(self, lead_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM leads_v3 WHERE id=?",
                (int(lead_id),),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        try:
            result["payload"] = json.loads(result.pop("payload_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            result["payload"] = {}
        return result


__all__ = ["DDL", "SQLiteLeadRepository"]
