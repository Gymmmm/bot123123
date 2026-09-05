"""归因存储：复用 leads + user_attribution，不重建业务表。"""
from __future__ import annotations
import json
from typing import Any
from .common import db
from .db import row_to_dict


def ensure_attribution_schema() -> None:
    with db.connect() as conn:
        conn.execute("""
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
            )""")
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
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
            if column not in cols:
                conn.execute(f"ALTER TABLE leads ADD COLUMN {column} {ddl}")
                cols.add(column)
        conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_leads_source_type_created ON leads(source_type, created_at);
            CREATE INDEX IF NOT EXISTS idx_leads_status_created ON leads(lead_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_attr_latest ON user_attribution(latest_touch_at);
        """)


def get_user_attribution(user_id: int):
    ensure_attribution_schema()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM user_attribution WHERE user_id=? LIMIT 1", (int(user_id),)).fetchone()
    return row_to_dict(row)


def upsert_user_attribution(touch: dict[str, Any]) -> None:
    ensure_attribution_schema()
    user_id = int(touch.get("user_id") or 0)
    if user_id <= 0:
        return
    with db.connect() as conn:
        conn.execute("""
            INSERT INTO user_attribution (
                user_id, username, display_name,
                first_source_type, first_source_detail, first_entry_at,
                first_listing_id, first_deep_link, first_entry_action, first_legacy,
                latest_source_type, latest_source_detail, latest_touch_at,
                latest_listing_id, latest_deep_link, latest_entry_action,
                channel_message_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                display_name=excluded.display_name,
                latest_source_type=excluded.latest_source_type,
                latest_source_detail=excluded.latest_source_detail,
                latest_touch_at=excluded.latest_touch_at,
                latest_listing_id=excluded.latest_listing_id,
                latest_deep_link=excluded.latest_deep_link,
                latest_entry_action=excluded.latest_entry_action,
                channel_message_id=COALESCE(excluded.channel_message_id, user_attribution.channel_message_id),
                payload_json=excluded.payload_json
        """, (
            user_id, str(touch.get("username") or ""), str(touch.get("display_name") or ""),
            str(touch.get("first_source_type") or ""), str(touch.get("first_source_detail") or ""),
            str(touch.get("first_entry_at") or ""), str(touch.get("first_listing_id") or ""),
            str(touch.get("first_deep_link") or ""), str(touch.get("first_entry_action") or ""),
            1 if touch.get("first_legacy") or touch.get("legacy") else 0,
            str(touch.get("latest_source_type") or ""), str(touch.get("latest_source_detail") or ""),
            str(touch.get("latest_touch_at") or ""), str(touch.get("latest_listing_id") or ""),
            str(touch.get("latest_deep_link") or ""), str(touch.get("latest_entry_action") or ""),
            touch.get("channel_message_id"), json.dumps({"legacy": bool(touch.get("legacy"))}, ensure_ascii=False),
        ))


def apply_lead_attribution_columns(lead_id, touch):
    if not lead_id:
        return
    ensure_attribution_schema()
    with db.connect() as conn:
        conn.execute("""
            UPDATE leads SET source_type=?, source_detail=?, first_source_type=?, first_source_detail=?,
                first_entry_at=?, latest_touch_at=?, entry_action=?, deep_link_payload=?,
                channel_message_id=COALESCE(?, channel_message_id) WHERE id=?
        """, (
            str(touch.get("latest_source_type") or touch.get("source_type") or ""),
            str(touch.get("latest_source_detail") or touch.get("source_detail") or ""),
            str(touch.get("first_source_type") or ""), str(touch.get("first_source_detail") or ""),
            str(touch.get("first_entry_at") or ""), str(touch.get("latest_touch_at") or ""),
            str(touch.get("entry_action") or ""),
            str(touch.get("deep_link_payload") or touch.get("latest_deep_link") or ""),
            touch.get("channel_message_id"), int(lead_id),
        ))


def list_leads_by_status(status=None, limit=20):
    ensure_attribution_schema()
    sql, params = "SELECT * FROM leads", []
    if status and status != "all":
        sql += " WHERE lead_status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))
    with db.connect() as conn:
        return [row_to_dict(r) or {} for r in conn.execute(sql, params).fetchall()]


def list_listing_leads(limit=20):
    ensure_attribution_schema()
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM leads WHERE listing_id IS NOT NULL AND listing_id <> '' ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
    return [row_to_dict(r) or {} for r in rows]


def source_stats(limit=20):
    ensure_attribution_schema()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT CASE WHEN first_source_type <> '' THEN first_source_type ELSE source_type END AS src, COUNT(*) AS total
            FROM leads GROUP BY src ORDER BY total DESC LIMIT ?""", (int(limit),)).fetchall()
    return [row_to_dict(r) or {} for r in rows]


def list_today_appointments(today_prefix, limit=20):
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM appointments WHERE appointment_date LIKE ? OR created_at LIKE ? ORDER BY id DESC LIMIT ?",
                            (f"%{today_prefix}%", f"{today_prefix}%", int(limit))).fetchall()
    return [row_to_dict(r) or {} for r in rows]


def list_service_tickets(limit=20):
    if "repair_tickets" not in db._table_names():
        return []
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM repair_tickets ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
    return [row_to_dict(r) or {} for r in rows]


def update_lead_status(lead_id, status, *, advisor_id="", advisor_name=""):
    allowed = {"new", "followup", "claimed", "contacted", "booked", "done", "converted", "invalid"}
    normalized = str(status or "").strip()
    if normalized not in allowed:
        return False
    ensure_attribution_schema()
    if normalized in {"new", "claimed", "contacted", "invalid", "converted"}:
        return db.update_lead_workflow(lead_id, status=normalized, advisor_id=advisor_id, advisor_name=advisor_name)
    with db.connect() as conn:
        cur = conn.execute("""UPDATE leads SET lead_status=?,
            advisor_id=CASE WHEN ?<>'' THEN ? ELSE advisor_id END,
            advisor_name=CASE WHEN ?<>'' THEN ? ELSE advisor_name END WHERE id=?""",
            (normalized, advisor_id, advisor_id, advisor_name, advisor_name, int(lead_id)))
        return cur.rowcount > 0
