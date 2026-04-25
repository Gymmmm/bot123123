from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from .config import DB_PATH, logger

LISTING_STATUSES = {"active", "rented", "inactive"}

SCHEMA = '''
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS listings (
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
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL DEFAULT '',
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS favorites (
    user_id INTEGER NOT NULL,
    listing_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, listing_id)
);

CREATE TABLE IF NOT EXISTS leads (
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
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS appointments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    listing_id TEXT NOT NULL DEFAULT '',
    viewing_mode TEXT NOT NULL DEFAULT '',
    appointment_date TEXT NOT NULL DEFAULT '',
    appointment_time TEXT NOT NULL DEFAULT '',
    contact_value TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenant_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    binding_code TEXT NOT NULL UNIQUE,
    property_name TEXT NOT NULL DEFAULT '',
    lease_end_date TEXT NOT NULL DEFAULT '',
    rent_day INTEGER,
    monthly_rent REAL NOT NULL DEFAULT 0,
    contract_start_date TEXT NOT NULL DEFAULT '',
    contract_end_date TEXT NOT NULL DEFAULT '',
    deposit_months INTEGER NOT NULL DEFAULT 2,
    contract_notes TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repair_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    binding_id INTEGER,
    issue_type TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    user_id INTEGER PRIMARY KEY,
    daily_enabled INTEGER NOT NULL DEFAULT 1,
    area_alerts_json TEXT NOT NULL DEFAULT '[]',
    lease_reminder_enabled INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS renewal_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    binding_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    listing_id TEXT NOT NULL DEFAULT '',
    renewal_status TEXT NOT NULL DEFAULT 'pending',
    user_response TEXT NOT NULL DEFAULT '',
    advisor_notes TEXT NOT NULL DEFAULT '',
    contacted_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lease_reminder_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    binding_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    lease_end_date TEXT NOT NULL DEFAULT '',
    remind_for_date TEXT NOT NULL DEFAULT '',
    remind_type TEXT NOT NULL DEFAULT '',
    remind_date TEXT NOT NULL DEFAULT '',
    sent_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publish_analytics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id TEXT NOT NULL DEFAULT '',
    post_id TEXT NOT NULL DEFAULT '',
    message_id INTEGER,
    listing_id TEXT NOT NULL DEFAULT '',
    area TEXT NOT NULL DEFAULT '',
    property_type TEXT NOT NULL DEFAULT '',
    monthly_rent REAL NOT NULL DEFAULT 0,
    caption_variant TEXT NOT NULL DEFAULT 'a',
    publish_hour INTEGER,
    publish_day_of_week INTEGER,
    published_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
'''

def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


class Database:
    def __init__(self, path: Path | str = DB_PATH):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_column(conn, "leads", "message_id", "INTEGER")
            self._ensure_column(conn, "leads", "post_token", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "leads", "caption_variant", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "leads", "agent_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "leads", "response_at", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "leads", "conversion_value", "REAL NOT NULL DEFAULT 0")

    def _table_columns(self, table: str) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row["name"]) for row in rows}

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column in cols:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def upsert_user(self, user_id: int, username: str, first_name: str, last_name: str, ts: str) -> None:
        with self.connect() as conn:
            conn.execute(
                '''
                INSERT INTO users (user_id, username, first_name, last_name, created_at, last_active_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    last_active_at=excluded.last_active_at
                ''',
                (user_id, username or "", first_name or "", last_name or "", ts, ts),
            )

    def next_listing_id(self) -> str:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT listing_id FROM listings WHERE listing_id LIKE 'l_%'"
            ).fetchall()
        numbers = []
        for row in rows:
            raw = str(row["listing_id"] or "")
            if raw.startswith("l_") and raw[2:].isdigit():
                numbers.append(int(raw[2:]))
        if numbers:
            return f"l_{max(numbers) + 1}"
        return "l_1001"

    def create_listing(self, data: dict[str, Any]) -> None:
        tags_json = json.dumps(data.get("tags", []), ensure_ascii=False)
        with self.connect() as conn:
            conn.execute(
                '''
                INSERT INTO listings (
                    listing_id, title, property_type, area, community, price, currency, layout, size_sqm,
                    tags_json, highlights, hidden_costs, drawbacks, deposit_rule, available_date,
                    media_file_id, media_type, channel_message_id, source_post_url, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    data["listing_id"],
                    data["title"],
                    data["property_type"],
                    data["area"],
                    data["community"],
                    int(data["price"]),
                    data.get("currency", "USD"),
                    data.get("layout", ""),
                    data.get("size_sqm", ""),
                    tags_json,
                    data.get("highlights", ""),
                    data.get("hidden_costs", ""),
                    data.get("drawbacks", ""),
                    data.get("deposit_rule", ""),
                    data.get("available_date", ""),
                    data.get("media_file_id", ""),
                    data.get("media_type", ""),
                    data.get("channel_message_id"),
                    data.get("source_post_url", ""),
                    data.get("status", "active"),
                    data["created_at"],
                    data["updated_at"],
                ),
            )

    def update_listing_publish_meta(self, listing_id: str, *, channel_message_id: int | None, source_post_url: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE listings SET channel_message_id=?, source_post_url=?, updated_at=datetime('now', 'localtime') WHERE listing_id=?",
                (channel_message_id, source_post_url, listing_id),
            )

    def update_listing_status(self, listing_id: str, status: str) -> bool:
        if status not in LISTING_STATUSES:
            return False
        with self.connect() as conn:
            cur = conn.execute(
                "UPDATE listings SET status=?, updated_at=datetime('now', 'localtime') WHERE listing_id=?",
                (status, listing_id),
            )
            return cur.rowcount > 0

    def get_listing(self, listing_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM listings WHERE listing_id=?", (listing_id,)).fetchone()
        item = row_to_dict(row)
        if item:
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
        return item

    def list_recent_listings(self, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM listings ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        result = []
        for row in rows:
            item = row_to_dict(row) or {}
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
            result.append(item)
        return result

    def search_listings(
        self,
        *,
        property_type: str | None = None,
        areas: Iterable[str] | None = None,
        budget_min: int | None = None,
        budget_max: int | None = None,
        ilike_fragment: str | None = None,
        limit: int = 6,
    ) -> list[dict[str, Any]]:
        clauses = ["status='active'"]
        params: list[Any] = []
        if property_type:
            clauses.append("property_type=?")
            params.append(property_type)
        cleaned_areas = [area for area in (areas or []) if area and area != "不限"]
        if cleaned_areas:
            placeholders = ",".join("?" for _ in cleaned_areas)
            clauses.append(f"area IN ({placeholders})")
            params.extend(cleaned_areas)
        if budget_min is not None:
            clauses.append("price>=?")
            params.append(budget_min)
        if budget_max is not None:
            clauses.append("price<=?")
            params.append(budget_max)
        frag = (ilike_fragment or "").strip()
        if frag:
            k = f"%{frag[:120]}%"
            like_cols = [
                "title",
                "highlights",
                "layout",
                "area",
                "community",
                "tags_json",
                "hidden_costs",
                "drawbacks",
            ]
            existing = self._table_columns("listings")
            like_cols = [c for c in like_cols if c in existing]
            if like_cols:
                clauses.append("(" + " OR ".join(f"{c} LIKE ?" for c in like_cols) + ")")
                params.extend([k] * len(like_cols))
        sql = f"SELECT * FROM listings WHERE {' AND '.join(clauses)} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = row_to_dict(row) or {}
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
            items.append(item)
        return items

    def favorite_listing(self, user_id: int, listing_id: str, created_at: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO favorites (user_id, listing_id, created_at) VALUES (?, ?, ?)",
                (user_id, listing_id, created_at),
            )

    def unfavorite_listing(self, user_id: int, listing_id: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM favorites WHERE user_id=? AND listing_id=?", (user_id, listing_id))

    def is_favorite(self, user_id: int, listing_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM favorites WHERE user_id=? AND listing_id=?",
                (user_id, listing_id),
            ).fetchone()
        return row is not None

    def list_favorites(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                '''
                SELECT l.* FROM favorites f
                JOIN listings l ON l.listing_id = f.listing_id
                WHERE f.user_id=?
                ORDER BY f.created_at DESC
                ''',
                (user_id,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            item = row_to_dict(row) or {}
            item["tags"] = json.loads(item.pop("tags_json", "[]") or "[]")
            items.append(item)
        return items

    def create_lead(self, data: dict[str, Any]) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                '''
                INSERT INTO leads (
                    user_id, username, display_name, source, action, listing_id, area,
                    property_type, budget_min, budget_max, payload_json, message_id, post_token,
                    caption_variant, agent_id, response_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    data.get("user_id"),
                    data.get("username", ""),
                    data.get("display_name", ""),
                    data.get("source", ""),
                    data.get("action", ""),
                    data.get("listing_id", ""),
                    data.get("area", ""),
                    data.get("property_type", ""),
                    data.get("budget_min"),
                    data.get("budget_max"),
                    json.dumps(data.get("payload", {}), ensure_ascii=False),
                    data.get("message_id"),
                    data.get("post_token", ""),
                    data.get("caption_variant", ""),
                    data.get("agent_id", ""),
                    data.get("response_at", ""),
                    data["created_at"],
                ),
            )
            return int(cur.lastrowid)

    def mark_lead_responded(self, lead_id: int, *, agent_id: str, response_at: str) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE leads
                SET agent_id=?, response_at=?
                WHERE id=?
                """,
                (agent_id or "", response_at or "", int(lead_id)),
            )
            return cur.rowcount > 0

    def create_appointment(self, data: dict[str, Any]) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                '''
                INSERT INTO appointments (
                    user_id, username, display_name, listing_id, viewing_mode,
                    appointment_date, appointment_time, contact_value, note, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    data["user_id"],
                    data.get("username", ""),
                    data.get("display_name", ""),
                    data.get("listing_id", ""),
                    data.get("viewing_mode", ""),
                    data.get("appointment_date", ""),
                    data.get("appointment_time", ""),
                    data.get("contact_value", ""),
                    data.get("note", ""),
                    data.get("status", "pending"),
                    data["created_at"],
                ),
            )
            return int(cur.lastrowid)

    def list_appointments(self, user_id: int, limit: int = 10) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM appointments WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
        return [row_to_dict(row) or {} for row in rows]

    def create_binding(
        self,
        user_id: int,
        binding_code: str,
        property_name: str,
        lease_end_date: str,
        rent_day: int | None,
        created_at: str,
        status: str = "active",
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                '''
                INSERT INTO tenant_bindings (
                    user_id, binding_code, property_name, lease_end_date, rent_day,
                    monthly_rent, contract_start_date, contract_end_date, deposit_months, contract_notes,
                    status, created_at
                )
                VALUES (?, ?, ?, ?, ?, 0, '', '', 2, '', ?, ?)
                ''',
                (user_id, binding_code, property_name, lease_end_date, rent_day, status, created_at),
            )
            return int(cur.lastrowid)

    def get_active_binding(self, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM tenant_bindings WHERE user_id=? AND status='active' ORDER BY id DESC LIMIT 1",
                (user_id,),
            ).fetchone()
        return row_to_dict(row)

    def get_binding_by_id(self, binding_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM tenant_bindings WHERE id=? LIMIT 1",
                (binding_id,),
            ).fetchone()
        return row_to_dict(row)

    def list_active_bindings_by_property(self, keyword: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT b.*, u.first_name, u.username
                FROM tenant_bindings b
                LEFT JOIN users u ON u.user_id = b.user_id
                WHERE b.status='active'
                  AND b.property_name LIKE ?
                ORDER BY b.id ASC
                """,
                (f"%{keyword.strip()}%",),
            ).fetchall()
        return [row_to_dict(row) or {} for row in rows]

    def list_all_active_bindings(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT b.*, u.first_name, u.username
                FROM tenant_bindings b
                LEFT JOIN users u ON u.user_id = b.user_id
                WHERE b.status='active'
                  AND b.user_id > 0
                ORDER BY b.id ASC
                """,
            ).fetchall()
        return [row_to_dict(row) or {} for row in rows]

    def list_bindings_with_rent_day(self, day: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT b.*, u.first_name, u.username
                FROM tenant_bindings b
                LEFT JOIN users u ON u.user_id = b.user_id
                WHERE b.status='active'
                  AND b.rent_day = ?
                ORDER BY b.id ASC
                """,
                (day,),
            ).fetchall()
        return [row_to_dict(row) or {} for row in rows]

    def list_bindings_expiring_on(self, date_str: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT b.*, u.first_name, u.username
                FROM tenant_bindings b
                LEFT JOIN users u ON u.user_id = b.user_id
                WHERE b.status='active'
                  AND COALESCE(NULLIF(b.contract_end_date, ''), b.lease_end_date) = ?
                ORDER BY b.id ASC
                """,
                (date_str,),
            ).fetchall()
        return [row_to_dict(row) or {} for row in rows]

    def bind_by_code(self, user_id: int, binding_code: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM tenant_bindings
                WHERE binding_code=?
                  AND COALESCE(NULLIF(status, ''), 'pending') NOT IN ('expired', 'inactive', 'used', 'cancelled')
                LIMIT 1
                """,
                (binding_code,),
            ).fetchone()
            if row is None:
                return None
            owner_user_id = int(row["user_id"] or 0)
            if owner_user_id not in (0, user_id):
                return None
            conn.execute(
                "UPDATE tenant_bindings SET user_id=?, status='active' WHERE id=?",
                (user_id, row["id"]),
            )
            refreshed = conn.execute(
                "SELECT * FROM tenant_bindings WHERE id=? LIMIT 1",
                (row["id"],),
            ).fetchone()
        return row_to_dict(refreshed)

    def create_repair_ticket(self, user_id: int, binding_id: int | None, issue_type: str, description: str, created_at: str) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                '''
                INSERT INTO repair_tickets (user_id, binding_id, issue_type, description, status, created_at)
                VALUES (?, ?, ?, ?, 'new', ?)
                ''',
                (user_id, binding_id, issue_type, description, created_at),
            )
            return int(cur.lastrowid)

    def create_renewal_tracking(
        self,
        *,
        binding_id: int,
        user_id: int,
        listing_id: str = "",
        renewal_status: str = "pending",
        user_response: str = "",
        advisor_notes: str = "",
        created_at: str,
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO renewal_tracking (
                    binding_id, user_id, listing_id, renewal_status, user_response, advisor_notes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    binding_id,
                    user_id,
                    listing_id or "",
                    renewal_status or "pending",
                    user_response or "",
                    advisor_notes or "",
                    created_at,
                ),
            )
            return int(cur.lastrowid)

    def get_open_renewal_tracking(self, *, binding_id: int, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM renewal_tracking
                WHERE binding_id=?
                  AND user_id=?
                  AND COALESCE(NULLIF(renewal_status, ''), 'pending') NOT IN ('completed', 'cancelled', 'closed')
                ORDER BY id DESC
                LIMIT 1
                """,
                (binding_id, user_id),
            ).fetchone()
        return row_to_dict(row)

    def has_reminder_sent(self, *, binding_id: int, remind_type: str, remind_date: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM lease_reminder_logs
                WHERE binding_id=?
                  AND remind_type=?
                  AND COALESCE(NULLIF(remind_date, ''), remind_for_date) = ?
                LIMIT 1
                """,
                (binding_id, remind_type, remind_date),
            ).fetchone()
        return row is not None

    def log_reminder_sent(
        self,
        *,
        binding_id: int,
        user_id: int,
        lease_end_date: str,
        remind_for_date: str,
        remind_type: str,
        sent_at: str,
    ) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO lease_reminder_logs (
                    binding_id, user_id, lease_end_date, remind_for_date, remind_type, remind_date, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    binding_id,
                    user_id,
                    lease_end_date or "",
                    remind_for_date or "",
                    remind_type or "",
                    remind_for_date or "",
                    sent_at,
                ),
            )
            return int(cur.lastrowid)

    def get_subscription(self, user_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
        if row is None:
            return {
                "user_id": user_id,
                "daily_enabled": 1,
                "area_alerts_json": "[]",
                "lease_reminder_enabled": 1,
            }
        return row_to_dict(row) or {}

    def is_lease_reminder_enabled(self, user_id: int) -> bool:
        sub = self.get_subscription(user_id)
        return int(sub.get("lease_reminder_enabled", 1) or 1) == 1

    def toggle_daily_subscription(self, user_id: int, updated_at: str) -> dict[str, Any]:
        current = self.get_subscription(user_id)
        new_value = 0 if int(current.get("daily_enabled", 1)) else 1
        with self.connect() as conn:
            conn.execute(
                '''
                INSERT INTO subscriptions (user_id, daily_enabled, area_alerts_json, lease_reminder_enabled, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    daily_enabled=excluded.daily_enabled,
                    updated_at=excluded.updated_at
                ''',
                (user_id, new_value, current.get("area_alerts_json", "[]"), current.get("lease_reminder_enabled", 1), updated_at),
            )
        return self.get_subscription(user_id)

    def toggle_lease_reminder(self, user_id: int, updated_at: str) -> dict[str, Any]:
        current = self.get_subscription(user_id)
        new_value = 0 if int(current.get("lease_reminder_enabled", 1) or 1) else 1
        with self.connect() as conn:
            conn.execute(
                '''
                INSERT INTO subscriptions (user_id, daily_enabled, area_alerts_json, lease_reminder_enabled, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    lease_reminder_enabled=excluded.lease_reminder_enabled,
                    updated_at=excluded.updated_at
                ''',
                (user_id, current.get("daily_enabled", 1), current.get("area_alerts_json", "[]"), new_value, updated_at),
            )
        return self.get_subscription(user_id)

    def stats(self) -> dict[str, int]:
        with self.connect() as conn:
            listings = conn.execute("SELECT COUNT(*) AS c FROM listings").fetchone()["c"]
            active = conn.execute("SELECT COUNT(*) AS c FROM listings WHERE status='active'").fetchone()["c"]
            leads = conn.execute("SELECT COUNT(*) AS c FROM leads").fetchone()["c"]
            appointments = conn.execute("SELECT COUNT(*) AS c FROM appointments").fetchone()["c"]
            favorites = conn.execute("SELECT COUNT(*) AS c FROM favorites").fetchone()["c"]
        return {
            "listings": int(listings),
            "active_listings": int(active),
            "leads": int(leads),
            "appointments": int(appointments),
            "favorites": int(favorites),
        }


db = Database()
