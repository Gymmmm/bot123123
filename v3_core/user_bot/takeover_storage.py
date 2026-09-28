"""Production storage adapters for the V3 User Bot takeover.

These adapters deliberately reuse the live 3858 operational tables for
appointments, leads, and tenant bindings while keeping modern inventory and
repair-ticket contracts intact. Constructors do not create or migrate schema.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any, Sequence

from v3_core.storage.service_repository import (
    SQLiteTenantServiceRepository,
    TenantBinding,
)
from .appointment_history import AppointmentHistoryRecord, SQLiteAppointmentHistoryReader
from .admin_appointments import AdminAppointmentReader


UNFINISHED_APPOINTMENT_STATUSES = frozenset(
    {"pending", "assigned", "contacted", "confirmed"}
)


class ProductionAppointmentRepository:
    """Appointment persistence against the live appointments table."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def find_exact_unfinished(
        self, *, user_id: int, listing_id: str, mode: str, date: str, time: str
    ) -> dict[str, Any] | None:
        statuses = tuple(sorted(UNFINISHED_APPOINTMENT_STATUSES))
        placeholders = ",".join("?" for _ in statuses)
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT * FROM appointments
                    WHERE user_id=? AND listing_id=? AND viewing_mode=?
                      AND appointment_date=? AND appointment_time=?
                      AND status IN ({placeholders})
                    ORDER BY id DESC LIMIT 1""",
                (
                    int(user_id),
                    str(listing_id or "").strip(),
                    str(mode or "offline").strip(),
                    str(date or "").strip(),
                    str(time or "").strip(),
                    *statuses,
                ),
            ).fetchone()
        return dict(row) if row is not None else None

    def get_for_user(self, appointment_id: int, user_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM appointments WHERE id=? AND user_id=?",
                (int(appointment_id), int(user_id)),
            ).fetchone()
        return dict(row) if row is not None else None

    def create(self, values: dict[str, Any]) -> int:
        payload = dict(values or {})
        listing_id = str(payload.get("listing_id") or "").strip()
        appointment_date = str(payload.get("appointment_date") or "").strip()
        appointment_time = str(payload.get("appointment_time") or "").strip()
        if not listing_id:
            raise ValueError("appointment_listing_id_required")
        if not appointment_date or not appointment_time:
            raise ValueError("appointment_schedule_required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """INSERT INTO appointments
                   (user_id,username,display_name,listing_id,viewing_mode,
                    appointment_date,appointment_time,contact_value,note,status,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))""",
                (
                    int(payload.get("user_id") or 0),
                    str(payload.get("username") or ""),
                    str(payload.get("display_name") or ""),
                    listing_id,
                    str(payload.get("viewing_mode") or "offline"),
                    appointment_date,
                    appointment_time,
                    str(payload.get("contact_value") or ""),
                    str(payload.get("note") or ""),
                    str(payload.get("status") or "pending"),
                    str(payload.get("created_at") or ""),
                ),
            )
            appointment_id = int(cur.lastrowid)
            conn.commit()
        return appointment_id

    def update_schedule(
        self, *, appointment_id: int, user_id: int, mode: str,
        date: str, time: str, status: str
    ) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """UPDATE appointments
                   SET viewing_mode=?,appointment_date=?,appointment_time=?,status=?
                   WHERE id=? AND user_id=?""",
                (
                    str(mode or "offline").strip(),
                    str(date or "").strip(),
                    str(time or "").strip(),
                    str(status or "pending").strip(),
                    int(appointment_id),
                    int(user_id),
                ),
            )
            conn.commit()
        return int(cur.rowcount or 0) == 1

    def update_status(self, *, appointment_id: int, user_id: int, status: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE appointments SET status=? WHERE id=? AND user_id=?",
                (str(status or "").strip(), int(appointment_id), int(user_id)),
            )
            conn.commit()
        return int(cur.rowcount or 0) == 1

    def get_inventory_status(self, listing_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT inventory_status FROM listings_v3 WHERE listing_id=?",
                (str(listing_id or "").strip(),),
            ).fetchone()
        return str(row["inventory_status"]) if row is not None else None

    def count_appointments_by_statuses(
        self, *, listing_id: str, statuses: Sequence[str]
    ) -> int:
        clean = tuple(str(v).strip() for v in statuses if str(v or "").strip())
        if not clean:
            return 0
        placeholders = ",".join("?" for _ in clean)
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*) AS count FROM appointments
                    WHERE listing_id=? AND status IN ({placeholders})""",
                (str(listing_id or "").strip(), *clean),
            ).fetchone()
        return int(row["count"] if row is not None else 0)

    def update_inventory_status(self, *, listing_id: str, status: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """UPDATE listings_v3
                   SET inventory_status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE listing_id=?""",
                (str(status or "").strip(), str(listing_id or "").strip()),
            )
            conn.commit()
        return int(cur.rowcount or 0) == 1


class ProductionAppointmentHistoryReader(SQLiteAppointmentHistoryReader):
    """Read takeover history from the live appointments table."""

    def list_for_user(self, user_id: int, *, limit: int = 20) -> tuple[AppointmentHistoryRecord, ...]:
        cap = max(1, min(int(limit or 20), 100))
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT a.id,a.listing_id,a.viewing_mode,a.appointment_date,a.appointment_time,
                          a.status,a.created_at,l.public_listing_id
                   FROM appointments a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE a.user_id=?
                   ORDER BY a.created_at DESC,a.id DESC
                   LIMIT ?""",
                (int(user_id), cap),
            ).fetchall()
        records: list[AppointmentHistoryRecord] = []
        from v3_core.publishing.public_ids import normalize_public_id
        for row in rows:
            public_id = normalize_public_id(row["public_listing_id"]) or normalize_public_id(row["listing_id"])
            if public_id is None:
                continue
            records.append(
                AppointmentHistoryRecord(
                    appointment_id=int(row["id"]),
                    public_listing_id=public_id,
                    viewing_mode=str(row["viewing_mode"] or "offline"),
                    appointment_date=str(row["appointment_date"] or ""),
                    appointment_time=str(row["appointment_time"] or ""),
                    status=str(row["status"] or "pending").strip().lower(),
                    created_at=str(row["created_at"] or ""),
                )
            )
        return tuple(records)


class ProductionLeadRepository:
    """Lead writes against the live advisor-console leads table."""

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
        action = str(payload.get("action") or "").strip()
        created_at = str(payload.get("created_at") or "").strip()
        if user_id <= 0:
            raise ValueError("lead_user_id_required")
        if not action:
            raise ValueError("lead_action_required")
        if not created_at:
            raise ValueError("lead_created_at_required")

        raw_payload = payload.get("payload")
        if isinstance(raw_payload, dict):
            parsed = dict(raw_payload)
        elif isinstance(raw_payload, str):
            try:
                parsed = json.loads(raw_payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = {"raw": raw_payload}
        else:
            parsed = {}
        payload_json = json.dumps(parsed, ensure_ascii=False, sort_keys=True)
        raw_message_id = payload.get("message_id")
        message_id = None if raw_message_id in (None, "", 0, "0") else int(raw_message_id)
        source = str(payload.get("source") or "")
        source_detail = str(payload.get("source_detail") or action)
        listing_id = str(payload.get("listing_id") or "").strip()
        if listing_id and not listing_id.upper().startswith("QL-"):
            with self._connect() as lookup:
                row = lookup.execute(
                    "SELECT public_listing_id FROM listings_v3 WHERE listing_id=? LIMIT 1",
                    (listing_id,),
                ).fetchone()
            if row is not None:
                listing_id = str(row["public_listing_id"] or listing_id).strip()
        deep_link = str(
            payload.get("deep_link_payload")
            or parsed.get("start_payload")
            or parsed.get("deep_link_payload")
            or ""
        )
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """INSERT INTO leads
                   (user_id,username,display_name,source,action,listing_id,area,
                    property_type,budget_min,budget_max,payload_json,message_id,
                    post_token,caption_variant,agent_id,response_at,conversion_value,
                    advisor_id,advisor_name,assigned_at,intent,lead_status,
                    source_type,source_detail,first_source_type,first_source_detail,
                    first_entry_at,latest_touch_at,entry_action,deep_link_payload,
                    channel_message_id,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    user_id,
                    str(payload.get("username") or ""),
                    str(payload.get("display_name") or ""),
                    source,
                    action,
                    listing_id,
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
                    source,
                    source_detail,
                    source,
                    source_detail,
                    str(payload.get("first_entry_at") or created_at),
                    str(payload.get("latest_touch_at") or created_at),
                    str(payload.get("entry_action") or action),
                    deep_link,
                    message_id,
                    created_at,
                ),
            )
            lead_id = int(cur.lastrowid)
            conn.commit()
        return lead_id

    def get(self, lead_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM leads WHERE id=?", (int(lead_id),)).fetchone()
        if row is None:
            return None
        result = dict(row)
        try:
            result["payload"] = json.loads(result.pop("payload_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            result["payload"] = {}
        return result

    def find_tenancy_request(self, *, user_id: int, action: str, binding_id: int) -> int | None:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id,payload_json FROM leads
                   WHERE user_id=? AND action=? ORDER BY id DESC""",
                (int(user_id), str(action or "")),
            ).fetchall()
        for row in rows:
            try:
                payload = json.loads(str(row["payload_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            try:
                existing = int(payload.get("binding_id") or 0)
            except (TypeError, ValueError):
                existing = 0
            if existing == int(binding_id):
                return int(row["id"])
        return None


class ProductionTenantServiceRepository(SQLiteTenantServiceRepository):
    """Read live tenant bindings; keep repair writes in repair_tickets_v3."""

    def get_active_binding(self, user_id: int) -> TenantBinding | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT id,user_id,binding_code,property_name,lease_end_date,rent_day,
                          monthly_rent,contract_start_date,contract_end_date,deposit_months,
                          contract_notes,status
                   FROM tenant_bindings
                   WHERE user_id=? AND status='active'
                   ORDER BY id DESC LIMIT 1""",
                (int(user_id),),
            ).fetchone()
        return None if row is None else self._binding(row)


class ProductionAdminAppointmentReader(AdminAppointmentReader):
    """Admin appointment console over the same live appointments table."""

    @classmethod
    def _public_row(cls, conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        value = super()._public_row(conn, row)
        if not str(value.get("public_listing_id") or "").strip():
            from v3_core.publishing.public_ids import normalize_public_id
            direct = normalize_public_id(value.get("listing_id"))
            if direct:
                value["public_listing_id"] = direct
        return value

    @staticmethod
    def _select_sql(where_clause: str) -> str:
        return f"""SELECT a.*,l.public_listing_id,l.display_title,l.project_name
                   FROM appointments a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   WHERE {where_clause}"""

    def update_status(self, appointment_id: int, status: str):
        clean_status = str(status or "").strip().lower()
        from .admin_appointments import _ALLOWED_PREVIOUS_BY_TARGET
        allowed_previous = _ALLOWED_PREVIOUS_BY_TARGET.get(clean_status)
        if allowed_previous is None:
            raise ValueError("unsupported_admin_appointment_status")
        current = self.get(int(appointment_id))
        if current is None:
            return None, False
        previous = str(current.get("status") or "pending").strip().lower()
        if previous == clean_status or previous not in allowed_previous:
            return current, False
        with self._connect(readonly=False) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE appointments SET status=? WHERE id=? AND status=?",
                (clean_status, int(appointment_id), previous),
            )
            conn.commit()
        changed = int(cur.rowcount or 0) == 1
        return self.get(int(appointment_id)), changed


__all__ = [
    "ProductionAdminAppointmentReader",
    "ProductionAppointmentHistoryReader",
    "ProductionAppointmentRepository",
    "ProductionLeadRepository",
    "ProductionTenantServiceRepository",
]
