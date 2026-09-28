"""Side-by-side SQLite persistence for V3 User Bot appointments.

This repository deliberately owns only ``appointments_v3`` plus the minimal
``listings_v3`` availability operations required by the extracted appointment
domain. It never reads or writes the legacy ``appointments`` table and its
constructor performs no schema initialization; additive DDL is executed only by
``initialize_v3_storage``.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any, Sequence


UNFINISHED_APPOINTMENT_STATUSES = frozenset(
    {"pending", "assigned", "contacted", "confirmed"}
)


class SQLiteAppointmentRepository:
    DDL = """
    CREATE TABLE IF NOT EXISTS appointments_v3 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        username TEXT NOT NULL DEFAULT '',
        display_name TEXT NOT NULL DEFAULT '',
        listing_id TEXT NOT NULL,
        viewing_mode TEXT NOT NULL DEFAULT 'offline',
        appointment_date TEXT NOT NULL,
        appointment_time TEXT NOT NULL,
        contact_value TEXT NOT NULL DEFAULT '',
        note TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(listing_id) REFERENCES listings_v3(listing_id)
    );
    CREATE INDEX IF NOT EXISTS idx_appointments_v3_user
        ON appointments_v3(user_id, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_appointments_v3_listing_status
        ON appointments_v3(listing_id, status);
    CREATE INDEX IF NOT EXISTS idx_appointments_v3_duplicate
        ON appointments_v3(
            user_id, listing_id, viewing_mode,
            appointment_date, appointment_time, status
        );
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def find_exact_unfinished(
        self,
        *,
        user_id: int,
        listing_id: str,
        mode: str,
        date: str,
        time: str,
    ) -> dict[str, Any] | None:
        statuses = tuple(sorted(UNFINISHED_APPOINTMENT_STATUSES))
        placeholders = ",".join("?" for _ in statuses)
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT * FROM appointments_v3
                    WHERE user_id=? AND listing_id=? AND viewing_mode=?
                      AND appointment_date=? AND appointment_time=?
                      AND status IN ({placeholders})
                    ORDER BY id DESC
                    LIMIT 1""",
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
                "SELECT * FROM appointments_v3 WHERE id=? AND user_id=?",
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
            cursor = conn.execute(
                """INSERT INTO appointments_v3
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
            appointment_id = int(cursor.lastrowid)
            conn.commit()
        return appointment_id

    def update_schedule(
        self,
        *,
        appointment_id: int,
        user_id: int,
        mode: str,
        date: str,
        time: str,
        status: str,
    ) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE appointments_v3
                   SET viewing_mode=?,appointment_date=?,appointment_time=?,status=?,
                       updated_at=CURRENT_TIMESTAMP
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
        return int(cursor.rowcount or 0) == 1

    def update_status(
        self,
        *,
        appointment_id: int,
        user_id: int,
        status: str,
    ) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE appointments_v3
                   SET status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=? AND user_id=?""",
                (str(status or "").strip(), int(appointment_id), int(user_id)),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1

    def get_inventory_status(self, listing_id: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT inventory_status FROM listings_v3 WHERE listing_id=?",
                (str(listing_id or "").strip(),),
            ).fetchone()
        return str(row["inventory_status"]) if row is not None else None

    def count_appointments_by_statuses(
        self,
        *,
        listing_id: str,
        statuses: Sequence[str],
    ) -> int:
        clean_statuses = tuple(
            str(value).strip() for value in statuses if str(value or "").strip()
        )
        if not clean_statuses:
            return 0
        placeholders = ",".join("?" for _ in clean_statuses)
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*) AS count FROM appointments_v3
                    WHERE listing_id=? AND status IN ({placeholders})""",
                (str(listing_id or "").strip(), *clean_statuses),
            ).fetchone()
        return int(row["count"] if row is not None else 0)

    def update_inventory_status(self, *, listing_id: str, status: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE listings_v3
                   SET inventory_status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE listing_id=?""",
                (str(status or "").strip(), str(listing_id or "").strip()),
            )
            conn.commit()
        return int(cursor.rowcount or 0) == 1


class V3ListingBookabilityChecker:
    """Recheck the durable public publication boundary at appointment submit."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def is_bookable(self, listing_id: str) -> bool:
        clean = str(listing_id or "").strip()
        if not clean:
            return False
        with self._connect() as conn:
            row = conn.execute(
                """SELECT 1
                   FROM listings_v3 l
                   JOIN listing_offers o
                     ON o.listing_id=l.listing_id
                    AND o.offer_type='rent'
                    AND o.offer_status='active'
                    AND o.publication_policy='telegram_rent'
                   JOIN publication_instances pi
                     ON pi.listing_id=l.listing_id
                    AND pi.offer_id=o.offer_id
                    AND pi.platform='telegram'
                    AND pi.publish_status='published'
                   JOIN publication_packages_v3 pp
                     ON pp.package_id=pi.package_id
                    AND pp.listing_id=l.listing_id
                    AND pp.offer_id=o.offer_id
                    AND pp.status IN ('approved','published')
                   WHERE l.listing_id=?
                     AND l.inventory_status IN ('active','reserved')
                   LIMIT 1""",
                (clean,),
            ).fetchone()
        return row is not None


__all__ = [
    "SQLiteAppointmentRepository",
    "UNFINISHED_APPOINTMENT_STATUSES",
    "V3ListingBookabilityChecker",
]
