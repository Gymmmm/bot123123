"""After-rental and operations persistence aligned to V3 inventory/publication identities.

This layer is deliberately additive. It does not create another listing/post truth
model: a rental case points at ``listings_v3`` and, when available, the durable
``publication_instances`` row that brought the customer in. ``draft_id`` is kept
only as provenance because the extraction branch has no independent ``drafts_v3``.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import uuid

DDL = """
CREATE TABLE IF NOT EXISTS rental_cases_v3 (
    case_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    tenant_binding_id INTEGER,
    draft_id TEXT NOT NULL DEFAULT '',
    listing_id TEXT NOT NULL,
    post_id TEXT NOT NULL DEFAULT '',
    offer_id TEXT NOT NULL DEFAULT '',
    contract_start_date TEXT NOT NULL DEFAULT '',
    contract_end_date TEXT NOT NULL DEFAULT '',
    rent_day INTEGER,
    monthly_rent_usd REAL NOT NULL DEFAULT 0,
    deposit_months INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending_handover',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_rental_cases_v3_user_status
ON rental_cases_v3(user_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_rental_cases_v3_listing
ON rental_cases_v3(listing_id, post_id, status);

CREATE TABLE IF NOT EXISTS operations_tasks_v3 (
    task_id TEXT PRIMARY KEY,
    request_token TEXT NOT NULL UNIQUE,
    case_id TEXT NOT NULL DEFAULT '',
    repair_ticket_id INTEGER,
    draft_id TEXT NOT NULL DEFAULT '',
    listing_id TEXT NOT NULL,
    post_id TEXT NOT NULL DEFAULT '',
    task_type TEXT NOT NULL,
    priority TEXT NOT NULL DEFAULT 'normal',
    assignee_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    due_at TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_operations_tasks_v3_queue
ON operations_tasks_v3(status, priority, due_at, created_at);
CREATE INDEX IF NOT EXISTS idx_operations_tasks_v3_listing
ON operations_tasks_v3(listing_id, post_id, status);

CREATE TABLE IF NOT EXISTS operations_task_events_v3 (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    from_status TEXT NOT NULL DEFAULT '',
    to_status TEXT NOT NULL,
    actor_id TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_operations_task_events_v3_task
ON operations_task_events_v3(task_id, created_at);
"""

CASE_STATUSES = frozenset({"pending_handover", "active", "ending", "closed", "cancelled"})
TASK_TYPES = frozenset({"repair", "handover", "cleaning", "utilities", "contract", "rent_followup", "inspection", "moveout", "other"})
TASK_PRIORITIES = frozenset({"low", "normal", "high", "urgent"})
TASK_TRANSITIONS = {
    "new": frozenset({"acknowledged", "assigned", "cancelled"}),
    "acknowledged": frozenset({"assigned", "in_progress", "cancelled"}),
    "assigned": frozenset({"in_progress", "resolved", "cancelled"}),
    "in_progress": frozenset({"resolved", "cancelled"}),
    "resolved": frozenset(),
    "cancelled": frozenset(),
}

@dataclass(frozen=True)
class RentalCase:
    case_id: str
    user_id: int
    draft_id: str
    listing_id: str
    post_id: str
    offer_id: str
    status: str

@dataclass(frozen=True)
class OperationsTask:
    task_id: str
    request_token: str
    case_id: str
    repair_ticket_id: int | None
    draft_id: str
    listing_id: str
    post_id: str
    task_type: str
    priority: str
    assignee_id: str
    status: str
    due_at: str
    summary: str
    detail: str

class AftercareOperationsRepository:
    DDL = DDL

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _case(row: sqlite3.Row) -> RentalCase:
        return RentalCase(str(row["case_id"]), int(row["user_id"]), str(row["draft_id"] or ""),
                          str(row["listing_id"]), str(row["post_id"] or ""), str(row["offer_id"] or ""), str(row["status"]))

    @staticmethod
    def _task(row: sqlite3.Row) -> OperationsTask:
        return OperationsTask(
            str(row["task_id"]), str(row["request_token"]), str(row["case_id"] or ""),
            int(row["repair_ticket_id"]) if row["repair_ticket_id"] is not None else None,
            str(row["draft_id"] or ""), str(row["listing_id"]), str(row["post_id"] or ""),
            str(row["task_type"]), str(row["priority"]), str(row["assignee_id"] or ""),
            str(row["status"]), str(row["due_at"] or ""), str(row["summary"] or ""), str(row["detail"] or ""),
        )

    @staticmethod
    def _require_lineage(conn: sqlite3.Connection, *, listing_id: str, post_id: str = "", offer_id: str = "") -> None:
        listing = conn.execute("SELECT listing_id FROM listings_v3 WHERE listing_id=? LIMIT 1", (listing_id,)).fetchone()
        if listing is None:
            raise ValueError("aftercare_listing_not_found")
        if offer_id:
            offer = conn.execute("SELECT listing_id FROM listing_offers WHERE offer_id=? LIMIT 1", (offer_id,)).fetchone()
            if offer is None or str(offer[0]) != listing_id:
                raise ValueError("aftercare_offer_listing_mismatch")
        if post_id:
            post = conn.execute("SELECT listing_id FROM publication_instances WHERE instance_id=? LIMIT 1", (post_id,)).fetchone()
            if post is None:
                raise ValueError("aftercare_post_not_found")
            if str(post[0]) != listing_id:
                raise ValueError("aftercare_post_listing_mismatch")

    def create_case(self, *, user_id: int, listing_id: str, post_id: str = "", draft_id: str = "",
                    offer_id: str = "", tenant_binding_id: int | None = None,
                    contract_start_date: str = "", contract_end_date: str = "",
                    rent_day: int | None = None, monthly_rent_usd: float = 0,
                    deposit_months: int = 0, notes: str = "", status: str = "pending_handover") -> RentalCase:
        uid, listing, clean_status = int(user_id or 0), str(listing_id or "").strip(), str(status or "").strip()
        if uid <= 0 or not listing:
            raise ValueError("aftercare_user_and_listing_required")
        if clean_status not in CASE_STATUSES:
            raise ValueError("aftercare_case_status_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_lineage(conn, listing_id=listing, post_id=str(post_id or "").strip(), offer_id=str(offer_id or "").strip())
            case_id = "CASE_" + uuid.uuid4().hex
            conn.execute("""INSERT INTO rental_cases_v3
                (case_id,user_id,tenant_binding_id,draft_id,listing_id,post_id,offer_id,contract_start_date,
                 contract_end_date,rent_day,monthly_rent_usd,deposit_months,status,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                case_id, uid, tenant_binding_id, str(draft_id or ""), listing, str(post_id or ""), str(offer_id or ""),
                str(contract_start_date or ""), str(contract_end_date or ""), rent_day, float(monthly_rent_usd or 0),
                int(deposit_months or 0), clean_status, str(notes or ""),
            ))
            row = conn.execute("SELECT * FROM rental_cases_v3 WHERE case_id=?", (case_id,)).fetchone()
            conn.commit()
        assert row is not None
        return self._case(row)

    def create_task(self, *, request_token: str, listing_id: str, task_type: str, priority: str = "normal",
                    case_id: str = "", repair_ticket_id: int | None = None, draft_id: str = "", post_id: str = "",
                    assignee_id: str = "", due_at: str = "", summary: str = "", detail: str = "", created_by: str = "") -> OperationsTask:
        token, listing = str(request_token or "").strip(), str(listing_id or "").strip()
        kind, prio = str(task_type or "").strip(), str(priority or "").strip()
        if not token or not listing:
            raise ValueError("operations_task_identity_required")
        if kind not in TASK_TYPES:
            raise ValueError("operations_task_type_invalid")
        if prio not in TASK_PRIORITIES:
            raise ValueError("operations_task_priority_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM operations_tasks_v3 WHERE request_token=?", (token,)).fetchone()
            if existing is not None:
                conn.commit()
                return self._task(existing)
            self._require_lineage(conn, listing_id=listing, post_id=str(post_id or "").strip())
            if case_id:
                case = conn.execute("SELECT listing_id,post_id FROM rental_cases_v3 WHERE case_id=?", (str(case_id),)).fetchone()
                if case is None or str(case[0]) != listing:
                    raise ValueError("operations_case_listing_mismatch")
                if post_id and str(case[1] or "") and str(case[1]) != str(post_id):
                    raise ValueError("operations_case_post_mismatch")
            if repair_ticket_id is not None:
                ticket = conn.execute("SELECT id FROM repair_tickets_v3 WHERE id=? LIMIT 1", (int(repair_ticket_id),)).fetchone()
                if ticket is None:
                    raise ValueError("operations_repair_ticket_not_found")
            task_id = "OPS_" + uuid.uuid4().hex
            conn.execute("""INSERT INTO operations_tasks_v3
                (task_id,request_token,case_id,repair_ticket_id,draft_id,listing_id,post_id,task_type,priority,
                 assignee_id,status,due_at,summary,detail,created_by)
                VALUES (?,?,?,?,?,?,?,?,?,?,'new',?,?,?,?)""", (
                task_id, token, str(case_id or ""), repair_ticket_id, str(draft_id or ""), listing,
                str(post_id or ""), kind, prio, str(assignee_id or ""), str(due_at or ""),
                str(summary or ""), str(detail or ""), str(created_by or ""),
            ))
            row = conn.execute("SELECT * FROM operations_tasks_v3 WHERE task_id=?", (task_id,)).fetchone()
            conn.commit()
        assert row is not None
        return self._task(row)

    def transition_task(self, task_id: str, *, to_status: str, actor_id: str = "", note: str = "", assignee_id: str | None = None) -> OperationsTask:
        target = str(to_status or "").strip()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM operations_tasks_v3 WHERE task_id=?", (str(task_id),)).fetchone()
            if row is None:
                raise KeyError(task_id)
            current = str(row["status"])
            if target not in TASK_TRANSITIONS.get(current, frozenset()):
                raise ValueError(f"operations_transition_invalid:{current}->{target}")
            assignment = str(row["assignee_id"] or "") if assignee_id is None else str(assignee_id or "")
            if target in {"assigned", "in_progress"} and not assignment:
                raise ValueError("operations_assignee_required")
            resolved_sql = "CURRENT_TIMESTAMP" if target == "resolved" else "NULL"
            conn.execute(f"""UPDATE operations_tasks_v3 SET status=?,assignee_id=?,updated_at=CURRENT_TIMESTAMP,
                resolved_at={resolved_sql} WHERE task_id=?""", (target, assignment, str(task_id)))
            conn.execute("""INSERT INTO operations_task_events_v3
                (event_id,task_id,from_status,to_status,actor_id,note) VALUES (?,?,?,?,?,?)""",
                ("EVT_" + uuid.uuid4().hex, str(task_id), current, target, str(actor_id or ""), str(note or "")))
            updated = conn.execute("SELECT * FROM operations_tasks_v3 WHERE task_id=?", (str(task_id),)).fetchone()
            conn.commit()
        assert updated is not None
        return self._task(updated)

    def queue(self, *, statuses: tuple[str, ...] = ("new", "acknowledged", "assigned", "in_progress"), limit: int = 100) -> list[OperationsTask]:
        clean = tuple(status for status in statuses if status in TASK_TRANSITIONS)
        if not clean:
            return []
        placeholders = ",".join("?" for _ in clean)
        with self._connect() as conn:
            rows = conn.execute(f"""SELECT * FROM operations_tasks_v3 WHERE status IN ({placeholders})
                ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END,
                         CASE WHEN due_at='' THEN 1 ELSE 0 END,due_at,created_at LIMIT ?""",
                         (*clean, max(1, int(limit)))).fetchall()
        return [self._task(row) for row in rows]

__all__ = ["AftercareOperationsRepository", "CASE_STATUSES", "DDL", "OperationsTask", "RentalCase", "TASK_PRIORITIES", "TASK_TRANSITIONS", "TASK_TYPES"]
