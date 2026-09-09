"""Lifecycle/service layer for V3 aftercare operations.

The repository owns generic persistence. This layer adds cross-table business
invariants that require tenant binding + rental case + repair ticket context and
records rental-case status history.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import uuid

from v3_core.storage.aftercare_repository import AftercareOperationsRepository, RentalCase

DDL = """
CREATE TABLE IF NOT EXISTS rental_case_events_v3 (
    event_id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    from_status TEXT NOT NULL DEFAULT '',
    to_status TEXT NOT NULL,
    actor_id TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_rental_case_events_v3_case
ON rental_case_events_v3(case_id, created_at);
"""

CASE_TRANSITIONS = {
    "pending_handover": frozenset({"active", "cancelled"}),
    "active": frozenset({"ending", "closed", "cancelled"}),
    "ending": frozenset({"active", "closed", "cancelled"}),
    "closed": frozenset(),
    "cancelled": frozenset(),
}


@dataclass(frozen=True)
class CaseEvent:
    event_id: str
    case_id: str
    from_status: str
    to_status: str
    actor_id: str
    note: str
    created_at: str


class AftercareLifecycleService:
    DDL = DDL

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        self.repo = AftercareOperationsRepository(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def create_case_from_binding(
        self,
        *,
        tenant_binding_id: int,
        listing_id: str,
        offer_id: str = "",
        post_id: str = "",
        draft_id: str = "",
        notes: str = "",
    ) -> RentalCase:
        binding_id = int(tenant_binding_id)
        listing_id = str(listing_id or "").strip()
        if not listing_id:
            raise ValueError("aftercare_listing_required")
        with self._connect() as conn:
            binding = conn.execute(
                """SELECT id,user_id,contract_start_date,contract_end_date,rent_day,
                          monthly_rent,deposit_months,status
                   FROM tenant_bindings_v3 WHERE id=? LIMIT 1""",
                (binding_id,),
            ).fetchone()
            if binding is None:
                raise ValueError("aftercare_tenant_binding_not_found")
            if str(binding["status"] or "") != "active":
                raise ValueError("aftercare_tenant_binding_not_active")
            existing = conn.execute(
                """SELECT case_id FROM rental_cases_v3
                   WHERE tenant_binding_id=? AND listing_id=?
                     AND status IN ('pending_handover','active','ending')
                   ORDER BY created_at DESC LIMIT 1""",
                (binding_id, listing_id),
            ).fetchone()
        if existing is not None:
            with self.repo._connect() as conn:
                row = conn.execute("SELECT * FROM rental_cases_v3 WHERE case_id=?", (str(existing[0]),)).fetchone()
            assert row is not None
            return self.repo._case(row)
        return self.repo.create_case(
            user_id=int(binding["user_id"]),
            tenant_binding_id=binding_id,
            listing_id=listing_id,
            offer_id=offer_id,
            post_id=post_id,
            draft_id=draft_id,
            contract_start_date=str(binding["contract_start_date"] or ""),
            contract_end_date=str(binding["contract_end_date"] or ""),
            rent_day=int(binding["rent_day"]) if binding["rent_day"] is not None else None,
            monthly_rent_usd=float(binding["monthly_rent"] or 0),
            deposit_months=int(binding["deposit_months"] or 0),
            notes=notes,
        )

    def transition_case(self, case_id: str, *, to_status: str, actor_id: str = "", note: str = "") -> RentalCase:
        case_id = str(case_id or "").strip()
        target = str(to_status or "").strip()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM rental_cases_v3 WHERE case_id=?", (case_id,)).fetchone()
            if row is None:
                raise KeyError(case_id)
            current = str(row["status"])
            if target not in CASE_TRANSITIONS.get(current, frozenset()):
                raise ValueError(f"aftercare_case_transition_invalid:{current}->{target}")
            conn.execute(
                "UPDATE rental_cases_v3 SET status=?,updated_at=CURRENT_TIMESTAMP WHERE case_id=?",
                (target, case_id),
            )
            conn.execute(
                """INSERT INTO rental_case_events_v3
                   (event_id,case_id,from_status,to_status,actor_id,note)
                   VALUES (?,?,?,?,?,?)""",
                ("CASEEVT_" + uuid.uuid4().hex, case_id, current, target, str(actor_id or ""), str(note or "")),
            )
            updated = conn.execute("SELECT * FROM rental_cases_v3 WHERE case_id=?", (case_id,)).fetchone()
            conn.commit()
        assert updated is not None
        return self.repo._case(updated)

    def create_repair_task(
        self,
        *,
        case_id: str,
        repair_ticket_id: int,
        priority: str = "normal",
        assignee_id: str = "",
        due_at: str = "",
        created_by: str = "",
    ):
        case_id = str(case_id or "").strip()
        ticket_id = int(repair_ticket_id)
        with self._connect() as conn:
            case = conn.execute("SELECT * FROM rental_cases_v3 WHERE case_id=?", (case_id,)).fetchone()
            if case is None:
                raise ValueError("operations_case_not_found")
            if str(case["status"]) not in {"pending_handover", "active", "ending"}:
                raise ValueError("operations_case_not_open")
            ticket = conn.execute("SELECT * FROM repair_tickets_v3 WHERE id=?", (ticket_id,)).fetchone()
            if ticket is None:
                raise ValueError("operations_repair_ticket_not_found")
            if int(ticket["user_id"]) != int(case["user_id"]):
                raise ValueError("operations_repair_ticket_user_mismatch")
            case_binding = case["tenant_binding_id"]
            ticket_binding = ticket["binding_id"]
            if case_binding is not None and ticket_binding is not None and int(case_binding) != int(ticket_binding):
                raise ValueError("operations_repair_ticket_binding_mismatch")
            token = "ops:repair:" + str(ticket["request_token"])
            summary = str(ticket["issue_type"] or ticket["issue_key"] or "报修")
            detail = str(ticket["description"] or "")
        return self.repo.create_task(
            request_token=token,
            listing_id=str(case["listing_id"]),
            task_type="repair",
            priority=priority,
            case_id=case_id,
            repair_ticket_id=ticket_id,
            draft_id=str(case["draft_id"] or ""),
            post_id=str(case["post_id"] or ""),
            assignee_id=assignee_id,
            due_at=due_at,
            summary=summary,
            detail=detail,
            created_by=created_by,
        )

    def expiring_cases(self, *, before_date: str, limit: int = 100) -> list[RentalCase]:
        cutoff = str(before_date or "").strip()
        if not cutoff:
            raise ValueError("aftercare_before_date_required")
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM rental_cases_v3
                   WHERE status IN ('active','ending')
                     AND contract_end_date<>'' AND contract_end_date<=?
                   ORDER BY contract_end_date,updated_at LIMIT ?""",
                (cutoff, max(1, int(limit))),
            ).fetchall()
        return [self.repo._case(row) for row in rows]


__all__ = ["AftercareLifecycleService", "CASE_TRANSITIONS", "CaseEvent", "DDL"]
