"""Adapter 6: Dual binding identity + V3 repair_tickets_v3 token persistence."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable


_ALLOWED_REPAIR_STATUSES = {"accepted", "scheduled", "in_progress", "done", "need_info"}


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(Path(db_path).expanduser().resolve()), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def persist_repair_ticket_v3(
    db_path: str | Path,
    *,
    request_token: str,
    user_id: int,
    binding_id: int | None,
    property_name: str,
    issue_key: str,
    issue_type: str,
    description: str,
    time_slot: str,
    created_at: str,
) -> dict[str, Any]:
    """Persist only the V3 ticket row after Dual tenant_bindings authenticates the user."""
    token = str(request_token or "").strip()
    if not token:
        raise ValueError("service_request_token_required")
    with _connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM repair_tickets_v3 WHERE request_token=? LIMIT 1",
            (token,),
        ).fetchone()
        if existing is not None:
            conn.commit()
            result = dict(existing)
            result["created"] = False
            return result
        cur = conn.execute(
            """INSERT INTO repair_tickets_v3
               (request_token,user_id,binding_id,property_name,issue_key,issue_type,
                description,time_slot,status,created_at)
               VALUES (?,?,?,?,?,?,?,?,'new',?)""",
            (
                token,
                int(user_id),
                int(binding_id) if binding_id is not None else None,
                str(property_name or ""),
                str(issue_key or ""),
                str(issue_type or ""),
                str(description or ""),
                str(time_slot or ""),
                str(created_at or ""),
            ),
        )
        row = conn.execute(
            "SELECT * FROM repair_tickets_v3 WHERE id=?",
            (int(cur.lastrowid),),
        ).fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError("service_ticket_insert_missing_row")
    result = dict(row)
    result["created"] = True
    return result


def update_repair_ticket_v3_status(
    db_path: str | Path,
    ticket_id: int,
    status: str,
) -> dict[str, Any] | None:
    normalized = str(status or "").strip()
    if normalized not in _ALLOWED_REPAIR_STATUSES:
        return None
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE repair_tickets_v3 SET status=? WHERE id=?",
            (normalized, int(ticket_id)),
        )
        if cur.rowcount <= 0:
            return None
        row = conn.execute(
            "SELECT * FROM repair_tickets_v3 WHERE id=? LIMIT 1",
            (int(ticket_id),),
        ).fetchone()
        conn.commit()
    return dict(row) if row is not None else None


class RepairAdapter:
    def __init__(
        self,
        *,
        get_active_binding: Callable[[int], dict | None],
        persist_ticket: Callable[..., Any],
    ):
        self.get_active_binding = get_active_binding
        self.persist_ticket = persist_ticket

    @staticmethod
    def request_token(
        user_id: int,
        binding_id: int | None,
        issue_key: str,
        description: str,
        time_slot: str,
        day: str,
    ) -> str:
        clean_description = re.sub(r"\s+", " ", str(description or "").strip())
        raw = "|".join(
            (
                str(int(user_id)),
                str(int(binding_id or 0)),
                str(issue_key or "").strip().lower(),
                clean_description,
                str(time_slot or "").strip().lower(),
                str(day or "").strip(),
            )
        )
        return sha256(raw.encode("utf-8")).hexdigest()[:40]

    def create(
        self,
        *,
        user_id: int,
        issue_key: str,
        issue_type: str,
        description: str,
        time_slot: str,
        day: str,
        created_at: str,
    ) -> dict[str, Any]:
        binding = self.get_active_binding(int(user_id))
        if not binding:
            raise ValueError("tenant_binding_required")
        if str(binding.get("status") or "active") != "active":
            raise ValueError("tenant_binding_inactive")
        binding_id = int(binding.get("id") or 0) or None
        token = self.request_token(
            int(user_id), binding_id, issue_key, description, time_slot, day
        )
        ticket = self.persist_ticket(
            request_token=token,
            user_id=int(user_id),
            binding_id=binding_id,
            property_name=str(binding.get("property_name") or ""),
            issue_key=issue_key,
            issue_type=issue_type,
            description=description,
            time_slot=time_slot,
            created_at=created_at,
        )
        return {
            "ticket_id": int(ticket["id"]) if isinstance(ticket, dict) else int(ticket),
            "request_token": token,
            "binding_id": binding_id,
            "created": True if not isinstance(ticket, dict) else bool(ticket.get("created", True)),
        }


__all__ = [
    "RepairAdapter",
    "persist_repair_ticket_v3",
    "update_repair_ticket_v3_status",
]
