"""Adapter 6: Dual binding identity + V3 repair_tickets_v3 token persistence."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Callable


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
    def request_token(user_id: int, binding_id: int | None, issue_key: str, day: str) -> str:
        raw = f"{user_id}|{binding_id or 0}|{issue_key}|{day}"
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
    ) -> dict[str, Any]:
        binding = self.get_active_binding(int(user_id))
        if not binding:
            raise ValueError("tenant_binding_required")
        if str(binding.get("status") or "active") != "active":
            raise ValueError("tenant_binding_inactive")
        binding_id = int(binding.get("id") or 0) or None
        token = self.request_token(int(user_id), binding_id, issue_key, day)
        ticket = self.persist_ticket(
            request_token=token,
            user_id=int(user_id),
            binding_id=binding_id,
            property_name=str(binding.get("property_name") or ""),
            issue_key=issue_key,
            issue_type=issue_type,
            description=description,
            time_slot=time_slot,
        )
        return {
            "ticket_id": int(ticket["id"]) if isinstance(ticket, dict) else int(ticket),
            "request_token": token,
            "binding_id": binding_id,
            "created": True if not isinstance(ticket, dict) else bool(ticket.get("created", True)),
        }
