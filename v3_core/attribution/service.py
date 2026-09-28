"""Explicit attribution orchestration for V3 User Bot/lead flows.

The production runtime patch replaces ``search.create_lead`` at process start.
V3 instead injects a repository and calls this service directly.  The service
owns no schema, Telegram object, or legacy DB manager.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .classification import (
    attach_to_lead_payload,
    classify_bot_event,
    classify_public_start_arg,
    merge_touch,
)


class AttributionRepository(Protocol):
    def get(self, user_id: int) -> dict[str, Any] | None: ...

    def upsert(self, touch: dict[str, Any]) -> None: ...


@dataclass(frozen=True)
class UserIdentity:
    user_id: int
    username: str = ""
    display_name: str = ""


@dataclass(frozen=True)
class PreparedLeadAttribution:
    touch: dict[str, Any]
    payload: dict[str, Any]


class AttributionService:
    def __init__(self, repository: AttributionRepository):
        self.repository = repository

    def remember(
        self,
        identity: UserIdentity,
        *,
        action: str = "",
        source: str = "",
        listing_id: str = "",
        payload: dict[str, Any] | None = None,
        start_arg: str = "",
        now: str = "",
    ) -> dict[str, Any]:
        pack = dict(payload or {}) if isinstance(payload, dict) else {}
        if start_arg:
            pack.setdefault("start_arg", str(start_arg))
            incoming = classify_public_start_arg(start_arg)
            if action:
                incoming["entry_action"] = str(action)
        else:
            incoming = classify_bot_event(action=action, source=source, payload=pack)

        touch = merge_touch(
            self.repository.get(int(identity.user_id)),
            incoming,
            user_id=int(identity.user_id),
            username=str(identity.username or ""),
            display_name=str(identity.display_name or ""),
            listing_id=str(
                listing_id
                or pack.get("listing_id")
                or incoming.get("listing_hint")
                or ""
            ),
            now=now,
        )
        self.repository.upsert(touch)
        return touch

    def prepare_lead(
        self,
        identity: UserIdentity,
        *,
        action: str,
        source: str,
        listing_id: str = "",
        payload: dict[str, Any] | None = None,
        now: str = "",
    ) -> PreparedLeadAttribution:
        touch = self.remember(
            identity,
            action=action,
            source=source,
            listing_id=listing_id,
            payload=payload,
            now=now,
        )
        return PreparedLeadAttribution(
            touch=touch,
            payload=attach_to_lead_payload(payload, touch),
        )


__all__ = [
    "AttributionRepository",
    "AttributionService",
    "PreparedLeadAttribution",
    "UserIdentity",
]
