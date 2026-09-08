"""Explicit V3 lead recording contracts for User Bot effects.

Locked production normalizes lead metadata in ``search.create_lead`` and then
writes the legacy ``leads`` table. V3 keeps that normalization explicit and
writes only through an injected repository (normally ``leads_v3``). Telegram
admin notification is deliberately a separate effect.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .appointment_submit_executor import AppointmentSubmitExecution
from .consult import ConsultIntent
from .public_appointment import PublicAppointmentDraft
from .transition_actions import SearchSubmitIntent


class LeadRepository(Protocol):
    def create(self, values: dict[str, Any]) -> int: ...


@dataclass(frozen=True)
class LeadUser:
    user_id: int
    username: str = ""
    display_name: str = ""


@dataclass(frozen=True)
class LeadRequest:
    action: str
    source: str
    listing_id: str = ""
    area: str = ""
    property_type: str = ""
    budget_min: int | None = None
    budget_max: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LeadRecordResult:
    lead_id: int
    action: str
    source: str


class LeadService:
    def __init__(self, repository: LeadRepository):
        self.repository = repository

    def record(
        self,
        *,
        user: LeadUser,
        request: LeadRequest,
        created_at: str,
    ) -> LeadRecordResult:
        if int(user.user_id or 0) <= 0:
            raise ValueError("lead_user_id_required")
        action = str(request.action or "").strip()
        if not action:
            raise ValueError("lead_action_required")
        timestamp = str(created_at or "").strip()
        if not timestamp:
            raise ValueError("lead_created_at_required")

        payload = dict(request.payload or {})
        raw_message_id = payload.get("channel_message_id", payload.get("message_id"))
        try:
            message_id = int(raw_message_id) if raw_message_id not in (None, "", 0) else None
        except (TypeError, ValueError):
            message_id = None

        lead_id = self.repository.create(
            {
                "user_id": int(user.user_id),
                "username": str(user.username or ""),
                "display_name": str(user.display_name or ""),
                "source": str(request.source or ""),
                "action": action,
                "listing_id": str(request.listing_id or ""),
                "area": str(request.area or ""),
                "property_type": str(request.property_type or ""),
                "budget_min": request.budget_min,
                "budget_max": request.budget_max,
                "payload": payload,
                "message_id": message_id,
                "post_token": str(payload.get("post_token") or "").strip(),
                "caption_variant": str(payload.get("caption_variant") or "").strip(),
                "agent_id": str(payload.get("agent_id") or "").strip(),
                "response_at": str(payload.get("response_at") or "").strip(),
                "created_at": timestamp,
            }
        )
        return LeadRecordResult(
            lead_id=int(lead_id),
            action=action,
            source=str(request.source or ""),
        )


def search_lead_request(intent: SearchSubmitIntent) -> LeadRequest:
    area = str(intent.area_display or "").strip()
    if area == "不限":
        area = ""
    return LeadRequest(
        action="search_pref_submit",
        source=str(intent.source or "user_search"),
        area=area,
        property_type=str(intent.criteria.property_type or ""),
        budget_min=int(intent.criteria.budget_min) if intent.criteria.budget_min is not None else None,
        budget_max=int(intent.criteria.budget_max) if intent.criteria.budget_max is not None else None,
        payload={
            "goal": str(intent.goal or "any"),
            "area_hint": str(intent.area_display or ""),
            "budget_label": str(intent.budget_label or ""),
            **dict(intent.touch_payload or {}),
        },
    )


def appointment_lead_request(
    execution: AppointmentSubmitExecution,
    draft: PublicAppointmentDraft,
) -> LeadRequest | None:
    action = execution.submission.lead_action
    if not action:
        return None
    return LeadRequest(
        action=action,
        source=str(execution.submission.lead_source or draft.source or "user_bot"),
        listing_id=str(execution.listing_id or ""),
        payload={
            "appointment_id": int(execution.submission.appointment_id),
            "viewing_mode": draft.mode,
            "appointment_date": draft.date,
            "appointment_time": draft.time,
        },
    )


def general_contact_lead_request(*, source: str = "hub") -> LeadRequest:
    return LeadRequest(
        action="consult_menu_click",
        source=str(source or "hub"),
        payload={"listing_id": ""},
    )


def listing_contact_lead_request(intent: ConsultIntent) -> LeadRequest:
    return LeadRequest(
        action="consult_menu_click",
        source=str(intent.source or "listing_callback"),
        listing_id=str(intent.listing_id or ""),
        payload={"listing_id": str(intent.listing_id or "")},
    )


__all__ = [
    "LeadRecordResult",
    "LeadRepository",
    "LeadRequest",
    "LeadService",
    "LeadUser",
    "appointment_lead_request",
    "general_contact_lead_request",
    "listing_contact_lead_request",
    "search_lead_request",
]
