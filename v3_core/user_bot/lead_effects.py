"""Non-fatal V3 lead side-effect execution.

Locked production treats lead persistence as best-effort: a lead write failure is
logged and does not undo a successful search/appointment/contact flow. This
executor preserves that contract while keeping lead writes outside Telegram
handlers and business services.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Literal

from .appointment_submit_executor import AppointmentSubmitExecution
from .consult import ConsultIntent
from .lead_service import (
    LeadRecordResult,
    LeadService,
    LeadUser,
    appointment_lead_request,
    general_contact_lead_request,
    listing_contact_lead_request,
    search_lead_request,
)
from .public_appointment import PublicAppointmentDraft
from .transition_actions import SearchSubmitIntent


LeadEffectStatus = Literal["recorded", "skipped", "failed"]


@dataclass(frozen=True)
class LeadEffectResult:
    status: LeadEffectStatus
    record: LeadRecordResult | None = None
    error: str = ""


class LeadEffectExecutor:
    def __init__(
        self,
        service: LeadService,
        *,
        now: Callable[[], str] | None = None,
    ):
        self.service = service
        self.now = now or (lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def _record(self, *, user: LeadUser, request) -> LeadEffectResult:
        try:
            record = self.service.record(
                user=user,
                request=request,
                created_at=self.now(),
            )
        except Exception as exc:  # fixed-SHA create_lead is best-effort
            return LeadEffectResult(status="failed", error=str(exc))
        return LeadEffectResult(status="recorded", record=record)

    def record_search(
        self,
        *,
        user: LeadUser,
        intent: SearchSubmitIntent,
    ) -> LeadEffectResult:
        return self._record(user=user, request=search_lead_request(intent))

    def record_appointment(
        self,
        *,
        user: LeadUser,
        execution: AppointmentSubmitExecution,
        draft: PublicAppointmentDraft,
    ) -> LeadEffectResult:
        request = appointment_lead_request(execution, draft)
        if request is None:
            return LeadEffectResult(status="skipped")
        return self._record(user=user, request=request)

    def record_general_contact(
        self,
        *,
        user: LeadUser,
        source: str = "hub",
    ) -> LeadEffectResult:
        return self._record(
            user=user,
            request=general_contact_lead_request(source=source),
        )

    def record_listing_contact(
        self,
        *,
        user: LeadUser,
        intent: ConsultIntent,
    ) -> LeadEffectResult:
        return self._record(
            user=user,
            request=listing_contact_lead_request(intent),
        )


__all__ = ["LeadEffectExecutor", "LeadEffectResult", "LeadEffectStatus"]
