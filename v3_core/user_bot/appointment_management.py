"""Explicit V3 management actions for existing appointments.

The Telegram UI keeps the two-step cancellation confirmation. This service
provides a read-only preview for the first step, re-validates ownership/status
on final confirmation, and returns explicit follow-up side effects instead of
hiding listing/channel updates inside the repository.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .appointments import AppointmentDraft, editable_status, normalize_mode


class AppointmentManagementRepository(Protocol):
    def get_for_user(self, appointment_id: int, user_id: int) -> dict[str, Any] | None: ...

    def update_status(
        self,
        *,
        appointment_id: int,
        user_id: int,
        status: str,
    ) -> bool: ...


@dataclass(frozen=True)
class AppointmentCancelPreview:
    appointment_id: int
    listing_id: str
    date: str
    time: str
    status: str


@dataclass(frozen=True)
class AppointmentCancellationResult:
    appointment_id: int
    listing_id: str
    previous_status: str
    status: str = "cancelled"
    # Production Database.update_appointment_status recomputes active/reserved
    # implicitly; V3 makes that follow-up visible to the caller.
    should_recompute_listing_availability: bool = True
    should_sync_channel: bool = True
    # Locked production callback does not create a lead/admin notification when
    # the user cancels an appointment.
    should_notify_admin: bool = False
    lead_action: str | None = None


@dataclass(frozen=True)
class AppointmentEditContext:
    appointment_id: int
    draft: AppointmentDraft


class AppointmentManagementService:
    def __init__(self, *, repository: AppointmentManagementRepository):
        self.repository = repository

    def _editable_for_user(self, appointment_id: int, user_id: int) -> dict[str, Any]:
        row = self.repository.get_for_user(int(appointment_id), int(user_id))
        if not row or not editable_status(row.get("status")):
            raise ValueError("appointment_not_editable")
        return row

    def prepare_cancel(self, *, appointment_id: int, user_id: int) -> AppointmentCancelPreview:
        """Validate without mutating; used before rendering the confirm page."""
        row = self._editable_for_user(appointment_id, user_id)
        return AppointmentCancelPreview(
            appointment_id=int(appointment_id),
            listing_id=str(row.get("listing_id") or "").strip(),
            date=str(row.get("appointment_date") or "").strip(),
            time=str(row.get("appointment_time") or "").strip(),
            status=str(row.get("status") or "").strip().lower(),
        )

    def confirm_cancel(
        self,
        *,
        appointment_id: int,
        user_id: int,
    ) -> AppointmentCancellationResult:
        """Re-check ownership/status and execute the final cancellation write."""
        row = self._editable_for_user(appointment_id, user_id)
        listing_id = str(row.get("listing_id") or "").strip()
        previous_status = str(row.get("status") or "").strip().lower()
        updated = self.repository.update_status(
            appointment_id=int(appointment_id),
            user_id=int(user_id),
            status="cancelled",
        )
        if not updated:
            raise RuntimeError("appointment_cancel_failed")
        return AppointmentCancellationResult(
            appointment_id=int(appointment_id),
            listing_id=listing_id,
            previous_status=previous_status,
        )

    def prepare_edit(self, *, appointment_id: int, user_id: int) -> AppointmentEditContext:
        """Match production edit entry: keep mode/date, force time re-selection."""
        row = self._editable_for_user(appointment_id, user_id)
        listing_id = str(row.get("listing_id") or "").strip()
        if not listing_id:
            raise ValueError("appointment_requires_concrete_listing")
        draft = AppointmentDraft(
            listing_id=listing_id,
            mode=normalize_mode(row.get("viewing_mode")),
            date=str(row.get("appointment_date") or "").strip(),
            time="",
            source="appointment_edit",
        )
        return AppointmentEditContext(
            appointment_id=int(appointment_id),
            draft=draft,
        )


__all__ = [
    "AppointmentCancellationResult",
    "AppointmentCancelPreview",
    "AppointmentEditContext",
    "AppointmentManagementRepository",
    "AppointmentManagementService",
]
