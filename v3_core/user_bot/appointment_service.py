"""Explicit V3 appointment submission orchestration.

Production currently mixes validation, SQLite writes, lead creation, listing
availability recomputation, channel sync and Telegram rendering inside the
callback/DB layers. V3 makes the business transaction explicit and leaves
lead/notification/availability/channel-sync side effects to the caller after a
successful result.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from .appointments import AppointmentDraft, duplicate_identity, editable_status


class AppointmentRepository(Protocol):
    def find_exact_unfinished(
        self,
        *,
        user_id: int,
        listing_id: str,
        mode: str,
        date: str,
        time: str,
    ) -> dict[str, Any] | None: ...

    def get_for_user(self, appointment_id: int, user_id: int) -> dict[str, Any] | None: ...

    def create(self, values: dict[str, Any]) -> int: ...

    def update_schedule(
        self,
        *,
        appointment_id: int,
        user_id: int,
        mode: str,
        date: str,
        time: str,
        status: str,
    ) -> bool: ...


class BookabilityChecker(Protocol):
    def is_bookable(self, listing_id: str) -> bool: ...


@dataclass(frozen=True)
class AppointmentUser:
    user_id: int
    username: str = ""
    display_name: str = ""

    @property
    def contact_value(self) -> str:
        clean = str(self.username or "").strip().lstrip("@")
        return f"@{clean}" if clean else str(int(self.user_id))


SubmissionKind = Literal["created", "reused", "updated"]


@dataclass(frozen=True)
class AppointmentSubmissionResult:
    kind: SubmissionKind
    appointment_id: int
    lead_action: str | None
    lead_source: str
    # Legacy Database._sync_listing_appointment_state performed this implicitly
    # on create/update. V3 callers must run it explicitly after a write.
    should_recompute_listing_availability: bool = True
    should_sync_channel: bool = True
    should_notify_admin: bool = True


class AppointmentSubmissionService:
    def __init__(
        self,
        *,
        repository: AppointmentRepository,
        bookability: BookabilityChecker,
    ):
        self.repository = repository
        self.bookability = bookability

    def submit(
        self,
        *,
        user: AppointmentUser,
        draft: AppointmentDraft,
        edit_appointment_id: int = 0,
        created_at: str = "",
    ) -> AppointmentSubmissionResult:
        listing_id = str(draft.listing_id or "").strip()
        if not listing_id or listing_id in {"待推荐", "未知"}:
            raise ValueError("appointment_requires_concrete_listing")
        if not draft.ready:
            raise ValueError("appointment_date_and_time_required")
        if not self.bookability.is_bookable(listing_id):
            raise ValueError("listing_not_bookable")

        edit_id = int(edit_appointment_id or 0)
        if edit_id:
            existing = self.repository.get_for_user(edit_id, int(user.user_id))
            if not existing or not editable_status(existing.get("status")):
                raise ValueError("appointment_not_editable")
            if str(existing.get("listing_id") or "").strip() != listing_id:
                raise ValueError("appointment_listing_mismatch")
            updated = self.repository.update_schedule(
                appointment_id=edit_id,
                user_id=int(user.user_id),
                mode=draft.mode,
                date=draft.date,
                time=draft.time,
                status="pending",
            )
            if not updated:
                raise RuntimeError("appointment_update_failed")
            return AppointmentSubmissionResult(
                kind="updated",
                appointment_id=edit_id,
                lead_action="appointment_time_update",
                lead_source="appointment_edit",
            )

        identity = duplicate_identity(
            user_id=int(user.user_id),
            listing_id=listing_id,
            mode=draft.mode,
            date=draft.date,
            time=draft.time,
        )
        duplicate = self.repository.find_exact_unfinished(
            user_id=identity[0],
            listing_id=identity[1],
            mode=identity[2],
            date=identity[3],
            time=identity[4],
        )
        if duplicate:
            return AppointmentSubmissionResult(
                kind="reused",
                appointment_id=int(duplicate["id"]),
                lead_action=None,
                lead_source=draft.source,
                # Production performs no appointment write here, so there is no
                # hidden DB-level availability recompute. It still invokes channel
                # sync and sends the admin notification afterwards.
                should_recompute_listing_availability=False,
                should_sync_channel=True,
                should_notify_admin=True,
            )

        appointment_id = self.repository.create(
            {
                "user_id": int(user.user_id),
                "username": str(user.username or ""),
                "display_name": str(user.display_name or ""),
                "listing_id": listing_id,
                "viewing_mode": draft.mode,
                "appointment_date": draft.date,
                "appointment_time": draft.time,
                "contact_value": user.contact_value,
                "note": "",
                "status": "pending",
                "created_at": str(created_at or ""),
            }
        )
        return AppointmentSubmissionResult(
            kind="created",
            appointment_id=int(appointment_id),
            lead_action="appointment_submit",
            lead_source=draft.source,
        )


__all__ = [
    "AppointmentRepository",
    "AppointmentSubmissionResult",
    "AppointmentSubmissionService",
    "AppointmentUser",
    "BookabilityChecker",
]
