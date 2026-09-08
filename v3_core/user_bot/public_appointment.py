"""Public-ID appointment draft boundary for the V3 User Bot.

Telegram/user-session state must not carry internal listing identifiers.  The
public draft therefore keeps only the QL public listing id while the user chooses
mode/date/time.  Immediately before persistence, ``PublicAppointmentResolver``
re-validates the durably published listing and converts to the existing internal
``AppointmentDraft`` required by the appointment transaction service.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from v3_core.publishing.public_ids import normalize_public_id

from .appointments import AppointmentDraft, AppointmentMode, AppointmentStep, normalize_mode
from .public_inventory import PublicInventoryReader


@dataclass(frozen=True)
class PublicAppointmentDraft:
    public_listing_id: str
    mode: AppointmentMode = "offline"
    date: str = ""
    time: str = ""
    source: str = "user_bot"

    def __post_init__(self) -> None:
        public_id = normalize_public_id(self.public_listing_id)
        if public_id is None:
            raise ValueError("appointment_requires_public_listing_id")
        object.__setattr__(self, "public_listing_id", public_id)
        object.__setattr__(self, "mode", normalize_mode(self.mode))
        object.__setattr__(self, "source", str(self.source or "user_bot").strip())

    @property
    def step(self) -> AppointmentStep:
        if not self.date:
            return "date"
        if not self.time:
            return "time"
        return "ready"

    @property
    def ready(self) -> bool:
        return self.step == "ready"

    def with_mode(self, mode: object) -> "PublicAppointmentDraft":
        return replace(self, mode=normalize_mode(mode))

    def with_date(self, value: object) -> "PublicAppointmentDraft":
        clean = str(value or "").strip()
        if not clean:
            raise ValueError("appointment_date_required")
        return replace(self, date=clean, time="")

    def with_time(self, value: object) -> "PublicAppointmentDraft":
        clean = str(value or "").strip()
        if not self.date:
            raise ValueError("appointment_date_required_before_time")
        if not clean:
            raise ValueError("appointment_time_required")
        return replace(self, time=clean)


class PublicAppointmentResolver:
    """Convert public session state to an internal draft only at submit time."""

    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def resolve(self, draft: PublicAppointmentDraft) -> AppointmentDraft:
        view = self.inventory.resolve(draft.public_listing_id)
        if view is None:
            raise ValueError("listing_not_publicly_published")
        if not view.bookable:
            raise ValueError("listing_not_bookable")
        return AppointmentDraft(
            listing_id=view.listing_id,
            mode=draft.mode,
            date=draft.date,
            time=draft.time,
            source=draft.source,
        )


__all__ = ["PublicAppointmentDraft", "PublicAppointmentResolver"]
