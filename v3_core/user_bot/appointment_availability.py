"""Explicit listing availability recomputation from appointment state.

Locked production hides this rule inside ``Database._sync_listing_appointment_state``.
V3 keeps it visible: appointments may only toggle inventory ``active`` and
``reserved``. They must never overwrite manual/terminal states such as pending,
rented or offline.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from .appointments import ACTIVE_APPOINTMENT_STATUSES


AUTO_APPOINTMENT_INVENTORY_STATUSES = frozenset({"active", "reserved"})


class AppointmentAvailabilityRepository(Protocol):
    def get_inventory_status(self, listing_id: str) -> str | None: ...

    def count_appointments_by_statuses(
        self,
        *,
        listing_id: str,
        statuses: Sequence[str],
    ) -> int: ...

    def update_inventory_status(self, *, listing_id: str, status: str) -> bool: ...


@dataclass(frozen=True)
class AppointmentAvailabilityResult:
    listing_id: str
    previous_status: str
    next_status: str
    active_appointment_count: int
    changed: bool
    protected_status: bool = False


class AppointmentAvailabilityService:
    def __init__(self, *, repository: AppointmentAvailabilityRepository):
        self.repository = repository

    def recompute(self, listing_id: str) -> AppointmentAvailabilityResult:
        clean_listing_id = str(listing_id or "").strip()
        if not clean_listing_id:
            raise ValueError("listing_id_required")

        previous = str(self.repository.get_inventory_status(clean_listing_id) or "").strip().lower()
        if not previous:
            raise ValueError("listing_not_found")
        if previous not in AUTO_APPOINTMENT_INVENTORY_STATUSES:
            return AppointmentAvailabilityResult(
                listing_id=clean_listing_id,
                previous_status=previous,
                next_status=previous,
                active_appointment_count=0,
                changed=False,
                protected_status=True,
            )

        active_count = int(
            self.repository.count_appointments_by_statuses(
                listing_id=clean_listing_id,
                statuses=tuple(sorted(ACTIVE_APPOINTMENT_STATUSES)),
            )
            or 0
        )
        next_status = "reserved" if active_count > 0 else "active"
        if next_status == previous:
            return AppointmentAvailabilityResult(
                listing_id=clean_listing_id,
                previous_status=previous,
                next_status=next_status,
                active_appointment_count=active_count,
                changed=False,
            )

        if not self.repository.update_inventory_status(
            listing_id=clean_listing_id,
            status=next_status,
        ):
            raise RuntimeError("listing_availability_update_failed")
        return AppointmentAvailabilityResult(
            listing_id=clean_listing_id,
            previous_status=previous,
            next_status=next_status,
            active_appointment_count=active_count,
            changed=True,
        )


__all__ = [
    "AUTO_APPOINTMENT_INVENTORY_STATUSES",
    "AppointmentAvailabilityRepository",
    "AppointmentAvailabilityResult",
    "AppointmentAvailabilityService",
]
