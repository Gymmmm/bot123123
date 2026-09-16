"""Explicit listing availability recomputation from appointment state.

This mirrors locked production's appointment lock rule: bookable inventory is
``active`` with no open appointments, ``reserved`` with 1-4, and ``pending`` at
5 or more. Manual/terminal ``pending``/``rented``/``inactive``/``offline`` states
are never relaxed automatically.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from .appointments import ACTIVE_APPOINTMENT_STATUSES


APPOINTMENT_LOCK_COUNT = 5
AUTO_APPOINTMENT_INVENTORY_STATUSES = frozenset({"active", "reserved", "pending"})


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
        if previous in {"rented", "offline", "inactive"}:
            return AppointmentAvailabilityResult(
                listing_id=clean_listing_id,
                previous_status=previous,
                next_status=previous,
                active_appointment_count=0,
                changed=False,
                protected_status=True,
            )
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

        # Locked production treats pending as sticky/manual: once pending, open
        # appointment count must not silently re-open the listing.
        if previous == "pending":
            return AppointmentAvailabilityResult(
                listing_id=clean_listing_id,
                previous_status=previous,
                next_status=previous,
                active_appointment_count=active_count,
                changed=False,
                protected_status=True,
            )

        if active_count >= APPOINTMENT_LOCK_COUNT:
            next_status = "pending"
        elif active_count >= 1:
            next_status = "reserved"
        else:
            next_status = "active"

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
    "APPOINTMENT_LOCK_COUNT",
    "AUTO_APPOINTMENT_INVENTORY_STATUSES",
    "AppointmentAvailabilityRepository",
    "AppointmentAvailabilityResult",
    "AppointmentAvailabilityService",
]
