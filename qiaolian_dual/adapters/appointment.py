"""Adapter 5: submit-time guard then Dual appointments. Leaves claim to 3858."""

from __future__ import annotations

from typing import Any, Callable


class AppointmentAdapter:
    def __init__(
        self,
        *,
        inventory: Any,
        create_appointment: Callable[[dict], int],
        list_appointments: Callable[..., list] | None = None,
    ):
        self.inventory = inventory
        self.create_appointment = create_appointment
        self.list_appointments = list_appointments

    def submit(self, data: dict[str, Any]) -> int:
        raw_public_id = str(data.get("listing_id") or "").strip()
        if not raw_public_id:
            raise ValueError("appointment_listing_required")
        normalize_public_id = getattr(self.inventory, "normalize_public_id", None)
        public_id = (
            str(normalize_public_id(raw_public_id) or "").strip()
            if callable(normalize_public_id)
            else raw_public_id
        )
        if not public_id:
            raise ValueError("listing_not_published")

        listing = self.inventory.get_listing(public_id)
        if listing is None:
            raise ValueError("listing_not_published")
        if not listing.get("bookable"):
            raise ValueError("listing_not_bookable")

        internal_listing_id = str(listing.get("internal_listing_id") or "").strip()
        if not internal_listing_id:
            resolve_internal_id = getattr(self.inventory, "resolve_internal_id", None)
            if callable(resolve_internal_id):
                internal_listing_id = str(resolve_internal_id(public_id) or "").strip()
        if not internal_listing_id:
            raise ValueError("listing_not_published")

        if self.list_appointments is not None:
            existing = self.list_appointments(int(data["user_id"]), limit=20) or []
            for row in existing:
                status = str(row.get("status") or "").lower()
                same = str(row.get("listing_id") or "") == internal_listing_id
                unfinished = status in {"pending", "assigned", "contacted"}
                if same and unfinished:
                    raise ValueError("appointment_duplicate")

        payload = dict(data)
        payload["listing_id"] = internal_listing_id
        payload.setdefault("status", "pending")
        return int(self.create_appointment(payload))
