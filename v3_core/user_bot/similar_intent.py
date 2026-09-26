"""Guided similar-listing intent for the side-by-side V3 User Bot.

Fixed-SHA ``listing:similar`` does not immediately execute a relaxed search. It
uses the current listing's area, resets the goal to ``any``, and asks the user
for a budget. V3 preserves that behavior as a pure intent derived only from the
frozen public listing snapshot.

For rented/offline listings: include budget and room_type from frozen facts so
the similar search runs immediately without asking for known conditions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .public_inventory import PublicInventoryReader

# Listings with these statuses are non-bookable and should auto-trigger similar search
_RENTED_OFFLINE_STATUSES = frozenset({"rented", "offline", "inactive", "withdrawn"})


SimilarIntentStatus = Literal["ok", "invalid_link", "not_found"]

# User-facing similar search: budget is asked
_SIMILAR_GOAL_BUDGET = "budget"
# Direct similar search (rented/offline): no budget question
_SIMILAR_GOAL_ANY = "any"


@dataclass(frozen=True)
class SimilarSearchIntent:
    listing_id: str
    public_listing_id: str
    source: str
    goal: str
    location_keys: tuple[str, ...]
    area_display: str
    next_step: str
    # Budget from frozen facts (for rented/offline listings)
    budget_min: int | None = None
    budget_max: int | None = None
    budget_label: str = ""
    # Room type from frozen facts (for rented/offline listings)
    room_type: str = ""
    property_type: str = ""


@dataclass(frozen=True)
class SimilarIntentResult:
    status: SimilarIntentStatus
    public_listing_id: str = ""
    reason: str = ""
    intent: SimilarSearchIntent | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.intent is not None


def _location_from_frozen_listing(listing: dict) -> tuple[tuple[str, ...], str]:
    keys: list[str] = []
    for field in ("public_location_key", "canonical_area_key"):
        value = str(listing.get(field) or "").strip()
        if value and value not in keys:
            keys.append(value)
    display = str(
        listing.get("public_location_display")
        or listing.get("canonical_area_display")
        or ""
    ).strip()
    return tuple(keys), display


def _parse_budget_from_frozen(frozen_offer: dict) -> tuple[int | None, int | None, str]:
    """Extract budget range from frozen offer facts."""
    rent = frozen_offer.get("monthly_rent_usd")
    if rent is not None and int(rent) > 0:
        return int(rent), int(rent), f"${int(rent):,}/月"
    return None, None, ""


def _parse_room_type_from_frozen(frozen_listing: dict) -> tuple[str, str]:
    """Extract room_type and property_type from frozen listing facts."""
    return (
        str(frozen_listing.get("room_type") or "").strip(),
        str(frozen_listing.get("property_type") or "").strip(),
    )


class SimilarIntentService:
    """Resolve a guided-search intent without mutating search session state.

    For rented/offline listings: extract budget and room_type from frozen facts
    and set goal="any" so similar search runs immediately.
    For bookable listings: goal="budget" to ask for budget preference.
    """

    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def resolve(
        self,
        public_listing_id: object,
        *,
        source: str = "similar_listing",
    ) -> SimilarIntentResult:
        public_id = normalize_public_id(public_listing_id)
        if public_id is None:
            return SimilarIntentResult(
                status="invalid_link",
                reason="invalid_public_listing_id",
            )

        view = self.inventory.resolve(public_id)
        if view is None:
            return SimilarIntentResult(
                status="not_found",
                public_listing_id=public_id,
                reason="listing_not_publicly_published",
            )

        location_keys, area_display = _location_from_frozen_listing(
            view.frozen_listing
        )
        inventory_status = str(view.listing.get("inventory_status") or "").strip().lower()
        is_rented_offline = inventory_status in _RENTED_OFFLINE_STATUSES

        budget_min: int | None = None
        budget_max: int | None = None
        budget_label: str = ""
        room_type: str = ""
        property_type: str = ""

        if is_rented_offline:
            # Extract budget and room_type from frozen facts for direct similar search
            budget_min, budget_max, budget_label = _parse_budget_from_frozen(
                view.frozen_offer
            )
            room_type, property_type = _parse_room_type_from_frozen(
                view.frozen_listing
            )

        goal = _SIMILAR_GOAL_ANY if is_rented_offline else _SIMILAR_GOAL_BUDGET
        next_step = "search_submit" if is_rented_offline else "budget"

        return SimilarIntentResult(
            status="ok",
            public_listing_id=public_id,
            intent=SimilarSearchIntent(
                listing_id=view.listing_id,
                public_listing_id=public_id,
                source=str(source or "similar_listing").strip(),
                goal=goal,
                location_keys=location_keys,
                area_display=area_display,
                next_step=next_step,
                budget_min=budget_min,
                budget_max=budget_max,
                budget_label=budget_label,
                room_type=room_type,
                property_type=property_type,
            ),
        )


__all__ = [
    "SimilarIntentResult",
    "SimilarIntentService",
    "SimilarSearchIntent",
]
