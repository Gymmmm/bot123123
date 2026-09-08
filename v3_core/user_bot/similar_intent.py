"""Guided similar-listing intent for the side-by-side V3 User Bot.

Fixed-SHA ``listing:similar`` does not immediately execute a relaxed search. It
uses the current listing's area, resets the goal to ``any``, and asks the user
for a budget. V3 preserves that behavior as a pure intent derived only from the
frozen public listing snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .public_inventory import PublicInventoryReader


SimilarIntentStatus = Literal["ok", "invalid_link", "not_found"]


@dataclass(frozen=True)
class SimilarSearchIntent:
    listing_id: str
    public_listing_id: str
    source: str
    goal: str
    location_keys: tuple[str, ...]
    area_display: str
    next_step: str


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


class SimilarIntentService:
    """Resolve a guided-search intent without mutating search session state."""

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
        return SimilarIntentResult(
            status="ok",
            public_listing_id=public_id,
            intent=SimilarSearchIntent(
                listing_id=view.listing_id,
                public_listing_id=public_id,
                source=str(source or "similar_listing").strip(),
                goal="any",
                location_keys=location_keys,
                area_display=area_display,
                next_step="budget",
            ),
        )


__all__ = [
    "SimilarIntentResult",
    "SimilarIntentService",
    "SimilarSearchIntent",
]
