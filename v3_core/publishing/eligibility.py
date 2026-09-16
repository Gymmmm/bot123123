"""Offer-aware V3 publication eligibility.

The production publishability contract remains the source of truth for rental
fact/media/cover requirements.  V3 applies that contract to a specific offer so
a listing containing both rent and sale can publish only its rent offer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.inventory.publishability import evaluate_publishability


@dataclass(frozen=True)
class PublicationEligibility:
    ok: bool
    blocking: tuple[str, ...]
    warnings: tuple[str, ...]
    score: int


def evaluate_offer_eligibility(
    *,
    facts: dict[str, Any],
    offer: dict[str, Any],
    review_approved: bool,
    media_count: int,
    cover_exists: bool,
) -> PublicationEligibility:
    offer_type = str(offer.get("offer_type") or "").strip()
    policy = str(offer.get("publication_policy") or "").strip()
    offer_status = str(offer.get("offer_status") or "").strip()

    blocking: list[str] = []
    warnings: list[str] = []

    if offer_type != "rent":
        blocking.append("offer_type_not_rent")
    if policy != "telegram_rent":
        blocking.append("publication_policy_not_telegram_rent")
    if offer_status != "active":
        blocking.append("offer_not_active")
    if not review_approved:
        blocking.append("review_required")

    rent = offer.get("monthly_rent_usd")
    try:
        rent_value = int(float(rent or 0))
    except (TypeError, ValueError):
        rent_value = 0
    if rent_value <= 0:
        blocking.append("missing_rent")

    # The legacy contract is listing-fact oriented and checks deal_type=rent.
    # Apply it to a rent-offer view instead of allowing a mixed canonical record
    # to block a valid rental offer.
    rent_facts = dict(facts or {})
    rent_facts["deal_type"] = "rent"
    rent_facts["monthly_rent_usd"] = rent_value or None
    contract = evaluate_publishability(
        rent_facts,
        media_count=int(media_count or 0),
        cover_exists=bool(cover_exists),
    )
    blocking.extend(str(value) for value in contract.get("blocking") or [])
    warnings.extend(str(value) for value in contract.get("warnings") or [])

    blocking = list(dict.fromkeys(value for value in blocking if value))
    warnings = list(dict.fromkeys(value for value in warnings if value))
    return PublicationEligibility(
        ok=not blocking,
        blocking=tuple(blocking),
        warnings=tuple(warnings),
        score=100 if not blocking else 0,
    )


__all__ = ["PublicationEligibility", "evaluate_offer_eligibility"]
