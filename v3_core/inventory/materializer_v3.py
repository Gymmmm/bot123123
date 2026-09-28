"""V3 canonical inventory projections.

This is the clean successor to the legacy ``draft_projection`` -> ``listings``
materialization.  Shared property facts become one listing identity; rental and
sale terms become separate offers.  Publication eligibility remains a separate
policy concern.
"""
from __future__ import annotations

from typing import Any


def _positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def listing_projection_v3(facts: dict[str, Any]) -> dict[str, Any]:
    facts = dict(facts or {})
    return {
        "project_name": str(facts.get("project_name") or ""),
        "project_alias": str(facts.get("project_alias") or ""),
        "project_brand": str(facts.get("project_brand") or ""),
        "property_type": str(facts.get("property_type") or "未知"),
        "property_subtype": str(facts.get("property_subtype") or ""),
        "public_location_key": str(facts.get("public_location_key") or ""),
        "public_location_display": str(facts.get("public_location_display") or ""),
        "canonical_area_key": str(facts.get("canonical_area_key") or ""),
        "canonical_area_display": str(facts.get("canonical_area_display") or ""),
        "publication_location_level": str(
            facts.get("publication_location_level") or "unknown"
        ),
        "layout": str(facts.get("layout") or ""),
        "bedrooms": facts.get("bedrooms"),
        "bathrooms": facts.get("bathrooms"),
        "size_sqm": facts.get("size_sqm"),
        "floor": str(facts.get("floor") or ""),
        "display_title": str(facts.get("display_title") or "待确认房源"),
        "canonical_facts_hash": str(facts.get("canonical_facts_hash") or ""),
        "canonical_facts_schema": str(facts.get("schema_version") or ""),
    }


def offer_projections_v3(facts: dict[str, Any]) -> list[dict[str, Any]]:
    """Create zero, one, or two offers from explicit canonical prices.

    A mixed source is represented by two offers, not a ``mixed`` listing type.
    Sale is stored normally but explicitly blocked from the rent Telegram
    publisher until a future sale publisher policy is enabled.
    """
    facts = dict(facts or {})
    offers: list[dict[str, Any]] = []

    rent = _positive_number(facts.get("monthly_rent_usd"))
    if rent is not None:
        offers.append(
            {
                "offer_type": "rent",
                "currency": "USD",
                "monthly_rent_usd": int(rent),
                "sale_price_usd": None,
                "deposit_terms": str(facts.get("deposit_payment_terms") or ""),
                "payment_terms": str(facts.get("deposit_payment_terms") or ""),
                "contract_term": str(facts.get("contract_term_display") or ""),
                "available_date": str(facts.get("available_date") or ""),
                "offer_status": "active",
                "publication_policy": "telegram_rent",
                "publishable": False,
                "publish_block_reason": "review_required",
            }
        )

    sale = _positive_number(facts.get("sale_price_usd"))
    if sale is not None:
        offers.append(
            {
                "offer_type": "sale",
                "currency": "USD",
                "monthly_rent_usd": None,
                "sale_price_usd": int(sale),
                "deposit_terms": "",
                "payment_terms": "",
                "contract_term": "",
                "available_date": str(facts.get("available_date") or ""),
                "offer_status": "active",
                "publication_policy": "store_only",
                "publishable": False,
                "publish_block_reason": "sale_not_enabled_for_telegram",
            }
        )

    return offers


def telegram_rent_candidate(offer: dict[str, Any]) -> bool:
    return (
        str(offer.get("offer_type") or "") == "rent"
        and str(offer.get("publication_policy") or "") == "telegram_rent"
        and str(offer.get("offer_status") or "") == "active"
    )


__all__ = [
    "listing_projection_v3",
    "offer_projections_v3",
    "telegram_rent_candidate",
]
