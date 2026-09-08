"""V3 canonical parser built on the locked V2.2 canonical implementation.

The locked parser logic is preserved verbatim in ``canonical_v22_locked.py``.
This module adapts only V3 contracts: V3-local taxonomy, three-state deal type,
conflict review semantics, safe enrichment, and a business-facts-only hash.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import sys
import types
from typing import Any

from .safe_enrichment import enrich_safe
from .taxonomy import classify_listing_taxonomy, public_location_from_fields

SCHEMA_VERSION = "canonical_facts.v3"
PARSER_REVISION = "v3.phase3"

# Load the exact locked V2.2 canonical blob while binding its taxonomy imports to
# the V3-extracted taxonomy implementation. No production legacy code is edited.
_TAXONOMY_MODULE = "qiaolian_dual.listing_taxonomy"
_shim = types.ModuleType(_TAXONOMY_MODULE)
_shim.classify_listing_taxonomy = classify_listing_taxonomy
_shim.public_location_from_fields = public_location_from_fields
_previous_taxonomy = sys.modules.get(_TAXONOMY_MODULE)
sys.modules[_TAXONOMY_MODULE] = _shim
try:
    _locked = importlib.import_module("qiaolian_v3.parser.canonical_v22_locked")
finally:
    if _previous_taxonomy is None:
        sys.modules.pop(_TAXONOMY_MODULE, None)
    else:
        sys.modules[_TAXONOMY_MODULE] = _previous_taxonomy

# Stable business facts only. Operational evidence, source/revision identity,
# local paths, transport metadata, DB ids, timestamps, review notes and copy are
# deliberately excluded from the canonical business hash.
_BUSINESS_FACT_KEYS = (
    "city_key",
    "city_display",
    "deal_type",
    "deal_type_candidates",
    "canonical_area_key",
    "canonical_area_display",
    "area_status",
    "canonical_area_level",
    "market_location_keys",
    "market_location_displays",
    "project_name",
    "project_alias",
    "project_key",
    "project_brand",
    "project_brand_key",
    "community_name",
    "property_type",
    "property_subtype",
    "property_type_display",
    "property_type_status",
    "layout",
    "bedrooms",
    "living_rooms",
    "bathrooms",
    "helper_rooms",
    "monthly_rent_usd",
    "original_monthly_rent_usd",
    "price_status",
    "sale_price_usd",
    "sale_price_status",
    "size_sqm",
    "land_dimension",
    "building_dimension",
    "land_size_sqm",
    "building_size_sqm",
    "unlabelled_dimension",
    "floor",
    "deposit_payment_terms",
    "deposit_months",
    "prepay_months",
    "contract_term_months",
    "contract_term_display",
    "available_date",
    "management_fee",
    "internet_fee",
    "water_rate",
    "electric_rate",
    "parking_fee",
    "viewing_time",
    "video_viewing",
    "highlights",
    "special_tags",
    "public_location_key",
    "public_location_display",
    "publication_location_level",
    "services",
    "included",
    "amenities",
    "rental",
    "house",
)


def canonical_business_projection(facts: dict[str, Any]) -> dict[str, Any]:
    """Return the allowlisted, stable business-facts projection used for hashing."""
    return {key: facts.get(key) for key in _BUSINESS_FACT_KEYS if key in facts}


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_deal_contract(facts: dict[str, Any]) -> None:
    deal_type = str(facts.get("deal_type") or "unknown")
    candidate_flags = [str(value) for value in (facts.get("candidate_flags") or [])]
    quality = dict(facts.get("quality") or {})
    review_flags = [str(value) for value in (quality.get("review_flags") or [])]
    hard_flags = [str(value) for value in (quality.get("hard_flags") or [])]
    warning_flags = [str(value) for value in (quality.get("warning_flags") or [])]
    info_flags = [str(value) for value in (quality.get("info_flags") or [])]

    # Remove V2-only deal labels from the formal V3 contract.
    candidate_flags = [v for v in candidate_flags if v not in {"ambiguous_deal_type", "mixed_sale_rent_terms"}]
    review_flags = [v for v in review_flags if v not in {"ambiguous_deal_type", "mixed_sale_rent_terms", "missing_rental_intent"}]
    hard_flags = [v for v in hard_flags if v != "non_rental_source"]

    if deal_type == "mixed":
        facts["deal_type"] = "unknown"
        facts["deal_type_candidates"] = ["rent", "sale"]
        candidate_flags.append("conflicting_deal_type")
        review_flags.append("conflicting_deal_type")
        evidence = facts.setdefault("evidence", {})
        evidence["deal_type"] = [{
            "value": ["rent", "sale"],
            "source": "raw_deal_terms",
            "confidence": "high",
            "raw_excerpt": "rent_and_sale_terms",
        }]
    elif deal_type in {"rent", "sale"}:
        facts["deal_type"] = deal_type
        facts["deal_type_candidates"] = []
    else:
        facts["deal_type"] = "unknown"
        facts["deal_type_candidates"] = list(facts.get("deal_type_candidates") or [])
        if not facts["deal_type_candidates"]:
            review_flags.append("missing_rental_intent")

    facts["candidate_flags"] = list(dict.fromkeys(candidate_flags))
    review_flags = list(dict.fromkeys(review_flags))
    hard_flags = list(dict.fromkeys(hard_flags))
    warning_flags = list(dict.fromkeys(warning_flags))
    info_flags = list(dict.fromkeys(info_flags))
    blocking = list(dict.fromkeys(hard_flags + review_flags))
    quality.update({
        "hard_flags": hard_flags,
        "review_flags": review_flags,
        "warning_flags": warning_flags,
        "info_flags": info_flags,
        "blocking_flags": blocking,
        "all_flags": list(dict.fromkeys(hard_flags + review_flags + warning_flags + info_flags)),
        "score": max(0, 100 - 30 * len(hard_flags) - 12 * len(review_flags) - 4 * len(warning_flags)),
    })
    facts["quality"] = quality


def canonicalize_source(
    raw_text: str,
    sanitized_text: str | None = None,
    source_identity: dict[str, Any] | None = None,
    media_summary: dict[str, Any] | None = None,
    manual_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create V3 canonical facts without shrinking locked V2.2 extraction ability."""
    facts = _locked.canonicalize_source(
        raw_text=raw_text,
        sanitized_text=sanitized_text,
        source_identity=source_identity,
        media_summary=media_summary,
        manual_overrides=manual_overrides,
    )
    facts["schema_version"] = SCHEMA_VERSION
    facts["parser_revision"] = PARSER_REVISION
    _normalize_deal_contract(facts)

    parse_text = str(sanitized_text if sanitized_text is not None else raw_text or "")
    facts = enrich_safe(parse_text, facts)

    # Safe enrichment is additive. Re-assert the formal deal contract in case a
    # future enrichment layer introduces review metadata around it.
    _normalize_deal_contract(facts)
    facts["canonical_facts_hash"] = _stable_hash(canonical_business_projection(facts))
    return facts


def draft_projection(facts: dict[str, Any]) -> dict[str, Any]:
    return _locked.draft_projection(facts)


def is_buildable(facts: dict[str, Any]) -> bool:
    return not bool((facts.get("quality") or {}).get("blocking_flags"))


def has_confirmed_physical_area(facts: dict[str, Any]) -> bool:
    return bool(facts.get("canonical_area_key")) and facts.get("publication_location_level") == "level_2_physical_confirmed"


__all__ = [
    "SCHEMA_VERSION",
    "PARSER_REVISION",
    "canonical_business_projection",
    "canonicalize_source",
    "draft_projection",
    "is_buildable",
    "has_confirmed_physical_area",
]
