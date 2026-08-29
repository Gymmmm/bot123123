"""One-way projections and gates for canonical listing facts.

This module is the sole bridge from normalized_data to draft/listing/package
consumers.  It deliberately has no parser, template or Telegram dependencies.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from qiaolian_dual.canonical_facts import SCHEMA_VERSION, draft_projection
from qiaolian_dual.listing_taxonomy import market_location_by_key, physical_area_by_key
from qiaolian_dual.publishability_contract import evaluate_publishability


def facts_hash(facts: dict[str, Any]) -> str:
    payload = {key: value for key, value in dict(facts or {}).items() if key != "canonical_facts_hash"}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def validate_facts(facts: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not isinstance(facts, dict):
        return ["canonical_facts_invalid"]
    if facts.get("schema_version") != SCHEMA_VERSION:
        errors.append("canonical_facts_schema_invalid")
    if not facts.get("canonical_facts_hash"):
        errors.append("canonical_facts_hash_missing")
    elif facts_hash(facts) != facts.get("canonical_facts_hash"):
        errors.append("canonical_facts_hash_mismatch")
    quality = facts.get("quality") or {}
    if not isinstance(quality, dict):
        errors.append("canonical_quality_invalid")
    canonical_key = str(facts.get("canonical_area_key") or "").strip()
    canonical_display = str(facts.get("canonical_area_display") or "").strip()
    public_key = str(facts.get("public_location_key") or "").strip()
    public_display = str(facts.get("public_location_display") or "").strip()
    publication_level = str(facts.get("publication_location_level") or "unknown").strip()
    area_status = str(facts.get("area_status") or "unconfirmed").strip()
    city_values = {"金边", "金边市", "金边市区", "phnom_penh", "phnom penh", "phnompenh", "phnom penh city"}
    if canonical_key:
        physical = physical_area_by_key(canonical_key)
        if not physical:
            errors.append("canonical_area_not_in_physical_catalog")
        elif not canonical_display:
            errors.append("canonical_area_display_missing")
        elif canonical_display != physical.display:
            errors.append("canonical_area_display_mismatch")
        if area_status != "confirmed":
            errors.append("canonical_area_status_mismatch")
    elif canonical_display:
        errors.append("canonical_area_key_missing_for_display")
    elif area_status == "confirmed":
        errors.append("canonical_area_status_mismatch")
    if not canonical_key and facts.get("canonical_area_level"):
        errors.append("canonical_area_level_without_area")
    if canonical_key.casefold() in city_values or canonical_display.casefold() in city_values:
        errors.append("city_used_as_canonical_area")
    if public_key.casefold() in city_values or public_display.casefold() in city_values:
        errors.append("city_used_as_public_location")
    if bool(public_key) != bool(public_display):
        errors.append("public_location_key_display_incomplete")

    market_keys = facts.get("market_location_keys") or []
    market_displays = facts.get("market_location_displays") or []
    if not isinstance(market_keys, list) or not isinstance(market_displays, list):
        errors.append("market_location_candidates_invalid")
        market_keys, market_displays = [], []
    elif len(market_keys) != len(market_displays):
        errors.append("market_location_candidate_count_mismatch")
    for index, key in enumerate(market_keys):
        market = market_location_by_key(key)
        if not market:
            errors.append("market_location_candidate_not_in_catalog")
            continue
        if index >= len(market_displays) or str(market_displays[index] or "").strip() != market.display:
            errors.append("market_location_candidate_display_mismatch")

    known_levels = {
        "unknown", "level_2_physical_confirmed",
        "level_1_market_confirmed", "level_1_project_confirmed",
    }
    if publication_level not in known_levels:
        errors.append("publication_location_level_invalid")
    if publication_level == "level_2_physical_confirmed":
        if not canonical_key or not physical_area_by_key(canonical_key):
            errors.append("level_2_physical_area_missing")
        if public_key != canonical_key or public_display != canonical_display:
            errors.append("level_2_public_location_mismatch")
    elif publication_level == "level_1_market_confirmed":
        market = market_location_by_key(public_key)
        if not market:
            errors.append("level_1_market_not_in_catalog")
        elif public_display != market.display:
            errors.append("level_1_market_display_mismatch")
        if public_key not in [str(value or "").strip() for value in market_keys]:
            errors.append("level_1_market_not_in_fact_candidates")
        if canonical_key:
            errors.append("market_location_promoted_to_canonical_area")
    elif publication_level == "level_1_project_confirmed":
        if not facts.get("project_key") or public_key != str(facts.get("project_key")):
            errors.append("level_1_project_location_mismatch")
        if public_display != str(facts.get("project_name") or "").strip():
            errors.append("level_1_project_display_mismatch")
    elif publication_level == "unknown" and (public_key or public_display):
        errors.append("unknown_level_has_public_location")
    return errors


def listing_projection(facts: dict[str, Any], listing_id: str, source_post_url: str = "") -> dict[str, Any]:
    """Project one canonical fact object into user-search listing values.

    `area` intentionally receives the safe public location.  Exact geography
    remains in canonical fields, not in this legacy searchable display column.
    """
    draft = draft_projection(facts)
    return {
        "listing_id": str(listing_id),
        "title": draft["title"],
        "property_type": draft["property_type"],
        "property_subtype": draft["property_subtype"],
        "property_type_display": draft["property_type_display"],
        "area": draft["public_location_display"],
        "public_location_key": draft["public_location_key"],
        "public_location_display": draft["public_location_display"],
        "publication_location_level": draft["publication_location_level"],
        "canonical_area_key": facts.get("canonical_area_key"),
        "canonical_area_display": facts.get("canonical_area_display"),
        "community": draft["community"],
        "project": draft["project"],
        "project_name": facts.get("project_name"),
        "project_alias": facts.get("project_alias"),
        "project_brand": facts.get("project_brand"),
        "price": draft["price"],
        "currency": "USD",
        "layout": draft["layout"],
        "size_sqm": facts.get("size_sqm"),
        "floor": draft["floor"],
        "deposit_rule": draft["deposit"],
        "contract_term": draft["contract_term"],
        "available_date": draft["available_date"],
        "hidden_costs": "；".join(
            f"{label}：{value}"
            for label, value in (
                ("管理费", draft["management_fee"]),
                ("网络", draft["internet_fee"]),
                ("水费", draft["water_rate"]),
                ("电费", draft["electric_rate"]),
                ("停车", draft["parking_fee"]),
            )
            if value
        ),
        "tags_json": json.dumps({
            "market_location_keys": facts.get("market_location_keys") or [],
            "special_tags": facts.get("special_tags") or [],
            "property_subtype": facts.get("property_subtype"),
        }, ensure_ascii=False, sort_keys=True),
        "highlights": "\n".join(draft["highlights"]),
        "source_post_url": str(source_post_url or ""),
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
        "canonical_facts_schema": facts.get("schema_version"),
    }


def package_gate(facts: dict[str, Any], media_count: int, *, cover_exists: bool = True) -> dict[str, Any]:
    """Evaluate package eligibility using the single publishability contract."""
    structural_errors = validate_facts(facts)
    contract = evaluate_publishability(facts, media_count=media_count, cover_exists=cover_exists)
    errors = list(dict.fromkeys(structural_errors + contract["blocking"]))
    return {
        "ok": not errors,
        "errors": errors,
        "warnings": contract["warnings"],
        "score": contract["score"],
        "publication_location_level": facts.get("publication_location_level") or "unknown",
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
    }


def package_snapshot(facts: dict[str, Any], listing_id: str, media_hashes: list[str]) -> dict[str, Any]:
    """Return the fact portion of a frozen package snapshot."""
    validation_errors = validate_facts(facts)
    if validation_errors:
        raise ValueError(";".join(validation_errors))
    return {
        "schema": "canonical_package_facts.v1",
        "listing_id": str(listing_id),
        "canonical_facts_schema": facts["schema_version"],
        "canonical_facts_hash": facts["canonical_facts_hash"],
        "publication_location_level": facts.get("publication_location_level"),
        "public_location_key": facts.get("public_location_key"),
        "public_location_display": facts.get("public_location_display"),
        "canonical_area_key": facts.get("canonical_area_key"),
        "project_name": facts.get("project_name"),
        "project_alias": facts.get("project_alias"),
        "project_brand": facts.get("project_brand"),
        "property_type": facts.get("property_type"),
        "property_subtype": facts.get("property_subtype"),
        "property_type_display": facts.get("property_type_display"),
        "price": facts.get("monthly_rent_usd"),
        "original_price": facts.get("original_monthly_rent_usd"),
        "layout": facts.get("layout"),
        "floor": facts.get("floor"),
        "size_sqm": facts.get("size_sqm"),
        "land_dimension": facts.get("land_dimension"),
        "building_dimension": facts.get("building_dimension"),
        "deposit_payment_terms": facts.get("deposit_payment_terms"),
        "contract_term_display": facts.get("contract_term_display"),
        "available_date": facts.get("available_date"),
        "management_fee": facts.get("management_fee"),
        "internet_fee": facts.get("internet_fee"),
        "water_rate": facts.get("water_rate"),
        "electric_rate": facts.get("electric_rate"),
        "parking_fee": facts.get("parking_fee"),
        "viewing_time": facts.get("viewing_time"),
        "video_viewing": facts.get("video_viewing"),
        "media_asset_hashes": list(media_hashes or []),
    }


def facts_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return bool(left and right and left.get("canonical_facts_hash") and left.get("canonical_facts_hash") == right.get("canonical_facts_hash"))


__all__ = ["facts_equal", "facts_hash", "listing_projection", "package_gate", "package_snapshot", "validate_facts"]
