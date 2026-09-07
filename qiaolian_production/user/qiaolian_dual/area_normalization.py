"""Compatibility projections derived from the canonical listing taxonomy.

This module intentionally owns no independent area/project catalog. Automatic
classification and administrator overrides both resolve through
listing_taxonomy.py.
"""
from __future__ import annotations

import re

from .listing_taxonomy import (
    MARKET_LOCATIONS,
    PHYSICAL_AREAS,
    PROJECT_IDENTITIES,
    classify_listing_taxonomy,
    public_location,
    resolve_location_alias,
)


CANONICAL_AREAS = tuple(item.display for item in PHYSICAL_AREAS)
ALIASES = {
    item.display: tuple(dict.fromkeys((item.key, item.display, *item.aliases)))
    for item in PHYSICAL_AREAS
}
PROJECT_ALIASES = {
    item.display: tuple(dict.fromkeys((item.display, *item.aliases)))
    for item in PROJECT_IDENTITIES
    if item.kind == "project"
}


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_area(value: object, text: object = "") -> str:
    """Return only a Level-2 physical area; market/project labels stay separate."""
    direct = _clean(value)
    if direct:
        resolution = resolve_location_alias(direct)
        if resolution and resolution.kind == "physical_area":
            return resolution.display
        # value comes from a field, so classify it as an explicit location
        # while retaining the canonical taxonomy's alias/token rules.
        taxonomy = classify_listing_taxonomy(f"区域：{direct}")
        if taxonomy.canonical_area_display:
            return taxonomy.canonical_area_display
    raw = _clean(text)
    if raw:
        taxonomy = classify_listing_taxonomy(raw)
        if taxonomy.canonical_area_display:
            return taxonomy.canonical_area_display
    return ""


def normalize_public_location(value: object) -> str:
    """Resolve an exact operator-entered physical or market location alias."""
    resolution = resolve_location_alias(value)
    return resolution.display if resolution else ""


def normalize_project_name(value: object, text: object = "") -> str:
    """Normalize a project from the canonical project catalog only."""
    direct = _clean(value)
    if direct:
        taxonomy = classify_listing_taxonomy(f"项目：{direct}")
        if taxonomy.project_name:
            return taxonomy.project_name
    raw = _clean(text)
    if raw:
        taxonomy = classify_listing_taxonomy(raw)
        if taxonomy.project_name:
            return taxonomy.project_name
    return ""


def apply_to_parsed(parsed: dict, raw_text: str = "") -> dict:
    """Legacy parsed-dict adapter backed by the canonical taxonomy."""
    data = dict(parsed or {})
    taxonomy = classify_listing_taxonomy(str(raw_text or ""))
    canonical_project = (
        normalize_project_name(data.get("project"))
        or normalize_project_name(data.get("community"))
        or taxonomy.project_name
        or ""
    )
    if canonical_project:
        data["project"] = canonical_project
        if normalize_project_name(data.get("community")):
            data["community"] = canonical_project

    direct = resolve_location_alias(data.get("normalized_area") or data.get("area"))
    if direct:
        data["area"] = direct.display
        data["location_anchor"] = direct.key
        data["normalized_area"] = direct.key if direct.kind == "physical_area" else None
    else:
        public_key, public_display, _level = public_location(taxonomy)
        data["normalized_area"] = taxonomy.canonical_area_key
        data["area"] = public_display or ""
        data["location_anchor"] = public_key or ""
    raw = _clean(raw_text).lower()
    anchor = str(data.get("area") or "")
    aliases: tuple[str, ...] = ()
    for item in (*PHYSICAL_AREAS, *MARKET_LOCATIONS):
        if item.display == anchor:
            aliases = item.aliases
            break
    data["nearby"] = any(
        re.search(re.escape(alias.lower()) + r"\s*(?:附近|周边)", raw)
        for alias in aliases
        if alias
    )
    return data


__all__ = [
    "ALIASES",
    "CANONICAL_AREAS",
    "PROJECT_ALIASES",
    "apply_to_parsed",
    "normalize_area",
    "normalize_project_name",
    "normalize_public_location",
]
