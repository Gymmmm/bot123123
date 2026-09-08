"""Compatibility area projection derived from the canonical listing taxonomy."""
from __future__ import annotations

import re

from .listing_taxonomy import classify_listing_taxonomy, resolve_location_alias


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_area(value: object, text: object = "") -> str:
    """Return only a Level-2 physical area; market/project labels stay separate."""
    direct = _clean(value)
    if direct:
        resolution = resolve_location_alias(direct)
        if resolution and resolution.kind == "physical_area":
            return resolution.display
        taxonomy = classify_listing_taxonomy(f"区域：{direct}")
        if taxonomy.canonical_area_display:
            return taxonomy.canonical_area_display
    raw = _clean(text)
    if raw:
        taxonomy = classify_listing_taxonomy(raw)
        if taxonomy.canonical_area_display:
            return taxonomy.canonical_area_display
    return ""


__all__ = ["normalize_area"]
