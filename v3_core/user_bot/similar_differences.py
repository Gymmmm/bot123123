"""Verified differences between a search card listing and the user's criteria.

V4.0 「相似房源明确标注差异」: only label a difference the data can actually
prove. Every rule mirrors the strict SQL filter in ``public_search`` so a
strict hit never gets a label, and unknown data never produces one.
Adjacency of areas is not modelled anywhere, so we only ever say the listing
is outside the chosen area — never "相邻区域".
"""
from __future__ import annotations

import re
from typing import Any, Mapping

from .search_query import SearchCriteria

_ROOM_ALIASES = {
    "studio": "studio", "开间": "studio", "单间": "studio",
    "1房": "1房", "一房": "1房",
    "2房": "2房", "两房": "2房", "二房": "2房",
    "3房": "3房", "三房": "3房",
    "4房": "4房", "四房": "4房", "四房+": "4房",
}
_LAYOUT_PATTERNS = {
    "1房": ("1房", "一房", "1br", "1 bed"),
    "2房": ("2房", "两房", "二房", "2br", "2 bed"),
    "3房": ("3房", "三房", "3br", "3 bed"),
    "4房": ("4房", "四房", "4br", "4 bed", "5房", "五房"),
}
_BEDROOMS = {"1房": 1, "2房": 2, "3房": 3}


def _int(value: object) -> int | None:
    try:
        number = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None
    return number


def _room_token(room_type: object) -> str:
    raw = str(room_type or "").strip()
    if not raw or raw in {"any", "不限"}:
        return ""
    return _ROOM_ALIASES.get(raw, "")


def room_matches(listing: Mapping[str, Any], room_type: object) -> bool | None:
    """Python mirror of ``public_search._room_type_sql``; ``None`` = unknown."""
    token = _room_token(room_type)
    if not token:
        return True
    layout = str(listing.get("layout") or "").strip().lower()
    bedrooms = _int(listing.get("bedrooms")) or 0
    if token == "studio":
        if not layout:
            return None
        return any(word in layout for word in ("studio", "开间", "单间"))
    if bedrooms > 0:
        return bedrooms >= 4 if token == "4房" else bedrooms == _BEDROOMS[token]
    if not layout:
        return None
    return any(pattern in layout for pattern in _LAYOUT_PATTERNS[token])


def area_matches(listing: Mapping[str, Any], location_keys: tuple[str, ...]) -> bool | None:
    keys = {str(k).strip() for k in location_keys if str(k or "").strip()}
    if not keys:
        return True
    listing_keys = {
        str(listing.get(name) or "").strip()
        for name in ("public_location_key", "canonical_area_key")
    } - {""}
    if not listing_keys:
        return None
    return bool(keys & listing_keys)


def listing_differences(
    view: Any,
    criteria: SearchCriteria | None,
    *,
    rent: int | None,
    location: str = "",
    layout: str = "",
) -> tuple[str, ...]:
    if criteria is None:
        return ()
    listing = getattr(view, "listing", None)
    if not isinstance(listing, Mapping):
        return ()
    labels: list[str] = []
    if rent is not None and rent > 0 and criteria.budget_max is not None and rent > int(criteria.budget_max):
        labels.append(f"💰 比预算高 ${rent - int(criteria.budget_max):,}")
    if area_matches(listing, tuple(criteria.location_keys or ())) is False:
        where = re.sub(r"\s+", " ", str(location or "")).strip()
        labels.append(f"📍 不在你选的区域（在 {where}）" if where else "📍 不在你选的区域")
    if room_matches(listing, criteria.room_type) is False:
        labels.append(f"🛏️ 户型不同：{layout}" if layout else "🛏️ 户型不同")
    return tuple(labels)


__all__ = ["area_matches", "listing_differences", "room_matches"]
