"""Canonical V3 guided-search navigation options.

This module is deliberately pure.  It owns the short callback codes and their
user-facing labels, then delegates location/property normalization to the V3
search parser instead of reviving fixed-SHA callback maps in Telegram handlers.
"""
from __future__ import annotations

from .search_query import detect_location_keys, detect_property_type


AREA_OPTIONS: tuple[tuple[str, str], ...] = (
    ("bkk1", "BKK1"),
    ("bkk23", "BKK2/3"),
    ("koh", "钻石岛"),
    ("rf", "富力城"),
    ("aeon1", "永旺1"),
    ("tk", "TK"),
    ("russian", "俄市"),
    ("pp", "炳发城"),
    ("chroy", "水净华"),
    ("sen", "森速"),
)

LAYOUT_OPTIONS: tuple[tuple[str, str], ...] = (
    ("studio", "单间"),
    ("1br", "一房"),
    ("2br", "两房"),
    ("3br", "三房"),
    ("4br", "四房+"),
    ("any", "不限"),
)

_AREA_LABELS = dict(AREA_OPTIONS)
_LAYOUT_LABELS = dict(LAYOUT_OPTIONS)


def area_selection(code: object) -> tuple[str, tuple[str, ...]]:
    clean = str(code or "").strip().lower()
    display = _AREA_LABELS.get(clean)
    if not display:
        raise ValueError("unsupported_search_area_choice")
    keys = detect_location_keys(display)
    if not keys:
        raise ValueError("search_area_choice_has_no_canonical_key")
    return display, keys


def layout_selection(code: object) -> tuple[str, str, str]:
    """Return display label, room-type token, and fixed-SHA property filter.

    Fixed production only derived a property-type filter from the selected room
    token.  That means Studio maps to apartment while bedroom-count choices do
    not become a strict property-type filter.  Keep that behavior until a
    separately approved canonical room-count query is added.
    """
    clean = str(code or "").strip().lower()
    display = _LAYOUT_LABELS.get(clean)
    if not display:
        raise ValueError("unsupported_search_layout_choice")
    if clean == "any":
        return display, "", ""
    room_token = {
        "studio": "studio",
        "1br": "1房",
        "2br": "2房",
        "3br": "3房",
        "4br": "4房",
    }[clean]
    return display, room_token, detect_property_type(room_token)


__all__ = [
    "AREA_OPTIONS",
    "LAYOUT_OPTIONS",
    "area_selection",
    "layout_selection",
]
