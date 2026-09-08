"""User-search projections derived from the canonical location taxonomy."""
from __future__ import annotations

from .listing_taxonomy import MARKET_LOCATIONS, PHYSICAL_AREAS, clean_text


def _build_location_map() -> dict[str, tuple[str, list[str]]]:
    result: dict[str, tuple[str, list[str]]] = {}
    for item in (*MARKET_LOCATIONS, *PHYSICAL_AREAS):
        current_display, current_aliases = result.get(item.key, (item.display, []))
        aliases = list(dict.fromkeys([
            item.key, current_display, item.display, *current_aliases, *item.aliases,
        ]))
        result[item.key] = (current_display, aliases)
    result["其他区域"] = ("其他区域", ["其他区域", "其他", "其他位置"])
    return result


LOCATION_MAP = _build_location_map()


def _canonical_key(value: object) -> str:
    candidate = clean_text(value).casefold()
    if not candidate:
        return ""
    for key, (display, aliases) in LOCATION_MAP.items():
        if candidate in {clean_text(item).casefold() for item in (key, display, *aliases)}:
            return key
    return ""


def get_display_location(db_area: str) -> str:
    if not db_area:
        return "位置待确认"
    key = _canonical_key(db_area)
    return LOCATION_MAP[key][0] if key else str(db_area)


def normalize_user_input(user_text: str) -> list[str]:
    if not user_text:
        return []
    text = clean_text(user_text).casefold()
    matched: list[str] = []
    for key, (_display, aliases) in LOCATION_MAP.items():
        for alias in aliases:
            alias_l = clean_text(alias).casefold()
            if alias_l and (alias_l in text or text in alias_l):
                if key not in matched:
                    matched.append(key)
                break
    return matched


def get_all_location_aliases(db_area: str) -> list[str]:
    key = _canonical_key(db_area)
    if not key:
        return [str(db_area)] if db_area else []
    display, aliases = LOCATION_MAP[key]
    return list(dict.fromkeys([key, display, *aliases]))


__all__ = [
    "LOCATION_MAP",
    "get_all_location_aliases",
    "get_display_location",
    "normalize_user_input",
]
