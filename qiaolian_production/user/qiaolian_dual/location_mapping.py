"""User-search location map derived from the canonical location taxonomy."""
from __future__ import annotations

from .listing_taxonomy import MARKET_LOCATIONS, PHYSICAL_AREAS


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

__all__ = ["LOCATION_MAP"]
