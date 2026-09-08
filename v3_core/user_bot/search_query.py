"""Pure renter search parsing for the V3 User Bot.

Budget/property/room parsing follows the locked production User Bot. Location
matching is derived from the V3 canonical taxonomy instead of maintaining a
second alias table. Room type is intentionally recorded but not applied as a
strict DB filter yet, matching fixed-SHA production behavior.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from v3_core.inventory.listing_taxonomy import MARKET_LOCATIONS, PHYSICAL_AREAS, clean_text


_ROOM_TYPE_HINTS = {
    "studio": ("studio", "开间", "单间"),
    "1房": ("1房", "一房", "1br", "1 bed", "一居"),
    "2房": ("2房", "二房", "2br", "2 bed", "两居"),
    "3房": ("3房", "三房", "3br", "3 bed", "三居"),
}

_PROPERTY_TYPES = (
    ("别墅", ("别墅", "villa")),
    ("排屋", ("排屋", "townhouse")),
    ("商铺", ("商铺", "店铺", "shophouse", "shop")),
    ("办公室", ("办公室", "office")),
    ("公寓", ("公寓", "apartment", "studio")),
    ("住宅", ("住宅",)),
)


def parse_budget_range(text: str) -> tuple[int | None, int | None]:
    raw = str(text or "")
    values = [int(value) for value in re.findall(r"\d{2,5}", raw)]
    if not values:
        return (None, None)
    if len(values) == 1:
        value = values[0]
        if re.search(r"(以内|以下|不超过|最多)", raw):
            return (None, value)
        if re.search(r"(以上|起|至少)", raw):
            return (value, None)
        return (max(0, value - 200), value + 200)
    low, high = min(values[0], values[1]), max(values[0], values[1])
    return (low, high)


def detect_room_type(text: str) -> str:
    lowered = str(text or "").lower()
    for label, variants in _ROOM_TYPE_HINTS.items():
        if any(value in lowered for value in variants):
            return label
    return ""


def detect_property_type(text: str) -> str:
    lowered = str(text or "").lower()
    for canonical, aliases in _PROPERTY_TYPES:
        if any(alias in lowered for alias in aliases):
            return canonical
    return ""


def _location_aliases() -> tuple[tuple[str, tuple[str, ...]], ...]:
    merged: dict[str, list[str]] = {}
    displays: dict[str, str] = {}
    for item in (*MARKET_LOCATIONS, *PHYSICAL_AREAS):
        displays.setdefault(item.key, item.display)
        bucket = merged.setdefault(item.key, [])
        for value in (item.key, item.display, *item.aliases):
            cleaned = clean_text(value)
            if cleaned and cleaned not in bucket:
                bucket.append(cleaned)
    return tuple((key, tuple((displays[key], *values))) for key, values in merged.items())


_LOCATION_ALIASES = _location_aliases()


def detect_location_keys(text: str) -> tuple[str, ...]:
    raw = clean_text(text).casefold()
    if not raw or "不限" in raw:
        return ()
    matched: list[str] = []
    for key, aliases in _LOCATION_ALIASES:
        for alias in aliases:
            token = clean_text(alias).casefold()
            if token and (token in raw or raw in token):
                if key not in matched:
                    matched.append(key)
                break

    # The canonical taxonomy intentionally contains both the broad BKK market
    # label and the specific BKK1/BKK2/BKK3 locations.  Production detect_area()
    # takes the first specific match, so a query such as "BKK1" must not widen
    # itself to the parent BKK search bucket.  Preserve multiple explicit child
    # matches (for example "BKK2 / BKK3").
    if "BKK" in matched and any(key in matched for key in ("BKK1", "BKK2", "BKK3")):
        matched = [key for key in matched if key != "BKK"]
    return tuple(matched)


@dataclass(frozen=True)
class SearchCriteria:
    property_type: str = ""
    location_keys: tuple[str, ...] = ()
    budget_min: int | None = None
    budget_max: int | None = None
    room_type: str = ""
    raw_text: str = ""

    @property
    def has_filter(self) -> bool:
        return bool(
            self.property_type
            or self.location_keys
            or self.budget_min is not None
            or self.budget_max is not None
        )


def parse_search_criteria(text: str) -> SearchCriteria:
    raw = str(text or "").strip()
    budget_min, budget_max = parse_budget_range(raw)
    return SearchCriteria(
        property_type=detect_property_type(raw),
        location_keys=detect_location_keys(raw),
        budget_min=budget_min,
        budget_max=budget_max,
        room_type=detect_room_type(raw),
        raw_text=raw,
    )


__all__ = [
    "SearchCriteria",
    "detect_location_keys",
    "detect_property_type",
    "detect_room_type",
    "parse_budget_range",
    "parse_search_criteria",
]
