"""Canonical V3 guided-search navigation options.

This module is deliberately pure.  It owns the short callback codes and their
user-facing labels, then delegates location/property normalization to the V3
search parser instead of reviving fixed-SHA callback maps in Telegram handlers.
"""
from __future__ import annotations

from .search_query import detect_location_keys, detect_property_type


AREA_OPTIONS: tuple[tuple[str, str], ...] = (
    ("bkk1", "BKK1（市中心）"),
    ("bkk23", "BKK2 / BKK3（BKK周边）"),
    ("koh", "钻石岛"),
    ("jinjie", "金街附近"),
    ("rf", "富力城"),
    ("aeon1", "永旺1附近"),
    ("tk", "堆谷（TK）"),
    ("russian", "俄罗斯市场附近"),
    ("pp", "炳发城"),
    ("chroy", "水净华半岛"),
    ("sen", "森速（永旺2一带）"),
)

LAYOUT_OPTIONS: tuple[tuple[str, str], ...] = (
    ("studio", "单间"),
    ("1br", "一房"),
    ("2br", "两房"),
    ("3br", "三房"),
    ("4br", "四房+"),
    ("any", "不限"),
)

AREA_DESCRIPTIONS = {
    "bkk1": "商务与餐饮较集中，生活便利；部分路段高峰较拥堵。",
    "bkk23": "邻近BKK1，住宅与本地商业混合，可比较不同租金。",
    "koh": "公寓与商业项目集中，部分住宅有河景或社区设施。",
    "jinjie": "中餐、超市和日常配套较多，适合重视生活便利的租客。",
    "rf": "大型公寓社区，户型与物业收费因楼栋而异。",
    "aeon1": "靠近购物中心，餐饮、办公与住宅项目较集中。",
    "tk": "学校、住宅及商业项目较多，也有排屋与别墅。",
    "russian": "市场、咖啡馆和日常生活设施丰富，住宅选择多。",
    "pp": "多个排屋与别墅社区，适合需要较多卧室的家庭。",
    "chroy": "滨河住宅和新项目较多，部分公寓可选河景。",
    "sen": "商场、超市与住宅项目较多，房型选择较丰富。",
}


def area_description(label: str) -> str:
    for code, display in AREA_OPTIONS:
        if str(label or "").strip() == display:
            return AREA_DESCRIPTIONS.get(code, "")
    return ""

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
    "AREA_DESCRIPTIONS",
    "area_description",
    "LAYOUT_OPTIONS",
    "area_selection",
    "layout_selection",
]
