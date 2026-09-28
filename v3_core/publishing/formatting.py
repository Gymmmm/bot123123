"""Pure public display formatting extracted from qiaolian_dual.utils_formatting.

Only the layout/floor behavior used by public rendering lives here. Public
labels are normalized for the Chinese-facing QiaoLian channel and covers.
"""
from __future__ import annotations

import re


def display_layout(layout: object, property_type: object = "") -> str:
    raw = re.sub(r"\s+", "", str(layout or "").strip())
    if not raw:
        return ""

    lowered = raw.lower()
    # Public channel/cover terminology is Chinese-first. Keep parser/database
    # aliases flexible, but never expose Studio/开间 variants to customers.
    if lowered in {"studio", "studio公寓", "开间", "开间公寓", "单间", "单间公寓"}:
        return "单间"

    kind = str(property_type or "").strip()
    is_commercial = any(
        word in kind for word in ("办公室", "办公楼", "写字楼", "商铺", "商业")
    )
    if not is_commercial:
        match = re.fullmatch(r"(\d+)房(\d+)办公(\d+)卫", raw)
        if match:
            rooms, studies, baths = match.groups()
            study_text = "书房" if studies == "1" else f"{studies}书房"
            return f"{rooms}房＋{study_text}｜{baths}卫"
        match = re.fullmatch(r"(\d+)房(\d+)厅(\d+)卫", raw)
        if match:
            rooms, halls, baths = match.groups()
            return f"{rooms}房{halls}厅｜{baths}卫"
    return raw.replace("|", "｜")


def display_property_type(value: object) -> str:
    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if not raw:
        return ""
    identity = raw.lower()
    if any(token in identity for token in ("独栋", "detached villa")):
        return "独栋别墅"
    if any(token in identity for token in ("双拼", "twin villa", "semi-detached")):
        return "双拼别墅"
    if any(token in identity for token in ("联排", "townhouse", "town house", "row house", "link villa")):
        return "联排别墅"
    if any(token in identity for token in ("villa", "别墅")):
        return "别墅"
    if any(token in identity for token in ("公寓", "apartment", "condo", "condominium")):
        return "公寓"
    if any(token in identity for token in ("写字楼", "office")):
        return "写字楼"
    if any(token in identity for token in ("商铺", "shop", "storefront")):
        return "商铺"
    if any(token in identity for token in ("仓库", "warehouse")):
        return "仓库"
    if any(token in identity for token in ("厂房", "factory")):
        return "厂房"
    return raw.replace("|", "｜")


def display_floor(floor: object, property_type: object = "") -> str:
    raw = str(floor or "").strip()
    if not raw or raw in {"未知", "待确认", "暂无", "[暂无]", "-", "--"}:
        return ""
    compact = re.sub(r"\s+", "", raw)
    kind = display_property_type(property_type)
    is_house = any(token in kind for token in ("别墅", "排屋"))

    numeric = re.fullmatch(r"(\d+)(?:楼|层|层楼)?", compact)
    if numeric:
        value = int(numeric.group(1))
        return f"{value}层楼" if is_house else f"{value}楼"
    return raw.replace("|", "｜")


# Compatibility aliases while extracted channel_post is being rewired.
_display_layout = display_layout
_display_floor = display_floor


__all__ = [
    "display_layout",
    "display_property_type",
    "display_floor",
    "_display_layout",
    "_display_floor",
]
