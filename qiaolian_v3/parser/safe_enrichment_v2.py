"""侨联 Parser V2 SAFE enrichment layer.

V1.1 is authoritative. This module never reparses or rewrites V1.1 facts.
It accepts an already-parsed V1.1 dict, deep-copies it, then:
- fills only empty fields explicitly covered by safe rules;
- appends additive enrichment fields (services / included / amenities / features / review);
- never parses or guesses rent/sale amounts;
- sends uncertain candidates to review.

This module is intentionally standalone and must not become a replacement
parser entry point for V1.1.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any


_ADDITIVE_LIST_PATHS = {
    ("included",),
    ("amenities",),
    ("house", "features"),
    ("review", "possible_projects"),
    ("review", "possible_locations"),
    ("review", "possible_roads"),
    ("review", "possible_amenities"),
    ("review", "ambiguous_terms"),
    ("review", "unrecognized_terms"),
}


def append_unique(arr: list[Any], value: Any) -> None:
    if value not in (None, "") and value not in arr:
        arr.append(value)


def _ensure_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    current = data.get(key)
    if current in (None, ""):
        current = {}
        data[key] = current
    if not isinstance(current, dict):
        raise TypeError(f"v2_safe_expected_dict:{key}")
    return current


def _ensure_list(data: dict[str, Any], key: str) -> list[Any]:
    current = data.get(key)
    if current in (None, ""):
        current = []
        data[key] = current
    if not isinstance(current, list):
        raise TypeError(f"v2_safe_expected_list:{key}")
    return current


def _fill_empty(mapping: dict[str, Any], key: str, value: Any) -> None:
    """Fill only a missing/empty scalar. Existing V1.1 values are immutable."""
    if value in (None, ""):
        return
    if key not in mapping or mapping.get(key) in (None, ""):
        mapping[key] = value


def ensure_v2_fields(data: dict[str, Any]) -> dict[str, Any]:
    _ensure_dict(data, "services")
    _ensure_list(data, "included")
    _ensure_list(data, "amenities")

    rental = _ensure_dict(data, "rental")
    for key in ("deposit", "payment", "lease", "available_date"):
        rental.setdefault(key, None)

    house = _ensure_dict(data, "house")
    _ensure_list(house, "features")
    for key in ("furniture", "appliances", "decoration", "source_type", "pets"):
        house.setdefault(key, None)

    review = _ensure_dict(data, "review")
    for key in (
        "possible_projects",
        "possible_locations",
        "possible_roads",
        "possible_amenities",
        "ambiguous_terms",
        "unrecognized_terms",
    ):
        _ensure_list(review, key)

    return data


def enrich_services(text: str, parsed: dict[str, Any]) -> None:
    services = parsed["services"]

    cleaning_frequency = re.search(
        r"(?:保洁|房间保洁).{0,6}每周\s*(\d+)\s*次",
        text,
    )
    if cleaning_frequency:
        _fill_empty(services, "cleaning", f"每周{cleaning_frequency.group(1)}次")
    elif "房间保洁" in text or "保洁服务" in text:
        _fill_empty(services, "cleaning", "包含")

    if "管家服务" in text:
        _fill_empty(services, "concierge", "包含")

    if "灭虫" in text:
        _fill_empty(services, "pest_control", "包含")

    linen_frequency = re.search(
        r"(?:换床品|更换床品).{0,6}每周\s*(\d+)\s*次",
        text,
    )
    if linen_frequency:
        _fill_empty(services, "linen_change", f"每周{linen_frequency.group(1)}次")


def enrich_furniture(text: str, parsed: dict[str, Any]) -> None:
    house = parsed["house"]

    if "家具家电齐全" in text:
        _fill_empty(house, "furniture", "家具齐全")
        _fill_empty(house, "appliances", "家电齐全")
    elif "全套家具齐全" in text:
        _fill_empty(house, "furniture", "家具齐全")

    if "精装修" in text:
        _fill_empty(house, "decoration", "精装修")

    # 拎包入住不能推导成家具齐全。
    if "拎包入住" in text:
        append_unique(house["features"], "拎包入住")


def enrich_house_features(text: str, parsed: dict[str, Any]) -> None:
    features = parsed["house"]["features"]
    mapping = {
        "采光好": "采光好",
        "朝北": "朝北",
        "独立院子": "独立院子",
        "房屋钥匙已备": "钥匙已备",
        "随时可安排看房": "可预约看房",
    }
    for raw, normalized in mapping.items():
        if raw in text:
            append_unique(features, normalized)


def enrich_amenities(text: str, parsed: dict[str, Any]) -> None:
    amenities = parsed["amenities"]
    mapping = {
        "游泳池": ("游泳池", "泳池"),
        "健身房": ("健身房",),
        "匹克球": ("匹克球",),
        "乒乓球": ("乒乓球",),
        "网球": ("网球",),
        "篮球": ("篮球",),
        "羽毛球": ("羽毛球",),
        "儿童乐园": ("儿童乐园",),
        "桑拿": ("桑拿",),
        "超市": ("超市",),
    }
    for normalized, aliases in mapping.items():
        if any(alias in text for alias in aliases):
            append_unique(amenities, normalized)


def enrich_included(text: str, parsed: dict[str, Any]) -> None:
    included = parsed["included"]

    # 必须有明确“包/包含/免费”上下文。
    patterns = (
        (r"(?:包|包含).{0,4}物业", "物业费"),
        (r"(?:包|包含).{0,4}(?:网络|Wi-?Fi)", "Wi-Fi"),
        (r"免费.{0,3}(?:游泳池|泳池)", "游泳池免费"),
    )
    for pattern, value in patterns:
        if re.search(pattern, text, flags=re.I):
            append_unique(included, value)


def enrich_lease(text: str, parsed: dict[str, Any]) -> None:
    """Only fill an empty V1.1 lease field."""
    rental = parsed["rental"]
    if rental.get("lease") not in (None, ""):
        return

    if "半年或1年" in text:
        rental["lease"] = "半年或1年"
    elif "长租" in text:
        # 长租绝对不等于 1 年。
        rental["lease"] = "长租"


def enrich_dimensions(text: str, parsed: dict[str, Any]) -> None:
    """Unlabelled dimensions never become area; review only."""
    review = parsed["review"]
    pattern = r"\d+(?:\.\d+)?\s*米?\s*[×xX*]\s*\d+(?:\.\d+)?\s*米?"
    for value in re.findall(pattern, text):
        append_unique(review["unrecognized_terms"], value)


def enrich_pending_review(text: str, parsed: dict[str, Any]) -> None:
    review = parsed["review"]
    pending_projects = (
        "雅居乐",
        "Agile",
        "太子寰宇",
        "Picasso",
        "Picasso City Garden",
        "王子",
        "粤泰",
        "金边公馆",
        "时代广场",
        "吴哥城",
        "太子国际广场",
        "白金湾",
        "集茂城6A",
        "金边中央广场",
        "金边首都国金",
        "紫晶壹号",
        "钻石名邸",
        "富力B11",
    )
    lowered = text.casefold()
    for project in sorted(pending_projects, key=len, reverse=True):
        if project.casefold() in lowered:
            append_unique(review["possible_projects"], project)

    if "啊雷莎" in text:
        append_unique(review["possible_locations"], "啊雷莎")

    for road in ("50米路", "598路"):
        if road in text:
            append_unique(review["possible_roads"], road)


def enrich_ambiguous(text: str, parsed: dict[str, Any]) -> None:
    review = parsed["review"]
    phrases = (
        "半年或1年",
        "免费游泳池",
        "泳池健身房等都有",
        "物业费、游泳池、健身房",
        "网络、保洁、停车位",
        "钻石岛金街附近",
        "配套齐全",
        "体育馆等",
    )
    for phrase in phrases:
        if phrase in text:
            append_unique(review["ambiguous_terms"], phrase)

    # 单独“免费”不打 ambiguous，避免无意义噪音。


def _assert_preserved(before: Any, after: Any, path: tuple[str, ...] = ()) -> None:
    """Assert every pre-existing non-empty V1.1 value is still intact."""
    if isinstance(before, dict):
        if not isinstance(after, dict):
            raise AssertionError(f"v2_safe_overwrite:{'.'.join(path) or '<root>'}")
        for key, old_value in before.items():
            if key not in after:
                raise AssertionError(f"v2_safe_removed:{'.'.join(path + (str(key),))}")
            _assert_preserved(old_value, after[key], path + (str(key),))
        return

    if isinstance(before, list):
        if not isinstance(after, list):
            raise AssertionError(f"v2_safe_overwrite:{'.'.join(path)}")
        if not before:
            return
        if path in _ADDITIVE_LIST_PATHS:
            if after[: len(before)] != before:
                raise AssertionError(f"v2_safe_changed_list:{'.'.join(path)}")
        elif after != before:
            raise AssertionError(f"v2_safe_changed_list:{'.'.join(path)}")
        return

    # None / empty string are the only scalar states V2 may fill.
    if before in (None, ""):
        return
    if after != before:
        raise AssertionError(f"v2_safe_overwrite:{'.'.join(path)}")


def assert_v1_1_preserved(parsed_v1_1: dict[str, Any], enriched: dict[str, Any]) -> None:
    _assert_preserved(parsed_v1_1, enriched)


def enrich_v2(original_text: str, parsed_v1_1: dict[str, Any]) -> dict[str, Any]:
    """Return V1.1 + additive V2 SAFE enrichment without recalculating V1.1."""
    if not isinstance(parsed_v1_1, dict):
        raise TypeError("parsed_v1_1_must_be_dict")

    parsed = deepcopy(parsed_v1_1)
    ensure_v2_fields(parsed)

    enrich_services(original_text, parsed)
    enrich_furniture(original_text, parsed)
    enrich_house_features(original_text, parsed)
    enrich_amenities(original_text, parsed)
    enrich_included(original_text, parsed)
    enrich_lease(original_text, parsed)
    enrich_dimensions(original_text, parsed)
    enrich_pending_review(original_text, parsed)
    enrich_ambiguous(original_text, parsed)

    # Preserve V1.1 parser metadata. V2 identifies itself separately.
    parsed.setdefault("parser_version", "v1_1")
    parsed["enrichment_version"] = "v2_safe"
    parsed["parser_chain"] = "v1_1+v2_safe"

    assert_v1_1_preserved(parsed_v1_1, parsed)
    return parsed


__all__ = [
    "append_unique",
    "assert_v1_1_preserved",
    "ensure_v2_fields",
    "enrich_v2",
]
