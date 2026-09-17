"""Evidence-driven V2 copy engine for the public adviser section.

The public copy is either empty or one judgement with one practical viewing
focus. It never exists merely to restate ordinary listing parameters.
"""
from __future__ import annotations

import re
from typing import Any


_SUPPORTED_TAGS = (
    "pest_control", "cleaning_1x", "cleaning_2x", "cleaning_3x", "cleaning_included",
    "linen_weekly", "management_included", "wifi_included", "wifi_ready",
    "management_wifi", "management_wifi_ready", "owner_direct", "never_lived",
    "new_condition", "furnished", "balcony", "city_view", "river_view", "pool",
    "gym", "pool_gym", "pickleball", "tennis", "table_tennis", "billiards",
    "kids_area", "sauna", "jacuzzi", "rooftop", "coworking", "concierge",
    "parking", "private_pool", "large_layout", "high_floor", "low_floor",
    "pet_allowed",
)
PHRASES: dict[str, tuple[str, ...]] = {tag: () for tag in _SUPPORTED_TAGS}
VALID_TAGS = frozenset(_SUPPORTED_TAGS)
COMMON_AMENITY_TAGS = frozenset({"pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi"})

_PRIORITY = (
    "pet_allowed", "never_lived", "new_condition", "private_pool", "river_view",
    "city_view", "owner_direct", "cleaning_3x", "cleaning_2x", "cleaning_1x",
    "cleaning_included", "management_wifi", "management_wifi_ready",
    "management_included", "wifi_included", "wifi_ready", "coworking",
    "kids_area", "pickleball", "tennis", "concierge", "parking", "balcony",
    "furnished", "high_floor", "low_floor", "pool_gym", "pool", "gym",
    "table_tennis", "billiards", "sauna", "jacuzzi", "rooftop",
    "linen_weekly", "pest_control", "large_layout",
)

TAG_CATEGORY = {
    "pet_allowed": "fit",
    "never_lived": "condition", "new_condition": "condition", "furnished": "condition",
    "large_layout": "layout", "balcony": "layout",
    "river_view": "view", "city_view": "view",
    "high_floor": "floor", "low_floor": "floor",
    "owner_direct": "source",
    "cleaning_1x": "cleaning", "cleaning_2x": "cleaning",
    "cleaning_3x": "cleaning", "cleaning_included": "cleaning",
    "linen_weekly": "service", "pest_control": "service",
    "management_included": "cost", "wifi_included": "network_cost",
    "wifi_ready": "network_ready", "management_wifi": "cost",
    "management_wifi_ready": "cost_network", "private_pool": "special",
    **{tag: "amenity" for tag in (
        "coworking", "kids_area", "parking", "pickleball", "tennis", "concierge",
        "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards",
        "sauna", "jacuzzi",
    )},
}


def _strings(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    return set() if value in (None, "") else {str(value).strip().lower()}


def _explicit_signals(facts: dict[str, Any]) -> list[str]:
    raw = facts.get("adviser_signals") or []
    if not isinstance(raw, (list, tuple, set)):
        return []
    return [str(tag).strip() for tag in raw if str(tag).strip() in VALID_TAGS]


def verified_canonical_adviser_facts(facts: dict[str, Any]) -> dict[str, Any]:
    result = dict(facts or {})
    if result.get("adviser_signals_version") != "explicit-v1":
        result["adviser_signals"] = [
            tag for tag in _explicit_signals(result) if tag not in {"high_floor", "low_floor"}
        ]
    return result


def adviser_tags_from_facts(facts: dict[str, Any] | None) -> list[str]:
    facts = dict(facts or {})
    tags = _explicit_signals(facts)

    direct_true = {
        "pet_allowed": "pet_allowed", "never_lived": "never_lived",
        "balcony": "balcony", "river_view": "river_view", "city_view": "city_view",
        "furnished": "furnished", "private_pool": "private_pool",
        "owner_direct": "owner_direct", "management_included": "management_included",
        "wifi_included": "wifi_included", "wifi_ready": "wifi_ready",
    }
    for field, tag in direct_true.items():
        if facts.get(field) is True:
            tags.append(tag)

    condition = str(facts.get("condition") or "").strip().lower()
    if any(token in condition for token in ("全新未入住", "从未入住", "never lived")):
        tags.append("never_lived")
    elif any(token in condition for token in ("全新", "较新", "新装修", "new")):
        tags.append("new_condition")

    services = facts.get("services") if isinstance(facts.get("services"), dict) else {}
    cleaning_value = services.get("cleaning", facts.get("cleaning"))
    cleaning = str(cleaning_value or "").strip()
    frequencies = [int(value) for value in re.findall(r"(?:每周|一周)?\s*([123])\s*次", cleaning)]
    if frequencies:
        tags.append(f"cleaning_{max(frequencies)}x")
    elif cleaning.lower() in {"包含", "有", "提供", "yes", "included"}:
        tags.append("cleaning_included")

    for field, tag in (("pest_control", "pest_control"), ("linen_change", "linen_weekly"), ("concierge", "concierge")):
        if services.get(field) is True:
            tags.append(tag)

    included = _strings(facts.get("included"))
    management_included = any(value in included for value in {"物业费", "物业", "management", "management fee"})
    wifi_included = any(value in included for value in {"wi-fi", "wifi", "网费", "网络费", "internet"})
    if management_included and wifi_included:
        tags.append("management_wifi")
    elif management_included:
        tags.append("management_included")
    elif wifi_included:
        tags.append("wifi_included")

    amenities = _strings(facts.get("amenities"))
    amenity_map = {
        "游泳池": "pool", "泳池": "pool", "私人泳池": "private_pool",
        "健身房": "gym", "匹克球": "pickleball", "网球": "tennis", "网球场": "tennis",
        "乒乓球": "table_tennis", "台球": "billiards", "儿童游乐区": "kids_area",
        "儿童活动区": "kids_area", "桑拿": "sauna", "按摩池": "jacuzzi",
        "按摩浴缸": "jacuzzi", "共享办公": "coworking", "停车位": "parking", "停车场": "parking",
    }
    tags.extend(tag for source, tag in amenity_map.items() if source.lower() in amenities)

    house = facts.get("house") if isinstance(facts.get("house"), dict) else {}
    features = _strings(house.get("features"))
    feature_map = {
        "全新未入住": "never_lived", "房况较新": "new_condition", "阳台": "balcony",
        "河景": "river_view", "市景": "city_view", "大户型": "large_layout",
    }
    tags.extend(tag for source, tag in feature_map.items() if source.lower() in features)
    if house.get("furnished") is True:
        tags.append("furnished")

    source_type = str(house.get("source_type") or "").strip()
    if "房东直租" in source_type or "业主直租" in source_type:
        tags.append("owner_direct")

    pets = str(house.get("pets") or "").strip().lower()
    negative_pet = bool(re.search(r"不允许|不可以|不可|禁止|不接受|不能|not\s+allowed|no\b", pets))
    if not negative_pet and (pets in {"允许", "可以", "yes", "allowed"} or "允许" in pets or "可养" in pets):
        tags.append("pet_allowed")

    tagset = {tag for tag in tags if tag in VALID_TAGS}
    if negative_pet:
        tagset.discard("pet_allowed")
    if "never_lived" in tagset:
        tagset.discard("new_condition")
    if "private_pool" in tagset:
        tagset.difference_update({"pool", "pool_gym"})
    if "pool" in tagset and "gym" in tagset:
        tagset.difference_update({"pool", "gym"})
        tagset.add("pool_gym")
    if "river_view" in tagset:
        tagset.discard("city_view")

    cleaning_tags = [tag for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included") if tag in tagset]
    if cleaning_tags:
        tagset.difference_update({"cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included"})
        tagset.add(cleaning_tags[0])
    if "management_wifi" in tagset:
        tagset.difference_update({"management_included", "wifi_included"})
    if "management_wifi_ready" in tagset:
        tagset.difference_update({"management_included", "wifi_ready"})

    return [tag for tag in _PRIORITY if tag in tagset]


def _floor_number(value: Any) -> int | None:
    match = re.search(r"(?<!\d)(\d{1,3})(?!\d)", str(value or ""))
    if not match:
        return None
    number = int(match.group(1))
    return number if 0 < number < 200 else None


def _plus_layout(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or ""))
    match = re.search(r"(?<!\d)(\d{1,2})\+(\d{1,2})(?!\d)", text)
    return f"{match.group(1)}+{match.group(2)}" if match else ""


def _bedrooms(facts: dict[str, Any]) -> int | None:
    value = facts.get("bedrooms")
    if isinstance(value, (int, float)) and int(value) == value and 0 <= int(value) <= 20:
        return int(value)
    layout = str(facts.get("layout") or "").strip()
    if re.search(r"\bstudio\b|单间", layout, re.I):
        return 0
    match = re.search(r"(?<!\d)(\d{1,2})\s*房", layout)
    return int(match.group(1)) if match else None


def _room_word(value: int) -> str:
    words = {1: "一", 2: "两", 3: "三", 4: "四", 5: "五", 6: "六"}
    return words.get(value, str(value))


def _single_adviser_judgement(facts: dict[str, Any], tags: list[str]) -> str:
    tagset = set(tags)
    layout = str(facts.get("layout") or "").strip()
    bedrooms = _bedrooms(facts)
    floor = _floor_number(facts.get("floor"))
    view = "河景" if "river_view" in tagset else "市景" if "city_view" in tagset else ""

    if "pet_allowed" in tagset:
        return "如果带宠物入住，这套值得优先确认；看房时重点看宠物实际活动空间，并现场确认物业对宠物的具体要求。"
    if "never_lived" in tagset:
        return "全新未入住是已经确认的房况信息；看房时重点确认家具家电实际状态、使用痕迹和交付细节。"
    if "new_condition" in tagset:
        return "这套房况偏新的信息已经确认；看房时重点确认家具家电实际状态、使用痕迹和交付细节。"
    if view and floor:
        return f"楼层和{view}信息都比较明确；现场更值得确认客厅视野和窗面，重点看实际采光和遮挡情况。"
    if view:
        return f"{view}信息已经明确；现场更值得确认窗面和实际遮挡，重点看客厅视野与采光是否符合自己的使用习惯。"

    plus_layout = _plus_layout(layout)
    if plus_layout:
        return f"{plus_layout} 的额外空间需要现场确认实际尺度；看房时重点看它更适合做书房、收纳还是临时使用空间。"

    if bedrooms == 0:
        return "这套单间更值得现场确认睡眠区和主要活动区的实际尺度；看房时重点看收纳、采光和动线是否适合长期住。"
    if bedrooms == 1:
        return "这套一房更值得现场确认客厅和卧室的实际尺度；看房时重点看收纳、采光和动线是否适合长期住。"
    if bedrooms == 2:
        return "这套两房更值得现场确认实际空间分配；看房时重点看次卧大小、收纳和客厅采光是否符合自己的使用习惯。"
    if bedrooms is not None and bedrooms >= 3:
        room_word = _room_word(bedrooms)
        return f"这套{room_word}房更值得现场确认各房间的实际尺度；看房时重点看次卧大小、收纳和公共空间够不够用。"

    if "private_pool" in tagset:
        return "私人泳池是已经确认的独立配置；看房时重点确认实际尺寸、维护状态和使用管理要求。"
    if "owner_direct" in tagset:
        return "房东直租是已经确认的来源信息；看房时重点把交付清单、押付条件和后续维修责任确认清楚。"

    cleaning = next((tag for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included") if tag in tagset), "")
    if cleaning:
        return "保洁服务是已经确认的租住条件；看房时重点确认服务范围、执行方式以及交付后是否按同一条件持续提供。"

    if "management_wifi" in tagset:
        return "物业费和网费的包含关系已经明确；看房时重点确认这两项是否随租金持续包含，以及实际结算和开通方式。"
    if "management_wifi_ready" in tagset:
        return "物业费包含且宽带已就绪的信息已经明确；看房时重点确认网络实际可用状态和后续费用承担方式。"
    if "management_included" in tagset:
        return "物业费包含关系已经明确；看房时重点确认包含周期、物业服务范围和合同里的实际写法。"
    if "wifi_included" in tagset:
        return "网费包含关系已经明确；看房时重点确认网络套餐、实际可用状态和合同里的费用写法。"
    if "wifi_ready" in tagset:
        return "宽带已就绪的信息已经明确；看房时重点确认现场网络实际可用状态、套餐和后续费用。"

    special = {
        "coworking": "共享办公空间",
        "kids_area": "儿童活动区",
        "pickleball": "匹克球设施",
        "tennis": "网球设施",
        "concierge": "前台管家服务",
    }
    for tag, label in special.items():
        if tag in tagset:
            return f"{label}是已经确认的特殊配套；看房时重点确认实际开放状态、使用规则和与房屋之间的动线。"

    if floor:
        return "楼层信息已经明确；看房时更值得确认窗外遮挡和电梯实际使用感受，重点看采光与上下楼等待是否能接受。"

    return ""


def generate_adviser_lines(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 1,
    allow_fallback: bool = False,
) -> list[str]:
    del seed, allow_fallback
    try:
        limit = max(0, min(int(max_points), 1))
    except (TypeError, ValueError):
        limit = 1
    if limit == 0:
        return []

    clean_facts = dict(facts or {})
    tags = adviser_tags_from_facts(clean_facts)
    line = re.sub(r"\s+", " ", _single_adviser_judgement(clean_facts, tags)).strip()
    return [line] if line else []


def generate_adviser_text(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 1,
    allow_fallback: bool = False,
) -> str:
    lines = generate_adviser_lines(
        facts, seed=seed, max_points=max_points, allow_fallback=allow_fallback
    )
    return lines[0] if lines else ""


__all__ = [
    "PHRASES", "TAG_CATEGORY", "VALID_TAGS", "COMMON_AMENITY_TAGS",
    "verified_canonical_adviser_facts", "adviser_tags_from_facts",
    "generate_adviser_lines", "generate_adviser_text",
]
