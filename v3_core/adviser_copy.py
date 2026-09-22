"""Evidence-driven copy engine for ``💬 侨联判断``.

侨联判断 is a compact renter-facing judgement, not a second facts list. It may
combine several verified facts into one useful observation, but it must never
invent market comparisons or turn a single generic field into boilerplate.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

PHRASES: dict[str, tuple[str, ...]] = {
    "pest_control": ("这边有灭虫服务安排，比较在意这一点可以记一下。",),
    "cleaning_1x": ("每周有一次保洁，日常维护会省事一些。",),
    "cleaning_2x": ("每周有两次保洁，日常维护会轻松不少。",),
    "cleaning_3x": ("每周有三次保洁，保洁频率比较高。",),
    "cleaning_included": ("这套包含保洁服务，日常维护会省事一些。",),
    "linen_weekly": ("床品服务也有安排，日常维护会省事一些。",),
    "management_included": ("物业费已经包含，不需要另外单独计算。",),
    "wifi_included": ("网费已经包含，每月不用另外支付网络费用。",),
    "wifi_ready": ("宽带已经装好，入住后不用再另外安排安装。",),
    "management_wifi": ("物业费和网费都已经包含，每月少两项固定支出。",),
    "management_wifi_ready": ("物业费已经包含，宽带也已经装好了。",),
    "owner_direct": ("这套是房东直接放租，租赁条件沟通会更直接。",),
    "never_lived": ("这套目前全新未入住，比较在意房况新旧可以优先看。",),
    "new_condition": ("这套整体房况偏新，比较在意房况可以优先看。",),
    "furnished": ("家具配置比较完整，入住前要添置的东西会少一些。",),
    "balcony": ("这套带阳台，实际使用感受主要看大小和朝向。",),
    "city_view": ("这套有市景信息。",),
    "river_view": ("这套有河景信息。",),
    "pool": ("小区配有泳池。",),
    "gym": ("小区配有健身房。",),
    "pool_gym": ("泳池和健身房都有，平时运动会比较方便。",),
    "pickleball": ("小区配有匹克球场，这项配套比较少见。",),
    "tennis": ("小区配有网球场，平时会打球可以留意。",),
    "table_tennis": ("小区配有乒乓球设施。",),
    "billiards": ("小区配有台球设施。",),
    "kids_area": ("小区有儿童活动区，带小孩入住可以留意。",),
    "sauna": ("小区配有桑拿设施。",),
    "jacuzzi": ("小区配有按摩池。",),
    "rooftop": ("楼内有公共天台。",),
    "coworking": ("楼内有共享办公空间，经常远程办公会比较方便。",),
    "concierge": ("楼内有前台管家服务，日常需要协助时有人可以找。",),
    "parking": ("这边有停车条件，有车的话这点比较实用。",),
    "private_pool": ("这套自带私人泳池，是比较明显的一项配置。",),
    "large_layout": ("这套空间偏大，对居住空间有要求可以留意。",),
    "high_floor": ("这套属于较高楼层。",),
    "low_floor": ("这套属于较低楼层。",),
    "pet_allowed": ("这套允许带宠物入住，有宠物可以优先看。",),
}

_PRIORITY = (
    "pet_allowed", "never_lived", "private_pool", "river_view", "large_layout",
    "owner_direct", "new_condition", "furnished", "balcony", "city_view",
    "high_floor", "low_floor", "cleaning_3x", "cleaning_2x", "cleaning_1x",
    "cleaning_included", "linen_weekly", "pest_control", "management_wifi",
    "management_wifi_ready", "management_included", "wifi_included", "wifi_ready",
    "coworking", "kids_area", "parking", "pickleball", "tennis", "concierge",
    "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards",
    "sauna", "jacuzzi",
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

VALID_TAGS = frozenset(PHRASES)
COMMON_AMENITY_TAGS = frozenset({"pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi"})
_GENERIC_STANDALONE_TAGS = frozenset({
    "river_view", "city_view", "high_floor", "low_floor", "furnished", "balcony",
    "management_included", "wifi_included", "wifi_ready", "management_wifi",
    "management_wifi_ready", "pool_gym", "pool", "gym", "table_tennis",
    "billiards", "sauna", "jacuzzi", "rooftop",
})


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

    services = facts.get("services") if isinstance(facts.get("services"), dict) else {}
    cleaning = str(services.get("cleaning") or "").strip()
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


def _phrase(tag: str, seed: str) -> str:
    choices = PHRASES[tag]
    digest = hashlib.sha256(f"{seed}|{tag}".encode("utf-8")).hexdigest()
    return choices[int(digest[:12], 16) % len(choices)]


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


def _contextual_lines(facts: dict[str, Any], tags: list[str]) -> list[str]:
    result: list[str] = []
    tagset = set(tags)
    layout = str(facts.get("layout") or "").strip()
    plus_layout = _plus_layout(layout)
    floor = _floor_number(facts.get("floor"))
    view = "河景" if "river_view" in tagset else "市景" if "city_view" in tagset else ""

    if plus_layout:
        result.append(f"{plus_layout} 的布局多一个可用空间，做书房、储物或临时房会更灵活。")

    if view and floor:
        result.append(f"{floor} 楼这套又有{view}信息，看房时重点看客厅视野、窗面和采光。")
    elif view and layout:
        result.append(f"{layout} 又带{view}信息，这套更值得现场看的是客厅视野、采光和空间怎么分配。")

    return result


def _select_tags(tags: list[str], limit: int, *, suppress_generic: bool = False) -> list[str]:
    if limit <= 0 or not tags:
        return []
    selected: list[str] = []
    used: set[str] = set()
    for tag in tags:
        if suppress_generic and tag in _GENERIC_STANDALONE_TAGS:
            continue
        category = TAG_CATEGORY.get(tag, tag)
        if category in used:
            continue
        selected.append(tag)
        used.add(category)
        if len(selected) >= limit:
            break
    return selected


def generate_adviser_lines(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 2,
    allow_fallback: bool = False,
) -> list[str]:
    del allow_fallback
    try:
        limit = max(0, min(int(max_points), 2))
    except (TypeError, ValueError):
        limit = 2
    if limit == 0:
        return []

    clean_facts = dict(facts or {})
    publisher_mode = str(clean_facts.get("_publisher_adviser_mode") or "") == "composite"
    tags = adviser_tags_from_facts(clean_facts)
    lines = _contextual_lines(clean_facts, tags)[:limit] if publisher_mode else []
    if len(lines) >= limit:
        return lines

    for tag in _select_tags(tags, limit, suppress_generic=publisher_mode):
        line = _phrase(tag, seed)
        if line not in lines:
            lines.append(line)
        if len(lines) >= limit:
            break
    return lines


def generate_adviser_text(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 2,
    allow_fallback: bool = False,
) -> str:
    return "\n".join(generate_adviser_lines(facts, seed=seed, max_points=max_points, allow_fallback=allow_fallback))


__all__ = [
    "PHRASES", "TAG_CATEGORY", "VALID_TAGS", "COMMON_AMENITY_TAGS",
    "verified_canonical_adviser_facts", "adviser_tags_from_facts",
    "generate_adviser_lines", "generate_adviser_text",
]
