"""Evidence-driven copy engine for ``💬 侨联说``.

侨联说 is a compact renter-facing judgement, not a second facts list. It may
combine several verified facts into one useful observation, but it must never
invent market comparisons or turn a single generic field into boilerplate.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

PHRASES: dict[str, tuple[str, ...]] = {
    "pest_control": (
        "灭虫这块已经安排了，不用自己另外找人。",
        "怕虫的可以留意下，这边有灭虫安排。",
        "灭虫这种细节有人管，不用自己操心。",
    ),
    "cleaning_1x": (
        "每周有人来打扫一次，日常维护够用。",
        "保洁一周上门一次，自己少操心一点。",
        "每周固定有一次保洁。",
    ),
    "cleaning_2x": (
        "保洁一周两次，算比较勤快。",
        "一周来两次打扫，日常维护够用。",
        "每周固定两次保洁上门。",
    ),
    "cleaning_3x": (
        "一周三次保洁，频率确实挺高。",
        "保洁一周三次，挺省心。",
        "每周三次上门，日常基本不用太操心。",
    ),
    "cleaning_included": (
        "这套包含保洁，日常维护会省事一些。",
        "保洁已经包在里面，自己少打扫一点。",
        "有保洁服务，入住会轻松一些。",
    ),
    "linen_weekly": (
        "床品每周会处理一次，日常少一件事。",
        "每周有人处理床品，比较省心。",
        "床品服务有安排，入住少操心。",
    ),
    "management_included": (
        "物业费已经含在租金里了，不用另外算。",
        "这边物业费是包含的。",
        "物业这块不用再单独付。",
    ),
    "wifi_included": (
        "网费已经含在租金里了，不用另外算。",
        "这边网费不用再单独付。",
        "网络费用已经包进租金了。",
    ),
    "wifi_ready": (
        "宽带已经装好，搬进去少折腾一步。",
        "网络已经接通，不用自己再装。",
        "宽带提前弄好了，直接能用。",
    ),
    "management_wifi": (
        "物业和网费都包在租金里了，每月少两项固定支出。",
        "这边物业费、网费都不用另外算。",
        "物业和网络费用这两块都包了。",
    ),
    "management_wifi_ready": (
        "物业费含着，宽带也装好了。",
        "物业不用另外算，网络也可以直接用。",
        "这边物业费包着，宽带也提前接通了。",
    ),
    "owner_direct": (
        "这套是房东自己放出来的，沟通会更直接。",
        "这边是房东直租，条件好商量一些。",
        "房东自己在放，对接会比较干脆。",
    ),
    "never_lived": (
        "这套还没人住过，喜欢新房的可以优先看。",
        "全新未入住，比较在意房况新旧可以留意。",
        "第一次放租，还没住过人。",
    ),
    "new_condition": (
        "整体看着偏新，喜欢新一点的可以留意。",
        "这套房况比较新。",
        "家具和房况都偏新，喜欢新房的可以看。",
    ),
    "furnished": (
        "家具已经配齐，入住少添不少东西。",
        "基本家具都配好了。",
        "家具比较齐全，拎包入住压力小一点。",
    ),
    "balcony": (
        "带阳台，晾晒透气都方便。",
        "有个阳台，平时能出去透透气。",
        "这套带阳台，实用性会好一点。",
    ),
    "city_view": (
        "窗外是城市景观，看房时可以重点看视野。",
        "这边视野朝市区方向。",
        "这套有市景，现场看看采光和窗面。",
    ),
    "river_view": (
        "这套能看到河景，现场重点确认视野。",
        "窗外朝河那一面，看房时值得看一眼。",
        "有河景信息，实际观感建议现场确认。",
    ),
    "pool": (
        "小区自带泳池，日常能用。",
        "楼下有泳池，属于这类公寓常见配置。",
        "这边配了泳池。",
    ),
    "gym": (
        "小区自带健身房，日常够用。",
        "楼下有健身房。",
        "这边配了健身房。",
    ),
    "pool_gym": (
        "泳池和健身房都有，楼下就能用。",
        "楼下泳池健身房都配了，运动比较方便。",
        "小区自带泳池和健身房。",
    ),
    "pickleball": (
        "楼下有匹克球场，这类配套不算多。",
        "小区配了匹克球，算个小亮点。",
        "这边居然有匹克球场，喜欢打球可以留意。",
    ),
    "tennis": (
        "小区自带网球场，平时打球方便。",
        "楼下有网球场。",
        "这边配了网球场。",
    ),
    "table_tennis": (
        "楼下有乒乓球台。",
        "小区自带乒乓球设施。",
        "这边配了乒乓球台。",
    ),
    "billiards": (
        "楼下有台球桌。",
        "小区里配了台球设施。",
        "这边有台球桌。",
    ),
    "kids_area": (
        "有儿童区，带小孩的可以留意。",
        "楼下有儿童游乐区，带娃会方便一点。",
        "小区自带儿童活动区。",
    ),
    "sauna": (
        "楼下有桑拿房。",
        "小区自带桑拿设施。",
        "这边配了桑拿房。",
    ),
    "jacuzzi": (
        "这边配了按摩池。",
        "小区带按摩浴池。",
        "楼下有按摩浴缸。",
    ),
    "rooftop": (
        "楼上有公共天台，偶尔上去透气方便。",
        "这边带屋顶公共空间。",
        "楼顶有平台可以上去。",
    ),
    "coworking": (
        "楼下有共享办公区，在家办公会方便点。",
        "这边配了共享办公空间。",
        "楼里有 coworking，经常远程办公可以留意。",
    ),
    "concierge": (
        "楼下有前台管家，日常有事可以找。",
        "这边配了前台接待。",
        "小区有前台服务。",
    ),
    "parking": (
        "这边有停车条件，开车的比较实用。",
        "小区自带停车场。",
        "楼下可以停车。",
    ),
    "private_pool": (
        "这套自带私人泳池，是比较明显的一项配置。",
        "泳池是这套自己独用的，不跟其他住户共用。",
        "带私人泳池，喜欢私密空间的可以优先看。",
    ),
    "large_layout": (
        "这套空间偏大，对居住空间有要求可以留意。",
        "户型面积比较宽敞。",
        "空间偏大，喜欢宽敞的可以看。",
    ),
    "high_floor": (
        "这套楼层偏高，如果在意楼层可以放进优先看房范围。",
        "楼层比较高，实际视野和采光建议现场确认。",
        "偏高楼层，比较在意楼层的可以留意。",
    ),
    "low_floor": (
        "这套楼层偏低，出入会方便一些。",
        "偏低楼层，不喜欢爬太高的可以看。",
        "楼层比较低，上下更省事。",
    ),
    "pet_allowed": (
        "这套允许带宠物，有宠物可以优先看。",
        "可以养宠物，带毛孩的可以留意。",
        "宠物政策比较友好，有宠物的值得看一眼。",
    ),
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
    """Only combine verified listing-specific signals; never invent layout boilerplate."""
    result: list[str] = []
    tagset = set(tags)
    layout = str(facts.get("layout") or "").strip()
    floor = _floor_number(facts.get("floor"))
    view = "河景" if "river_view" in tagset else "市景" if "city_view" in tagset else ""

    # Require both a verified view signal and another concrete fact for this unit.
    if view and floor:
        result.append(f"{floor} 楼这套又有{view}信息，看房时重点看客厅视野、窗面和采光。")
    elif view and layout and ("private_pool" in tagset or "never_lived" in tagset or "pet_allowed" in tagset):
        result.append(f"{layout} 又带{view}信息，这套更值得现场看的是客厅视野、采光和空间怎么分配。")

    return result


def _decision_insights(facts: dict[str, Any], *, seed: str, max_points: int) -> list[str]:
    """Build 1–2 short decision points from verified listing-specific facts only.

    Layout alone is not enough. Do not invent location selling points from
    ``public_location_display``. Do not emit customer-facing incompleteness copy.
    """
    insights: list[str] = []

    # Floor: only when explicitly verified high_floor — never invent quietness/view/sun.
    if facts.get("high_floor") is True or "high_floor" in _explicit_signals(facts):
        insights.append(
            "楼层偏高，如果在意楼层可以放进优先看房范围，实际视野和采光现场确认更稳。"
        )

    if _kitchen_is_independent(facts):
        insights.append("有独立厨房，平时自己做饭会更实用。")

    # Fill remaining slots with the evidence-driven phrase engine (no marketing fluff).
    remaining = max(0, max_points - len(insights))
    if remaining:
        for line in generate_adviser_lines(facts, seed=seed, max_points=remaining, allow_fallback=False):
            if line and line not in insights:
                # Reject leftover layout-only boilerplate if any generator still emits it.
                if any(phrase in line for phrase in ("更灵活", "资料不完整")):
                    continue
                insights.append(line)
            if len(insights) >= max_points:
                break

    return insights[:max_points]


def build_adviser_copy(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 2,
) -> str:
    """Publisher-side adviser_copy generator.

    Rules:
    - Only use verified/frozen facts specific to this listing.
    - Do not invent advantages from layout alone.
    - Do not show incompleteness disclaimers to customers.
    - Prefer 1–2 decision-relevant points.
    - Stay empty when nothing decision-relevant is verified (caller hides the block).
    - User Bot must only read the frozen adviser_copy (no header here;
      the detail view adds ``💬 侨联说``).
    """
    try:
        limit = max(0, min(int(max_points), 2))
    except (TypeError, ValueError):
        limit = 2
    if limit == 0:
        return ""

    clean = verified_canonical_adviser_facts(dict(facts or {}))
    selected = _decision_insights(clean, seed=seed, max_points=limit)
    if not selected:
        return ""

    body = "\n".join(selected)
    # Prefer dropping the second insight over exceeding the hard length gate.
    while len(body) > _ADVISER_COPY_MAX_LEN and "\n" in body:
        body = "\n".join(body.splitlines()[:-1]).strip()
    if len(body) > _ADVISER_COPY_MAX_LEN:
        body = body[: _ADVISER_COPY_MAX_LEN - 1].rstrip() + "…"
    try:
        validate_adviser_copy(body)
    except ValueError:
        return ""
    return body


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


FORBIDDEN_ADVISER_PHRASES = frozenset(
    {
        "性价比极高",
        "不容错过",
        "绝佳选择",
        "顶级配套",
        "高端生活",
        "稀缺房源",
        "房东好说话",
        "价格还能谈",
        "视野无遮挡",
        "采光非常好",
        "非常安静",
        "空间更灵活",
        "会更灵活",
        "资料不完整",
    }
)

_ADVISER_COPY_MAX_LEN = 220

_STUDIO_LAYOUTS = frozenset({"单间", "开间", "studio", "Studio", "STUDIO"})
_ONE_BED_LAYOUTS = frozenset({"一房", "1房", "1BR", "1br", "一居", "1居"})
_TWO_BED_LAYOUTS = frozenset({"两房", "2房", "2BR", "2br", "两居", "2居"})


def _is_unknown(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"", "unknown", "none", "null", "未知", "待确认", "不详", "暂无"}


def _layout_bucket(layout: str) -> str:
    text = re.sub(r"\s+", "", str(layout or "").strip())
    if not text:
        return ""
    if text in _STUDIO_LAYOUTS or re.fullmatch(r"studio|开间|单间", text, re.I):
        return "studio"
    if text in _ONE_BED_LAYOUTS or re.match(r"^1\s*房", text) or re.match(r"^一房", text):
        return "one"
    if text in _TWO_BED_LAYOUTS or re.match(r"^2\s*房", text) or re.match(r"^两房", text):
        return "two"
    # Compact forms like 1房1厅 / 2房1厅
    if re.match(r"^1房", text) or re.match(r"^一房", text):
        return "one"
    if re.match(r"^2房", text) or re.match(r"^两房", text):
        return "two"
    return ""


def _kitchen_is_independent(facts: dict[str, Any]) -> bool:
    kitchen = str(facts.get("kitchen") or "").strip().lower()
    if kitchen in {"independent", "独立厨房", "独立厨"} or ("独立" in kitchen and "厨" in kitchen):
        return True
    house = facts.get("house") if isinstance(facts.get("house"), dict) else {}
    features = _strings(house.get("features"))
    return any(token in features for token in {"独立厨房", "独立厨"})


def _incomplete_reminder_labels(facts: dict[str, Any], tags: list[str] | None = None) -> list[str]:
    """Remind only when a decision-relevant fee/policy is truly unknown."""
    included = _strings(facts.get("included"))
    amenities = _strings(facts.get("amenities"))
    house = facts.get("house") if isinstance(facts.get("house"), dict) else {}
    services = facts.get("services") if isinstance(facts.get("services"), dict) else {}
    tagset = set(tags if tags is not None else adviser_tags_from_facts(facts))
    reminders: list[str] = []

    management_known = (
        not _is_unknown(facts.get("management_fee"))
        or any(value in included for value in {"物业费", "物业", "management", "management fee"})
        or services.get("management_included") is True
    )
    if not management_known:
        reminders.append("管理费")

    parking_known = (
        not _is_unknown(facts.get("parking"))
        or not _is_unknown(facts.get("parking_fee"))
        or any(value in amenities for value in {"停车位", "停车场", "parking"})
        or "parking" in tagset
    )
    if not parking_known:
        reminders.append("停车")

    pets = str(house.get("pets") or facts.get("pet_policy") or "").strip()
    if _is_unknown(pets) and "pet_allowed" not in tagset:
        reminders.append("宠物政策")

    wifi_known = (
        not _is_unknown(facts.get("wifi"))
        or not _is_unknown(facts.get("internet_fee"))
        or any(value in included for value in {"wi-fi", "wifi", "网费", "网络费", "internet"})
        or bool(tagset & {"wifi_included", "wifi_ready", "management_wifi", "management_wifi_ready"})
    )
    if not wifi_known:
        reminders.append("网络")

    return reminders


def validate_adviser_copy(text: str) -> None:
    """Hard gate against marketing fluff and oversized copy."""
    value = str(text or "")
    if not value.strip():
        return
    for phrase in FORBIDDEN_ADVISER_PHRASES:
        if phrase in value:
            raise ValueError(f"adviser_copy contains unsupported phrase: {phrase}")
    if len(value) > _ADVISER_COPY_MAX_LEN:
        raise ValueError("adviser_copy is too long")


__all__ = [
    "PHRASES",
    "TAG_CATEGORY",
    "VALID_TAGS",
    "COMMON_AMENITY_TAGS",
    "FORBIDDEN_ADVISER_PHRASES",
    "verified_canonical_adviser_facts",
    "adviser_tags_from_facts",
    "generate_adviser_lines",
    "generate_adviser_text",
    "build_adviser_copy",
    "validate_adviser_copy",
]
