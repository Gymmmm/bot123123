"""Rule-based rental-detail voice for the public User Bot.

The engine never invents facts: it only talks about fields already present on
the listing/canonical payload. Manual talk always wins. Automatic output uses
fixed copy plus factual fee/service values and may be empty when nothing useful
is supported.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


TALK_LIBRARY: dict[str, tuple[str, ...]] = {
    "pickleball": (
        "这边居然还有匹克球😂 没玩过的也可以下来试试。",
        "楼里有匹克球场，想玩的话下楼就行。",
    ),
    "pest_control": (
        "怕虫的可以看过来了😂 这边灭虫也有人处理。",
        "这边有安排灭虫服务，不用自己再单独找。",
    ),
    "cleaning_3x": (
        "一周有三次保洁，房间会有人定期来收拾。",
        "这边保洁是一周三次，日常不用全靠自己打扫。",
    ),
    "cleaning_2x": (
        "一周有两次保洁，房间会有人定期来收拾。",
        "这边保洁是一周两次，平时有人帮着打扫。",
    ),
    "cleaning_1x": (
        "每周有一次保洁，会有人定期来打扫。",
        "这边是一周一次保洁，基础卫生有人处理。",
    ),
    "linen_weekly": (
        "床品这边每周也会换一次。",
        "每周会更换一次床品，这项也包含在服务里。",
    ),
    "never_lived": (
        "这套目前是未入住状态，还没人住过。",
        "这套是全新未住，之前没有入住记录。",
    ),
    "pet_allowed": (
        "带宠物的可以看，这套允许宠物入住。",
        "这套可以带宠物住，有猫狗的话不用先排除。",
    ),
    "management_wifi": (
        "物业和网络都包着，这两项不用另外交。",
        "物业费、网络费都包含在里面了。",
    ),
    "river_view": (
        "这套能看到河，照片里可以直接看景观。",
        "窗外有河景，想看视野的话直接看实拍。",
    ),
    "coworking": (
        "楼里有共享办公区，不想在房间办公也可以下楼。",
        "这边有共享办公区，在楼里就能找地方办公。",
    ),
    "kids_area": (
        "楼里有儿童活动区，带孩子住可以直接用。",
        "这边有儿童活动区，小朋友有地方活动。",
    ),
    "balcony": (
        "这套带阳台，晾晒、通风都能用上。",
        "房子有阳台，日常晾东西会方便一点。",
    ),
    "large_layout": (
        "这套面积比较大，实际空间可以直接看完整实拍。",
        "面积在120㎡以上，属于大户型。",
    ),
    "owner_direct": (
        "这套是房东本人放租，房源信息直接跟业主确认。",
        "这是业主直租的房源，确认条件时直接对房东信息。",
    ),
    "new_condition": (
        "资料里写的是新装修，具体房况可以看实拍。",
        "这套标注了装修较新，现场状态以实拍和看房为准。",
    ),
}

PRIORITY = {
    "pickleball": 100,
    "never_lived": 96,
    "pest_control": 94,
    "cleaning_3x": 92,
    "cleaning_2x": 90,
    "linen_weekly": 88,
    "pet_allowed": 86,
    "river_view": 80,
    "coworking": 78,
    "kids_area": 76,
    "owner_direct": 72,
    "new_condition": 70,
    "management_wifi": 68,
    "large_layout": 64,
    "balcony": 58,
    "cleaning_1x": 55,
}

_EMPTY_FACTS = {"", "待确认", "暂无", "[暂无]", "未知", "--", "-", "null", "none"}
_INCLUDED_FACTS = {"包含", "已包含", "包", "included", "include", "免费", "免"}


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fact_mappings(listing: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    return (
        listing,
        _json_mapping(listing.get("canonical_facts")),
        _json_mapping(listing.get("normalized_data")),
        _json_mapping(listing.get("extracted_data")),
    )


def _fact_value(listing: dict[str, Any], key: str) -> str:
    for mapping in _fact_mappings(listing):
        raw = mapping.get(key)
        value = str(raw or "").strip()
        if value and value.lower() not in _EMPTY_FACTS:
            return value
    return ""


def _service_facts(listing: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for mapping in _fact_mappings(listing):
        services = mapping.get("services")
        if isinstance(services, dict):
            for key, value in services.items():
                if key not in merged or merged.get(key) in (None, ""):
                    merged[key] = value
    return merged


def _is_included(value: str) -> bool:
    normalized = str(value or "").strip().lower().replace(" ", "")
    if normalized in _INCLUDED_FACTS:
        return True
    return any(token in normalized for token in ("包含", "已包", "包物业", "包网络", "included"))


def _cost_talk_line(listing: dict[str, Any]) -> str:
    """Turn known recurring-cost fields into one plain-language factual line."""
    management = _fact_value(listing, "management_fee")
    internet = _fact_value(listing, "internet_fee")
    electric = _fact_value(listing, "electric_rate") or _fact_value(listing, "electricity_rate")
    water = _fact_value(listing, "water_rate")
    parking = _fact_value(listing, "parking_fee")

    bits: list[str] = []
    if management:
        bits.append("物业包了" if _is_included(management) else f"物业费按{management}")
    if internet:
        bits.append("网络包了" if _is_included(internet) else f"网络费按{internet}")
    if electric:
        bits.append(f"电费{electric}")
    if water:
        bits.append(f"水费{water}")
    if parking:
        bits.append("停车包了" if _is_included(parking) else f"停车费{parking}")
    if not bits:
        return ""
    return "，".join(bits) + "。"


def _combined_listing_text(listing: dict[str, Any]) -> str:
    values = [listing]
    for key in ("normalized_data", "extracted_data", "canonical_facts"):
        raw = listing.get(key)
        if raw:
            values.append(raw)
    return " ".join(_as_text(v) for v in values)


def detect_talk_tags(listing: dict[str, Any]) -> list[str]:
    text = _combined_listing_text(listing).lower()
    services = _service_facts(listing)
    tags: list[str] = []

    def has(*words: str) -> bool:
        return any(word.lower() in text for word in words)

    if has("匹克球", "pickleball"):
        tags.append("pickleball")
    if has("灭虫", "除虫", "pest control", "pest_control") or str(services.get("pest_control") or "").strip():
        tags.append("pest_control")

    cleaning = str(services.get("cleaning") or "").strip()
    if cleaning:
        if any(token in cleaning for token in ("每周3次", "每周三次", "一周3次", "一周三次")):
            tags.append("cleaning_3x")
        elif any(token in cleaning for token in ("每周2次", "每周两次", "一周2次", "一周两次")):
            tags.append("cleaning_2x")
        elif any(token in cleaning for token in ("每周1次", "每周一次", "一周1次", "一周一次")):
            tags.append("cleaning_1x")
    elif has("每周3次", "每周三次", "一周3次", "一周三次") and has("保洁", "清洁", "打扫"):
        tags.append("cleaning_3x")
    elif has("每周2次", "每周两次", "一周2次", "一周两次") and has("保洁", "清洁", "打扫"):
        tags.append("cleaning_2x")
    elif has("每周1次", "每周一次", "一周1次", "一周一次") and has("保洁", "清洁", "打扫"):
        tags.append("cleaning_1x")

    if has("每周换床", "每周更换床", "每周换床品", "每周更换床品", "linen"):
        tags.append("linen_weekly")
    if has("未入住", "从未入住", "没人住过", "全新未住"):
        tags.append("never_lived")
    if has("可养宠物", "允许宠物", "宠物友好", "pet allowed", "pet_allowed"):
        tags.append("pet_allowed")
    if has("河景", "river view", "riverview", "湄公河景"):
        tags.append("river_view")
    if has("共享办公", "coworking", "co-working"):
        tags.append("coworking")
    if has("儿童活动区", "儿童区", "儿童游乐"):
        tags.append("kids_area")
    if has("房东直租", "业主直租", "房东本人", "owner direct"):
        tags.append("owner_direct")
    if has("全新装修", "装修新", "房况新", "家具家电全新") and "never_lived" not in tags:
        tags.append("new_condition")

    has_management = has("物业费包含", "包物业", "物业包含", "管理费包含", "包管理费")
    has_wifi = has("wifi包含", "wi-fi包含", "包网络", "网络费包含", "网费包含", "宽带包含")
    if has_management and has_wifi:
        tags.append("management_wifi")

    if has("阳台", "balcony"):
        tags.append("balcony")

    try:
        size = float(listing.get("size_sqm") or 0)
    except (TypeError, ValueError):
        size = 0
    if size >= 120:
        tags.append("large_layout")

    return list(dict.fromkeys(tags))


def choose_talk_tags(tags: list[str], max_points: int = 2) -> list[str]:
    return sorted(tags, key=lambda x: PRIORITY.get(x, 0), reverse=True)[: max(0, max_points)]


def _stable_choice(options: tuple[str, ...], listing_id: str, tag: str) -> str:
    if not options:
        return ""
    digest = hashlib.sha256(f"{listing_id}|{tag}".encode("utf-8")).digest()
    return options[int.from_bytes(digest[:4], "big") % len(options)]


def generate_talk(listing: dict[str, Any], max_points: int = 2, allow_empty: bool = True) -> str:
    manual = listing.get("talk") or listing.get("advisor_note") or listing.get("qiaolian_talk")
    if isinstance(manual, str) and manual.strip():
        return manual.strip()
    if isinstance(manual, (list, tuple)):
        values = [str(x).strip() for x in manual if str(x).strip()][:max_points]
        if values:
            return "\n".join(values)

    listing_id = str(listing.get("listing_id") or listing.get("id") or listing.get("title") or "listing")
    lines: list[str] = []

    cost_line = _cost_talk_line(listing)
    if cost_line:
        lines.append(cost_line)

    remaining = max(0, max_points - len(lines))
    selected = choose_talk_tags(detect_talk_tags(listing), max_points=remaining)
    for tag in selected:
        if TALK_LIBRARY.get(tag):
            lines.append(_stable_choice(TALK_LIBRARY[tag], listing_id, tag))

    if not lines:
        return "" if allow_empty else "具体条件按上面的房源资料来，有合适的再约现场看。"
    return "\n".join(line for line in lines if line)


__all__ = ["TALK_LIBRARY", "PRIORITY", "detect_talk_tags", "choose_talk_tags", "generate_talk"]
