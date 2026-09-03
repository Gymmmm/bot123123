"""Rule-based rental-detail voice for the public User Bot.

The engine never invents facts: it only talks about tags detected from fields
already present on the listing. Manual talk always wins. Automatic output is
kept to one or two short points and may be empty when nothing distinctive is
supported.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


TALK_LIBRARY: dict[str, tuple[str, ...]] = {
    "pickleball": (
        "这边居然还有匹克球😂 没玩过的可以下来试试。",
        "楼下有匹克球，这个在公寓里还挺少见。",
        "健身房泳池先不说，这边还有匹克球😂",
    ),
    "pest_control": (
        "怕虫的可以多看一眼，这边灭虫服务也包含了。",
        "灭虫这块有人处理，对怕虫的人算个实用点。",
        "这边有安排灭虫服务，不用自己再单独找。",
    ),
    "cleaning_3x": (
        "一周三次保洁，这个频率确实比较少见。",
        "平时工作忙的话会省不少事，一周有人来三次。",
    ),
    "cleaning_2x": (
        "平时工作忙的，这边一周有人来收拾两次。",
        "一周两次保洁，卫生这块能少操点心。",
        "有包每周两次保洁，属于挺实际的配置。",
    ),
    "cleaning_1x": (
        "每周会有人来打扫一次，日常维持够用。",
        "有每周一次保洁，自己能少收拾一点。",
    ),
    "linen_weekly": (
        "床品这边每周也会换一次，住进去省事不少。",
        "每周保洁还会处理床品，这个对嫌麻烦的人挺友好。",
    ),
    "never_lived": (
        "全新没住过的，比较介意使用痕迹的可以多看两眼。",
        "这套是未入住状态，喜欢新房的可以留一下。",
    ),
    "pet_allowed": (
        "有宠物的可以留意，这套允许带宠物入住。",
        "宠物友好，这个在筛房时还是挺重要的。",
    ),
    "management_wifi": (
        "物业和网络都算在里面了，住进去以后少算两笔。",
        "物业、Wi‑Fi 都包着，平时交费用会省事一点。",
    ),
    "river_view": (
        "窗外能看到河，比较在意视野的可以看看照片。",
        "这套有河景，景观这一项算比较明确。",
    ),
    "coworking": (
        "楼里有共享办公区，在家办公的人会比较方便。",
        "不想一直窝在房间办公的话，楼里还有共享工作区。",
    ),
    "kids_area": (
        "带孩子住的话可以留意，楼里有儿童活动区。",
        "有儿童活动区，家庭住会更方便一点。",
    ),
    "balcony": (
        "有阳台，晾晒和透气都方便一点。",
        "这套带阳台，属于日常挺实用的配置。",
    ),
    "large_layout": (
        "这套空间感比较足，东西多或者家庭住会舒服一点。",
        "户型偏宽敞，不喜欢住得挤的可以多看两眼。",
    ),
    "owner_direct": (
        "这套是业主直接放出来的，沟通链路比较简单。",
        "房东本人放租，信息确认起来会更直接一点。",
    ),
    "new_condition": (
        "整体房况看着比较新，介意旧装修的可以多看两眼。",
        "喜欢新一点房子的，这套可以留一下。",
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


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def _contains(value: Any, *keywords: str) -> bool:
    text = _as_text(value).lower()
    return any(str(k).lower() in text for k in keywords)


def _combined_listing_text(listing: dict[str, Any]) -> str:
    # Some production rows expose normalized/extracted payloads as JSON strings.
    # Include them for detection, but never create a fact that is not literally
    # present somewhere in the listing payload.
    values = [listing]
    for key in ("normalized_data", "extracted_data", "canonical_facts"):
        raw = listing.get(key)
        if raw:
            values.append(raw)
    return " ".join(_as_text(v) for v in values)


def detect_talk_tags(listing: dict[str, Any]) -> list[str]:
    text = _combined_listing_text(listing).lower()
    tags: list[str] = []

    def has(*words: str) -> bool:
        return any(word.lower() in text for word in words)

    if has("匹克球", "pickleball"):
        tags.append("pickleball")
    if has("灭虫", "除虫", "pest control", "pest_control"):
        tags.append("pest_control")

    if has("每周3次", "每周三次", "一周3次", "一周三次") and has("保洁", "清洁", "打扫"):
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

    # Stable de-duplication.
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

    selected = choose_talk_tags(detect_talk_tags(listing), max_points=max_points)
    if not selected:
        return "" if allow_empty else "这套我就不硬夸了😂 大家主要看照片，合眼缘再约现场。"

    listing_id = str(listing.get("listing_id") or listing.get("id") or listing.get("title") or "listing")
    lines = [_stable_choice(TALK_LIBRARY[tag], listing_id, tag) for tag in selected if TALK_LIBRARY.get(tag)]
    return "\n".join(line for line in lines if line)


__all__ = ["TALK_LIBRARY", "PRIORITY", "detect_talk_tags", "choose_talk_tags", "generate_talk"]
