"""Shared evidence-driven copy engine for ``💬 侨联说``.

The engine consumes frozen canonical facts only.  It does not invent facts and
uses deterministic phrase selection so the same listing keeps the same wording
across channel, preview and User Bot detail views.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any


PHRASES: dict[str, tuple[str, ...]] = {
    "pest_control": (
        "说出来你可能不信，灭虫这种小事人家都管😂",
        "灭虫这块已经安排了，不用自己另外找人。",
        "怕虫的可以留意下，这边有灭虫安排。",
        "这种细节一般没人提，但这边确实有虫害处理。",
        "备注一下：灭虫这块有人管。",
    ),
    "cleaning_1x": (
        "每周有人来打扫一次。",
        "保洁一周上门一次，日常维护够用。",
        "每周固定有一次保洁上门。",
        "打扫这块每周会有人来一次。",
        "备注：保洁一周一次。",
    ),
    "cleaning_2x": (
        "一周有人来打扫两次。",
        "保洁频率是一周两次，日常维护够用。",
        "每周固定两次会有保洁上门。",
        "一周来两次，算是比较勤快了。",
        "备注：保洁一周两次。",
    ),
    "cleaning_3x": (
        "一周三次保洁，频率确实挺高。",
        "这边一周来三次保洁，挺勤快的。",
        "每周三次上门，日常基本不用太操心。",
        "打扫这块安排得挺勤，一周三次。",
        "备注：保洁一周三次。",
    ),
    "linen_weekly": (
        "每周有人上门打扫，床品也会一起处理。",
        "床品这块每周会处理一次。",
        "每周会有人来处理床品和卫生。",
        "一周一次上门，床品也会更换。",
        "备注：每周上门，含床品处理。",
    ),
    "management_included": (
        "物业费已经包含在内。",
        "这边物业费是包含的，不用另外算。",
        "物业费已经算进租金里了。",
        "备注：物业费已含。",
        "物业这块不用再单独付。",
    ),
    "wifi_included": (
        "网络费已经包含在内。",
        "这边网费不用另外算。",
        "网络费用已经算进租金里了。",
        "备注：网络费已含。",
        "网费这块不用再单独付。",
    ),
    "wifi_ready": (
        "宽带已经装好了，直接能用。",
        "网络这块提前弄好了，不用自己再装。",
        "这套宽带已经安好了。",
        "网络已接通，搬进去少折腾一步。",
        "备注：宽带已装好。",
    ),
    "management_wifi": (
        "物业费和网络费都已经包含。",
        "这边物业和网费都不用另外算。",
        "物业费、网络费都算在租金里了。",
        "备注：物业费和网费都已含。",
        "物业和网络费用这两块都包了。",
    ),
    "management_wifi_ready": (
        "物业费已经包含，宽带也装好了。",
        "这边物业费含着，网络也已经接通。",
        "物业不用另外算，宽带也提前弄好了。",
        "备注：物业费已含，宽带已装好。",
        "物业这块包了，网络也可以直接用。",
    ),
    "owner_direct": (
        "这套是房东自己放出来的。",
        "这边是房东直租。",
        "房东自己在放这套，沟通比较直接。",
        "备注：房东直租。",
        "这套走的是房东直接放租。",
    ),
    "never_lived": (
        "这套是全新的，还没人住过。",
        "全新未入住，喜欢新房的可以留意。",
        "这套还没人入住过，是全新状态。",
        "备注：全新未入住。",
        "这边是第一次放租，还没住过人。",
    ),
    "new_condition": (
        "喜欢新一点的可以留意这套。",
        "这套整体状态比较新。",
        "家具和房况看着都偏新。",
        "备注：整体状态偏新。",
        "喜欢新一点房子的可以看这套。",
    ),
    "furnished": (
        "这套家具是齐全的。",
        "家具这块已经配齐了。",
        "基本家具都配好了。",
        "备注：家具齐全。",
        "家具已经备好，入住少添不少东西。",
    ),
    "balcony": (
        "这套带阳台。",
        "有个阳台，日常晾晒会方便一些。",
        "这边有阳台，平时能出去透透气。",
        "备注：带阳台。",
        "带个阳台，实用性会好一点。",
    ),
    "city_view": (
        "这套窗外是城市景观。",
        "这边视野朝市区方向。",
        "窗外能看到城市一带。",
        "备注：市景。",
        "这套是城市景观视野。",
    ),
    "river_view": (
        "这套能看到河景。",
        "窗外视野朝河那一面。",
        "这边能看到一片河景。",
        "备注：河景。",
        "站窗边能望到河面。",
    ),
    "pool": (
        "楼下有泳池，属于这类公寓的常规配置。",
        "这边配了泳池，日常能用。",
        "小区自带泳池。",
        "泳池是基础配套之一。",
        "备注：楼下有泳池。",
    ),
    "gym": (
        "楼下有健身房，日常够用。",
        "这边配了健身房。",
        "小区自带健身房。",
        "健身房是基础配套之一。",
        "备注：楼下有健身房。",
    ),
    "pool_gym": (
        "泳池健身房这些都有，属于正常配置。",
        "楼下泳池健身房都配了。",
        "小区自带泳池和健身房。",
        "泳池健身房都在楼里，日常方便。",
        "备注：泳池、健身房都有。",
    ),
    "pickleball": (
        "这边居然还有匹克球😂",
        "楼下有匹克球场，好奇的可以试试。",
        "这类小区带匹克球场的不算多。",
        "匹克球场就在楼下，算个小亮点。",
        "备注：带匹克球场。",
    ),
    "tennis": (
        "楼下有网球场。",
        "小区自带网球场。",
        "这边配了网球场。",
        "网球场就在小区里。",
        "备注：带网球场。",
    ),
    "table_tennis": (
        "楼下有乒乓球台。",
        "小区自带乒乓球设施。",
        "这边配了乒乓球台。",
        "楼下就能打乒乓球。",
        "备注：带乒乓球台。",
    ),
    "billiards": (
        "楼下有台球桌。",
        "小区里配了台球设施。",
        "这边有台球桌。",
        "楼下就能打台球。",
        "备注：带台球桌。",
    ),
    "kids_area": (
        "楼下有儿童游乐区，带娃会方便一点。",
        "小区自带儿童活动区。",
        "这边配了儿童游乐设施。",
        "有儿童区，带小孩的可以留意。",
        "备注：带儿童活动区。",
    ),
    "sauna": (
        "楼下有桑拿房。",
        "小区自带桑拿设施。",
        "这边配了桑拿房。",
        "桑拿房就在楼里。",
        "备注：带桑拿房。",
    ),
    "jacuzzi": (
        "这边配了按摩池。",
        "小区带按摩浴池。",
        "楼下有按摩浴缸。",
        "按摩池也是这边的配套之一。",
        "备注：带按摩浴池。",
    ),
    "rooftop": (
        "楼上有公共天台。",
        "这边带屋顶公共空间。",
        "楼顶有个平台可以上去。",
        "天台也是这栋的公共配套之一。",
        "备注：带公共天台。",
    ),
    "coworking": (
        "楼下有共享办公区，在家办公会方便点。",
        "这边配了共享办公空间。",
        "小区自带 coworking 区。",
        "楼下有办公空间可以用。",
        "备注：带共享办公区。",
    ),
    "concierge": (
        "楼下有前台管家服务。",
        "这边配了前台接待。",
        "小区有前台管家。",
        "日常有事可以找楼下前台。",
        "备注：带前台服务。",
    ),
    "parking": (
        "这边有停车位。",
        "小区自带停车场。",
        "楼下可以停车。",
        "有停车场，开车的可以留意。",
        "备注：带停车位。",
    ),
    "private_pool": (
        "这套自带私人泳池。",
        "这边是独立泳池，不跟其他住户共用。",
        "泳池是这套自己独用的。",
        "私人泳池算这套比较特别的一项。",
        "备注：带私人泳池。",
    ),
    "villa": (
        "这套是别墅类型，空间比较独立。",
        "这边是别墅，不是公寓户型。",
        "备注：别墅类型。",
        "这套房型属于别墅。",
        "别墅类型，适合更看重独立空间的。",
    ),
    "large_layout": (
        "这套属于大户型。",
        "这边户型面积比较大。",
        "空间偏宽敞的一套。",
        "备注：大户型。",
        "喜欢大空间的可以留意这套。",
    ),
    "high_floor": (
        "这套楼层比较高。",
        "这边是高楼层。",
        "楼层位置靠上。",
        "备注：高楼层。",
        "这套属于偏高楼层。",
    ),
    "low_floor": (
        "这套楼层比较低，出入会方便一些。",
        "这边是低楼层。",
        "楼层位置靠下。",
        "备注：低楼层。",
        "这套属于偏低楼层。",
    ),
    "pet_allowed": (
        "这套允许养宠物。",
        "养宠物是可以的。",
        "这边允许带宠物入住。",
        "有猫狗的可以留意，这套允许养宠物。",
        "备注：允许养宠物。",
    ),
    "fallback": (
        "这套没什么特别要强调的，直接看实拍和现场更准。",
        "这套我就不硬找东西夸了😂 看照片顺眼再约。",
        "中规中矩的一套，喜欢风格的可以再约看。",
        "没有特别要强调的点，房子还是现场看最实在。",
        "这套就按实际资料看，合适再约。",
    ),
}

_PRIORITY = (
    "never_lived",
    "cleaning_3x",
    "cleaning_2x",
    "linen_weekly",
    "pest_control",
    "management_wifi",
    "management_wifi_ready",
    "owner_direct",
    "private_pool",
    "river_view",
    "pickleball",
    "coworking",
    "pet_allowed",
    "large_layout",
    "new_condition",
    "furnished",
    "balcony",
    "city_view",
    "cleaning_1x",
    "management_included",
    "wifi_included",
    "wifi_ready",
    "tennis",
    "kids_area",
    "concierge",
    "rooftop",
    "parking",
    "pool_gym",
    "pool",
    "gym",
    "table_tennis",
    "billiards",
    "sauna",
    "jacuzzi",
    "high_floor",
    "low_floor",
    "villa",
)


def _strings(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    if value in (None, ""):
        return set()
    return {str(value).strip().lower()}


def adviser_tags_from_facts(facts: dict[str, Any] | None) -> list[str]:
    facts = dict(facts or {})
    tags = [str(tag).strip() for tag in (facts.get("adviser_signals") or []) if str(tag).strip()]

    included = _strings(facts.get("included"))
    if "物业费" in included or "管理费" in included:
        tags.append("management_included")
    if included.intersection({"wi-fi", "wifi", "网络", "网络费", "网费"}):
        tags.append("wifi_included")

    services = facts.get("services") if isinstance(facts.get("services"), dict) else {}
    cleaning = str(services.get("cleaning") or "")
    match = re.search(r"(?:每周|一周)?\s*([123])\s*次", cleaning)
    if match:
        tags.append(f"cleaning_{match.group(1)}x")
    if services.get("pest_control"):
        tags.append("pest_control")
    if services.get("linen_change"):
        tags.append("linen_weekly")
    if services.get("internet"):
        tags.append("wifi_ready")
    if services.get("concierge"):
        tags.append("concierge")

    amenities = _strings(facts.get("amenities"))
    amenity_map = {
        "游泳池": "pool",
        "泳池": "pool",
        "私人泳池": "private_pool",
        "健身房": "gym",
        "匹克球": "pickleball",
        "网球": "tennis",
        "网球场": "tennis",
        "乒乓球": "table_tennis",
        "台球": "billiards",
        "儿童游乐区": "kids_area",
        "儿童活动区": "kids_area",
        "桑拿": "sauna",
        "按摩池": "jacuzzi",
        "按摩浴缸": "jacuzzi",
        "共享办公": "coworking",
        "停车位": "parking",
        "停车场": "parking",
    }
    for source, tag in amenity_map.items():
        if source.lower() in amenities:
            tags.append(tag)

    house = facts.get("house") if isinstance(facts.get("house"), dict) else {}
    features = _strings(house.get("features"))
    feature_map = {
        "全新未入住": "never_lived",
        "房况较新": "new_condition",
        "阳台": "balcony",
        "河景": "river_view",
        "市景": "city_view",
        "大户型": "large_layout",
    }
    for source, tag in feature_map.items():
        if source.lower() in features:
            tags.append(tag)
    furniture = str(house.get("furniture") or "").lower()
    if "齐" in furniture or "全" in furniture:
        tags.append("furnished")
    source_type = str(house.get("source_type") or "")
    if "房东直租" in source_type or "业主直租" in source_type:
        tags.append("owner_direct")
    pets = str(house.get("pets") or "").lower()
    if pets in {"允许", "可以", "yes", "allowed"} or "允许" in pets or "可养" in pets:
        tags.append("pet_allowed")

    property_type = str(facts.get("property_type") or "").lower()
    if "别墅" in property_type or "villa" in property_type:
        tags.append("villa")

    # Compatibility for older frozen packages that predate adviser_signals.
    floor = str(facts.get("floor") or "")
    floor_match = re.search(r"\d{1,3}", floor)
    if floor_match:
        level = int(floor_match.group(0))
        if 1 <= level <= 5:
            tags.append("low_floor")
        elif level >= 20:
            tags.append("high_floor")

    tagset = set(tags)
    if "management_included" in tagset and "wifi_included" in tagset:
        tagset.discard("management_included")
        tagset.discard("wifi_included")
        tagset.discard("wifi_ready")
        tagset.add("management_wifi")
    elif "management_included" in tagset and "wifi_ready" in tagset:
        tagset.discard("management_included")
        tagset.discard("wifi_ready")
        tagset.add("management_wifi_ready")

    cleaning = [tag for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x") if tag in tagset]
    if cleaning:
        for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x"):
            tagset.discard(tag)
        tagset.add(cleaning[0])
    if "never_lived" in tagset:
        tagset.discard("new_condition")
    if "private_pool" in tagset:
        tagset.discard("pool")
    if "pool" in tagset and "gym" in tagset:
        tagset.discard("pool")
        tagset.discard("gym")
        tagset.add("pool_gym")
    if "river_view" in tagset:
        tagset.discard("city_view")

    return [tag for tag in _PRIORITY if tag in tagset]


def _phrase(tag: str, seed: str) -> str:
    choices = PHRASES[tag]
    digest = hashlib.sha256(f"{seed}|{tag}".encode("utf-8")).hexdigest()
    return choices[int(digest[:12], 16) % len(choices)]


def generate_adviser_lines(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 2,
    allow_fallback: bool = True,
) -> list[str]:
    tags = adviser_tags_from_facts(facts)
    limit = max(0, int(max_points))
    if tags and limit:
        return [_phrase(tag, seed) for tag in tags[:limit]]
    if allow_fallback and limit:
        return [_phrase("fallback", seed)]
    return []


def generate_adviser_text(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    max_points: int = 2,
    allow_fallback: bool = True,
) -> str:
    return "\n".join(
        generate_adviser_lines(
            facts,
            seed=seed,
            max_points=max_points,
            allow_fallback=allow_fallback,
        )
    )


__all__ = [
    "PHRASES",
    "adviser_tags_from_facts",
    "generate_adviser_lines",
    "generate_adviser_text",
]
