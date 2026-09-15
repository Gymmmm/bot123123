"""Shared evidence-driven copy engine for ``💬 侨联说`` — Production V1.

Frozen facts only; at most two different-category sentences. No useful signal
means silence. Renderers own headings, emoji and bullets. Management/internet
are deliberately excluded. Stable listing ID seeds keep wording deterministic.
"""
from __future__ import annotations
import hashlib
import re
from typing import Any

PHRASES: dict[str, tuple[str, ...]] = {
    "never_lived": ("这套目前是全新未入住，比较在意房况新旧的话可以优先看看。", "这套还没有人入住过，喜欢新房状态的可以重点看看。", "比较在意房况的话可以留意，这套目前是全新未入住。"),
    "pet_allowed": ("有宠物的话可以留意，这套允许带宠物入住。", "这套允许养宠物，有猫狗的可以优先看看。", "养宠物的租客可以留意，这套目前接受宠物入住。"),
    "private_pool": ("这套自带私人泳池，比较看重独立休闲空间的话可以重点看看。", "私人泳池是这套比较明显的特点，实际空间建议现场看看。", "如果比较在意独立性，这套带私人泳池可以优先看看。"),
    "river_view": ("这套有河景，实际视野和朝向建议到现场再确认。", "比较在意窗外视野的话可以留意，这套目前资料标注为河景。", "河景是这套比较明显的特点，现场可以重点看看实际视野。"),
    "large_layout": ("这套空间偏大，对居住空间有要求的话可以重点看看。", "如果比较在意空间感，这套属于面积偏宽松的一类。", "喜欢大一点空间的，可以把这套放进第一轮比较。"),
    "owner_direct": ("这套目前是房东直接放租，具体租赁条件仍以确认后的信息为准。", "这套属于房东直租，预约时可以再确认具体租赁条件。", "目前资料显示为房东直接放租，具体条件以最终确认为准。"),
    "new_condition": ("这套整体房况偏新，比较在意新旧程度的话可以重点看看。", "喜欢房况新一点的，可以把这套放进第一轮比较。", "从现有资料看，这套整体状态偏新。"),
    "furnished": ("这套家具配置比较完整，入住后需要另外添置的东西会少一些。", "基本家具已经配好，看房时可以重点确认实际配置和状态。", "家具已经配齐，入住前主要确认实际配置是否符合自己的使用习惯。"),
    "balcony": ("这套带阳台，如果平时需要晾晒，可以现场看看实际空间。", "有阳台是这套比较实用的一点，实际大小和朝向建议看房时确认。", "如果比较在意晾晒或室外空间，这套带阳台可以重点看看。"),
    "city_view": ("这套目前资料标注为市景，实际视野建议到现场再确认。", "比较在意窗外视野的话，这套可以现场看看实际市景效果。", "这套是城市景观方向，具体视野以现场为准。"),
    "villa": ("这套属于别墅类型，比较在意独立空间的话可以重点看看。", "如果更喜欢独立一点的居住空间，这套别墅可以优先看看。", "这套不是常规公寓户型，属于独立性更强的别墅类型。"),
    "high_floor": ("这套属于较高楼层，比较在意楼层和视野的话可以重点看看。", "喜欢高楼层的话可以留意这套，实际朝向和视野建议现场判断。", "这套楼层较高，现场可以重点看看实际视野和居住感受。"),
    "low_floor": ("这套属于较低楼层，如果不喜欢住太高可以留意。", "比较偏好低楼层的话，这套可以放进第一轮比较。", "这套楼层位置偏低，实际出入体验可以看房时感受。"),
    "cleaning_1x": ("这套包含每周一次保洁，日常打理会省事一些。", "每周有一次保洁服务，比较在意日常维护的话可以留意。", "目前租赁条件包含每周一次保洁。"),
    "cleaning_2x": ("这套包含每周两次保洁，日常打理会省事不少。", "每周有两次保洁服务，比较在意日常维护的话可以重点看看。", "目前租赁条件包含每周两次保洁。"),
    "cleaning_3x": ("这套包含每周三次保洁，保洁频率比较高。", "每周有三次保洁服务，日常打理会省事不少。", "目前租赁条件包含每周三次保洁服务。"),
    "linen_weekly": ("这套每周包含床品处理，对希望减少日常打理的人比较方便。", "目前服务包含每周床品处理，具体内容可以预约时再确认。", "每周会处理床品和卫生，比较在意日常维护的话可以留意。"),
    "pest_control": ("这边有虫害处理安排，比较在意这一点的话可以留意。", "目前服务里包含虫害处理，具体频率可以预约时再确认。", "虫害处理已经有安排，入住前可以再确认具体服务方式。"),
    "coworking": ("楼内有共享办公空间，经常在家办公的话会比较实用。", "如果平时有远程办公需求，可以留意楼内的共享办公区。", "这边配有共享办公区，对居家办公的人比较方便。"),
    "kids_area": ("小区有儿童活动区，带小孩入住的话可以现场重点看看。", "有儿童活动空间，家庭租客可以把这一项一起纳入比较。", "如果有小孩，可以现场看看儿童活动区是否符合日常需要。"),
    "parking": ("这边有停车条件，有车的话建议预约前再确认车位和当前费用。", "开车的话可以留意，这边有停车位，具体收费建议提前确认。", "小区可以停车，车位情况和费用以预约时确认为准。"),
    "pickleball": ("小区带匹克球场，如果平时会运动，这项配套比较少见。", "这边有匹克球场，对喜欢运动的人算是一个额外加分项。", "匹克球场是这边比较特别的一项公共配套。"),
    "tennis": ("小区带网球场，如果平时会打球可以留意。", "这边配有网球场，对喜欢运动的人比较实用。", "网球场是这边的一项公共配套。"),
    "concierge": ("楼内有前台服务，日常需要协助时会方便一些。", "这边配有前台接待，日常居住会多一个服务入口。", "小区有前台服务，需要日常协助时会方便一些。"),
    "rooftop": ("楼内有公共天台，比较在意公共活动空间的话可以现场看看。", "这边带公共天台，可以看房时顺便看看实际空间。", "公共天台是这边的一项额外配套。"),
    "pool_gym": ("小区配有泳池和健身房，如果平时会使用可以现场一起看看。", "泳池和健身房都有，比较在意公共配套的话可以纳入比较。", "这边同时有泳池和健身房，实际开放情况建议看房时确认。"),
    "pool": ("小区配有泳池，如果平时会使用可以现场看看。", "这边有公共泳池，实际开放情况建议看房时确认。", "泳池是这边的一项公共配套。"),
    "gym": ("小区配有健身房，如果平时会使用可以现场看看。", "这边有健身房，实际设备情况建议看房时确认。", "健身房是这边的一项公共配套。"),
    "table_tennis": ("小区有乒乓球设施，平时会使用的话可以留意。", "这边配有乒乓球设施。", "乒乓球设施是这边的一项公共配套。"),
    "billiards": ("小区有台球设施，平时会使用的话可以留意。", "这边配有台球设施。", "台球设施是这边的一项公共配套。"),
    "sauna": ("小区配有桑拿设施，平时会使用的话可以留意。", "这边有桑拿设施，可以看房时顺便确认开放情况。", "桑拿是这边的一项公共配套。"),
    "jacuzzi": ("小区配有按摩池，平时会使用的话可以留意。", "这边有按摩池，可以看房时顺便确认开放情况。", "按摩池是这边的一项公共配套。"),
}
_PRIORITY = ("pet_allowed", "never_lived", "private_pool", "river_view", "large_layout", "owner_direct", "new_condition", "furnished", "balcony", "city_view", "high_floor", "low_floor", "villa", "cleaning_3x", "cleaning_2x", "cleaning_1x", "linen_weekly", "pest_control", "coworking", "kids_area", "parking", "pickleball", "tennis", "concierge", "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi")
TAG_CATEGORY: dict[str, str] = {
    "pet_allowed": "fit", "never_lived": "condition", "new_condition": "condition", "furnished": "condition",
    "large_layout": "layout", "balcony": "layout", "villa": "layout", "river_view": "view", "city_view": "view",
    "high_floor": "floor", "low_floor": "floor", "owner_direct": "source", "cleaning_1x": "cleaning",
    "cleaning_2x": "cleaning", "cleaning_3x": "cleaning", "linen_weekly": "service", "pest_control": "service", "private_pool": "special",
    **{tag: "amenity" for tag in ("coworking", "kids_area", "parking", "pickleball", "tennis", "concierge", "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi")},
}
VALID_TAGS = frozenset(PHRASES)
COMMON_AMENITY_TAGS = frozenset({"pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi"})


def verified_canonical_adviser_facts(facts: dict[str, Any]) -> dict[str, Any]:
    """Conservative compatibility for old canonical rows with inferred floor tags."""
    result = dict(facts)
    if result.get("adviser_signals_version") != "explicit-v1":
        result["adviser_signals"] = [tag for tag in _explicit_signals(result)
                                     if tag not in {"high_floor", "low_floor"}]
    return result


def _strings(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    if value in (None, ""):
        return set()
    return {str(value).strip().lower()}


def _explicit_signals(facts: dict[str, Any]) -> list[str]:
    raw = facts.get("adviser_signals") or []
    if not isinstance(raw, (list, tuple, set)):
        return []
    return [str(tag).strip() for tag in raw if str(tag).strip() in VALID_TAGS]


def adviser_tags_from_facts(facts: dict[str, Any] | None) -> list[str]:
    facts = dict(facts or {})
    tags = _explicit_signals(facts)
    services = facts.get("services") if isinstance(facts.get("services"), dict) else {}
    cleaning = str(services.get("cleaning") or "").strip()
    for frequency in re.findall(r"(?:每周|一周)?\s*([123])\s*次", cleaning):
        tags.append(f"cleaning_{frequency}x")
    for field, tag in (("pest_control", "pest_control"), ("linen_change", "linen_weekly"), ("concierge", "concierge")):
        if services.get(field) is True:
            tags.append(tag)
    amenities = _strings(facts.get("amenities"))
    amenity_map = {"游泳池": "pool", "泳池": "pool", "私人泳池": "private_pool", "健身房": "gym", "匹克球": "pickleball", "网球": "tennis", "网球场": "tennis", "乒乓球": "table_tennis", "台球": "billiards", "儿童游乐区": "kids_area", "儿童活动区": "kids_area", "桑拿": "sauna", "按摩池": "jacuzzi", "按摩浴缸": "jacuzzi", "共享办公": "coworking", "停车位": "parking", "停车场": "parking"}
    tags.extend(tag for source, tag in amenity_map.items() if source.lower() in amenities)
    house = facts.get("house") if isinstance(facts.get("house"), dict) else {}
    features = _strings(house.get("features"))
    feature_map = {"全新未入住": "never_lived", "房况较新": "new_condition", "阳台": "balcony", "河景": "river_view", "市景": "city_view", "大户型": "large_layout"}
    tags.extend(tag for source, tag in feature_map.items() if source.lower() in features)
    if house.get("furnished") is True:
        tags.append("furnished")
    source_type = str(house.get("source_type") or "").strip()
    if "房东直租" in source_type or "业主直租" in source_type:
        tags.append("owner_direct")
    pets = str(house.get("pets") or "").strip().lower()
    if not re.search(r"不允许|不可以|不可|禁止|不接受|不能|not\s+allowed|no\b", pets):
        if pets in {"允许", "可以", "yes", "allowed"} or "允许" in pets or "可养" in pets:
            tags.append("pet_allowed")
    property_type = str(facts.get("property_type") or "").strip().lower()
    if "别墅" in property_type or "villa" in property_type:
        tags.append("villa")
    # Floor number alone is never evidence for high_floor / low_floor.
    tagset = {tag for tag in tags if tag in VALID_TAGS}
    if re.search(r"不允许|不可以|不可|禁止|不接受|不能|not\s+allowed|no\b", pets):
        tagset.discard("pet_allowed")
    if "never_lived" in tagset:
        tagset.discard("new_condition")
    if "private_pool" in tagset:
        tagset.discard("pool")
        tagset.discard("pool_gym")
    if "pool" in tagset and "gym" in tagset:
        tagset.difference_update({"pool", "gym"})
        tagset.add("pool_gym")
    if "river_view" in tagset:
        tagset.discard("city_view")
    cleaning_tags = [tag for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x") if tag in tagset]
    if cleaning_tags:
        tagset.difference_update({"cleaning_3x", "cleaning_2x", "cleaning_1x"})
        tagset.add(cleaning_tags[0])
    return [tag for tag in _PRIORITY if tag in tagset]


def _phrase(tag: str, seed: str) -> str:
    choices = PHRASES[tag]
    digest = hashlib.sha256(f"{seed}|{tag}".encode("utf-8")).hexdigest()
    return choices[int(digest[:12], 16) % len(choices)]


def _select_tags(tags: list[str], limit: int) -> list[str]:
    if limit <= 0 or not tags or not any(tag not in COMMON_AMENITY_TAGS for tag in tags):
        return []
    selected: list[str] = []
    used_categories: set[str] = set()
    for tag in tags:
        category = TAG_CATEGORY.get(tag, tag)
        if category in used_categories:
            continue
        selected.append(tag)
        used_categories.add(category)
        if len(selected) >= limit:
            break
    return selected


def generate_adviser_lines(facts: dict[str, Any] | None, *, seed: str = "", max_points: int = 2,
                           allow_fallback: bool = False) -> list[str]:
    """Plain sentences only; allow_fallback is compatibility-only, never filler."""
    try:
        limit = max(0, min(int(max_points), 2))
    except (TypeError, ValueError):
        limit = 2
    if limit == 0:
        return []
    return [_phrase(tag, seed) for tag in _select_tags(adviser_tags_from_facts(facts), limit)]


def generate_adviser_text(facts: dict[str, Any] | None, *, seed: str = "", max_points: int = 2,
                          allow_fallback: bool = False) -> str:
    return "\n".join(generate_adviser_lines(facts, seed=seed, max_points=max_points, allow_fallback=allow_fallback))


__all__ = ["PHRASES", "TAG_CATEGORY", "VALID_TAGS", "COMMON_AMENITY_TAGS", "adviser_tags_from_facts", "generate_adviser_lines", "generate_adviser_text"]
