"""Shared evidence-driven copy engine for ``💬 侨联说``.

Frozen facts only; at most two different-category sentences. No useful signal
means silence. Stable listing ID seeds keep wording deterministic.
"""
from __future__ import annotations
import hashlib
import re
from typing import Any

PHRASES: dict[str, tuple[str, ...]] = {
    "pest_control": ("怕虫的可以记一下，这边灭虫服务也安排了。", "连灭虫这块都有安排，属于不起眼但挺实用的小细节。", "虫害处理这边有人管，不用入住后自己再到处找。", "灭虫服务也包含在里面，这点还挺省心。", "平时比较怕虫的话，这条可以重点记一下。"),
    "cleaning_1x": ("每周有人来打扫一次，平时能少操点心。", "一周一次保洁，日常维持起来会轻松一点。", "每周固定有一次保洁上门。", "打扫这块一周有人来一次，忙的时候挺省事。", "保洁频率是一周一次，基本日常够用。"),
    "cleaning_2x": ("一周两次保洁，这个频率住起来挺舒服。", "每周两次有人来打扫，日常维护会轻松不少。", "保洁一周来两次，平时忙的人会比较喜欢。", "打扫这块每周安排两次。", "一周两次有人收拾，基本不用天天自己弄。"),
    "cleaning_3x": ("一周三次保洁，这个频率已经挺勤快了。", "每周三次有人来打扫，日常确实省心很多。", "保洁一周来三次，平时基本不用太操心。", "一周三次清洁，这种频率不算常见。", "打扫安排得挺勤，每周三次。"),
    "cleaning_included": ("这套有保洁服务，具体频率可以预约时再确认。", "打扫这块有人负责，多久来一次再问清楚就行。", "保洁服务有包含，具体安排让顾问再帮你确认。", "平时打扫有人管，频率以实际确认为准。", "这套带保洁，具体怎么安排可以看房前问一下。"),
    "linen_weekly": ("床品这块每周也有人处理，能少操心一件事。", "每周会处理床品，平时住起来会省事一点。", "床品和清洁每周都有安排。", "不想自己折腾床品的话，这项服务挺实用。", "每周有人处理床品和卫生。"),
    "management_included": ("物业费已经包了，每个月少一项要另外算的钱。", "这套物业费不用单独付，算预算会简单一点。", "物业这块已经包含，月底不用再多算一笔。", "物业费算在里面了，每月固定开销少一项。", "物业费不用另外交，这点挺直接。"),
    "wifi_included": ("网费已经包了，每个月少一项固定开销。", "这套网络费用不用另外交。", "网费算在里面了，月底账单能少一项。", "网络费已经包含，算每月预算会简单一点。", "这套网费不用另外付，住进去少一笔固定支出。"),
    "wifi_ready": ("宽带已经装好了，搬进去不用再从装网开始折腾。", "网络这块已经接通，入住以后会省事一点。", "宽带是现成的，搬进去少处理一步。", "网络已经装好，不用自己再另外安排安装。", "宽带已经准备好了，入住会顺手很多。"),
    "management_wifi": ("物业费和网费都包了，每个月固定支出能少算两项。", "物业和网络费用都不用另外交，算预算挺清楚。", "物业费、网费都已经包含，月底少看两行账单。", "这两项都包在里面了，每月固定开销简单不少。", "物业和网费都不用单算，这点挺省事。"),
    "management_wifi_ready": ("物业费已经包了，宽带也装好了，入住能少处理两件事。", "物业不用另外算，网络也已经接通。", "物业费已含，宽带也是现成的。", "这套物业费包着，网络也提前弄好了。", "物业这项不用另外付，宽带也已经准备好。"),
    "owner_direct": ("这套是房东自己放出来的，沟通会直接一点。", "房东直租，具体租赁条件可以继续往下确认。", "这套走的是房东直接放租。", "目前资料是房东直租，具体条件再谈清楚就行。", "房东自己在放这套，沟通链路比较直接。"),
    "never_lived": ("这套还没人住过，喜欢新房状态的可以留意。", "全新未入住，这种状态不是每套都有。", "比较介意别人住过的话，这套刚好是全新未入住。", "这套目前还是全新状态，没入住过。", "全新未入住，喜欢新的可以重点看看。"),
    "new_condition": ("这套整体房况偏新，比较在意新旧程度的可以留意。", "喜欢状态新一点的，这套可以放进候选。", "从现有资料看，这套整体比较新。", "房况偏新，不喜欢太旧的可以先留着。", "这套整体状态比较新，可以现场再看看细节。"),
    "furnished": ("家具家电基本都配好了，搬进去不用从零开始买。", "该有的基本都配着，入住能少添不少东西。", "家具家电比较齐，搬进去会省事一点。", "家具这块已经配得比较完整。", "基本家具家电都有，入住前确认实际配置就行。"),
    "balcony": ("这套带阳台，晾衣服、透透气都方便一点。", "有个阳台，平时多一个能出去站站的地方。", "带阳台这件事不大，但每天用起来挺实在。", "喜欢有点室外空间的话，这套可以留意。", "这边有阳台，实际大小和朝向现场看最准。"),
    "city_view": ("这套是城市景观方向，实际视野现场看最准。", "窗外看城市这边，喜欢市景的可以留意。", "这边资料标的是市景。", "喜欢城市景观的话，这套可以现场重点看看。", "窗外是市区方向，实际效果还是看房时最直观。"),
    "river_view": ("这套能看河景，实际视野现场站窗边看最准。", "窗外朝河这边，喜欢景观的可以留意。", "河景是这套比较明显的一点。", "这边资料标的是河景，现场看会更直观。", "喜欢河景的话，这套可以重点看看实际视野。"),
    "pool": ("楼下有泳池，平时想游两圈还是挺方便。", "这边配了泳池，日常想用不用另外找地方。", "小区有泳池，喜欢游泳的可以记一下。", "泳池是有的，实际开放情况看房时顺便确认。", "楼下能游泳，平时用起来会方便一点。"),
    "gym": ("楼下有健身房，至少少一个“太远所以不去”的借口。", "健身房就在楼里，平时想动一下挺方便。", "这边配了健身房，住进去下楼就能练。", "小区有健身房，平时会用的可以记一下。", "健身不用特意往外跑，楼里就有。"),
    "pool_gym": ("泳池健身房都有，日常运动不用跑太远。", "楼下泳池健身房都配了，平时用起来挺方便。", "游泳、健身在楼里基本都能解决。", "泳池健身房都有，喜欢运动的可以记一下。", "这两个常用配套都齐了，住进去会比较方便。"),
    "pickleball": ("这边连匹克球场都有，算是比较少见的小彩蛋。", "楼下有匹克球场，喜欢运动的可以试试。", "除了常规配套，这边还配了匹克球场。", "匹克球场都有，这项还挺少见。", "平时会玩匹克球的话，这边场地是现成的。"),
    "tennis": ("楼下有网球场，平时会打球的话挺方便。", "喜欢网球的可以记一下，小区里就有场地。", "这边配了网球场。", "不用特意往外找场地，楼下就能打网球。", "网球场是有的，运动党可以留意。"),
    "table_tennis": ("楼下能打乒乓球，平时想活动一下挺方便。", "小区有乒乓球设施。", "喜欢打乒乓球的话，这边场地是现成的。", "楼下有乒乓球台，偶尔打两局挺方便。", "乒乓球设施也配了。"),
    "billiards": ("楼下有台球桌，偶尔想玩两杆不用往外跑。", "这边有台球设施，平时休闲多一个选择。", "小区里就能打台球。", "台球桌也有，闲的时候可以玩两局。", "喜欢打台球的话，这边是现成的。"),
    "kids_area": ("带小孩的话可以留意，楼下有儿童活动区。", "小朋友也有地方放电，不用天天往外跑。", "家里有小朋友的话，这个儿童区会挺实用。", "小区有儿童活动空间。", "带娃住的话，这项配置会比较方便。"),
    "sauna": ("楼里还有桑拿，平时会用的人可以留意。", "桑拿设施也有，想放松一下多一个选择。", "这边配了桑拿房。", "喜欢蒸一蒸的话，这边是现成的。", "桑拿也算在这边的公共配套里。"),
    "jacuzzi": ("这边还有按摩池，休闲配置多一个选择。", "按摩池也有，平时想放松一下可以用。", "小区配了按摩池。", "喜欢泡一泡的话，这边有现成的设施。", "按摩池也算这边的一项公共配套。"),
    "rooftop": ("楼上有公共天台，想透透气的时候多一个地方。", "这边有公共天台，偶尔上去坐坐挺不错。", "楼顶还有公共空间可以用。", "有天台，平时多一个能待的地方。", "公共天台也配了，实际空间可以现场看看。"),
    "coworking": ("楼下有共享办公区，在家办公的人会比较懂这个配置。", "经常带电脑工作的话，这个共享办公区挺实用。", "不想天天窝房间办公，楼里还有地方可以坐。", "这边配了共享办公空间。", "平时远程办公的话，这项会挺方便。"),
    "concierge": ("楼下有前台管家，平时碰到点事至少知道找谁。", "有前台服务，日常住起来会方便一点。", "小区有管家服务，需要协助时有人可以找。", "楼下前台有人管事，住久了会比较方便。", "这边配了前台管家服务。"),
    "parking": ("有车的话这点挺重要，这边有停车条件。", "开车的可以记一下，这边能停车。", "至少不用先头疼车放哪，具体车位和费用再确认。", "这边配了停车位，有车的可以留意。", "停车条件是有的，具体收费让顾问再确认。"),
    "private_pool": ("这个就不太一样了，这套自己带私人泳池。", "泳池不是公用的，是这套自己用的。", "喜欢独立空间的话，私人泳池算挺明显的加分项。", "这套带独立泳池，不用跟其他住户共用。", "私人泳池是这套比较特别的一项配置。"),
    "large_layout": ("这套空间比较放得开，喜欢大一点的可以留意。", "户型偏大，不喜欢住得太挤的可以看看。", "空间比较宽松，住起来会更从容一点。", "这套属于大户型路线。", "对空间要求高的话，这套可以重点看看。"),
    "high_floor": ("这套楼层比较高，喜欢高层的可以现场看看实际感觉。", "偏高楼层，比较在意楼层的可以留意。", "这套位置靠上，实际视野现场看最准。", "喜欢住高一点的话，这套可以记一下。", "楼层偏高，具体朝向和视野看房时再判断。"),
    "low_floor": ("这套楼层比较低，不喜欢住太高的可以留意。", "偏低楼层，比较喜欢低层的可以看看。", "这套位置靠下，实际居住感受现场看最准。", "如果本身偏好低楼层，这套可以记一下。", "楼层偏低，是否合适主要看你的个人习惯。"),
    "pet_allowed": ("家里有猫猫狗狗的话可以看，这套允许带宠物。", "养宠物的先记一下，这套可以带着一起住。", "有毛孩子的话，这套不用一开始就淘汰。", "宠物这块是允许的。", "这套可以养宠物，有需要的可以重点留意。"),
}

_PRIORITY = ("pet_allowed", "never_lived", "private_pool", "river_view", "large_layout", "owner_direct", "new_condition", "furnished", "balcony", "city_view", "high_floor", "low_floor", "cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included", "linen_weekly", "pest_control", "management_wifi", "management_wifi_ready", "management_included", "wifi_included", "wifi_ready", "coworking", "kids_area", "parking", "pickleball", "tennis", "concierge", "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi")
TAG_CATEGORY = {
    "pet_allowed": "fit", "never_lived": "condition", "new_condition": "condition", "furnished": "condition",
    "large_layout": "layout", "balcony": "layout", "river_view": "view", "city_view": "view", "high_floor": "floor", "low_floor": "floor",
    "owner_direct": "source", "cleaning_1x": "cleaning", "cleaning_2x": "cleaning", "cleaning_3x": "cleaning", "cleaning_included": "cleaning",
    "linen_weekly": "service", "pest_control": "service", "management_included": "cost", "wifi_included": "cost", "wifi_ready": "network",
    "management_wifi": "cost", "management_wifi_ready": "cost_network", "private_pool": "special",
    **{tag: "amenity" for tag in ("coworking", "kids_area", "parking", "pickleball", "tennis", "concierge", "rooftop", "pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi")},
}
VALID_TAGS = frozenset(PHRASES)
COMMON_AMENITY_TAGS = frozenset({"pool_gym", "pool", "gym", "table_tennis", "billiards", "sauna", "jacuzzi"})


def verified_canonical_adviser_facts(facts: dict[str, Any]) -> dict[str, Any]:
    result = dict(facts)
    if result.get("adviser_signals_version") != "explicit-v1":
        result["adviser_signals"] = [tag for tag in _explicit_signals(result) if tag not in {"high_floor", "low_floor"}]
    return result


def _strings(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip().lower() for item in value if str(item).strip()}
    return set() if value in (None, "") else {str(value).strip().lower()}


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
    match = re.search(r"(?:每周|一周)?\s*([123])\s*次", cleaning)
    if match:
        tags.append(f"cleaning_{match.group(1)}x")
    elif cleaning.lower() in {"包含", "有", "提供", "yes", "included"}:
        tags.append("cleaning_included")
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
        tagset.difference_update({"pool", "gym"}); tagset.add("pool_gym")
    if "river_view" in tagset:
        tagset.discard("city_view")
    cleaning_tags = [tag for tag in ("cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included") if tag in tagset]
    if cleaning_tags:
        tagset.difference_update({"cleaning_3x", "cleaning_2x", "cleaning_1x", "cleaning_included"}); tagset.add(cleaning_tags[0])
    return [tag for tag in _PRIORITY if tag in tagset]


def _phrase(tag: str, seed: str) -> str:
    choices = PHRASES[tag]
    digest = hashlib.sha256(f"{seed}|{tag}".encode("utf-8")).hexdigest()
    return choices[int(digest[:12], 16) % len(choices)]


def _select_tags(tags: list[str], limit: int) -> list[str]:
    if limit <= 0 or not tags or not any(tag not in COMMON_AMENITY_TAGS for tag in tags):
        return []
    selected, used = [], set()
    for tag in tags:
        category = TAG_CATEGORY.get(tag, tag)
        if category in used:
            continue
        selected.append(tag); used.add(category)
        if len(selected) >= limit:
            break
    return selected


def generate_adviser_lines(facts: dict[str, Any] | None, *, seed: str = "", max_points: int = 2, allow_fallback: bool = False) -> list[str]:
    try:
        limit = max(0, min(int(max_points), 2))
    except (TypeError, ValueError):
        limit = 2
    return [] if limit == 0 else [_phrase(tag, seed) for tag in _select_tags(adviser_tags_from_facts(facts), limit)]


def generate_adviser_text(facts: dict[str, Any] | None, *, seed: str = "", max_points: int = 2, allow_fallback: bool = False) -> str:
    return "\n".join(generate_adviser_lines(facts, seed=seed, max_points=max_points, allow_fallback=allow_fallback))


__all__ = ["PHRASES", "TAG_CATEGORY", "VALID_TAGS", "COMMON_AMENITY_TAGS", "adviser_tags_from_facts", "generate_adviser_lines", "generate_adviser_text"]
