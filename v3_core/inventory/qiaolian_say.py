"""Evidence-safe ``侨联说`` tagging and copy selection.

The source parser records only tags that are directly supported by source text
or already-canonical facts.  Public copy is then selected deterministically
from a controlled phrase library.  No free-form AI inference is used here.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable

from .hashing import facts_hash

MAX_NOTES = 2

# Curated from the operator-provided Qiaolian voice library.  Phrases stay
# deliberately factual: no claims about noise, hygiene, fees, booking rules,
# water quality, maintenance quality, or other conditions unless the tag itself
# proves them.
PHRASES: dict[str, tuple[str, ...]] = {
    "pest_control": (
        "怕虫的可以看过来了😂 这边连灭虫都给包了。",
        "灭虫服务有安排，不用自己另外找人。",
        "他们这边有定期灭虫，省心一点。",
        "这种细节一般没人提，但这边确实有安排灭虫。",
        "虫害处理有人管，怕虫的可以留意一下。",
        "灭虫这件小事也安排上了，算个实用点。",
    ),
    "cleaning_1x": (
        "每周有人来打扫一次。",
        "保洁一周上门一次，日常维护够用。",
        "一周一次的保洁已经安排好了。",
        "每周固定有保洁上门一次。",
        "打扫这块每周会有人来一次。",
        "保洁频率是一周一次。",
    ),
    "cleaning_2x": (
        "一周有人来打扫两次。",
        "保洁频率是一周两次，日常维护挺省心。",
        "每周固定两次会有保洁上门。",
        "一周来两次，算是比较勤快了。",
        "这边保洁频率是两次一周。",
        "每周会有两次保洁上门。",
    ),
    "cleaning_3x": (
        "一周三次保洁，频率确实比较高。",
        "这边一周来三次保洁，挺勤快的。",
        "每周三次上门，日常基本不用太操心。",
        "打扫这块安排得挺勤，一周三次。",
        "保洁安排是一周三次，比较到位。",
        "一周三次的清洁服务，这个频率不算常见。",
    ),
    "linen_weekly": (
        "每周有人上门打扫，床品也会一起处理。",
        "床品和打扫都是每周安排的。",
        "每周会有人来处理床品和卫生。",
        "一周一次上门，床品跟着一起处理。",
        "床品这块每周会处理一次。",
        "每周有人来打扫，顺带换床品。",
    ),
    "management_included": (
        "物业费已经包含在租金里。",
        "这边物业费是包含的，不用另外交。",
        "物业这块费用已经含了。",
        "物业费不用再单独付。",
        "租金里已经含物业费。",
        "物业费这块已经算进去了。",
    ),
    "wifi_included": (
        "网络费已经包含在内。",
        "这边网络费不用另外交。",
        "网费已经算在租金里。",
        "网络这块费用已经含了。",
        "租金里已经包含网络费。",
        "网络费不用再单独付。",
    ),
    "wifi_ready": (
        "宽带已经装好了，入住后直接能用。",
        "网络这块提前弄好了，不用再自己装。",
        "这套宽带已经安好了。",
        "网络已经接通，省了自己开通这一步。",
        "宽带这块已经处理好了。",
        "网络提前装好，比较省事。",
    ),
    "management_wifi": (
        "物业费和网络费都已经包含。",
        "物业和网络这两项费用都不用另外算。",
        "管理费跟网费一起含在租金里。",
        "物业费、网络费都已经算进去了。",
        "这套物业和网费都包含在内。",
        "基础的物业、网络费用都已经含了。",
    ),
    "owner_direct": (
        "这套是房东自己放出来的。",
        "这边是房东直租。",
        "房东自己在放这套，沟通比较直接。",
        "这套走的是房东直租。",
        "不是二手转租，是房东自己放的。",
        "这套是业主本人放出来的房源。",
    ),
    "new_condition": (
        "喜欢新一点的可以留意这套。",
        "这套整体状态比较新。",
        "家具和装修看起来都偏新。",
        "喜欢新装修风格的可以关注一下。",
        "这边整体偏新，喜欢新的可以约看。",
        "房况保持得比较新。",
    ),
    "never_lived": (
        "这套是全新的，还没人住过。",
        "全新房源，从没入住过。",
        "这套全新未入住，第一个住的就是你。",
        "全新交付，目前还没有入住痕迹。",
        "这边是全新未入住，喜欢新的可以留意。",
        "这套还是全新状态，之前没人住过。",
    ),
    "furnished": (
        "这套家具是齐全的。",
        "家具都配好了，基本大件不用再添。",
        "这边家具已经配齐。",
        "家具这块都已经安排好了。",
        "主要生活家具都有。",
        "这套自带家具，配置比较齐全。",
    ),
    "balcony": (
        "这套带阳台，日常晾晒比较方便。",
        "有个阳台，平时可以出去透透气。",
        "这边有阳台，属于实用配置。",
        "带个阳台，放点绿植也可以。",
        "这套有个小阳台。",
        "带阳台，喜欢有室外空间的可以留意。",
    ),
    "city_view": (
        "这套窗外是城市景观。",
        "这边视野朝市区方向。",
        "窗外能看到城市楼群。",
        "这套是市景视野。",
        "站在窗边能看到城市一带。",
        "这边标注的是城市景观。",
    ),
    "river_view": (
        "这套能看到江景。",
        "窗外视野朝江那一面。",
        "这边标注的是江景房。",
        "从房间能看到江面。",
        "这套窗外有一段江景。",
        "视野对着江那边，喜欢江景的可以留意。",
    ),
    "pool": (
        "楼里有泳池，属于这类公寓的常见配置。",
        "这边有泳池，日常可以用。",
        "小区自带泳池。",
        "楼下泳池是有的。",
        "泳池这块属于正常配置。",
        "有个泳池在楼里，想用就比较方便。",
    ),
    "gym": (
        "楼里有健身房，属于常见配置。",
        "这边有健身房，日常可以用。",
        "小区自带健身房。",
        "楼下健身房是有的。",
        "健身房这块属于正常配置。",
        "有个健身房在楼里，平时运动方便。",
    ),
    "pool_gym": (
        "泳池健身房这些都有，属于正常配置。",
        "楼里泳池、健身房都配了。",
        "小区自带泳池和健身房。",
        "这边泳池健身房都齐全。",
        "有泳池也有健身房，日常够用。",
        "泳池健身房都在楼里，不用另外跑。",
    ),
    "pickleball": (
        "这边居然还有匹克球😂 好奇的可以试试。",
        "楼下有匹克球场，这个配置不算常见。",
        "小区里有匹克球场，挺少见的。",
        "楼下能打匹克球，算个有意思的配置。",
        "这套所在小区带匹克球场。",
        "匹克球场就在楼里，喜欢运动的可以留意。",
    ),
    "tennis": (
        "小区里有网球场。",
        "这边配了网球场。",
        "楼下能打网球。",
        "这套所在小区带网球场。",
        "网球场就在小区里。",
        "喜欢打网球的可以留意这个配置。",
    ),
    "table_tennis": (
        "楼里有乒乓球台。",
        "小区自带乒乓球设施。",
        "这边配了乒乓球台。",
        "楼下能打乒乓球。",
        "乒乓球台就在楼里。",
        "有乒乓球台，平时能打两局。",
    ),
    "billiards": (
        "楼里有台球桌。",
        "小区自带台球设施。",
        "这边配了台球桌。",
        "楼下能打台球。",
        "台球桌就在楼里。",
        "有台球桌，平时想玩比较方便。",
    ),
    "kids_area": (
        "楼下有儿童活动区。",
        "小区自带儿童游乐设施。",
        "有儿童区，带小孩的可以留意。",
        "这套所在小区带儿童活动区。",
        "楼下有小朋友玩的地方。",
        "带娃的话，这边有儿童游乐区。",
    ),
    "sauna": (
        "楼里有桑拿房。",
        "小区自带桑拿设施。",
        "这边配了桑拿房。",
        "这套所在小区带桑拿房。",
        "桑拿房就在楼里。",
        "有桑拿房，需要的时候可以用。",
    ),
    "jacuzzi": (
        "楼里有按摩池。",
        "小区自带按摩浴池。",
        "这边配了按摩浴缸。",
        "这套所在小区带按摩池。",
        "按摩池属于这边的配套之一。",
        "有按摩浴池，喜欢这类配套的可以留意。",
    ),
    "rooftop": (
        "楼上有公共天台空间。",
        "这边有天台区域，可以上去走走。",
        "楼顶有公共空间。",
        "这套所在楼带天台。",
        "天台属于这栋楼的公共配套。",
        "有屋顶公共区域，喜欢户外空间的可以留意。",
    ),
    "coworking": (
        "楼下有共享办公区，平时办公方便一点。",
        "小区自带共享办公空间。",
        "这边配了 coworking 区。",
        "有共享办公区，不一定要一直窝在房间。",
        "这套所在小区带共享办公区。",
        "楼里有工作空间，需要的时候可以用。",
    ),
    "concierge": (
        "楼下有前台服务。",
        "这边配了前台接待。",
        "大堂有工作人员，日常有事比较方便。",
        "这套所在小区带礼宾/前台服务。",
        "楼下有服务台。",
        "前台服务是有的。",
    ),
    "parking": (
        "小区里有停车位。",
        "这边配了停车场。",
        "楼下可以停车。",
        "这套所在小区带停车位。",
        "停车场是有的，具体费用以实际确认为准。",
        "有停车位，开车的可以留意。",
    ),
    "private_pool": (
        "这套自带私人泳池。",
        "有独立泳池，不跟其他住户共用。",
        "这边是私人泳池，不是公共泳池。",
        "泳池是这套房自己独用的。",
        "这套带私家泳池，属于比较少见的配置。",
        "私人泳池就在房子自己的空间里。",
    ),
    "villa": (
        "这套是别墅类型，空间相对独立。",
        "这边房型是别墅。",
        "不是公寓，是别墅类型。",
        "这套属于独立住宅类型。",
        "别墅房型，喜欢独立空间的可以留意。",
        "这边是别墅结构。",
    ),
    "large_layout": (
        "这套户型比较大，空间更宽敞。",
        "面积算比较大的，喜欢大空间的可以留意。",
        "这边属于偏大户型。",
        "这套空间比较充裕。",
        "户型面积不小，家庭住会更从容一点。",
        "这套算大户型，空间感会更明显。",
    ),
    "high_floor": (
        "这套楼层比较高。",
        "这边是高楼层，喜欢高层的可以留意。",
        "楼层位置靠上。",
        "这套属于高楼层户型。",
        "楼层偏高，具体视野建议现场看。",
        "这套在比较高的楼层。",
    ),
    "low_floor": (
        "这套楼层比较低。",
        "这边是低楼层，喜欢低层的可以留意。",
        "楼层位置靠下。",
        "这套属于低楼层户型。",
        "楼层偏低，具体采光和环境建议现场看。",
        "这套在比较低的楼层。",
    ),
    "pet_allowed": (
        "这套明确允许养宠物。",
        "养宠物是可以的，有猫狗的可以留意。",
        "这边允许带宠物入住。",
        "宠物这块是允许的。",
        "这套属于宠物友好房源。",
        "允许养宠物，具体约定签约前再确认一下。",
    ),
    "fallback": (
        "这套我就不硬找东西夸了😂 大家直接看照片，装修喜欢再约。",
        "没什么特别要硬夸的，喜欢风格的可以约看。",
        "这套整体比较常规，照片和现场感受更重要。",
        "中规中矩的一套，合不合适还是看实际需求。",
        "没有特别要强调的点，直接看实拍更清楚。",
        "这套不硬吹，有兴趣就约现场看看。",
    ),
}

TAG_PRIORITY: tuple[str, ...] = (
    "never_lived",
    "management_wifi",
    "cleaning_3x",
    "cleaning_2x",
    "linen_weekly",
    "pest_control",
    "owner_direct",
    "private_pool",
    "river_view",
    "pet_allowed",
    "pickleball",
    "coworking",
    "large_layout",
    "new_condition",
    "furnished",
    "balcony",
    "city_view",
    "cleaning_1x",
    "management_included",
    "wifi_included",
    "wifi_ready",
    "kids_area",
    "concierge",
    "rooftop",
    "parking",
    "tennis",
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


def _text(value: object) -> str:
    return str(value or "").strip()


def _list(value: object) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    if value in (None, ""):
        return []
    return [_text(value)]


def _contains(text: str, *terms: str) -> bool:
    lowered = text.lower()
    return any(term.lower() in lowered for term in terms)


def _included(value: object) -> bool:
    normalized = _text(value).lower().replace(" ", "")
    return normalized in {"包含", "已包含", "含", "包", "included", "免费"}


def _numeric(value: object) -> float | None:
    match = re.search(r"\d+(?:\.\d+)?", _text(value).replace(",", ""))
    return float(match.group(0)) if match else None


def _manual_notes(raw_text: str) -> tuple[list[str], bool]:
    text = _text(raw_text)
    if not text:
        return [], False
    # Operator edits append a reset marker.  Only the segment after the latest
    # reset is authoritative, so replacing one manual line cannot leak an old
    # second line back into the public copy.
    if "侨联说重置" in text:
        text = text.rsplit("侨联说重置", 1)[-1]
    if re.search(r"(?:^|\n)\s*侨联说关闭\s*(?:$|\n)", text):
        return [], True
    notes: list[str] = []
    for match in re.finditer(r"(?:^|\n)\s*侨联说\s*[:：]\s*([^\n]+)", text):
        value = re.sub(r"\s+", " ", match.group(1)).strip(" ｜|；;")
        if value and value not in notes:
            notes.append(value[:120])
    return notes[-MAX_NOTES:], False


def _large_layout(facts: dict[str, Any]) -> bool:
    size = _numeric(facts.get("size_sqm"))
    if size is None:
        return False
    layout = _text(facts.get("layout"))
    bedrooms_raw = facts.get("bedrooms")
    bedrooms = int(bedrooms_raw) if str(bedrooms_raw or "").isdigit() else None
    if layout.lower() == "studio" or "单间" in layout:
        return size >= 55
    if bedrooms is None:
        match = re.search(r"(\d+)\s*房", layout)
        bedrooms = int(match.group(1)) if match else None
    thresholds = {1: 70, 2: 100, 3: 140}
    if bedrooms in thresholds:
        return size >= thresholds[bedrooms]
    if bedrooms is not None and bedrooms >= 4:
        return size >= 180
    return False


def extract_qiaolian_tags(raw_text: str, facts: dict[str, Any] | None = None) -> list[str]:
    """Return evidence-backed tags in product priority order."""
    facts = dict(facts or {})
    source_parts = [
        _text(raw_text),
        " ".join(_list(facts.get("highlights"))),
        " ".join(_list(facts.get("amenities"))),
        " ".join(_list(facts.get("included"))),
    ]
    text = "\n".join(part for part in source_parts if part)
    compact = re.sub(r"\s+", "", text.lower())
    tags: set[str] = set()

    if re.search(r"(?:每周|一周)(?:会|有|固定|安排)?[^\n，。]{0,12}(?:3|三)(?:次|趟|回)[^\n，。]{0,8}(?:保洁|清洁|打扫)|(?:保洁|清洁|打扫)[^\n，。]{0,12}(?:每周|一周)[^\n，。]{0,6}(?:3|三)(?:次|趟|回)", text, re.I):
        tags.add("cleaning_3x")
    elif re.search(r"(?:每周|一周)(?:会|有|固定|安排)?[^\n，。]{0,12}(?:2|两|二)(?:次|趟|回)[^\n，。]{0,8}(?:保洁|清洁|打扫)|(?:保洁|清洁|打扫)[^\n，。]{0,12}(?:每周|一周)[^\n，。]{0,6}(?:2|两|二)(?:次|趟|回)", text, re.I):
        tags.add("cleaning_2x")
    elif re.search(r"(?:每周|一周)(?:会|有|固定|安排)?[^\n，。]{0,12}(?:1|一)(?:次|趟|回)[^\n，。]{0,8}(?:保洁|清洁|打扫)|(?:保洁|清洁|打扫)[^\n，。]{0,12}(?:每周|一周)[^\n，。]{0,6}(?:1|一)(?:次|趟|回)", text, re.I):
        tags.add("cleaning_1x")

    if re.search(r"(?:每周|一周)[^\n，。]{0,20}(?:换|更换|换洗)[^\n，。]{0,8}(?:床单|床品|布草|被套)|(?:床单|床品|布草|被套)[^\n，。]{0,16}(?:每周|一周)[^\n，。]{0,8}(?:换|更换|换洗)", text, re.I):
        tags.add("linen_weekly")
    if _contains(text, "灭虫", "除虫", "虫控", "虫害处理", "pest control") and not _contains(text, "不包灭虫", "不含灭虫", "没有灭虫"):
        tags.add("pest_control")

    included_values = {item.lower() for item in _list(facts.get("included"))}
    management_included = _included(facts.get("management_fee")) or "物业费" in included_values or bool(re.search(r"(?:物业费|管理费)[^\n，。]{0,10}(?:已?含|包含|包了|免费)", text))
    internet_included = _included(facts.get("internet_fee")) or bool(included_values.intersection({"wi-fi", "wifi", "网络", "网络费", "网费"})) or bool(re.search(r"(?:网络费|网费|wifi费|宽带费)[^\n，。]{0,10}(?:已?含|包含|包了|免费)", text, re.I))
    wifi_ready = bool(re.search(r"(?:宽带|网络|wi-?fi)[^\n，。]{0,10}(?:已装|装好|安好|接通|已开通|现成|直接能用)", text, re.I))
    if management_included and internet_included:
        tags.add("management_wifi")
    else:
        if management_included:
            tags.add("management_included")
        if internet_included:
            tags.add("wifi_included")
    if wifi_ready and not internet_included:
        tags.add("wifi_ready")

    if re.search(r"(?:房东|业主)(?:本人|自己)?[^\n，。]{0,8}(?:直租|放租|出租|放出来|放盘)|(?:房东直租|业主直租)", text):
        tags.add("owner_direct")
    if re.search(r"(?:全新未入住|从未入住|没住过人|没人住过|未住过|从来没住过|全新未住)", text):
        tags.add("never_lived")
    elif re.search(r"(?:新装修|装修(?:很|比较|挺)?新|家具(?:很|比较|挺|都)?新|房况(?:很|比较|挺)?新|整体(?:很|比较|挺)?新)", text):
        tags.add("new_condition")
    if re.search(r"(?:家具家电齐全|家具齐全|家具已?配齐|全配家具|家具都配好|带齐家具)", text):
        tags.add("furnished")
    if _contains(text, "阳台", "balcony") and not _contains(text, "无阳台", "没有阳台"):
        tags.add("balcony")
    if _contains(text, "江景", "河景", "湄公河景", "river view") and not _contains(text, "无江景", "无河景"):
        tags.add("river_view")
    elif _contains(text, "市景", "城市景观", "城市视野", "city view"):
        tags.add("city_view")

    private_pool = bool(re.search(r"(?:私人|私家|独立|私用|独享)[^\n，。]{0,5}(?:泳池|游泳池)|(?:泳池|游泳池)[^\n，。]{0,5}(?:私人|私家|独立|私用|独享)", text))
    pool = _contains(text, "泳池", "游泳池", "swimming pool") and not _contains(text, "无泳池", "没有泳池")
    gym = _contains(text, "健身房", "gym") and not _contains(text, "无健身房", "没有健身房")
    if private_pool:
        tags.add("private_pool")
    elif pool and gym:
        tags.add("pool_gym")
    elif pool:
        tags.add("pool")
    if gym and "pool_gym" not in tags:
        tags.add("gym")

    direct_patterns = {
        "pickleball": ("匹克球", "pickleball"),
        "tennis": ("网球场", "tennis court"),
        "table_tennis": ("乒乓球台", "乒乓球桌", "table tennis"),
        "billiards": ("台球桌", "台球室", "billiards", "pool table"),
        "kids_area": ("儿童活动区", "儿童游乐区", "儿童设施", "kids area", "playground"),
        "sauna": ("桑拿", "sauna"),
        "jacuzzi": ("按摩浴缸", "按摩池", "按摩浴池", "jacuzzi"),
        "rooftop": ("天台", "屋顶花园", "屋顶平台", "rooftop"),
        "coworking": ("共享办公", "coworking", "联合办公"),
        "concierge": ("礼宾", "前台服务", "前台接待", "管家服务", "concierge"),
    }
    for tag, words in direct_patterns.items():
        if _contains(text, *words):
            tags.add(tag)

    if _contains(text, "停车位", "停车场", "地下停车", "parking") and not _contains(text, "无停车位", "不能停车", "没有停车"):
        tags.add("parking")

    property_type = _text(facts.get("property_type"))
    if "别墅" in property_type or re.search(r"(?:独栋别墅|别墅户型|别墅类型|villa)\b?", text, re.I):
        tags.add("villa")

    if _large_layout(facts) or _contains(text, "大户型", "大平层", "超大户型"):
        tags.add("large_layout")

    floor = _numeric(facts.get("floor"))
    if floor is not None:
        if floor >= 20:
            tags.add("high_floor")
        elif floor <= 5:
            tags.add("low_floor")

    pet_negative = _contains(compact, "不可养宠物", "不能养宠物", "不允许养宠物", "禁止养宠物", "禁宠", "nopets")
    pet_positive = _contains(text, "允许养宠物", "可以养宠物", "可养宠物", "宠物友好", "pet friendly", "pets allowed")
    if pet_positive and not pet_negative:
        tags.add("pet_allowed")

    # Defensive mutual exclusion even when upstream facts are inconsistent.
    if "never_lived" in tags:
        tags.discard("new_condition")
    if "management_wifi" in tags:
        tags.discard("management_included")
        tags.discard("wifi_included")
    if "private_pool" in tags:
        tags.discard("pool")
        tags.discard("pool_gym")
    if "pool_gym" in tags:
        tags.discard("pool")
        tags.discard("gym")
    if "river_view" in tags:
        tags.discard("city_view")
    if "cleaning_3x" in tags:
        tags.discard("cleaning_2x")
        tags.discard("cleaning_1x")
    elif "cleaning_2x" in tags:
        tags.discard("cleaning_1x")

    return [tag for tag in TAG_PRIORITY if tag in tags]


def enrich_qiaolian_facts(raw_text: str, facts: dict[str, Any]) -> dict[str, Any]:
    """Add Qiaolian metadata without changing quality / publishability gates."""
    enriched = dict(facts or {})
    manual, disabled = _manual_notes(raw_text)
    tags = extract_qiaolian_tags(raw_text, enriched)
    enriched["qiaolian_tags"] = tags
    if manual:
        enriched["qiaolian_say"] = manual
    else:
        enriched.pop("qiaolian_say", None)
    if disabled:
        enriched["qiaolian_say_disabled"] = True
    else:
        enriched.pop("qiaolian_say_disabled", None)
    enriched["canonical_facts_hash"] = facts_hash(enriched)
    return enriched


def _phrase(tag: str, seed: str) -> str:
    choices = PHRASES.get(tag) or ()
    if not choices:
        return ""
    digest = hashlib.sha256(f"{seed}|{tag}".encode("utf-8")).digest()
    return choices[int.from_bytes(digest[:4], "big") % len(choices)]


def qiaolian_notes_from_facts(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    limit: int = MAX_NOTES,
    allow_fallback: bool = True,
) -> list[str]:
    """Render at most ``limit`` deterministic public notes from frozen facts."""
    facts = dict(facts or {})
    limit = max(0, min(MAX_NOTES, int(limit)))
    if not limit or facts.get("qiaolian_say_disabled"):
        return []

    manual = []
    for value in _list(facts.get("qiaolian_say")):
        value = re.sub(r"\s+", " ", value).strip()
        if value and value not in manual:
            manual.append(value[:120])
    if len(manual) >= limit:
        return manual[:limit]

    tags = [tag for tag in _list(facts.get("qiaolian_tags")) if tag in PHRASES]
    if not tags:
        # Backward compatibility for already-frozen publications: derive only
        # from facts actually present in the snapshot, never from the live DB.
        tags = extract_qiaolian_tags("", facts)

    stable_seed = _text(seed) or _text(facts.get("canonical_facts_hash")) or facts_hash(facts)
    notes = list(manual)
    for tag in TAG_PRIORITY:
        if tag not in tags:
            continue
        phrase = _phrase(tag, stable_seed)
        if phrase and phrase not in notes:
            notes.append(phrase)
        if len(notes) >= limit:
            return notes[:limit]

    if not notes and allow_fallback:
        fallback = _phrase("fallback", stable_seed)
        if fallback:
            notes.append(fallback)
    return notes[:limit]


def qiaolian_notes_text(
    facts: dict[str, Any] | None,
    *,
    seed: str = "",
    limit: int = MAX_NOTES,
    allow_fallback: bool = True,
) -> str:
    return "\n".join(
        qiaolian_notes_from_facts(
            facts,
            seed=seed,
            limit=limit,
            allow_fallback=allow_fallback,
        )
    )


__all__ = [
    "MAX_NOTES",
    "PHRASES",
    "TAG_PRIORITY",
    "enrich_qiaolian_facts",
    "extract_qiaolian_tags",
    "qiaolian_notes_from_facts",
    "qiaolian_notes_text",
]
