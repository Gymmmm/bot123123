"""Evidence-only extraction for the public ``侨联说`` copy.

This stage is additive.  It never changes authoritative listing facts, price,
location, identity, property type or publication eligibility.  It only stores
explicit source-backed signals that the public copy layer may use later.
"""
from __future__ import annotations

import re
from typing import Any


_NEGATIVE_PREFIX = re.compile(r"(?:无|没有|没带|不带|不含|不包|不包含|不能|不可|禁止)\s*$", re.I)
_CN_COUNT = {"一": 1, "二": 2, "两": 2, "三": 3, "1": 1, "2": 2, "3": 3}


def _clean(text: object) -> str:
    return str(text or "").replace("\u00a0", " ").replace("\ufeff", " ")


def _feature_present(text: str, aliases: tuple[str, ...]) -> bool:
    lowered = text.lower()
    for alias in aliases:
        needle = alias.lower()
        start = 0
        while True:
            pos = lowered.find(needle, start)
            if pos < 0:
                break
            prefix = text[max(0, pos - 5):pos]
            if not _NEGATIVE_PREFIX.search(prefix):
                return True
            start = pos + max(1, len(needle))
    return False


def _pet_allowed(text: str) -> bool:
    """Return true only when a clause explicitly allows pets.

    Pet wording needs clause-aware handling because ``不可养宠物`` contains the
    positive substring ``可养宠物``.  A generic prefix check therefore cannot
    safely distinguish it from an affirmative statement.
    """
    for clause in re.split(r"[\n，,；;。.!！？]+", text):
        if not clause or not re.search(r"宠物", clause, re.I):
            continue
        negative = (
            r"(?:不允许|不可以|不可|不能|禁止|不准)\s*(?:养|带)?\s*宠物",
            r"宠物[^\n，,；;。.!！？]{0,8}(?:不允许|不可以|不可|不能|禁止|不准)",
        )
        if any(re.search(pattern, clause, re.I) for pattern in negative):
            continue
        positive = (
            r"(?:允许|可以|可)\s*(?:养|带)?\s*宠物",
            r"宠物友好",
        )
        if any(re.search(pattern, clause, re.I) for pattern in positive):
            return True
    return False


def _weekly_cleaning(text: str) -> int | None:
    patterns = (
        # 每周两次保洁 / 一周2次打扫
        r"(?:每周|一周)\s*([一二两三123])\s*(?:次|回|趟)?\s*(?:保洁|清洁|打扫)",
        # 每周保洁两次 / 每周保洁2次 / 一周安排保洁两次
        r"(?:每周|一周)[^\n，,；;]{0,8}(?:保洁|清洁|打扫)\s*([一二两三123])\s*(?:次|回|趟)",
        # 保洁每周两次 / 清洁一周2次
        r"(?:保洁|清洁|打扫)[^\n，,；;]{0,10}(?:每周|一周)\s*([一二两三123])\s*(?:次|回|趟)?",
        # 保洁一周来两次 / 打扫每周上门3次
        r"(?:保洁|清洁|打扫)[^\n，,；;]{0,8}(?:每周|一周)[^\n，,；;]{0,6}([一二两三123])\s*(?:次|回|趟)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        raw = match.group(1)
        if raw in _CN_COUNT:
            return _CN_COUNT[raw]
    return None


def _fee_included(text: str, nouns: str) -> bool:
    """Return true only for an explicit positive included-fee statement.

    Evaluate one punctuation-delimited clause at a time so text like
    ``不包物业费，不包网络费`` can never satisfy a generic ``包`` matcher.
    """
    for clause in re.split(r"[\n，,；;。.!！？]+", text):
        if not clause or not re.search(rf"(?:{nouns})", clause, re.I):
            continue
        negative = (
            rf"(?:不|未|没|无)\s*(?:包|包含|含|免)[^\n，,；;。.!！？]{{0,8}}(?:{nouns})",
            rf"(?:{nouns})[^\n，,；;。.!！？]{{0,10}}(?:不包|不含|未含|未包含|另付|另算|另外付|额外付|单独付|自付|自理)",
        )
        if any(re.search(pattern, clause, re.I) for pattern in negative):
            continue
        positive = (
            rf"(?:租金|房租|月租)[^\n，,；;。.!！？]{{0,14}}(?:包|包含|含)[^\n，,；;。.!！？]{{0,8}}(?:{nouns})",
            rf"(?:包|包含|含|已含|免)[^\n，,；;。.!！？]{{0,6}}(?:{nouns})",
            rf"(?:{nouns})[^\n，,；;。.!！？]{{0,10}}(?:已?包含|已?含|包了|房东包|不用(?:另外|额外|单独)?(?:交|付)|免收|免费)",
        )
        if any(re.search(pattern, clause, re.I) for pattern in positive):
            return True
    return False


def extract_adviser_signals(raw_text: str, facts: dict[str, Any] | None = None) -> list[str]:
    text = _clean(raw_text)
    signals: list[str] = []

    cleaning = _weekly_cleaning(text)
    if cleaning in {1, 2, 3}:
        signals.append(f"cleaning_{cleaning}x")

    if re.search(r"(?:每周|一周)[^\n，,；;]{0,14}(?:换|更换|换洗)[^\n，,；;]{0,10}(?:床品|床单|被套|布草)", text, re.I) or re.search(
        r"(?:床品|床单|被套|布草)[^\n，,；;]{0,12}(?:每周|一周)[^\n，,；;]{0,8}(?:换|更换|换洗|处理)", text, re.I
    ):
        signals.append("linen_weekly")

    if _feature_present(text, ("灭虫", "除虫", "虫害处理", "虫控")):
        signals.append("pest_control")

    if _fee_included(text, r"物业费|物业管理费|管理费"):
        signals.append("management_included")
    if _fee_included(text, r"网络费|网费|宽带费|Wi-?Fi(?:费)?"):
        signals.append("wifi_included")
    elif re.search(r"(?:宽带|网络|Wi-?Fi)[^\n，,；;]{0,10}(?:已(?:经)?(?:安装|装好|接通)|装好了|安好了|已开通|直接能用|现成)", text, re.I) or re.search(
        r"(?:已(?:经)?(?:安装|装好|接通)|装好了|安好了|已开通)[^\n，,；;]{0,8}(?:宽带|网络|Wi-?Fi)", text, re.I
    ):
        signals.append("wifi_ready")

    feature_rules: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("owner_direct", ("房东直租", "业主直租", "房东自己放", "房东本人放", "业主本人放", "房东直接招租", "业主自己放")),
        ("never_lived", ("全新未入住", "从未入住", "从没入住", "没人住过", "未住过人", "从来没住过", "全新未住")),
        ("new_condition", ("新装修", "全新装修", "房况新", "整体较新", "状态比较新", "家具很新", "家具挺新")),
        ("furnished", ("家具齐全", "家具配齐", "全套家具", "家具都配好", "家具已配齐")),
        ("balcony", ("带阳台", "有阳台", "阳台")),
        ("river_view", ("江景", "河景", "湄公河景", "河景房", "看到江", "看到河")),
        ("city_view", ("市景", "城市景观", "城市视野", "市区景观")),
        ("private_pool", ("私人泳池", "私家泳池", "独立泳池", "私池")),
        ("pool", ("泳池", "游泳池")),
        ("gym", ("健身房", "健身中心")),
        ("pickleball", ("匹克球",)),
        ("tennis", ("网球场", "网球")),
        ("table_tennis", ("乒乓球",)),
        ("billiards", ("台球桌", "台球")),
        ("kids_area", ("儿童游乐区", "儿童活动区", "儿童区", "儿童设施")),
        ("sauna", ("桑拿房", "桑拿")),
        ("jacuzzi", ("按摩浴缸", "按摩池", "jacuzzi")),
        ("rooftop", ("屋顶花园", "屋顶平台", "天台")),
        ("coworking", ("共享办公", "coworking")),
        ("concierge", ("管家服务", "礼宾服务", "前台管家", "礼宾")),
        ("parking", ("停车位", "停车场", "车位")),
        ("large_layout", ("大户型", "大平层", "超大户型")),
    )
    for tag, aliases in feature_rules:
        if _feature_present(text, aliases):
            signals.append(tag)
    if _pet_allowed(text):
        signals.append("pet_allowed")

    facts = dict(facts or {})
    property_type = str(facts.get("property_type") or "").lower()
    if "别墅" in property_type or "villa" in property_type:
        signals.append("villa")

    floor = str(facts.get("floor") or "")
    floor_match = re.search(r"\d{1,3}", floor)
    if floor_match:
        level = int(floor_match.group(0))
        if 1 <= level <= 5:
            signals.append("low_floor")
        elif level >= 20:
            signals.append("high_floor")

    # Preserve first-seen order and keep the persisted evidence compact.
    return list(dict.fromkeys(signals))


def enrich_adviser_signals(raw_text: str, facts: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(facts or {})
    enriched["adviser_signals"] = extract_adviser_signals(raw_text, enriched)
    return enriched


__all__ = ["enrich_adviser_signals", "extract_adviser_signals"]
