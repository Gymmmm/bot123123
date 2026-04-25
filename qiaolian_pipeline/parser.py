from __future__ import annotations

import re
from dataclasses import dataclass


AREA_ALIASES = {
    "bkk1": "BKK1",
    "bkk 1": "BKK1",
    "bkk2": "BKK2",
    "bkk 2": "BKK2",
    "bkk3": "BKK3",
    "bkk 3": "BKK3",
    "boeung keng kang": "BKK",
    "tonle bassac": "Tonle Bassac",
    "toul kork": "Toul Kork",
    "tuol kork": "Toul Kork",
    "russian market": "Russian Market",
    "toul tom poung": "Russian Market",
    "wat phnom": "Wat Phnom",
    "daun penh": "Daun Penh",
    "sen sok": "Sen Sok",
    "7 makara": "7 Makara",
    "mean chey": "Mean Chey",
    "chroy changvar": "Chroy Changvar",
    "olympic": "Olympic",
    "金边": "金边",
    "万景岗1": "BKK1",
    "万景岗2": "BKK2",
    "万景岗3": "BKK3",
    "独立碑": "BKK1",
    "百适河": "Tonle Bassac",
    "俄罗斯市场": "Russian Market",
    "塔仔山": "Wat Phnom",
    "堆谷": "Toul Kork",
}

AREA_WHITELIST = {
    "BKK1": ["bkk1", "分阁1", "万景岗1"],
    "BKK2": ["bkk2", "分阁2", "万景岗2"],
    "BKK3": ["bkk3", "分阁3", "万景岗3"],
    "钻石岛": ["diamond island", "koh pich"],
    "水净华": ["chroy changvar", "水静华"],
    "堆谷区": ["tuol kork", "tk"],
    "桑园区": ["chamkarmon", "chamkar mon"],
    "隆边区": ["daun penh", "皇宫附近"],
    "玛卡拉区": ["7 makara", "prampi makara"],
    "洪森大道": ["hun sen blvd", "60米路"],
    "俄罗斯市场": ["ttp", "tuol tom poung"],
    "百色河": ["tonle bassac"],
}

PROPERTY_WHITELIST = [
    "太子现代广场", "太子中央广场", "太子幸福广场", "太子国际广场",
    "Urban Village", "首都国金", "The Peak", "The Bridge",
    "The Gateway", "雅居乐", "Agile", "摩根", "Morgan",
    "皇家一号", "Royal One", "财富大厦", "Wealth Mansion",
    "毕加索", "Picasso", "玫瑰滨江", "Rose Apple",
]

RENTAL_KEYWORDS = [
    "租",
    "出租",
    "rent",
    "lease",
    "for rent",
    "monthly",
    "month",
    "/month",
    "per month",
    "/mo",
    "/月",
    "每月",
    "月租",
    "租房",
]

PROPERTY_KEYWORDS = [
    ("serviced apartment", "服务式公寓"),
    ("service apartment", "服务式公寓"),
    ("apartment", "公寓"),
    ("condo", "公寓"),
    ("studio", "公寓"),
    ("villa", "别墅"),
    ("townhouse", "排屋"),
    ("shophouse", "商铺"),
    ("shop house", "商铺"),
    ("office", "办公室"),
    ("building", "整栋"),
    ("house", "住宅"),
    ("公寓", "公寓"),
    ("别墅", "别墅"),
    ("排屋", "排屋"),
    ("写字楼", "办公室"),
    ("办公室", "办公室"),
    ("整栋", "整栋"),
]

HIGHLIGHT_PATTERNS = [
    ("家具家电齐全", "家具家电齐全"),
    ("fully furnished", "家具家电齐全"),
    ("furnished", "家具家电齐全"),
    ("pool", "带泳池"),
    ("swimming", "带泳池"),
    ("gym", "带健身房"),
    ("parking", "可停车"),
    ("security", "安保"),
    ("24/7", "24小时安保"),
    ("balcony", "带阳台"),
    ("pet friendly", "可养宠"),
    ("cleaning", "含保洁"),
    ("internet", "含网络"),
    ("wifi", "含网络"),
    ("elevator", "带电梯"),
    ("拎包入住", "拎包入住"),
    ("实拍", "实拍房源"),
]

DRAWBACK_PATTERNS = [
    ("no pet", "不接受宠物"),
    ("pets not allowed", "不接受宠物"),
    ("no elevator", "无电梯"),
    ("walk up", "需步梯"),
]

PRICE_PATTERNS = [
    re.compile(r"(?:\$|usd|us\$)\s*([0-9][0-9,]*(?:\.\d+)?\s*k?)", re.I),
    re.compile(r"(?:租金|月租)[：: ]*\$?\s*([0-9][0-9,]*(?:\.\d+)?\s*k?)", re.I),
    re.compile(r"([0-9]+(?:\.\d+)?\s*k)\s*(?:/month|per month|/月|每月|usd|美金|刀)", re.I),
    re.compile(r"([0-9][0-9,]*(?:\.\d+)?)\s*(?:usd|美金|刀)\s*(?:/month|per month|/月|每月)?", re.I),
    re.compile(r"asking price[：: ]*\$?\s*([0-9][0-9,]*(?:\.\d+)?)", re.I),
]

SIZE_PATTERNS = [
    re.compile(r"([0-9]{2,4}(?:\.\d+)?)\s*(?:m2|㎡|sqm|sq\.?m)", re.I),
    re.compile(r"(?:面积|size)[：: ]*([0-9]{2,4}(?:\.\d+)?)", re.I),
]

LAYOUT_PATTERNS = [
    re.compile(r"([1-9])\s*(?:bed|bedroom|房)(?:\s*[+/]\s*([1-9])\s*(?:bath|卫))?", re.I),
    re.compile(r"([一二三四五])房([一二三四五])卫"),
    re.compile(r"([一二三四五1-9])房"),
    re.compile(r"studio", re.I),
]

FLOOR_PATTERNS = [
    re.compile(r"(?:floor|楼层)[：: ]*([0-9]{1,2}(?:st|nd|rd|th)?(?:\s*floor)?)", re.I),
    re.compile(r"([0-9]{1,2})楼"),
]

DEPOSIT_PATTERNS = [
    re.compile(r"(押[一二三四五六七八九十0-9].{0,8}?付[一二三四五六七八九十0-9])"),
    re.compile(r"(deposit[^,\n]{0,30})", re.I),
]

CONTRACT_PATTERNS = [
    re.compile(
        r"(?:合同|租期|lease|contract|term|min(?:imum)?\s*lease)\s*(?:期限|期|:|：)?\s*([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月|month|months|year|years|yr|yrs)",
        re.I,
    ),
    re.compile(r"([0-9]{1,2})\s*(year|years|yr|yrs)\s*(?:lease|contract|term)?", re.I),
    re.compile(r"([0-9]{1,2})\s*(month|months|mo)\s*(?:lease|contract|term)?", re.I),
    re.compile(r"([一二三四五六七八九十两]{1,3})\s*年\s*(?:起租|合同|租期)?"),
]

AVAILABLE_PATTERNS = [
    re.compile(r"(available\s+(?:now|immediately|today))", re.I),
    re.compile(r"(随时入住)"),
    re.compile(r"(立即入住)"),
]

AREA_EXPLICIT_PATTERNS = [
    re.compile(r"(?:位置|地址|区域|地段)\s*[:：]\s*([^\n，,；;|｜]{2,40})", re.I),
    re.compile(r"(?:located in|location|area)\s*[:：]?\s*([^\n，,；;|｜]{2,40})", re.I),
]

AREA_NOISE_PATTERN = re.compile(
    r"(押[一二三四五六七八九十两0-9]|付[一二三四五六七八九十两0-9]|租金|月租|\$|/月|每月|"
    r"家具|家电|拎包|泳池|健身|停车|studio|bed|bath|公寓|别墅|排屋)",
    re.I,
)

COST_LINE_PATTERNS = [
    re.compile(r".*(electric|water|管理费|物业费|网费|停车费|电费|水费|internet|wifi).*", re.I),
    re.compile(r".*((?:水|电)\s*\$?\s*[0-9]+(?:\.[0-9]+)?).*", re.I),
]

UTILITY_RATE_PATTERNS = {
    "水": [
        re.compile(r"(?:水(?:费)?|water)\s*[:：]?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
    "电": [
        re.compile(r"(?:电(?:费)?|electric(?:ity)?)\s*[:：]?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", re.I),
    ],
}

NON_RENTAL_KEYWORD_PATTERN = re.compile(
    r"(sale price|business for sale|shop for sale|transfer|shop transfer|take over|urgent sale|"
    r"转让|顶让|出售|急售|诚售|土地卖|店面转|生意好|回国转|"
    r"លក់|ផ្ទេរ|លក់បន្ទាន់|លក់ហាង|ផ្ទេរហាង)",
    re.I,
)

USD_AMOUNT_PATTERN = re.compile(r"\$\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)")
NON_RENTAL_PRICE_LIMIT = 15000
CN_DIGIT_MAP = {
    "零": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}

DISPLAY_NOISE_TOKENS = ("啊雷莎", "阿雷莎", "🇨🇳", "🌵")
GENERIC_PROJECT_VALUES = {
    "",
    "公寓",
    "别墅",
    "排屋",
    "住宅",
    "社区",
    "小区",
    "金边",
}


def _clean_text(raw_text: str) -> str:
    text = raw_text.replace("\u00a0", " ").replace("\ufeff", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_project_candidate(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for token in DISPLAY_NOISE_TOKENS:
        text = text.replace(token, " ")
    # 去掉源帖常见的编号前缀，但保留像「60米路」这类真实地名。
    text = re.sub(r"^\s*\d{3,4}(?!米)", "", text)
    text = re.sub(r"[#⭐️✨🏠🏡🏢🔥📍💰✅📝☎️]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -｜|·•,，")
    if text in GENERIC_PROJECT_VALUES:
        return ""
    return text


def _lines(text: str) -> list[str]:
    return [line.strip(" -•·\t") for line in text.splitlines() if line.strip()]


def _first_match(patterns: list[re.Pattern[str]], text: str) -> str:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            if match.lastindex:
                return next((g for g in match.groups() if g), match.group(0)).strip()
            return match.group(0).strip()
    return ""


def _normalize_price(raw_price: str) -> int:
    cleaned = str(raw_price or "")
    cleaned = cleaned.replace(",", "").replace("+", " ").strip().lower()
    if not cleaned:
        return 0
    cleaned = re.sub(
        r"(usd|us\$|美金|刀|/month|per month|/月|每月|月租|租金|rent|monthly)",
        " ",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    k_match = re.search(r"([0-9]+(?:\.\d+)?)\s*k\b", cleaned, flags=re.I)
    if k_match:
        try:
            value = int(float(k_match.group(1)) * 1000)
            return value if 50 <= value <= 200000 else 0
        except ValueError:
            return 0
    multiplier = 1000 if cleaned.endswith("k") else 1
    cleaned = cleaned.rstrip("k").strip()
    num_match = re.search(r"([0-9]+(?:\.\d+)?)", cleaned)
    if num_match:
        cleaned = num_match.group(1)
    try:
        value = int(float(cleaned) * multiplier)
        return value if 50 <= value <= 200000 else 0
    except ValueError:
        return 0


def _cn_token_to_int(token: str) -> int | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        try:
            return int(float(raw))
        except ValueError:
            return None
    if not all(ch in CN_DIGIT_MAP for ch in raw):
        return None
    if len(raw) == 1:
        return CN_DIGIT_MAP.get(raw)
    if raw == "十":
        return 10
    if len(raw) == 2 and raw[0] == "十":
        return 10 + CN_DIGIT_MAP.get(raw[1], 0)
    if len(raw) == 2 and raw[1] == "十":
        return CN_DIGIT_MAP.get(raw[0], 0) * 10
    if len(raw) == 3 and raw[1] == "十":
        return CN_DIGIT_MAP.get(raw[0], 0) * 10 + CN_DIGIT_MAP.get(raw[2], 0)
    return None


def _normalize_num_token(token: str) -> str:
    raw = str(token or "").strip()
    if not raw:
        return ""
    num = _cn_token_to_int(raw)
    if num is not None:
        return str(num)
    return raw


def _normalize_contract_unit(raw_unit: str) -> str:
    unit = str(raw_unit or "").strip().lower()
    if not unit:
        return ""
    if unit in {"year", "years", "yr", "yrs", "年"}:
        return "年"
    if unit in {"month", "months", "mo", "个月", "月"}:
        return "个月"
    return unit


def _extract_payment_terms(text: str, deposit_hint: str = "") -> str:
    source = str(text or "")
    m_cn = re.search(
        r"押\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?\s*[，,/、\s]*付\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?",
        source,
        flags=re.I,
    )
    if m_cn:
        dep = _normalize_num_token(m_cn.group(1))
        pay = _normalize_num_token(m_cn.group(2))
        if dep and pay:
            return f"押{dep}付{pay}"

    dep_en = re.search(
        r"(?:deposit|押金)\s*[:：]?\s*([0-9]+(?:\.\d+)?)\s*(?:month|months|mo|个月|月)",
        source,
        flags=re.I,
    )
    pay_en = re.search(
        r"(?:advance|rent(?:al)?\s*(?:in\s*)?advance|预付)\s*[:：]?\s*([0-9]+(?:\.\d+)?)\s*(?:month|months|mo|个月|月)",
        source,
        flags=re.I,
    )
    dep_n = _normalize_num_token(dep_en.group(1)) if dep_en else ""
    pay_n = _normalize_num_token(pay_en.group(1)) if pay_en else ""
    if dep_n and pay_n:
        return f"押{dep_n}付{pay_n}"
    if dep_n:
        return f"押{dep_n}月"

    hint = str(deposit_hint or "").strip()
    if not hint:
        return ""
    if re.search(r"押\s*[一二三四五六七八九十两0-9]", hint):
        m = re.search(
            r"押\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?(?:\s*[，,/、\s]*付\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?)?",
            hint,
        )
        if m:
            dep = _normalize_num_token(m.group(1))
            pay = _normalize_num_token(m.group(2) or "")
            if dep and pay:
                return f"押{dep}付{pay}"
            if dep:
                return f"押{dep}月"
    hint_low = hint.lower()
    if "deposit" in hint_low:
        m = re.search(r"([0-9]+(?:\.\d+)?)", hint_low)
        if m:
            dep = _normalize_num_token(m.group(1))
            if dep:
                return f"押{dep}月"
    return hint


def _extract_contract_term(text: str) -> str:
    source = str(text or "")
    for pattern in CONTRACT_PATTERNS:
        m = pattern.search(source)
        if not m:
            continue
        groups = m.groups()
        num_raw = groups[0] if groups else ""
        unit_raw = groups[1] if len(groups) > 1 else "年"
        num = _normalize_num_token(num_raw)
        unit = _normalize_contract_unit(unit_raw)
        if num and unit:
            return f"{num}{unit}"
    return ""


def _normalize_size(raw_size: str) -> str:
    token = str(raw_size or "").strip().lower().replace(",", "")
    if not token:
        return ""
    m = re.search(r"([0-9]{2,4}(?:\.\d+)?)", token)
    if not m:
        return ""
    try:
        size_value = float(m.group(1))
    except ValueError:
        return ""
    if size_value < 10 or size_value > 2000:
        return ""
    if abs(size_value - round(size_value)) < 0.05:
        return f"{int(round(size_value))}㎡"
    return f"{size_value:.1f}㎡"


def _normalize_area_candidate(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for token in DISPLAY_NOISE_TOKENS:
        text = text.replace(token, " ")
    text = re.sub(r"[\[\]【】()（）]", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -｜|·•,，")
    if not text:
        return ""
    if len(text) > 18:
        return ""
    if AREA_NOISE_PATTERN.search(text):
        return ""
    lower = text.lower()
    # 命中同义词时回写标准区域名
    for area, aliases in AREA_WHITELIST.items():
        if area.lower() in lower or area in text:
            return area
        for alias in aliases:
            if alias.lower() in lower or alias in text:
                return area
    for alias, area in AREA_ALIASES.items():
        if alias in lower or alias in text:
            return area
    return text


def _extract_explicit_area(text: str) -> str:
    source = str(text or "")
    for pattern in AREA_EXPLICIT_PATTERNS:
        m = pattern.search(source)
        if not m:
            continue
        cand = _normalize_area_candidate(m.group(1))
        if cand:
            return cand
    return ""


def _detect_area(text: str) -> str:
    explicit = _extract_explicit_area(text)
    if explicit:
        return explicit
    lower = text.lower()
    for area, aliases in AREA_WHITELIST.items():
        if area.lower() in lower or area in text:
            return area
        for alias in aliases:
            if alias.lower() in lower or alias in text:
                return area
    for alias, area in AREA_ALIASES.items():
        if alias in lower or alias in text:
            return area
    return "金边"


def _detect_property_type(text: str) -> str:
    lower = text.lower()
    if any(token in text for token in ("独栋", "双拼", "泳池独栋")):
        return "别墅"
    if "排屋" in text:
        return "排屋"
    for keyword, label in PROPERTY_KEYWORDS:
        if keyword in lower or keyword in text:
            return label
    return "公寓"


def _detect_layout(text: str) -> str:
    for pattern in LAYOUT_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        if match.re.pattern.lower() == "studio":
            return "Studio"
        groups = [g for g in match.groups() if g]
        if not groups:
            return match.group(0).strip()
        if any(g in "一二三四五" for g in groups):
            return f"{groups[0]}房{groups[1]}卫" if len(groups) > 1 else f"{groups[0]}房"
        if len(groups) > 1:
            return f"{groups[0]}房{groups[1]}卫"
        return f"{groups[0]}房"
    return ""


def _detect_project(lines: list[str], area: str, property_type: str) -> str:
    for line in lines[:5]:
        stripped = re.sub(r"^[#🏠🏢⭐️✨📍💲$🔹\[\]【】]+", "", line).strip()
        if not stripped:
            continue
        lower = stripped.lower()
        cleaned_candidate = re.sub(
            r"(for rent|for sale|出租|招租|出售)$", "", stripped, flags=re.I
        ).strip(" -｜|")
        cleaned_candidate = _clean_project_candidate(cleaned_candidate)
        if not cleaned_candidate:
            continue
        lower = cleaned_candidate.lower()
        if cleaned_candidate and any(token in lower for token in ("apartment", "condo", "villa", "公寓", "别墅", "building")):
            return cleaned_candidate
        if any(
            token in lower
            for token in (
                "asking price",
                "price",
                "租金",
                "面积",
                "deposit",
                "size",
                "building size",
            )
        ):
            continue
        if len(stripped) > 45:
            continue
        if sum(ch.isdigit() for ch in stripped) > 8:
            continue
        if re.fullmatch(r"[0-9\sA-Za-z/+-]*(bed|bedroom|bath|bathroom)[0-9\sA-Za-z/+-]*", lower):
            continue
        return cleaned_candidate or stripped
    fallback = area if area != "金边" else property_type
    return _clean_project_candidate(fallback) or fallback


def _extract_utility_rate_notes(text: str) -> list[str]:
    notes: list[str] = []
    source = str(text or "")
    for label, patterns in UTILITY_RATE_PATTERNS.items():
        for pattern in patterns:
            m = pattern.search(source)
            if not m:
                continue
            value = str(m.group(1) or "").strip()
            if not value:
                continue
            note = f"{label}${value}"
            if note not in notes:
                notes.append(note)
            break
    return notes


def _extract_utility_rates(text: str) -> tuple[str, str]:
    source = str(text or "")
    water_rate = ""
    electric_rate = ""
    for pattern in UTILITY_RATE_PATTERNS["水"]:
        m = pattern.search(source)
        if m and str(m.group(1) or "").strip():
            water_rate = str(m.group(1)).strip()
            break
    for pattern in UTILITY_RATE_PATTERNS["电"]:
        m = pattern.search(source)
        if m and str(m.group(1) or "").strip():
            electric_rate = str(m.group(1)).strip()
            break
    return water_rate, electric_rate


def _extract_cost_notes(lines: list[str]) -> str:
    source_text = "\n".join(lines)
    hits = [line for line in lines if any(p.search(line) for p in COST_LINE_PATTERNS)]
    utility_notes = _extract_utility_rate_notes(source_text)
    if utility_notes:
        utility_line = "｜".join(utility_notes)
        if utility_line not in hits:
            hits.insert(0, utility_line)
    normalized: list[str] = []
    for line in hits:
        cleaned = str(line or "").strip(" ｜;；")
        if cleaned and cleaned not in normalized:
            normalized.append(cleaned)
    return "；".join(normalized[:3])


def _ensure_default_cost_note(cost_notes: str, water_rate: str, electric_rate: str) -> str:
    note = str(cost_notes or "").strip()
    if note:
        return note
    parts: list[str] = []
    if water_rate:
        parts.append(f"水${water_rate}")
    if electric_rate:
        parts.append(f"电${electric_rate}")
    if parts:
        return "｜".join(parts)
    return "水电按表（发布前复核）"


def _extract_tags(text: str, patterns: list[tuple[str, str]]) -> list[str]:
    lower = text.lower()
    found: list[str] = []
    for needle, label in patterns:
        if needle in lower or needle in text:
            found.append(label)
    seen: list[str] = []
    for item in found:
        if item not in seen:
            seen.append(item)
    return seen[:4]


def _has_rental_intent(text: str) -> bool:
    source = str(text or "")
    lower = source.lower()
    if any(keyword in lower or keyword in source for keyword in RENTAL_KEYWORDS):
        return True
    if re.search(
        r"(?:\$|usd|美金|刀)\s*[0-9][0-9,]*(?:\.\d+)?\s*(?:/month|per month|/月|每月)",
        source,
        flags=re.I,
    ):
        return True
    return False


def non_rental_source_reasons(text: str) -> list[str]:
    """Return hard-review reasons for sale/transfer/business posts."""
    if not text:
        return ["empty_text"]
    reasons: list[str] = []
    rental_intent = _has_rental_intent(text)
    if NON_RENTAL_KEYWORD_PATTERN.search(text) and not rental_intent:
        reasons.append("blacklist_keyword")
    for price_str in USD_AMOUNT_PATTERN.findall(text):
        try:
            price = float(price_str.replace(",", ""))
        except ValueError:
            continue
        if price > NON_RENTAL_PRICE_LIMIT and not rental_intent:
            reasons.append(f"price_over_{NON_RENTAL_PRICE_LIMIT}")
            break
    return reasons


def is_non_rental_source(text: str) -> bool:
    return bool(non_rental_source_reasons(text))


def whitelist_quality_tags(text: str) -> list[str]:
    lower = (text or "").lower()
    tags: list[str] = []
    if any(
        area.lower() in lower or area in text or any(alias.lower() in lower or alias in text for alias in aliases)
        for area, aliases in AREA_WHITELIST.items()
    ):
        tags.append("core_area")
    if any(prop.lower() in lower or prop in text for prop in PROPERTY_WHITELIST):
        tags.append("known_property")
    if any(keyword.lower() in lower or keyword in text for keyword in RENTAL_KEYWORDS):
        tags.append("rental_intent")
    return tags


def _build_advisor_comment(area: str, project: str, price: int, layout: str) -> str:
    parts = []
    if area and area != "金边":
        parts.append(f"{area} 片区")
    if project:
        parts.append(project)
    if layout:
        parts.append(layout)
    head = " / ".join(parts) if parts else "该房源"
    if price:
        return f"{head} 已完成采集，当前识别租金约 ${price}/月，建议人工复核费用和可入住时间。"
    return f"{head} 已完成采集，建议人工复核价格、配套和可入住时间后再入队发布。"


@dataclass
class ParsedListing:
    title: str
    project: str
    community: str
    area: str
    property_type: str
    price: int
    layout: str
    size: str
    floor: str
    deposit: str
    payment_terms: str
    contract_term: str
    available_date: str
    highlights: list[str]
    drawbacks: list[str]
    advisor_comment: str
    cost_notes: str
    water_rate: str
    electric_rate: str
    quality_score: int
    quality_flags: list[str]

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "project": self.project,
            "community": self.community,
            "area": self.area,
            "property_type": self.property_type,
            "price": self.price,
            "layout": self.layout,
            "size": self.size,
            "floor": self.floor,
            "deposit": self.deposit,
            "payment_terms": self.payment_terms,
            "contract_term": self.contract_term,
            "available_date": self.available_date,
            "highlights": self.highlights,
            "drawbacks": self.drawbacks,
            "advisor_comment": self.advisor_comment,
            "cost_notes": self.cost_notes,
            "water_rate": self.water_rate,
            "electric_rate": self.electric_rate,
            "quality_score": self.quality_score,
            "quality_flags": self.quality_flags,
        }


class RuleBasedListingParser:
    def parse(self, raw_text: str) -> dict:
        text = _clean_text(raw_text or "")
        lines = _lines(text)

        price = _normalize_price(_first_match(PRICE_PATTERNS, text))
        size = _normalize_size(_first_match(SIZE_PATTERNS, text))
        layout = _detect_layout(text)
        floor = _first_match(FLOOR_PATTERNS, text)
        deposit_hint = _first_match(DEPOSIT_PATTERNS, text)
        payment_terms = _extract_payment_terms(text, deposit_hint=deposit_hint)
        contract_term = _extract_contract_term(text)
        deposit = payment_terms or deposit_hint
        available_date = _first_match(AVAILABLE_PATTERNS, text)
        area = _detect_area(text)
        property_type = _detect_property_type(text)
        project = _detect_project(lines, area, property_type)
        project = _clean_project_candidate(project) or project
        community = project
        title = "｜".join(part for part in [project, layout or property_type, area] if part)
        highlights = _extract_tags(text, HIGHLIGHT_PATTERNS)
        drawbacks = _extract_tags(text, DRAWBACK_PATTERNS)
        water_rate, electric_rate = _extract_utility_rates(text)
        cost_notes = _ensure_default_cost_note(_extract_cost_notes(lines), water_rate, electric_rate)
        advisor_comment = _build_advisor_comment(area, project, price, layout)

        quality_score = 0
        quality_flags: list[str] = []
        for field_name, value, weight in (
            ("project", project, 20),
            ("price", price, 20),
            ("area", area if area != "金边" else "", 15),
            ("layout", layout, 15),
            ("size", size, 10),
            ("highlights", highlights, 10),
            ("cost_notes", cost_notes, 10),
        ):
            if value:
                quality_score += weight
            else:
                quality_flags.append(f"missing_{field_name}")

        non_rental_reasons = non_rental_source_reasons(text)
        if non_rental_reasons:
            quality_score = 0
            quality_flags.extend(
                ["non_rental_source", "commercial_waste"]
                + [f"non_rental_{reason}" for reason in non_rental_reasons]
            )
        else:
            whitelist_tags = whitelist_quality_tags(text)
            if NON_RENTAL_KEYWORD_PATTERN.search(text):
                quality_flags.append("mixed_sale_rent_terms")
                quality_score = min(quality_score, 65)
            if "core_area" in whitelist_tags:
                quality_flags.append("whitelist_core_area")
                quality_score += 5
            if "known_property" in whitelist_tags:
                quality_flags.append("whitelist_known_property")
                quality_score += 5
            if "rental_intent" not in whitelist_tags:
                quality_flags.append("missing_rental_intent")
                quality_score = min(quality_score, 59)

        parsed = ParsedListing(
            title=title or (lines[0] if lines else "待审核房源"),
            project=project,
            community=community,
            area=area,
            property_type=property_type,
            price=price,
            layout=layout,
            size=size,
            floor=floor,
            deposit=deposit,
            payment_terms=payment_terms,
            contract_term=contract_term,
            available_date=available_date,
            highlights=highlights,
            drawbacks=drawbacks,
            advisor_comment=advisor_comment,
            cost_notes=cost_notes,
            water_rate=water_rate,
            electric_rate=electric_rate,
            quality_score=quality_score,
            quality_flags=quality_flags,
        )
        return parsed.to_dict()
