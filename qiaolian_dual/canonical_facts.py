"""Deterministic canonical fact pipeline for real-estate source posts.

This module is deliberately independent from drafts, listings, review notes and
publication templates.  It accepts immutable source evidence and produces the
only business-fact object that downstream code may project.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from qiaolian_dual.listing_taxonomy import classify_listing_taxonomy, public_location_from_fields

SCHEMA_VERSION = "canonical_facts.v1"
PARSER_REVISION = "v1.2"
CITY_KEY = "phnom_penh"
CITY_DISPLAY = "金边"

CN_NUMBERS = {"零": "0", "一": "1", "二": "2", "两": "2", "三": "3", "四": "4", "五": "5", "六": "6", "七": "7", "八": "8", "九": "9"}


def _clean(value: object) -> str:
    text = str(value or "").replace("\u00a0", " ").replace("\ufeff", " ")
    return re.sub(r"[ \t]+", " ", text).strip()


def _source_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _normalize_cn_numbers(value: str) -> str:
    return "".join(CN_NUMBERS.get(ch, ch) for ch in str(value or ""))


def _span(text: str, start: int, end: int) -> dict[str, int]:
    return {"start": int(start), "end": int(end)}


def _evidence(value: Any, source: str, confidence: str, raw_excerpt: str, start: int | None = None, end: int | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "value": value,
        "source": source,
        "confidence": confidence,
        "raw_excerpt": _clean(raw_excerpt)[:240],
    }
    if start is not None and end is not None:
        item["span"] = _span(raw_excerpt, start, end)
    return item


def _has_any(text: str, aliases: tuple[str, ...]) -> tuple[str, int] | None:
    lowered = text.lower()
    for alias in sorted(aliases, key=len, reverse=True):
        pos = lowered.find(alias.lower())
        if pos >= 0:
            return alias, pos
    return None


def _extract_layout(text: str) -> tuple[str | None, dict[str, int | None], list[dict[str, Any]]]:
    source = _normalize_cn_numbers(text)
    english = re.search(
        r"\b(\d{1,2})\s*(?:bedrooms?|beds?|br)\b"
        r"(?:\s*[/|,，&+]\s*|\s+)"
        r"(\d{1,2})\s*(?:bathrooms?|baths?|ba)\b",
        source,
        flags=re.I,
    )
    if english:
        bedrooms = int(english.group(1))
        bathrooms = int(english.group(2))
        layout = f"{bedrooms}房{bathrooms}卫"
        return layout, {
            "bedrooms": bedrooms,
            "living_rooms": None,
            "bathrooms": bathrooms,
            "helper_rooms": None,
        }, [_evidence(layout, "raw_explicit_layout", "high", english.group(0), english.start(), english.end())]
    patterns = (
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*办公\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房\s*\+\s*\d{1,2}\s*房(?:[｜|/、,，\s]*\d{1,2}\s*卫)?)",
        r"(?<!\d)(\d{1,2}\s*房\s*\+\s*\d{1,2}\s*(?:保姆|佣人)房(?:[｜|/、,，\s]*\d{1,2}\s*卫)?)",
        r"(?<!\d)(\d{1,2}\s*\+\s*\d{1,2}\s*房(?:\s*\+\s*\d{1,2}\s*(?:保姆|佣人)房)?(?:\s*\d{1,2}\s*厅)?(?:[｜|/、,，\s]*\d{1,2}\s*卫)?)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*厅\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*厅)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房)",
        r"\b(studio)\b",
        r"(单间)",
    )
    for pattern in patterns:
        m = re.search(pattern, source, flags=re.I)
        if not m:
            continue
        raw_layout = m.group(1)
        layout = re.sub(r"[｜|/、,，\s]+", "", raw_layout)
        if layout.lower() == "studio" or layout == "单间":
            return "Studio", {"bedrooms": None, "living_rooms": None, "bathrooms": None, "helper_rooms": None}, [_evidence("Studio", "raw_explicit_layout", "high", m.group(0), m.start(1), m.end(1))]
        bedroom = re.match(r"(\d+)", layout)
        living = re.search(r"(\d+)厅", layout)
        bath = re.search(r"(\d+)卫", layout)
        helper = re.search(r"\+(\d+)(?:保姆|佣人)房", layout)
        fields = {
            "bedrooms": int(bedroom.group(1)) if bedroom else None,
            "living_rooms": int(living.group(1)) if living else None,
            "bathrooms": int(bath.group(1)) if bath else None,
            "helper_rooms": int(helper.group(1)) if helper else None,
        }
        return layout, fields, [_evidence(layout, "raw_explicit_layout", "high", m.group(0), m.start(1), m.end(1))]
    return None, {"bedrooms": None, "living_rooms": None, "bathrooms": None, "helper_rooms": None}, []


def _extract_floor(text: str) -> tuple[str | None, list[dict[str, Any]]]:
    patterns = (
        r"(?:楼层|层数|floor)\s*[:：]?\s*(\d{1,2})(?!\d)\s*(?:楼|层|f)?",
        r"(?:位于|在)\s*(\d{1,2})(?!\d)\s*(?:楼|层)",
    )
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            value = str(int(m.group(1)))
            return value, [_evidence(value, "raw_explicit_floor", "high", m.group(0), m.start(1), m.end(1))]
    return None, []


def _to_usd(raw: str) -> int | None:
    value = str(raw or "").replace(",", "").strip().lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(k)?", value)
    if not m:
        return None
    number = float(m.group(1)) * (1000 if m.group(2) else 1)
    if number < 50 or number > 200000:
        return None
    return int(number)


def _extract_monthly_rent(text: str) -> tuple[int | None, str, list[dict[str, Any]], list[str]]:
    # A discounted listing may legitimately contain two rents. When the
    # current rent is explicitly labelled, it is the publishable fact and the
    # former rent is retained separately for an optional strike-through. An
    # unlabelled pair remains a conflict: guessing which one is current would
    # violate the canonical-fact contract.
    current_candidates: list[tuple[int, str, int, int]] = []
    current_pattern = (
        r"(?:现价|现租金|现月租|优惠价|优惠出租|特价(?:出租|招租)|特价(?!出售|销售|售卖)|促销价)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)"
        r"\s*(?:美元|美金|usd|\$|/月|每月|/month|per month)?"
    )
    for match in re.finditer(current_pattern, text, flags=re.I):
        parsed = _to_usd(match.group(1))
        if parsed is not None:
            current_candidates.append((parsed, match.group(0), match.start(1), match.end(1)))
    current_unique = {value for value, _excerpt, _start, _end in current_candidates}
    if len(current_unique) > 1:
        evidence = [
            _evidence(value, "raw_explicit_current_monthly_rent", "high", excerpt, start, end)
            for value, excerpt, start, end in current_candidates
        ]
        return None, "conflict", evidence, ["conflicting_rental_price"]
    if current_unique:
        evidence = [
            _evidence(value, "raw_explicit_current_monthly_rent", "high", excerpt, start, end)
            for value, excerpt, start, end in current_candidates
        ]
        return next(iter(current_unique)), "confirmed", evidence, []

    candidates: list[tuple[int, str, int, int]] = []
    # Only a rental label or a monthly currency context constitutes rent. Sale,
    # deposits and utility amounts are intentionally not candidates.
    patterns = (
        r"(?:月租|租金价格|租金|出租情况|出租价格|出租价|房间价格|租赁价格)\s*[:：]?\s*(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$|/月|每月|[a-z])?",
        r"(?:\$|usd|美金|美元|💵|💰)\s*(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:/月|每月|/month|per month)",
        r"(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$)\s*(?:每月|/月|/month|per month)",
    )
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.I):
            raw = m.group(1)
            parsed = _to_usd(raw)
            if parsed is not None:
                candidates.append((parsed, m.group(0), m.start(1), m.end(1)))
    unique = {value for value, _excerpt, _start, _end in candidates}
    evidence = [_evidence(value, "raw_explicit_monthly_rent", "high", excerpt, start, end) for value, excerpt, start, end in candidates]
    if len(unique) > 1:
        return None, "conflict", evidence, ["conflicting_rental_price"]
    if unique:
        return next(iter(unique)), "confirmed", evidence, []
    return None, "missing", [], ["missing_price"]


def _extract_original_monthly_rent(text: str, current_rent: int | None) -> tuple[int | None, list[dict[str, Any]]]:
    """Extract an explicitly labelled former rent; never infer it by order."""
    candidates: list[tuple[int, str, int, int]] = []
    pattern = (
        r"(?:原价|原租金|原月租|旧价|之前租金)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)"
        r"\s*(?:美元|美金|usd|\$|/月|每月|/month|per month)?"
    )
    for match in re.finditer(pattern, text, flags=re.I):
        parsed = _to_usd(match.group(1))
        if parsed is not None:
            candidates.append((parsed, match.group(0), match.start(1), match.end(1)))
    unique = {value for value, _excerpt, _start, _end in candidates}
    evidence = [
        _evidence(value, "raw_explicit_original_monthly_rent", "high", excerpt, start, end)
        for value, excerpt, start, end in candidates
    ]
    if len(unique) != 1:
        return None, evidence
    original = next(iter(unique))
    if current_rent is None or original == current_rent:
        return None, evidence
    return original, evidence


def _to_usd_sale(raw: str) -> int | None:
    value = str(raw or "").replace(",", "").strip().lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(万|w|k)?", value, flags=re.I)
    if not m:
        return None
    unit = (m.group(2) or "").lower()
    multiplier = 10000 if unit in {"万", "w"} else (1000 if unit == "k" else 1)
    number = float(m.group(1)) * multiplier
    if number < 500 or number > 100000000:
        return None
    return int(number)


def _extract_sale_price(text: str) -> tuple[int | None, str, list[dict[str, Any]], list[str]]:
    candidates: list[tuple[int, str, int, int]] = []
    pattern = (
        r"(?:售价|出售价格|销售价格|销售价|卖价|sale\s*price)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元)?\s*(\d[\d,]*(?:\.\d+)?\s*(?:万|w|k)?)"
        r"\s*(?:美元|美金|usd|\$)?"
    )
    for match in re.finditer(pattern, text, flags=re.I):
        parsed = _to_usd_sale(match.group(1))
        if parsed is not None:
            candidates.append((parsed, match.group(0), match.start(1), match.end(1)))
    unique = {value for value, _excerpt, _start, _end in candidates}
    evidence = [
        _evidence(value, "raw_explicit_sale_price", "high", excerpt, start, end)
        for value, excerpt, start, end in candidates
    ]
    if len(unique) > 1:
        return None, "conflict", evidence, ["conflicting_sale_price"]
    if unique:
        return next(iter(unique)), "confirmed", evidence, []
    return None, "missing", [], []


def _extract_terms(text: str) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    source = _normalize_cn_numbers(text)
    facts: dict[str, Any] = {
        "deposit_payment_terms": None,
        "deposit_months": None,
        "prepay_months": None,
        "contract_term_months": None,
        "contract_term_display": None,
    }
    evidence: dict[str, list[dict[str, Any]]] = {key: [] for key in facts}
    deposit = re.search(r"押\s*(\d{1,2})\s*(?:个?月)?\s*付\s*(\d{1,2})\s*(?:个?月)?", source, flags=re.I)
    if deposit:
        dep, prepay = int(deposit.group(1)), int(deposit.group(2))
        value = f"押{dep}付{prepay}"
        facts.update({"deposit_payment_terms": value, "deposit_months": dep, "prepay_months": prepay})
        item = _evidence(value, "raw_explicit_payment_terms", "high", deposit.group(0), deposit.start(), deposit.end())
        evidence["deposit_payment_terms"].append(item)
        evidence["deposit_months"].append(item)
        evidence["prepay_months"].append(item)
    else:
        generic_pay = re.search(r"(?:合同情况|合同|租期|付款方式|支付方式)\s*[:：]?[^\n]{0,40}?\b(\d{1,2}\s*付\s*\d{1,2})", source, flags=re.I)
        if generic_pay:
            value = re.sub(r"\s+", "", generic_pay.group(1))
            facts["deposit_payment_terms"] = value
            evidence["deposit_payment_terms"].append(_evidence(value, "raw_payment_terms_without_deposit_marker", "medium", generic_pay.group(0), generic_pay.start(1), generic_pay.end(1)))
    contract = re.search(r"(?:合同情况|签约合同|合同|租期|最短租期)\s*[:：]?\s*([0-9]+)\s*(年|个月|月)", source, flags=re.I)
    if contract:
        amount, unit = int(contract.group(1)), contract.group(2)
        months = amount * 12 if unit == "年" else amount
        display = f"{amount}{unit}"
        facts["contract_term_months"] = months
        facts["contract_term_display"] = display
        item = _evidence(display, "raw_explicit_contract_term", "high", contract.group(0), contract.start(1), contract.end(2))
        evidence["contract_term_months"].append(item)
        evidence["contract_term_display"].append(item)
    return facts, evidence


def _extract_listing_details(text: str) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Extract labelled operational details used by the discussion card.

    Values are accepted only from explicit labels/status words. Unknown or
    bracket placeholders are left null so templates can omit them entirely.
    """
    facts: dict[str, Any] = {
        "available_date": None,
        "management_fee": None,
        "internet_fee": None,
        "water_rate": None,
        "electric_rate": None,
        "parking_fee": None,
        "viewing_time": None,
        "video_viewing": None,
    }
    evidence: dict[str, list[dict[str, Any]]] = {key: [] for key in facts}

    def store(key: str, match: re.Match[str] | None) -> None:
        if not match:
            return
        value = _clean(match.group(1)).strip("，,。；;｜| ")
        if not value or any(token in value for token in ("待确认", "待定", "未知", "___", "【", "】")):
            return
        facts[key] = value[:40]
        evidence[key].append(
            _evidence(value[:40], f"raw_explicit_{key}", "high", match.group(0), match.start(1), match.end(1))
        )

    money_or_status = r"(已含|包含|免费|另付|不含|按表|\$?\s*\d+(?:\.\d+)?\s*(?:/|每)?\s*(?:m³|m3|立方米?|度|kwh|kw/h|月|辆)?)"
    patterns = {
        "management_fee": rf"(?:管理费|物业费)\s*[:：｜]?\s*{money_or_status}",
        "internet_fee": rf"(?:网络费|网费|网络|wifi)\s*[:：｜]?\s*{money_or_status}",
        "water_rate": rf"(?:水费)\s*[:：｜]?\s*{money_or_status}",
        "electric_rate": rf"(?:电费)\s*[:：｜]?\s*{money_or_status}",
        "parking_fee": rf"(?:停车费|停车|车位)\s*[:：｜]?\s*{money_or_status}",
    }
    for key, pattern in patterns.items():
        store(key, re.search(pattern, text, flags=re.I))

    store(
        "available_date",
        re.search(r"(?:入住时间|可入住|入住)\s*[:：｜]?\s*([^\n；;]{1,24})", text, flags=re.I),
    )
    store(
        "viewing_time",
        re.search(r"(?:看房时间|可看房)\s*[:：｜]?\s*([^\n；;]{1,32})", text, flags=re.I),
    )
    store(
        "video_viewing",
        re.search(r"(?:视频看房|视频代看)\s*[:：｜]?\s*(可以安排|可安排|支持|可以|可)", text, flags=re.I),
    )
    return facts, evidence


def _dimension(value: str) -> str | None:
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:m|米)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(?:m|米)", value, flags=re.I)
    if not m:
        return None
    return f"{m.group(1)}m×{m.group(2)}m"


def _extract_sizes(text: str) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    facts: dict[str, Any] = {
        "size_sqm": None, "land_dimension": None, "building_dimension": None,
        "land_size_sqm": None, "building_size_sqm": None, "unlabelled_dimension": None,
    }
    evidence: dict[str, list[dict[str, Any]]] = {key: [] for key in facts}
    unit = r"(?:㎡|平方米|m2|m²|sqm|sq\.?m)"
    for label, dimension_key, sqm_key in (
        (r"(?:土地|地块)(?:面积|尺寸)?", "land_dimension", "land_size_sqm"),
        (r"(?:建筑|建面)(?:面积|尺寸)?", "building_dimension", "building_size_sqm"),
    ):
        m = re.search(label + r"\s*[:：]?\s*([^\n]{0,50})", text, flags=re.I)
        if not m:
            continue
        value = _dimension(m.group(1))
        if value:
            facts[dimension_key] = value
            evidence[dimension_key].append(_evidence(value, "raw_explicit_dimension", "high", m.group(0), m.start(1), m.end(1)))
            continue
        sqm = re.search(r"(\d+(?:\.\d+)?)\s*" + unit, m.group(1), flags=re.I)
        if sqm:
            number = float(sqm.group(1))
            if 10 <= number <= 200000:
                facts[sqm_key] = int(number) if number.is_integer() else number
                evidence[sqm_key].append(_evidence(facts[sqm_key], "raw_explicit_labeled_sqm", "high", m.group(0), m.start(1), m.end(1)))
    sqm_match = None
    for line in str(text or "").splitlines():
        if re.search(r"(?:土地|地块|建筑|建面)", line, flags=re.I):
            continue
        sqm_match = re.search(r"(?:面积|size)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*" + unit, line, flags=re.I)
        if not sqm_match:
            sqm_match = re.search(r"(?<![\d.])(\d+(?:\.\d+)?)\s*" + unit, line, flags=re.I)
        if sqm_match:
            break
    if sqm_match:
        number = float(sqm_match.group(1))
        if 10 <= number <= 2000:
            facts["size_sqm"] = int(number) if number.is_integer() else number
            evidence["size_sqm"].append(_evidence(facts["size_sqm"], "raw_explicit_sqm", "high", sqm_match.group(0), sqm_match.start(1), sqm_match.end(1)))
    generic = re.search(r"(?:面积|尺寸)\s*[:：]?\s*([^\n]{0,50})", text, flags=re.I)
    if generic:
        value = _dimension(generic.group(1))
        if value and not re.search(r"(?:土地|地块|建筑|建面)", generic.group(0), flags=re.I):
            facts["unlabelled_dimension"] = value
            evidence["unlabelled_dimension"].append(_evidence(value, "raw_unlabelled_dimension", "medium", generic.group(0), generic.start(1), generic.end(1)))
    return facts, evidence


def _extract_highlights(text: str) -> list[str]:
    rules = (
        ("家具家电齐全", ("家具家电齐全", "家具齐全", "全新家具", "家具家电")),
        ("含保洁", ("包打扫", "含保洁", "cleaning")),
        ("含网络", ("包网络", "含网络", "wifi", "网络")),
        ("管家服务", ("管家服务",)),
        ("带泳池", ("泳池", "pool")),
        ("带健身房", ("健身房", "gym")),
        ("24H安保", ("24小时安保", "24h安保", "24h security", "security")),
        ("拎包入住", ("拎包入住",)),
        ("可停车", ("停车", "parking")),
    )
    result: list[str] = []
    for label, aliases in rules:
        if _has_any(text, aliases) and label not in result:
            result.append(label)
    return result


def _special_tags(text: str) -> list[str]:
    tags: list[str] = []
    if re.search(r"特价|优惠", text):
        tags.append("special_price")
    if re.search(r"急租|急出|急转", text):
        tags.append("special_urgent")
    return tags


def _has_rental_intent(text: str) -> bool:
    return bool(re.search(
        r"出租|招租|租金|月租|租赁|房间价格|仅租|只租|for rent|only for rent|rent(?:al)? only|per month|/month|每月|/月",
        text,
        flags=re.I,
    ))


def _has_non_rental_intent(text: str) -> bool:
    # Remove explicit negative/exclusion phrases before looking for a sale
    # intent.  "仅出租，不出售" is rental evidence, not a mixed transaction.
    scrubbed = str(text or "")
    negated_patterns = (
        r"(?:不考虑|暂无计划|不是|不|非|无意|谢绝|拒绝|不可|不能)\s*(?:对外)?\s*(?:急售|出售|售卖|转让|顶让|卖)",
        r"(?:not\s+for\s+sale|no\s+sale|rent(?:al)?\s+only|for\s+rent\s+only)",
    )
    for pattern in negated_patterns:
        scrubbed = re.sub(pattern, " ", scrubbed, flags=re.I)
    return bool(re.search(r"(?:急售|出售|售价|可售|转让|顶让|for sale|sale price)", scrubbed, flags=re.I))


def _extract_deal_type(text: str) -> tuple[str, list[dict[str, Any]]]:
    rental = _has_rental_intent(text)
    sale = _has_non_rental_intent(text)
    if rental and sale:
        return "mixed", [_evidence("mixed", "raw_deal_terms", "high", "rent_and_sale_terms")]
    if rental:
        return "rent", [_evidence("rent", "raw_deal_terms", "high", "rental_terms")]
    if sale:
        return "sale", [_evidence("sale", "raw_deal_terms", "high", "sale_terms")]
    return "unknown", []


def _display_title(project: str | None, area: str | None, layout: str | None, property_type: str) -> str:
    parts: list[str] = []
    for part in (project, area, layout, None if property_type == "未知" else property_type):
        item = _clean(part)
        if not item or item in parts:
            continue
        parts.append(item)
    return "｜".join(parts) if parts else "待确认房源"


def _quality(facts: dict[str, Any]) -> dict[str, Any]:
    hard: list[str] = []
    review: list[str] = []
    warning: list[str] = []
    info: list[str] = []
    if not facts.get("public_location_key"):
        hard.append("missing_public_location")
    if not facts.get("canonical_area_key"):
        info.append("geo_precision_unconfirmed")
    deal_type = str(facts.get("deal_type") or "unknown")
    if deal_type in {"rent", "mixed"} and not facts.get("monthly_rent_usd"):
        hard.append("missing_price")
    elif deal_type == "sale" and not facts.get("sale_price_usd"):
        hard.append("missing_sale_price")
    elif deal_type == "unknown" and not facts.get("monthly_rent_usd") and not facts.get("sale_price_usd"):
        hard.append("missing_price")
    if facts.get("price_status") == "conflict":
        hard.append("conflicting_rental_price")
    if facts.get("sale_price_status") == "conflict":
        hard.append("conflicting_sale_price")
    if not facts.get("layout"):
        hard.append("missing_layout")
    if facts.get("property_type") == "未知":
        hard.append("ambiguous_property_type" if facts.get("property_type_status") == "ambiguous" else "unknown_property_type")
    candidate_flags = {str(flag) for flag in (facts.get("candidate_flags") or [])}
    for flag in ("ambiguous_area", "ambiguous_project"):
        if flag in candidate_flags:
            review.append(flag)
    if "ambiguous_market_location" in candidate_flags and not facts.get("canonical_area_key"):
        review.append("ambiguous_market_location")
    if "project_brand_only" in candidate_flags:
        info.append("project_brand_only")
    if deal_type == "sale":
        hard.append("non_rental_source")
    elif deal_type == "mixed":
        review.append("mixed_sale_rent_terms")
    elif deal_type == "unknown":
        review.append("missing_rental_intent")
    if facts.get("size_sqm") is None and not facts.get("land_dimension") and not facts.get("building_dimension") and not facts.get("unlabelled_dimension"):
        warning.append("missing_size")
    if not facts.get("highlights"):
        warning.append("missing_highlights")
    if not facts.get("deposit_payment_terms"):
        warning.append("missing_deposit_terms")
    if not facts.get("contract_term_months"):
        warning.append("missing_contract_term")
    for tag in facts.get("special_tags") or []:
        info.append(tag)
    blocking = list(dict.fromkeys(hard + review))
    score = max(0, 100 - 30 * len(hard) - 12 * len(review) - 4 * len(warning))
    return {
        "hard_flags": list(dict.fromkeys(hard)),
        "review_flags": list(dict.fromkeys(review)),
        "warning_flags": list(dict.fromkeys(warning)),
        "info_flags": list(dict.fromkeys(info)),
        "blocking_flags": blocking,
        "all_flags": list(dict.fromkeys(hard + review + warning + info)),
        "score": score,
    }


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def canonicalize_source(
    raw_text: str,
    sanitized_text: str | None = None,
    source_identity: dict[str, Any] | None = None,
    media_summary: dict[str, Any] | None = None,
    manual_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create final facts once from immutable source evidence and optional audited overrides.

    Manual overrides are intentionally narrow: they can supply confirmed fields,
    but every override is retained as evidence and facts are fully re-evaluated.
    """
    raw = str(raw_text or "")
    sanitized = str(sanitized_text or raw)
    # Public facts are parsed from the cleaned evidence projection. The raw post
    # remains hash-bound for audit, but source contacts, channel attribution and
    # promotional boilerplate cannot become project/highlight/caption facts.
    parse_text = sanitized or raw
    taxonomy = classify_listing_taxonomy(parse_text)
    area_key = taxonomy.canonical_area_key
    area_display = taxonomy.canonical_area_display
    project = taxonomy.project_name
    project_alias = taxonomy.project_alias
    project_key = taxonomy.project_key
    project_brand = taxonomy.project_brand
    property_type = taxonomy.property_type
    property_subtype = taxonomy.property_subtype
    property_type_display = taxonomy.property_type_display
    property_status = taxonomy.property_type_status
    layout, layout_fields, layout_evidence = _extract_layout(parse_text)
    floor, floor_evidence = _extract_floor(parse_text)
    rent, price_status, price_evidence, price_flags = _extract_monthly_rent(parse_text)
    original_rent, original_price_evidence = _extract_original_monthly_rent(parse_text, rent)
    sale_price, sale_price_status, sale_price_evidence, sale_price_flags = _extract_sale_price(parse_text)
    terms, terms_evidence = _extract_terms(parse_text)
    details, details_evidence = _extract_listing_details(parse_text)
    sizes, sizes_evidence = _extract_sizes(parse_text)
    market_keys = list(taxonomy.market_location_keys)
    market_displays = list(taxonomy.market_location_displays)
    deal_type, deal_evidence = _extract_deal_type(parse_text)

    facts: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "parser_revision": PARSER_REVISION,
        "source_identity": dict(source_identity or {}),
        "raw_text_sha256": _source_hash(raw),
        "sanitized_text_sha256": _source_hash(sanitized),
        "city_key": CITY_KEY,
        "city_display": CITY_DISPLAY,
        "deal_type": deal_type,
        "canonical_area_key": area_key,
        "canonical_area_display": area_display,
        "area_status": taxonomy.area_status,
        "canonical_area_level": taxonomy.canonical_area_level,
        "market_location_keys": market_keys,
        "market_location_displays": market_displays,
        "project_name": project,
        "project_alias": project_alias,
        "project_key": project_key,
        "project_brand": project_brand,
        "project_brand_key": taxonomy.project_brand_key,
        "community_name": project,
        "property_type": property_type,
        "property_subtype": property_subtype,
        "property_type_display": property_type_display,
        "property_type_status": property_status,
        "layout": layout,
        **layout_fields,
        "monthly_rent_usd": rent,
        "original_monthly_rent_usd": original_rent,
        "price_status": price_status,
        "sale_price_usd": sale_price,
        "sale_price_status": sale_price_status,
        **sizes,
        "floor": floor,
        **terms,
        **details,
        "highlights": _extract_highlights(parse_text),
        "special_tags": _special_tags(parse_text),
        "media_summary": dict(media_summary or {}),
        "evidence": {
            "deal_type": deal_evidence,
            "canonical_area_key": list(taxonomy.evidence.get("canonical_area_key") or []),
            "market_location_keys": list(taxonomy.evidence.get("market_location_keys") or []),
            "project": list(taxonomy.evidence.get("project") or []),
            "project_alias": list(taxonomy.evidence.get("project_alias") or []),
            "property_type": list(taxonomy.evidence.get("property_type") or []),
            "layout": layout_evidence,
            "floor": floor_evidence,
            "monthly_rent_usd": price_evidence,
            "original_monthly_rent_usd": original_price_evidence,
            "sale_price_usd": sale_price_evidence,
            **terms_evidence,
            **details_evidence,
            **sizes_evidence,
        },
        "candidate_flags": list(dict.fromkeys(list(taxonomy.flags) + price_flags + sale_price_flags)),
        "manual_overrides": [],
        "_source_text": parse_text,
    }
    for key, value in (manual_overrides or {}).items():
        if key not in facts or value in (None, ""):
            continue
        facts[key] = value
        facts["manual_overrides"].append({"field": key, "value": value})
        facts["evidence"].setdefault(key, []).append(_evidence(value, "manual_override", "high", "manual_override"))
        if key in {"canonical_area_key", "canonical_area_display"}:
            facts["area_status"] = "confirmed" if facts.get("canonical_area_key") else "unconfirmed"
    # Resolve after audited overrides so the public projection cannot retain a
    # stale pre-override location. The selection logic itself remains owned by
    # listing_taxonomy.py.
    public_key, public_display, publication_level = public_location_from_fields(
        canonical_area_key=facts.get("canonical_area_key"),
        canonical_area_display=facts.get("canonical_area_display"),
        area_status=facts.get("area_status"),
        market_location_keys=facts.get("market_location_keys"),
        market_location_displays=facts.get("market_location_displays"),
        project_key=facts.get("project_key"),
        project_name=facts.get("project_name"),
    )
    facts["public_location_key"] = public_key
    facts["public_location_display"] = public_display
    facts["publication_location_level"] = publication_level
    title_location = facts.get("canonical_area_display") or (public_display if not facts.get("project_name") else None)
    facts["display_title"] = _display_title(facts.get("project_name"), title_location, facts.get("layout"), facts.get("property_type_display") or "未知")
    facts["quality"] = _quality(facts)
    facts.pop("_source_text", None)
    hash_payload = {key: value for key, value in facts.items() if key != "canonical_facts_hash"}
    facts["canonical_facts_hash"] = _stable_hash(hash_payload)
    return facts


def draft_projection(facts: dict[str, Any]) -> dict[str, Any]:
    """Return the sole allowed draft/listing projection from canonical facts."""
    quality = dict(facts.get("quality") or {})
    payment = facts.get("deposit_payment_terms") or ""
    cost_notes = "；".join(
        f"{label}：{value}"
        for label, value in (
            ("管理费", facts.get("management_fee")),
            ("网络", facts.get("internet_fee")),
            ("水费", facts.get("water_rate")),
            ("电费", facts.get("electric_rate")),
            ("停车", facts.get("parking_fee")),
        )
        if value
    )
    return {
        "title": facts.get("display_title") or "待确认房源",
        "deal_type": facts.get("deal_type") or "unknown",
        "project": facts.get("project_name") or "",
        "project_name": facts.get("project_name") or "",
        "project_alias": facts.get("project_alias") or "",
        "community": facts.get("community_name") or "",
        "city": facts.get("city_display") or CITY_DISPLAY,
        "area": facts.get("public_location_display") or "",
        "normalized_area": facts.get("canonical_area_key"),
        "public_location_key": facts.get("public_location_key"),
        "public_location_display": facts.get("public_location_display") or "",
        "publication_location_level": facts.get("publication_location_level") or "unknown",
        "property_type": facts.get("property_type") or "未知",
        "property_subtype": facts.get("property_subtype") or "",
        "property_type_display": facts.get("property_type_display") or facts.get("property_type") or "未知",
        "price": facts.get("monthly_rent_usd"),
        "original_price": facts.get("original_monthly_rent_usd"),
        "sale_price": facts.get("sale_price_usd"),
        "layout": facts.get("layout") or "",
        "size": (f"{facts['size_sqm']}㎡" if facts.get("size_sqm") is not None else ""),
        "land_size": facts.get("land_dimension") or (f"{facts['land_size_sqm']}㎡" if facts.get("land_size_sqm") is not None else ""),
        "building_size": facts.get("building_dimension") or (f"{facts['building_size_sqm']}㎡" if facts.get("building_size_sqm") is not None else ""),
        "unlabelled_dimension": facts.get("unlabelled_dimension") or "",
        "floor": facts.get("floor") or "",
        "deposit": payment,
        "payment_terms": payment,
        "contract_term": facts.get("contract_term_display") or "",
        "available_date": facts.get("available_date") or "",
        "management_fee": facts.get("management_fee") or "",
        "internet_fee": facts.get("internet_fee") or "",
        "water_rate": facts.get("water_rate") or "",
        "electric_rate": facts.get("electric_rate") or "",
        "parking_fee": facts.get("parking_fee") or "",
        "viewing_time": facts.get("viewing_time") or "",
        "video_viewing": facts.get("video_viewing") or "",
        "cost_notes": cost_notes,
        "highlights": list(facts.get("highlights") or []),
        "special_tags": list(facts.get("special_tags") or []),
        "quality_flags": list(quality.get("all_flags") or []),
        "blocking_quality_flags": list(quality.get("blocking_flags") or []),
        "quality_score": int(quality.get("score") or 0),
        "canonical_facts_hash": facts.get("canonical_facts_hash"),
        "normalized_data": facts,
    }


def is_buildable(facts: dict[str, Any]) -> bool:
    """Level-1 publication eligibility; Level-2 geography is not required."""
    return not bool((facts.get("quality") or {}).get("blocking_flags"))


def has_confirmed_physical_area(facts: dict[str, Any]) -> bool:
    return bool(facts.get("canonical_area_key")) and facts.get("publication_location_level") == "level_2_physical_confirmed"


__all__ = ["SCHEMA_VERSION", "PARSER_REVISION", "canonicalize_source", "draft_projection", "is_buildable", "has_confirmed_physical_area"]
