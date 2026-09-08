"""V3 deterministic canonical parser extracted from locked V2.2 behavior.

The parser consumes source evidence only. It does not read drafts, build
publication packages, or call Telegram. V3 persists exactly three deal states:
``rent | sale | unknown``. Simultaneous rent/sale intent is represented as
``unknown`` plus candidates, never as a persisted ``mixed`` state.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .safe_enrichment import enrich_safe
from .taxonomy import classify_listing_taxonomy, public_location_from_fields

SCHEMA_VERSION = "canonical_facts.v3"
PARSER_REVISION = "v3.phase3"
CITY_KEY = "phnom_penh"
CITY_DISPLAY = "金边"

_CN_NUMBERS = {"零":"0","一":"1","二":"2","两":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9"}


def _clean(value: object) -> str:
    text = str(value or "").replace("\u00a0", " ").replace("\ufeff", " ")
    return re.sub(r"[ \t]+", " ", text).strip()


def _sha(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _normalize_cn_numbers(value: str) -> str:
    return "".join(_CN_NUMBERS.get(ch, ch) for ch in str(value or ""))


def _evidence(value: Any, source: str, excerpt: str, confidence: str = "high") -> dict[str, Any]:
    return {"value": value, "source": source, "confidence": confidence, "raw_excerpt": _clean(excerpt)[:240]}


def _to_rent_usd(raw: str) -> int | None:
    value = str(raw or "").replace(",", "").strip().lower()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(k)?", value)
    if not match:
        return None
    number = float(match.group(1)) * (1000 if match.group(2) else 1)
    if number < 50 or number > 200000:
        return None
    return int(number)


def _to_sale_usd(raw: str) -> int | None:
    value = str(raw or "").replace(",", "").strip().lower()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(万|w|k)?", value, flags=re.I)
    if not match:
        return None
    unit = (match.group(2) or "").lower()
    multiplier = 10000 if unit in {"万", "w"} else (1000 if unit == "k" else 1)
    number = float(match.group(1)) * multiplier
    if number < 500 or number > 100000000:
        return None
    return int(number)


def _extract_monthly_rent(text: str) -> tuple[int | None, str, list[dict[str, Any]], list[str]]:
    current_pattern = (
        r"(?:现价|现租金|现月租|优惠价|优惠出租|特价(?:出租|招租)|特价(?!出售|销售|售卖)|促销价)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)"
        r"\s*(?:美元|美金|usd|\$|/月|每月|/month|per month)?"
    )
    current: list[tuple[int, str]] = []
    for m in re.finditer(current_pattern, text, flags=re.I):
        value = _to_rent_usd(m.group(1))
        if value is not None:
            current.append((value, m.group(0)))
    unique_current = {v for v, _ in current}
    evidence = [_evidence(v, "raw_explicit_current_monthly_rent", excerpt) for v, excerpt in current]
    if len(unique_current) > 1:
        return None, "conflict", evidence, ["conflicting_rental_price"]
    if unique_current:
        return next(iter(unique_current)), "confirmed", evidence, []

    patterns = (
        r"(?:月租|租金价格|租金|出租情况|出租价格|出租价|房间价格|租赁价格)\s*[:：]?\s*(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$|/月|每月|[a-z])?",
        r"(?:\$|usd|美金|美元|💵|💰)\s*(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:/月|每月|/month|per month)",
        r"(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$)\s*(?:每月|/月|/month|per month)",
    )
    found: list[tuple[int, str]] = []
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.I):
            value = _to_rent_usd(m.group(1))
            if value is not None:
                found.append((value, m.group(0)))
    unique = {v for v, _ in found}
    evidence = [_evidence(v, "raw_explicit_monthly_rent", excerpt) for v, excerpt in found]
    if len(unique) > 1:
        return None, "conflict", evidence, ["conflicting_rental_price"]
    if unique:
        return next(iter(unique)), "confirmed", evidence, []
    return None, "missing", [], []


def _extract_original_rent(text: str, current: int | None) -> int | None:
    found: set[int] = set()
    pattern = r"(?:原价|原租金|原月租|旧价|之前租金)\s*[:：]?\s*(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)"
    for m in re.finditer(pattern, text, flags=re.I):
        value = _to_rent_usd(m.group(1))
        if value is not None:
            found.add(value)
    if len(found) != 1:
        return None
    value = next(iter(found))
    return value if current is not None and value != current else None


def _extract_sale_price(text: str) -> tuple[int | None, str, list[dict[str, Any]], list[str]]:
    pattern = (
        r"(?:售价|出售价格|销售价格|销售价|卖价|sale\s*price)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元)?\s*(\d[\d,]*(?:\.\d+)?\s*(?:万|w|k)?)\s*(?:美元|美金|usd|\$)?"
    )
    found: list[tuple[int, str]] = []
    for m in re.finditer(pattern, text, flags=re.I):
        value = _to_sale_usd(m.group(1))
        if value is not None:
            found.append((value, m.group(0)))
    unique = {v for v, _ in found}
    evidence = [_evidence(v, "raw_explicit_sale_price", excerpt) for v, excerpt in found]
    if len(unique) > 1:
        return None, "conflict", evidence, ["conflicting_sale_price"]
    if unique:
        return next(iter(unique)), "confirmed", evidence, []
    return None, "missing", [], []


def _has_rental_intent(text: str) -> bool:
    return bool(re.search(r"出租|招租|租金|月租|租赁|房间价格|仅租|只租|for rent|only for rent|rent(?:al)? only|per month|/month|每月|/月", text, flags=re.I))


def _has_sale_intent(text: str) -> bool:
    scrubbed = str(text or "")
    for pattern in (
        r"(?:不考虑|暂无计划|不是|不|非|无意|谢绝|拒绝|不可|不能)\s*(?:对外)?\s*(?:急售|出售|售卖|转让|顶让|卖)",
        r"(?:not\s+for\s+sale|no\s+sale|rent(?:al)?\s+only|for\s+rent\s+only)",
    ):
        scrubbed = re.sub(pattern, " ", scrubbed, flags=re.I)
    return bool(re.search(r"(?:急售|出售|售价|可售|转让|顶让|for sale|sale price)", scrubbed, flags=re.I))


def _deal_type(text: str) -> tuple[str, list[str], list[str], list[dict[str, Any]]]:
    rent = _has_rental_intent(text)
    sale = _has_sale_intent(text)
    if rent and sale:
        return "unknown", ["rent", "sale"], ["ambiguous_deal_type"], [_evidence(["rent", "sale"], "raw_deal_terms", "rent_and_sale_terms")]
    if rent:
        return "rent", [], [], [_evidence("rent", "raw_deal_terms", "rental_terms")]
    if sale:
        return "sale", [], [], [_evidence("sale", "raw_deal_terms", "sale_terms")]
    return "unknown", [], ["missing_deal_type"], []


def _extract_layout(text: str) -> tuple[str | None, dict[str, int | None]]:
    source = _normalize_cn_numbers(text)
    english = re.search(r"\b(\d{1,2})\s*(?:bedrooms?|beds?|br)\b(?:\s*[/|,，&+]\s*|\s+)(\d{1,2})\s*(?:bathrooms?|baths?|ba)\b", source, flags=re.I)
    if english:
        beds, baths = int(english.group(1)), int(english.group(2))
        return f"{beds}房{baths}卫", {"bedrooms":beds,"living_rooms":None,"bathrooms":baths,"helper_rooms":None}
    for pattern in (
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*厅\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*厅)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房)", r"\b(studio)\b", r"(单间)",
    ):
        m = re.search(pattern, source, flags=re.I)
        if not m:
            continue
        raw = re.sub(r"[｜|/、,，\s]+", "", m.group(1))
        if raw.lower() == "studio" or raw == "单间":
            return "Studio", {"bedrooms":None,"living_rooms":None,"bathrooms":None,"helper_rooms":None}
        bed = re.match(r"(\d+)", raw)
        living = re.search(r"(\d+)厅", raw)
        bath = re.search(r"(\d+)卫", raw)
        return raw, {
            "bedrooms": int(bed.group(1)) if bed else None,
            "living_rooms": int(living.group(1)) if living else None,
            "bathrooms": int(bath.group(1)) if bath else None,
            "helper_rooms": None,
        }
    return None, {"bedrooms":None,"living_rooms":None,"bathrooms":None,"helper_rooms":None}


def _extract_floor(text: str) -> str | None:
    for pattern in (r"(?:楼层|层数|floor)\s*[:：]?\s*(\d{1,2})(?!\d)\s*(?:楼|层|f)?", r"(?:位于|在)\s*(\d{1,2})(?!\d)\s*(?:楼|层)"):
        m = re.search(pattern, text, flags=re.I)
        if m:
            return str(int(m.group(1)))
    return None


def _extract_terms(text: str) -> dict[str, Any]:
    source = _normalize_cn_numbers(text)
    result: dict[str, Any] = {"deposit_payment_terms":None,"deposit_months":None,"prepay_months":None,"contract_term_months":None,"contract_term_display":None}
    dep = re.search(r"押\s*(\d{1,2})\s*(?:个?月)?\s*付\s*(\d{1,2})\s*(?:个?月)?", source)
    if dep:
        result.update(deposit_payment_terms=f"押{int(dep.group(1))}付{int(dep.group(2))}", deposit_months=int(dep.group(1)), prepay_months=int(dep.group(2)))
    contract = re.search(r"(?:合同情况|签约合同|合同|租期|最短租期)\s*[:：]?\s*([0-9]+)\s*(年|个月|月)", source, flags=re.I)
    if contract:
        amount, unit = int(contract.group(1)), contract.group(2)
        result["contract_term_months"] = amount * 12 if unit == "年" else amount
        result["contract_term_display"] = f"{amount}{unit}"
    return result


def _extract_sizes(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {"size_sqm":None,"land_dimension":None,"building_dimension":None,"land_size_sqm":None,"building_size_sqm":None,"unlabelled_dimension":None}
    unit = r"(?:㎡|平方米|m2|m²|sqm|sq\.?m)"
    for line in str(text or "").splitlines():
        if re.search(r"(?:土地|地块|建筑|建面)", line, flags=re.I):
            continue
        m = re.search(r"(?:面积|size)\s*[:：]?\s*(\d+(?:\.\d+)?)\s*" + unit, line, flags=re.I) or re.search(r"(?<![\d.])(\d+(?:\.\d+)?)\s*" + unit, line, flags=re.I)
        if m:
            number = float(m.group(1))
            if 10 <= number <= 2000:
                result["size_sqm"] = int(number) if number.is_integer() else number
                break
    generic = re.search(r"(?:面积|尺寸)\s*[:：]?\s*([^\n]{0,50})", text, flags=re.I)
    if generic:
        d = re.search(r"(\d+(?:\.\d+)?)\s*(?:m|米)\s*[x×*]\s*(\d+(?:\.\d+)?)\s*(?:m|米)", generic.group(1), flags=re.I)
        if d and not re.search(r"(?:土地|地块|建筑|建面)", generic.group(0), flags=re.I):
            result["unlabelled_dimension"] = f"{d.group(1)}m×{d.group(2)}m"
    return result


def _quality(facts: dict[str, Any], intent_review: list[str], price_flags: list[str], sale_flags: list[str]) -> dict[str, Any]:
    hard: list[str] = []
    review = list(intent_review)
    warning: list[str] = []
    if not facts.get("public_location_key"):
        hard.append("missing_public_location")
    if facts.get("price_status") == "conflict":
        hard.append("conflicting_rental_price")
    if facts.get("sale_price_status") == "conflict":
        hard.append("conflicting_sale_price")
    if not facts.get("layout"):
        warning.append("missing_layout")
    if facts.get("property_type") == "未知":
        warning.append("unknown_property_type")
    hard.extend(price_flags)
    hard.extend(sale_flags)
    return {
        "hard_flags": list(dict.fromkeys(hard)),
        "review_flags": list(dict.fromkeys(review)),
        "warning_flags": list(dict.fromkeys(warning)),
        "blocking_flags": list(dict.fromkeys(hard + review)),
    }


def canonicalize_source(raw_text: str, sanitized_text: str | None = None, source_identity: dict[str, Any] | None = None, media_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = str(raw_text or "")
    sanitized = str(sanitized_text if sanitized_text is not None else raw)
    text = sanitized or raw
    taxonomy = classify_listing_taxonomy(text)
    public_key, public_display, publication_level = public_location_from_fields(
        canonical_area_key=taxonomy.canonical_area_key,
        canonical_area_display=taxonomy.canonical_area_display,
        area_status=taxonomy.area_status,
        market_location_keys=taxonomy.market_location_keys,
        market_location_displays=taxonomy.market_location_displays,
        project_key=taxonomy.project_key,
        project_name=taxonomy.project_name,
    )
    layout, layout_fields = _extract_layout(text)
    rent, price_status, rent_evidence, price_flags = _extract_monthly_rent(text)
    sale, sale_status, sale_evidence, sale_flags = _extract_sale_price(text)
    deal_type, candidates, intent_review, deal_evidence = _deal_type(text)
    facts: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION, "parser_revision": PARSER_REVISION,
        "source_identity": dict(source_identity or {}), "raw_text_sha256": _sha(raw), "sanitized_text_sha256": _sha(sanitized),
        "city_key": CITY_KEY, "city_display": CITY_DISPLAY,
        "deal_type": deal_type, "deal_type_candidates": candidates,
        "canonical_area_key": taxonomy.canonical_area_key, "canonical_area_display": taxonomy.canonical_area_display,
        "canonical_area_level": taxonomy.canonical_area_level, "area_status": taxonomy.area_status,
        "market_location_keys": list(taxonomy.market_location_keys), "market_location_displays": list(taxonomy.market_location_displays),
        "public_location_key": public_key, "public_location_display": public_display, "publication_location_level": publication_level,
        "project_key": taxonomy.project_key, "project_name": taxonomy.project_name, "project_alias": taxonomy.project_alias,
        "project_brand_key": taxonomy.project_brand_key, "project_brand": taxonomy.project_brand,
        "property_type": taxonomy.property_type, "property_subtype": taxonomy.property_subtype,
        "property_type_display": taxonomy.property_type_display, "property_type_status": taxonomy.property_type_status,
        "layout": layout, **layout_fields,
        "monthly_rent_usd": rent, "original_monthly_rent_usd": _extract_original_rent(text, rent), "price_status": price_status,
        "sale_price_usd": sale, "sale_price_status": sale_status,
        **_extract_sizes(text), "floor": _extract_floor(text), **_extract_terms(text),
        "media_summary": dict(media_summary or {}),
        "evidence": {"deal_type":deal_evidence,"monthly_rent_usd":rent_evidence,"sale_price_usd":sale_evidence,**taxonomy.evidence},
        "candidate_flags": list(dict.fromkeys(taxonomy.flags + price_flags + sale_flags)),
        "review_flags": list(dict.fromkeys(intent_review)), "processing_flags": [],
    }
    quality = _quality(facts, intent_review, price_flags, sale_flags)
    facts["quality"] = quality
    facts["hard_flags"] = list(quality["hard_flags"])
    facts["review_flags"] = list(quality["review_flags"])
    enriched = enrich_safe(text, facts)
    # Safe enrichment is additive and cannot change monetary or deal facts.
    enriched["deal_type"] = deal_type
    enriched["deal_type_candidates"] = candidates
    enriched["monthly_rent_usd"] = rent
    enriched["sale_price_usd"] = sale
    enriched["price_status"] = price_status
    enriched["sale_price_status"] = sale_status
    hash_payload = {k:v for k,v in enriched.items() if k != "canonical_facts_hash"}
    enriched["canonical_facts_hash"] = _stable_hash(hash_payload)
    return enriched


__all__ = ["SCHEMA_VERSION", "PARSER_REVISION", "canonicalize_source"]
