"""Conservative v1.2 enrichment for the canonical real-estate parser.

This is not a second parser. It runs the existing canonicalize_source() first,
then fills only facts that are still missing or less precise when the source
contains explicit evidence covered by the locked Qiaolian lexicon / reviewed
zufang555 corpus.

Rules:
- no invented facts;
- compound text may yield multiple independent tokens (road + project + area);
- inventory/menu text must not override the current listing type;
- rent/sale values require explicit transaction context;
- every enrichment adds provenance and the canonical hash/quality are rebuilt.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable

from qiaolian_dual import canonical_facts as canonical_core
from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.listing_taxonomy import public_location_from_fields


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    raw = _clean(value).casefold()
    cooked = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", raw).strip("_")
    return cooked[:48] or hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _contains_alias(text: str, alias: str) -> bool:
    hay = text.casefold()
    needle = _clean(alias).casefold()
    if not needle:
        return False
    if re.search(r"[a-z0-9]", needle):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", hay))
    return needle in hay


def _first_alias(text: str, aliases: Iterable[str]) -> str | None:
    ordered = sorted({_clean(a) for a in aliases if _clean(a)}, key=len, reverse=True)
    for alias in ordered:
        if _contains_alias(text, alias):
            return alias
    return None


def _evidence(value: Any, source: str, excerpt: str, confidence: str = "high") -> dict[str, Any]:
    return {
        "value": value,
        "source": source,
        "confidence": confidence,
        "raw_excerpt": _clean(excerpt)[:240],
    }


# Locked lexicon V1 + exact project aliases observed in the reviewed 100-post
# zufang555 corpus. Corpus extensions are exact-token matches only; they never
# infer a project from a generic district, developer slogan or property type.
_PROJECT_SPECS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("peng_huoth", "炳发城", ("炳发城", "炳发", "Borey Peng Huoth", "Peng Huoth")),
    ("orkide", "奥凯德", ("奥凯德小区", "奥凯德")),
    ("aston_riverside", "滨江雅诗顿", ("滨江雅诗顿",)),
    ("rose_riverside", "玫瑰滨江", ("玫瑰滨江",)),
    ("koh_norea", "金银岛", ("金银岛",)),
    ("shangri_la", "香格里拉", ("香格里拉",)),
    ("sky_villa", "Sky Villa", ("Sky Villa",)),
    ("rf_city", "富力城", ("富力中心城", "金边中心城", "R&F City", "RF City", "富力城", "富力")),
    ("prince_central", "太子中央", ("太子中央广场", "Prince Central Plaza", "Prince Central", "太子中央", "太子广场")),
    ("olympia_city", "奥林匹亚城", ("奥林匹亚城", "Olympia City", "奥林匹亚")),
    ("chip_mong", "集茂", ("Chip Mong", "Chipmong", "集茂")),
    ("ming_shi_cheng", "名士城", ("名士城",)),
    ("the_bridge", "The Bridge", ("The Bridge", "桥牌")),
    ("the_peak", "The Peak", ("The Peak",)),
    ("wells", "威尔斯", ("威尔斯公馆", "威尔斯")),
    ("prince_huan_yu", "寰宇", ("太子寰宇", "寰宇中心", "寰宇")),
    ("ming_di", "名邸", ("名邸",)),
    ("guo_jin", "国金", ("金边首都国金", "首都国金", "国金")),
    ("happiness_apartment", "幸福公寓", ("幸福公寓",)),
    ("agile", "雅居乐", ("Agile 雅居乐", "雅居乐", "Agile")),
    ("picasso_city_garden", "Picasso City Garden", ("Picasso City Garden", "Picasso")),
    ("platinum_bay", "白金湾", ("白金湾",)),
    ("yuetai", "粤泰", ("粤泰",)),
    ("prince_international_plaza", "太子国际广场", ("太子国际广场",)),
    ("phnom_penh_central_plaza", "金边中央广场", ("金边中央广场",)),
    ("amethyst_one", "紫晶壹号", ("紫晶壹号",)),
    ("diamond_mansion", "钻石名邸", ("钻石名邸",)),
    ("chip_mong_6a", "集茂城6A", ("集茂城6A",)),
)

_ROAD_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("一号路", ("National Road 1", "一号公路", "1号公路", "一号路", "1号路", "NR1")),
    ("6号路", ("National Road 6", "6A路", "6号路", "6A")),
    ("洪森大道", ("Hun Sen Boulevard", "Hun Sen Blvd", "洪森大道")),
    ("60米大道", ("60米大道", "60米路", "60米")),
    ("271路", ("271路", "271")),
    ("598路", ("598路",)),
    ("50米路", ("50米路",)),
)

# These are public search/location concepts, not physical administrative areas.
# "nearby" wording is preserved in display instead of being silently stripped.
_MARKET_SPECS: tuple[tuple[str, str, tuple[str, ...], bool], ...] = (
    ("金街", "金街附近", ("金街附近",), True),
    ("金街", "金街", ("金街公寓", "金街周边", "金街"), False),
    ("永旺3", "永旺3附近", ("永旺3附近",), True),
    ("永旺3", "永旺3", ("永旺三", "永旺3", "AEON3"), False),
    ("老机场", "老机场附近", ("老机场附近",), True),
    ("老机场", "老机场", ("老机场", "旧机场"), False),
    ("金界", "金界", ("NagaWorld", "Naga", "金界"), False),
    ("新机场", "新机场附近", ("新机场附近",), True),
    ("新机场", "新机场", ("新国际机场", "德崇机场", "新机场"), False),
)

_PROPERTY_SPECS: tuple[tuple[str, str | None, str, tuple[str, ...]], ...] = (
    ("别墅", "双拼别墅", "双拼别墅", ("双拼别墅", "twin villa")),
    ("别墅", "独栋别墅", "独栋别墅", ("独栋别墅", "独立别墅", "泳池别墅")),
    ("排屋", "联排别墅", "联排别墅", ("联排别墅", "townhouse", "link villa")),
    ("排屋", None, "排屋", ("排屋",)),
    ("公寓", "大平层", "大平层", ("大平层公寓", "大平层")),
    ("公寓", "复式", "复式", ("复式公寓", "复式", "duplex")),
    ("公寓", "顶层公寓", "顶层公寓", ("顶层公寓", "penthouse")),
    ("公寓", "Studio", "Studio", ("单间公寓", "studio apartment", "studio")),
    ("公寓", None, "公寓", ("服务式公寓", "公寓", "apartment", "condo")),
    ("办公室", None, "写字楼", ("写字楼", "办公室", "office")),
    ("商铺", None, "商铺", ("商铺", "店面", "门面")),
    ("整栋", None, "整栋", ("整栋公寓", "整栋")),
    ("仓库", None, "仓库", ("仓库", "warehouse")),
    ("厂房", None, "厂房", ("厂房", "工厂", "factory")),
)


def _parse_money(raw: str) -> int | None:
    value = _clean(raw).replace(",", "").casefold()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(k)?", value)
    if not match:
        return None
    number = float(match.group(1)) * (1000 if match.group(2) else 1)
    if number < 50 or number > 2_000_000:
        return None
    return int(number)


def _extract_explicit_rent(text: str) -> tuple[int | None, str | None]:
    if re.search(
        r"(?:出租价格|出租情况|租金价格|租金|月租|特价出租|优惠出租)[^0-9\n]{0,12}"
        r"\d[\d,]*(?:\.\d+)?\s*(?:\$|美元|美金|usd)?\s*[-–—~至]\s*(?:\$|美元|美金|usd)?\s*\d",
        text,
        flags=re.I,
    ):
        return None, "rent_range_requires_review"

    patterns = (
        r"(?:出租价格|租金价格|现租金|现月租|月租|租金|出租情况|出租价)\s*[:：]?\s*"
        r"(?:\$|usd|美金|美元|💵|💰)?\s*(\d[\d,]*(?:\.\d+)?\s*k?)"
        r"\s*(?:美元|美金|usd|\$)?\s*(?:/月|每月|/month|per month)?",
        r"(?:特价|优惠)\s*出租\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*"
        r"(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$)?",
        r"出租\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*"
        r"(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$)\b",
    )
    values: list[tuple[int, str]] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.I):
            parsed = _parse_money(match.group(1))
            if parsed is not None:
                values.append((parsed, match.group(0)))
    unique = {value for value, _ in values}
    if len(unique) == 1:
        value = next(iter(unique))
        excerpt = next(ex for val, ex in values if val == value)
        return value, excerpt
    if len(unique) > 1:
        return None, "conflicting_rental_price"
    return None, None


def _extract_explicit_sale_price(text: str) -> tuple[int | None, str | None]:
    patterns = (
        r"(?:出售价格|售价|销售价格|卖价)\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*"
        r"(\d[\d,]*(?:\.\d+)?\s*k?)\s*(?:美元|美金|usd|\$)?",
        r"(?:出售|急售)\s*[:：]?\s*(?:\$|usd|美金|美元)\s*"
        r"(\d[\d,]*(?:\.\d+)?\s*k?)",
    )
    values: list[tuple[int, str]] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.I):
            parsed = _parse_money(match.group(1))
            if parsed is not None:
                values.append((parsed, match.group(0)))
    unique = {value for value, _ in values}
    if len(unique) == 1:
        value = next(iter(unique))
        excerpt = next(ex for val, ex in values if val == value)
        return value, excerpt
    return None, "conflicting_sale_price" if len(unique) > 1 else None


def _inventory_scrub(text: str) -> str:
    """Remove menu/inventory clauses before fallback property classification."""
    kept: list[str] = []
    inventory = re.compile(
        r"(?:户型选择|户型可选|单间\s*[/／|、]\s*1房|1房\s*[/／|、]\s*2房|"
        r"大量房源|多套房源|都有|均有|多户型可选)",
        flags=re.I,
    )
    for line in str(text or "").splitlines():
        if inventory.search(line):
            marker = re.search(r"(?:户型选择|户型可选|大量房源|多套房源|均有|都有|多户型可选)", line, flags=re.I)
            prefix = line[: marker.start()] if marker else ""
            if prefix.strip():
                kept.append(prefix)
            continue
        kept.append(line)
    return "\n".join(kept)


def _fallback_property(text: str) -> tuple[str, str | None, str, str] | None:
    scoped = _inventory_scrub(text)
    hits: list[tuple[str, str | None, str, str, int]] = []
    for family, subtype, display, aliases in _PROPERTY_SPECS:
        alias = _first_alias(scoped, aliases)
        if alias:
            hits.append((family, subtype, display, alias, len(alias)))
    if not hits:
        return None
    families = {item[0] for item in hits}
    if len(families) != 1:
        return None
    hits.sort(key=lambda item: -item[4])
    family, subtype, display, alias, _ = hits[0]
    return family, subtype, display, alias


def _append_evidence(facts: dict[str, Any], field: str, item: dict[str, Any]) -> None:
    evidence = facts.setdefault("evidence", {})
    values = evidence.setdefault(field, [])
    if item not in values:
        values.append(item)


def _append_candidate_flag(facts: dict[str, Any], flag: str) -> None:
    flags = facts.setdefault("candidate_flags", [])
    if flag not in flags:
        flags.append(flag)


def _enrich_road(facts: dict[str, Any], text: str) -> None:
    hits: list[tuple[str, str, int]] = []
    for standard, aliases in _ROAD_SPECS:
        alias = _first_alias(text, aliases)
        if alias:
            hits.append((standard, alias, len(alias)))
    if not hits:
        return
    facts["road_tokens"] = list(dict.fromkeys(item[0] for item in hits))
    if not facts.get("road"):
        facts["road"] = hits[0][0]
    for standard, alias, _ in hits:
        _append_evidence(facts, "road", _evidence(standard, "v12_explicit_road_alias", alias))


def _enrich_market(facts: dict[str, Any], text: str) -> None:
    keys = list(facts.get("market_location_keys") or [])
    displays = list(facts.get("market_location_displays") or [])
    nearby = bool(facts.get("nearby"))
    for key, display, aliases, is_nearby in _MARKET_SPECS:
        alias = _first_alias(text, aliases)
        if not alias:
            continue
        if key not in keys:
            keys.append(key)
            displays.append(display)
            _append_evidence(facts, "market_location_keys", _evidence(key, "v12_market_alias", alias))
        elif is_nearby:
            try:
                idx = keys.index(key)
                if idx < len(displays):
                    displays[idx] = display
            except ValueError:
                pass
        nearby = nearby or is_nearby
        if key == "金街":
            facts["location_anchor"] = "金街"
    facts["market_location_keys"] = keys
    facts["market_location_displays"] = displays
    if nearby:
        facts["nearby"] = True


def _enrich_project(facts: dict[str, Any], text: str) -> None:
    if facts.get("project_name"):
        return
    matches: list[tuple[str, str, str, int]] = []
    for key, display, aliases in _PROJECT_SPECS:
        alias = _first_alias(text, aliases)
        if alias:
            matches.append((key, display, alias, len(alias)))
    if not matches:
        return
    matches.sort(key=lambda item: -item[3])
    best_len = matches[0][3]
    top = [item for item in matches if item[3] == best_len]
    if len({item[0] for item in top}) != 1:
        _append_candidate_flag(facts, "ambiguous_project")
        return
    key, display, alias, _ = top[0]
    facts["project_key"] = key
    facts["project_name"] = display
    facts["community_name"] = display
    facts["project_alias"] = alias if re.search(r"[A-Za-z]", alias) else facts.get("project_alias")
    if display == "炳发城":
        facts["project_group"] = "炳发城"
    _append_evidence(facts, "project", _evidence(display, "v12_explicit_project_alias", alias))


def _enrich_property(facts: dict[str, Any], text: str) -> None:
    if facts.get("property_type") not in (None, "", "未知"):
        return
    resolved = _fallback_property(text)
    if not resolved:
        return
    family, subtype, display, alias = resolved
    facts["property_type"] = family
    facts["property_subtype"] = subtype
    facts["property_type_display"] = display
    facts["property_type_status"] = "confirmed"
    _append_evidence(facts, "property_type", _evidence(display, "v12_explicit_property_alias", alias))


def _enrich_prices(facts: dict[str, Any], text: str) -> None:
    if facts.get("monthly_rent_usd") in (None, "") and facts.get("deal_type") in {"rent", "mixed"}:
        rent, excerpt = _extract_explicit_rent(text)
        if rent is not None:
            facts["monthly_rent_usd"] = rent
            facts["price_status"] = "confirmed"
            _append_evidence(facts, "monthly_rent_usd", _evidence(rent, "v12_explicit_monthly_rent", excerpt or str(rent)))
        elif excerpt:
            _append_candidate_flag(facts, excerpt)
    sale, sale_excerpt = _extract_explicit_sale_price(text)
    if sale is not None:
        facts["sale_price_usd"] = sale
        _append_evidence(facts, "sale_price_usd", _evidence(sale, "v12_explicit_sale_price", sale_excerpt or str(sale)))
    elif sale_excerpt:
        _append_candidate_flag(facts, sale_excerpt)


def _enrich_layout_details(facts: dict[str, Any], text: str) -> None:
    plus = re.search(r"(?<!\d)(\d{1,2})\s*(?:房)?\s*\+\s*(\d{1,2})\s*房", text, flags=re.I)
    if plus:
        bedrooms = int(plus.group(1))
        extra = int(plus.group(2))
        if facts.get("bedrooms") is None:
            facts["bedrooms"] = bedrooms
        facts["extra_rooms"] = extra
        _append_evidence(facts, "extra_rooms", _evidence(extra, "v12_explicit_plus_room", plus.group(0)))

    if not facts.get("floor"):
        floor_match = re.search(
            r"(?:楼层情况|所在楼层|楼层)\s*[:：]?\s*(\d{1,2})(?!\d)\s*(?:楼|层|F)?",
            text,
            flags=re.I,
        )
        if floor_match:
            facts["floor"] = str(int(floor_match.group(1)))
            _append_evidence(facts, "floor", _evidence(facts["floor"], "v12_explicit_floor", floor_match.group(0)))


def _enrich_living_terms(facts: dict[str, Any], text: str) -> None:
    if "拎包入住" in text:
        facts["move_in_ready"] = True
        _append_evidence(facts, "move_in_ready", _evidence(True, "v12_explicit_move_in_ready", "拎包入住"))

    furniture_rules = (
        ("家具齐全", ("家具家电齐全", "全套家具齐全", "家具齐全", "全套家具", "全家具", "fully furnished", "full furnished")),
        ("部分家具", ("部分家具", "semi furnished", "partly furnished")),
        ("无家具", ("无家具", "空房", "unfurnished")),
    )
    if not facts.get("furniture_status"):
        for standard, aliases in furniture_rules:
            alias = _first_alias(text, aliases)
            if alias:
                facts["furniture_status"] = standard
                _append_evidence(facts, "furniture_status", _evidence(standard, "v12_explicit_furniture", alias))
                break

    if not facts.get("contract_term_display"):
        if re.search(r"(?:长租|long\s+term)", text, flags=re.I):
            facts["contract_term_display"] = "长租"
            _append_evidence(facts, "contract_term_display", _evidence("长租", "v12_explicit_lease_term", "长租"))
        elif re.search(r"(?:短租|short\s+term)", text, flags=re.I):
            facts["contract_term_display"] = "短租"
            _append_evidence(facts, "contract_term_display", _evidence("短租", "v12_explicit_lease_term", "短租"))

    if facts.get("deposit_months") is None:
        dep = re.search(r"(?:押金情况|押金)\s*[:：]?\s*(\d{1,2})\s*个?月", text, flags=re.I)
        if dep:
            facts["deposit_months"] = int(dep.group(1))
            _append_evidence(facts, "deposit_months", _evidence(facts["deposit_months"], "v12_explicit_deposit_months", dep.group(0)))

    amenities: list[str] = list(facts.get("amenities") or [])
    amenity_rules = (
        ("泳池", ("游泳池", "泳池", "pool")),
        ("健身房", ("健身房", "gym", "fitness")),
        ("停车", ("停车位", "停车", "parking")),
        ("电梯", ("电梯", "elevator", "lift")),
        ("24H安保", ("24小时安保", "security")),
        ("阳台", ("大阳台", "阳台", "balcony")),
        ("花园", ("花园", "garden")),
        ("桑拿", ("桑拿", "sauna")),
        ("备用发电", ("发电机", "generator")),
    )
    for standard, aliases in amenity_rules:
        alias = _first_alias(text, aliases)
        if alias and standard not in amenities:
            amenities.append(standard)
            _append_evidence(facts, "amenities", _evidence(standard, "v12_explicit_amenity", alias))
    facts["amenities"] = amenities

    if re.search(r"(?:宠物可谈|pets?\s+negotiable)", text, flags=re.I):
        facts["pet_policy"] = "宠物可谈"
        _append_evidence(facts, "pet_policy", _evidence("宠物可谈", "v12_explicit_pet_policy", "宠物可谈"))
    elif re.search(r"(?:允许宠物|可养宠物|pet\s+friendly)", text, flags=re.I):
        facts["pet_policy"] = "可养宠物"
        _append_evidence(facts, "pet_policy", _evidence("可养宠物", "v12_explicit_pet_policy", "可养宠物"))


def _rebuild_derived_fields(facts: dict[str, Any]) -> None:
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
    facts["display_title"] = canonical_core._display_title(
        facts.get("project_name"),
        title_location,
        facts.get("layout"),
        facts.get("property_type_display") or "未知",
    )
    quality = canonical_core._quality(facts)
    if facts.get("deal_type") == "sale" and facts.get("sale_price_usd"):
        for key in ("hard_flags", "blocking_flags", "all_flags"):
            quality[key] = [flag for flag in quality.get(key, []) if flag != "missing_price"]
        quality["score"] = max(
            0,
            100
            - 30 * len(quality.get("hard_flags", []))
            - 12 * len(quality.get("review_flags", []))
            - 4 * len(quality.get("warning_flags", [])),
        )
    facts["quality"] = quality
    facts.pop("canonical_facts_hash", None)
    facts["canonical_facts_hash"] = canonical_core._stable_hash(facts)


def enrich_canonical_facts_v12(
    facts: dict[str, Any],
    *,
    raw_text: str,
    sanitized_text: str | None = None,
) -> dict[str, Any]:
    text = str(sanitized_text or raw_text or "")
    _enrich_road(facts, text)
    _enrich_market(facts, text)
    _enrich_project(facts, text)
    _enrich_property(facts, text)
    _enrich_prices(facts, text)
    _enrich_layout_details(facts, text)
    _enrich_living_terms(facts, text)
    _rebuild_derived_fields(facts)
    return facts


def canonicalize_source_v12(
    raw_text: str,
    sanitized_text: str | None = None,
    source_identity: dict[str, Any] | None = None,
    media_summary: dict[str, Any] | None = None,
    manual_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    facts = canonicalize_source(
        raw_text=raw_text,
        sanitized_text=sanitized_text,
        source_identity=source_identity,
        media_summary=media_summary,
        manual_overrides=manual_overrides,
    )
    return enrich_canonical_facts_v12(
        facts,
        raw_text=raw_text,
        sanitized_text=sanitized_text,
    )


__all__ = ["canonicalize_source_v12", "enrich_canonical_facts_v12"]
