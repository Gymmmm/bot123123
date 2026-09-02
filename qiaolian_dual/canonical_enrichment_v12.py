"""Conservative enrichment for the existing canonical parser.

The existing canonicalize_source() remains the fact engine. This layer only
fills explicitly evidenced gaps found in the reviewed zufang555 corpus and
locked Qiaolian lexicon. It never uses templates/captions as fact input.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

from qiaolian_dual import canonical_facts as core
from qiaolian_dual.canonical_facts import canonicalize_source
from qiaolian_dual.listing_taxonomy import public_location_from_fields


PROJECTS = (
    ("peng_huoth", "炳发城", ("Borey Peng Huoth", "Peng Huoth", "炳发城", "炳发")),
    ("orkide", "奥凯德", ("奥凯德小区", "奥凯德")),
    ("aston_riverside", "滨江雅诗顿", ("滨江雅诗顿",)),
    ("rose_riverside", "玫瑰滨江", ("玫瑰滨江",)),
    ("koh_norea", "金银岛", ("金银岛",)),
    ("shangri_la", "香格里拉", ("香格里拉",)),
    ("sky_villa", "Sky Villa", ("Sky Villa",)),
    ("rf_city", "富力城", ("富力中心城", "金边中心城", "R&F City", "RF City", "富力城", "富力")),
    ("prince_central", "太子中央", ("太子中央广场", "Prince Central Plaza", "Prince Central", "太子中央", "太子广场")),
    ("olympia_city", "奥林匹亚城", ("奥林匹亚城", "Olympia City", "奥林匹亚")),
    ("chip_mong_6a", "集茂城6A", ("集茂城6A",)),
    ("chip_mong", "集茂", ("Chip Mong", "Chipmong", "集茂")),
    ("ming_shi_cheng", "名士城", ("名士城",)),
    ("the_bridge", "The Bridge", ("The Bridge", "桥牌")),
    ("the_peak", "The Peak", ("The Peak",)),
    ("wells", "威尔斯", ("威尔斯公馆", "威尔斯")),
    ("huanyu", "寰宇", ("太子寰宇", "寰宇中心", "寰宇")),
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
)

ROADS = (
    ("一号路", ("National Road 1", "一号公路", "1号公路", "一号路", "1号路", "NR1")),
    ("6号路", ("National Road 6", "6A路", "6号路", "6A")),
    ("洪森大道", ("Hun Sen Boulevard", "Hun Sen Blvd", "洪森大道")),
    ("60米大道", ("60米大道", "60米路", "60米")),
    ("271路", ("271路", "271")),
    ("598路", ("598路",)),
    ("50米路", ("50米路",)),
)

MARKETS = (
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

PROPERTIES = (
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


def _clean(v: object) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def _has(text: str, alias: str) -> bool:
    needle = _clean(alias).casefold()
    hay = text.casefold()
    if re.search(r"[a-z0-9]", needle):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", hay))
    return bool(needle and needle in hay)


def _alias(text: str, aliases: Iterable[str]) -> str | None:
    for a in sorted({_clean(x) for x in aliases if _clean(x)}, key=len, reverse=True):
        if _has(text, a):
            return a
    return None


def _ev(value: Any, source: str, excerpt: str) -> dict[str, Any]:
    return {"value": value, "source": source, "confidence": "high", "raw_excerpt": _clean(excerpt)[:240]}


def _add_ev(facts: dict[str, Any], field: str, value: Any, source: str, excerpt: str) -> None:
    facts.setdefault("evidence", {}).setdefault(field, []).append(_ev(value, source, excerpt))


def _flag(facts: dict[str, Any], flag: str) -> None:
    if flag not in facts.setdefault("candidate_flags", []):
        facts["candidate_flags"].append(flag)


def _money(raw: str) -> int | None:
    m = re.search(r"(\d+(?:\.\d+)?)\s*(k|万)?", _clean(raw).replace(",", "").casefold())
    if not m:
        return None
    mult = 1000 if m.group(2) == "k" else (10000 if m.group(2) == "万" else 1)
    value = float(m.group(1)) * mult
    return int(value) if 50 <= value <= 2_000_000 else None


def _money_candidates(text: str, patterns: tuple[str, ...]) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.I):
            value = _money(m.group(1))
            if value is not None:
                out.append((value, m.group(0)))
    return out


def _rent(text: str) -> tuple[int | None, str | None]:
    if re.search(
        r"(?:出租价格|出租情况|租金价格|租金|月租|特价出租|优惠出租)[^0-9\n]{0,12}"
        r"\d[\d,]*(?:\.\d+)?\s*(?:\$|美元|美金|usd)?\s*[-–—~至]\s*(?:\$|美元|美金|usd)?\s*\d",
        text, flags=re.I,
    ):
        return None, "rent_range_requires_review"
    num = r"(\d[\d,]*(?:\.\d+)?\s*(?:k|万)?)"
    values = _money_candidates(text, (
        rf"(?:出租价格|租金价格|现租金|现月租|月租|租金|出租情况|出租价)\s*[:：]?\s*(?:\$|usd|美金|美元|💵|💰)?\s*{num}\s*(?:美元|美金|usd|\$)?\s*(?:/月|每月|/month|per month)?",
        rf"(?:特价|优惠)\s*出租\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*{num}\s*(?:美元|美金|usd|\$)?",
        rf"出租\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*{num}\s*(?:美元|美金|usd|\$)\b",
    ))
    unique = {v for v, _ in values}
    if len(unique) == 1:
        v = next(iter(unique))
        return v, next(e for x, e in values if x == v)
    return None, "conflicting_rental_price" if len(unique) > 1 else None


def _sale(text: str) -> tuple[int | None, str | None]:
    num = r"(\d[\d,]*(?:\.\d+)?\s*(?:k|万)?)"
    values = _money_candidates(text, (
        rf"(?:出售价格|售价|销售价格|卖价)\s*[:：]?\s*(?:\$|usd|美金|美元)?\s*{num}\s*(?:美元|美金|usd|\$)?",
        rf"(?:出售|急售)\s*[:：]?\s*(?:\$|usd|美金|美元)\s*{num}",
    ))
    unique = {v for v, _ in values}
    if len(unique) == 1:
        v = next(iter(unique))
        return v, next(e for x, e in values if x == v)
    return None, "conflicting_sale_price" if len(unique) > 1 else None


def _inventory_scrub(text: str) -> str:
    marker = re.compile(r"(?:户型选择|户型可选|大量房源|多套房源|多户型可选|均有|都有)", re.I)
    menu = re.compile(r"(?:单间\s*[/／|、]\s*1房|1房\s*[/／|、]\s*2房)", re.I)
    kept: list[str] = []
    for line in text.splitlines():
        m = marker.search(line)
        if m or menu.search(line):
            prefix = line[:m.start()] if m else ""
            if prefix.strip():
                kept.append(prefix)
        else:
            kept.append(line)
    return "\n".join(kept)


def _enrich_locations(facts: dict[str, Any], text: str) -> None:
    road_hits: list[str] = []
    for standard, aliases in ROADS:
        hit = _alias(text, aliases)
        if hit:
            road_hits.append(standard)
            _add_ev(facts, "road", standard, "v12_explicit_road_alias", hit)
    if road_hits:
        facts["road_tokens"] = list(dict.fromkeys(road_hits))
        facts.setdefault("road", road_hits[0])

    keys = list(facts.get("market_location_keys") or [])
    displays = list(facts.get("market_location_displays") or [])
    for key, display, aliases, nearby in MARKETS:
        hit = _alias(text, aliases)
        if not hit:
            continue
        if key not in keys:
            keys.append(key)
            displays.append(display)
            _add_ev(facts, "market_location_keys", key, "v12_market_alias", hit)
        elif nearby:
            idx = keys.index(key)
            if idx < len(displays):
                displays[idx] = display
        if nearby:
            facts["nearby"] = True
        if key == "金街":
            facts["location_anchor"] = "金街"
    facts["market_location_keys"] = keys
    facts["market_location_displays"] = displays


def _enrich_project(facts: dict[str, Any], text: str) -> None:
    if facts.get("project_name"):
        return
    hits: list[tuple[str, str, str, int]] = []
    for key, display, aliases in PROJECTS:
        hit = _alias(text, aliases)
        if hit:
            hits.append((key, display, hit, len(hit)))
    if not hits:
        return
    hits.sort(key=lambda x: -x[3])
    best = [x for x in hits if x[3] == hits[0][3]]
    if len({x[0] for x in best}) != 1:
        _flag(facts, "ambiguous_project")
        return
    key, display, hit, _ = best[0]
    facts.update(project_key=key, project_name=display, community_name=display)
    if re.search(r"[A-Za-z]", hit):
        facts["project_alias"] = hit
    if display == "炳发城":
        facts["project_group"] = "炳发城"
    _add_ev(facts, "project", display, "v12_explicit_project_alias", hit)


def _enrich_property(facts: dict[str, Any], text: str) -> None:
    if facts.get("property_type") not in (None, "", "未知"):
        return
    scoped = _inventory_scrub(text)
    hits: list[tuple[str, str | None, str, str, int]] = []
    for family, subtype, display, aliases in PROPERTIES:
        hit = _alias(scoped, aliases)
        if hit:
            hits.append((family, subtype, display, hit, len(hit)))
    if not hits or len({x[0] for x in hits}) != 1:
        return
    hits.sort(key=lambda x: -x[4])
    family, subtype, display, hit, _ = hits[0]
    facts.update(
        property_type=family,
        property_subtype=subtype,
        property_type_display=display,
        property_type_status="confirmed",
    )
    _add_ev(facts, "property_type", display, "v12_explicit_property_alias", hit)


def _enrich_prices(facts: dict[str, Any], text: str) -> None:
    if facts.get("monthly_rent_usd") in (None, "") and facts.get("deal_type") in {"rent", "mixed"}:
        value, excerpt = _rent(text)
        if value is not None:
            facts["monthly_rent_usd"] = value
            facts["price_status"] = "confirmed"
            _add_ev(facts, "monthly_rent_usd", value, "v12_explicit_monthly_rent", excerpt or str(value))
        elif excerpt:
            _flag(facts, excerpt)
    value, excerpt = _sale(text)
    if value is not None:
        facts["sale_price_usd"] = value
        _add_ev(facts, "sale_price_usd", value, "v12_explicit_sale_price", excerpt or str(value))
    elif excerpt:
        _flag(facts, excerpt)


def _enrich_details(facts: dict[str, Any], text: str) -> None:
    plus = re.search(r"(?<!\d)(\d{1,2})\s*(?:房)?\s*\+\s*(\d{1,2})\s*房", text)
    if plus:
        facts.setdefault("bedrooms", int(plus.group(1)))
        facts["extra_rooms"] = int(plus.group(2))
        _add_ev(facts, "extra_rooms", facts["extra_rooms"], "v12_explicit_plus_room", plus.group(0))

    if not facts.get("floor"):
        m = re.search(r"(?:楼层情况|所在楼层|楼层)\s*[:：]?\s*(\d{1,2})(?!\d)\s*(?:楼|层|F)?", text, re.I)
        if m:
            facts["floor"] = str(int(m.group(1)))
            _add_ev(facts, "floor", facts["floor"], "v12_explicit_floor", m.group(0))

    if "拎包入住" in text:
        facts["move_in_ready"] = True
        _add_ev(facts, "move_in_ready", True, "v12_explicit_move_in_ready", "拎包入住")

    furniture = (
        ("家具齐全", ("家具家电齐全", "全套家具齐全", "家具齐全", "全套家具", "全家具", "fully furnished", "full furnished")),
        ("部分家具", ("部分家具", "semi furnished", "partly furnished")),
        ("无家具", ("无家具", "空房", "unfurnished")),
    )
    if not facts.get("furniture_status"):
        for standard, aliases in furniture:
            hit = _alias(text, aliases)
            if hit:
                facts["furniture_status"] = standard
                _add_ev(facts, "furniture_status", standard, "v12_explicit_furniture", hit)
                break

    if not facts.get("contract_term_display"):
        if re.search(r"(?:长租|long\s+term)", text, re.I):
            facts["contract_term_display"] = "长租"
            _add_ev(facts, "contract_term_display", "长租", "v12_explicit_lease_term", "长租")
        elif re.search(r"(?:短租|short\s+term)", text, re.I):
            facts["contract_term_display"] = "短租"
            _add_ev(facts, "contract_term_display", "短租", "v12_explicit_lease_term", "短租")

    if facts.get("deposit_months") is None:
        m = re.search(r"(?:押金情况|押金)\s*[:：]?\s*(\d{1,2})\s*个?月", text, re.I)
        if m:
            facts["deposit_months"] = int(m.group(1))
            _add_ev(facts, "deposit_months", facts["deposit_months"], "v12_explicit_deposit_months", m.group(0))

    amenities = list(facts.get("amenities") or [])
    for standard, aliases in (
        ("泳池", ("游泳池", "泳池", "pool")),
        ("健身房", ("健身房", "gym", "fitness")),
        ("停车", ("停车位", "停车", "parking")),
        ("电梯", ("电梯", "elevator", "lift")),
        ("24H安保", ("24小时安保", "security")),
        ("阳台", ("大阳台", "阳台", "balcony")),
        ("花园", ("花园", "garden")),
        ("桑拿", ("桑拿", "sauna")),
        ("备用发电", ("发电机", "generator")),
    ):
        hit = _alias(text, aliases)
        if hit and standard not in amenities:
            amenities.append(standard)
            _add_ev(facts, "amenities", standard, "v12_explicit_amenity", hit)
    facts["amenities"] = amenities

    if re.search(r"(?:宠物可谈|pets?\s+negotiable)", text, re.I):
        facts["pet_policy"] = "宠物可谈"
        _add_ev(facts, "pet_policy", "宠物可谈", "v12_explicit_pet_policy", "宠物可谈")
    elif re.search(r"(?:允许宠物|可养宠物|pet\s+friendly)", text, re.I):
        facts["pet_policy"] = "可养宠物"
        _add_ev(facts, "pet_policy", "可养宠物", "v12_explicit_pet_policy", "可养宠物")


def _rebuild(facts: dict[str, Any]) -> None:
    key, display, level = public_location_from_fields(
        canonical_area_key=facts.get("canonical_area_key"),
        canonical_area_display=facts.get("canonical_area_display"),
        area_status=facts.get("area_status"),
        market_location_keys=facts.get("market_location_keys"),
        market_location_displays=facts.get("market_location_displays"),
        project_key=facts.get("project_key"),
        project_name=facts.get("project_name"),
    )
    facts["public_location_key"] = key
    facts["public_location_display"] = display
    facts["publication_location_level"] = level
    title_location = facts.get("canonical_area_display") or (display if not facts.get("project_name") else None)
    facts["display_title"] = core._display_title(
        facts.get("project_name"), title_location, facts.get("layout"),
        facts.get("property_type_display") or "未知",
    )
    quality = core._quality(facts)
    if facts.get("deal_type") == "sale" and facts.get("sale_price_usd"):
        for name in ("hard_flags", "blocking_flags", "all_flags"):
            quality[name] = [x for x in quality.get(name, []) if x != "missing_price"]
        quality["score"] = max(
            0,
            100 - 30 * len(quality["hard_flags"])
            - 12 * len(quality["review_flags"])
            - 4 * len(quality["warning_flags"]),
        )
    facts["quality"] = quality
    facts.pop("canonical_facts_hash", None)
    facts["canonical_facts_hash"] = core._stable_hash(facts)


def enrich_canonical_facts_v12(
    facts: dict[str, Any], *, raw_text: str, sanitized_text: str | None = None
) -> dict[str, Any]:
    text = str(sanitized_text or raw_text or "")
    _enrich_locations(facts, text)
    _enrich_project(facts, text)
    _enrich_property(facts, text)
    _enrich_prices(facts, text)
    _enrich_details(facts, text)
    _rebuild(facts)
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
        facts, raw_text=raw_text, sanitized_text=sanitized_text
    )


__all__ = ["canonicalize_source_v12", "enrich_canonical_facts_v12"]
