"""Single taxonomy source for listing classification.

Taxonomy classifies *what source text explicitly says*.  It never produces a
price, a layout or formatted copy; those belong to the canonical fact layer.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from .phnom_penh_location_registry import (
    MANUAL_MARKET_ALIASES as REGISTRY_MANUAL_MARKET_ALIASES,
    MARKET_LOCATIONS as REGISTRY_MARKET_LOCATIONS,
    PHYSICAL_AREAS as REGISTRY_PHYSICAL_AREAS,
    PROJECT_IDENTITIES as LEGACY_REGISTRY_PROJECTS,
    project_by_key as legacy_project_by_key,
)
from .project_registry import (
    ACTIVE_PROJECTS as REGISTRY_V3_PROJECTS,
    FAMILIES as REGISTRY_V3_FAMILIES,
    canonical_location_projection as registry_v3_location_projection,
    project_by_key as registry_v3_project_by_key,
    project_identity_aliases as registry_v3_identity_aliases,
    resolve_project as resolve_project_v3,
)


@dataclass(frozen=True)
class PhysicalArea:
    key: str
    display: str
    level: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class MarketLocation:
    key: str
    display: str
    relation: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class ProjectIdentity:
    key: str
    display: str
    kind: str  # project or brand
    aliases: tuple[str, ...]
    property_family: str | None = None


@dataclass(frozen=True)
class PropertyRule:
    family: str
    subtype: str
    display: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class LocationResolution:
    """One operator-confirmed location resolved from the canonical taxonomy."""

    key: str
    display: str
    kind: str  # physical_area or market_location
    publication_level: str
    canonical_area_level: str | None = None


# Project, GEO and renter-facing market aliases come from one authoritative
# Phnom Penh registry. These assignments preserve the historical taxonomy API
# while removing per-consumer copies of the alias data.
PHYSICAL_AREAS = tuple(REGISTRY_PHYSICAL_AREAS)
MARKET_LOCATIONS = tuple(REGISTRY_MARKET_LOCATIONS)
MANUAL_MARKET_ALIASES = dict(REGISTRY_MANUAL_MARKET_ALIASES)
_LEGACY_KEY_TO_V3 = {"prince_huan_yu_center": "prince-universe"}
_LEGACY_PROJECT_BY_V3_KEY = {
    _LEGACY_KEY_TO_V3.get(item.key, item.key.replace("_", "-")): item
    for item in LEGACY_REGISTRY_PROJECTS
    if item.key != "peng_huoth_city"
}
_PARSER_COMPAT_PROJECT_KEYS = {
    item.key.replace("_", "-"): item.key for item in LEGACY_REGISTRY_PROJECTS
}
_PARSER_COMPAT_PROJECT_KEYS["prince-universe"] = "prince_huan_yu_center"

_LEGACY_PROJECT_DISPLAY = {
    "prince-universe": "太子·寰宇中心",
    "sky-villa": "Sky Villa 天空别墅",
    "picasso-city-garden": "Picasso City Garden 毕加索",
    "times-square-9": "Time Square 9",
}
_LEGACY_FAMILY_DISPLAY = {
    "family:peng-huoth": "Peng Huoth",
}

PROJECT_IDENTITIES: tuple[ProjectIdentity, ...] = tuple(
    ProjectIdentity(
        item.key,
        _LEGACY_PROJECT_DISPLAY.get(
            item.key,
            _LEGACY_PROJECT_BY_V3_KEY[item.key].canonical_project_name
            if item.key in _LEGACY_PROJECT_BY_V3_KEY
            else item.name,
        ),
        "project",
        tuple(dict.fromkeys((
            *registry_v3_identity_aliases(item),
            *(_LEGACY_PROJECT_BY_V3_KEY[item.key].project_aliases if item.key in _LEGACY_PROJECT_BY_V3_KEY else ()),
        ))),
        property_family=None,
    )
    for item in REGISTRY_V3_PROJECTS
) + tuple(
    ProjectIdentity(
        family.family_id,
        _LEGACY_FAMILY_DISPLAY.get(family.family_id, family.name_cn or family.name_en),
        "brand",
        tuple(dict.fromkeys((*family.ambiguous_aliases, family.name_cn, family.name_en))),
    )
    for family in REGISTRY_V3_FAMILIES
)


PROPERTY_RULES: tuple[PropertyRule, ...] = (
    PropertyRule("别墅", "双拼别墅", "双拼别墅", ("双拼别墅", "双拼")),
    PropertyRule("别墅", "独栋别墅", "独栋别墅", ("独栋别墅", "泳池独栋", "独栋", "villa", "别墅")),
    PropertyRule("排屋", "联排别墅", "联排别墅", ("联排别墅",)),
    PropertyRule("排屋", "", "排屋", ("联排", "排屋", "townhouse", "row house")),
    PropertyRule("公寓", "大平层", "大平层", ("大平层",)),
    PropertyRule("公寓", "Studio", "Studio", ("studio", "单间公寓")),
    PropertyRule("公寓", "", "公寓", ("服务式公寓", "公寓", "apartment", "condo")),
    PropertyRule("办公室", "", "办公室", ("办公室", "写字楼", "office")),
    PropertyRule("商铺", "商住楼", "商铺", ("商住楼", "商铺", "店面", "shophouse", "shop house")),
    PropertyRule("商业", "", "商业", ("商业楼", "商业", "商场")),
    PropertyRule("整栋", "", "整栋", ("整租楼", "整栋", "building for rent")),
    # Bare "土地" often appears in land-dimension fields.  Require an explicit
    # disposition phrase before classifying a listing itself as land.
    PropertyRule("土地", "", "土地", ("土地出售", "地皮出售", "land for sale")),
)


@dataclass
class TaxonomyResult:
    canonical_area_key: str | None = None
    canonical_area_display: str | None = None
    canonical_area_level: str | None = None
    area_status: str = "unconfirmed"
    market_location_keys: list[str] = field(default_factory=list)
    market_location_displays: list[str] = field(default_factory=list)
    project_key: str | None = None
    project_name: str | None = None
    project_alias: str | None = None
    project_brand_key: str | None = None
    project_brand: str | None = None
    property_type: str = "未知"
    property_subtype: str | None = None
    property_type_display: str = "未知"
    property_type_status: str = "unknown"
    evidence: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def clean_text(value: object) -> str:
    text = str(value or "").replace("\u00a0", " ").replace("\ufeff", " ")
    text = text.replace("－", "-").replace("—", "-").replace("–", "-").replace("_", " ")
    return re.sub(r"\s+", " ", text).strip()


def _exact_alias(value: object, aliases: tuple[str, ...]) -> bool:
    candidate = clean_text(value).casefold()
    if not candidate:
        return False
    return any(candidate == clean_text(alias).casefold() for alias in aliases if clean_text(alias))


def physical_area_by_key(key: object) -> PhysicalArea | None:
    wanted = clean_text(key).casefold()
    return next((item for item in PHYSICAL_AREAS if item.key.casefold() == wanted), None)


def market_location_by_key(key: object) -> MarketLocation | None:
    wanted = clean_text(key).casefold()
    return next((item for item in MARKET_LOCATIONS if item.key.casefold() == wanted), None)


def resolve_location_alias(value: object) -> LocationResolution | None:
    """Resolve an explicit administrator input without promoting markets to areas.

    Physical aliases become Level 2 canonical areas.  Market/search aliases
    become Level 1 public locations and deliberately leave canonical_area_key
    empty.  City-only labels are never accepted.
    """
    candidate = clean_text(value)
    if not candidate or candidate.casefold() in {
        "金边", "金边市", "金边市区", "phnom penh", "phnompenh", "phnom penh city",
        "未知", "区域待确认", "位置待确认",
    }:
        return None
    physical = [
        item for item in PHYSICAL_AREAS
        if _exact_alias(candidate, (item.key, item.display, *item.aliases))
    ]
    if len(physical) == 1:
        item = physical[0]
        return LocationResolution(
            key=item.key,
            display=item.display,
            kind="physical_area",
            publication_level="level_2_physical_confirmed",
            canonical_area_level=item.level,
        )
    if len(physical) > 1:
        return None
    markets = [
        item for item in MARKET_LOCATIONS
        if _exact_alias(
            candidate,
            (item.key, item.display, *item.aliases, *MANUAL_MARKET_ALIASES.get(item.key, ())),
        )
    ]
    if len(markets) != 1:
        return None
    item = markets[0]
    return LocationResolution(
        key=item.key,
        display=item.display,
        kind="market_location",
        publication_level="level_1_market_confirmed",
    )


def _find_alias(text: str, aliases: tuple[str, ...]) -> tuple[str, int] | None:
    normalized = clean_text(text).lower()
    for alias in sorted(aliases, key=len, reverse=True):
        needle = clean_text(alias).lower()
        # Latin aliases are tokens, not arbitrary substrings: "villa" must not
        # classify "Urban Village" as a villa, and "shop" must not match
        # "shopping". Chinese aliases intentionally keep substring matching.
        if re.search(r"[a-z0-9]", needle):
            match = re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", normalized)
            position = match.start() if match else -1
        else:
            position = normalized.find(needle)
        if position >= 0:
            return alias, position
    return None


def _evidence(value: str, source: str, confidence: str, excerpt: str) -> dict[str, Any]:
    return {"value": value, "source": source, "confidence": confidence, "raw_excerpt": clean_text(excerpt)[:240]}


def _address_scopes(text: str) -> list[str]:
    """Return explicitly labelled location/address values; headings alone are not Level 2 proof."""
    scopes: list[str] = []
    for line in str(text or "").splitlines():
        if re.search(r"(?:位置|地址|区域|地段)\s*[:：]", line, flags=re.I):
            scopes.append(line)
    return scopes


def _extract_physical_area(text: str) -> tuple[str | None, str | None, str | None, str, list[dict[str, Any]], list[str]]:
    scopes = _address_scopes(text)
    matches: list[tuple[PhysicalArea, str, int]] = []
    for scope in scopes:
        for item in PHYSICAL_AREAS:
            hit = _find_alias(scope, item.aliases)
            if hit:
                alias, position = hit
                matches.append((item, alias, position))
    if not matches:
        return None, None, None, "unconfirmed", [], []
    matches.sort(key=lambda item: (item[2], -len(item[1])))
    unique = {item.key for item, _alias, _position in matches}
    evidence = [_evidence(item.key, "raw_explicit_physical_address", "high", alias) for item, alias, _position in matches]
    if len(unique) != 1:
        return None, None, None, "conflict", evidence, ["ambiguous_area"]
    item = matches[0][0]
    return item.key, item.display, item.level, "confirmed", evidence, []


_NEARBY_LOCATION_CONTEXT = re.compile(
    r"(?:周边配套|周边|生活配套)\s*[:：][^\n]*",
    flags=re.I,
)


def _inside_nearby_context(text: str, position: int) -> bool:
    return any(match.start() <= position < match.end() for match in _NEARBY_LOCATION_CONTEXT.finditer(str(text or "")))


def _extract_markets(text: str) -> tuple[list[str], list[str], list[dict[str, Any]], list[str]]:
    matches: list[tuple[MarketLocation, str, int]] = []
    for item in MARKET_LOCATIONS:
        hit = _find_alias(text, item.aliases)
        if hit:
            alias, position = hit
            matches.append((item, alias, position))
    # Prefer one semantic location when aliases overlap in the same phrase.
    # Example: ``新机场附近`` matches both 新机场 and 机场附近; those are not two
    # independent location claims. Earliest start wins, with the longer alias
    # winning when both begin at the same position.
    non_overlapping: list[tuple[MarketLocation, str, int]] = []
    for match in sorted(matches, key=lambda value: (value[2], -len(value[1]))):
        start, end = match[2], match[2] + len(match[1])
        if any(start < kept_pos + len(kept_alias) and kept_pos < end for _kept, kept_alias, kept_pos in non_overlapping):
            continue
        non_overlapping.append(match)
    matches = non_overlapping

    relation_priority = {"district": 0, "corridor": 1, "project_market": 2, "nearby": 3}
    matches.sort(key=lambda match: (relation_priority.get(match[0].relation, 9), match[2], -len(match[1])))
    unique: list[tuple[MarketLocation, str, int]] = []
    seen: set[str] = set()
    for match in matches:
        if match[0].key not in seen:
            seen.add(match[0].key)
            unique.append(match)
    keys = [item.key for item, _alias, _position in unique]
    displays = [item.display for item, _alias, _position in unique]
    evidence = [_evidence(item.key, "raw_market_alias", "high", alias) for item, alias, _position in unique]
    flags: list[str] = []
    # Multiple landmarks explicitly labelled as 周边/生活配套 are useful
    # nearby evidence, not competing claims about the listing's own location.
    # Only non-nearby-context matches participate in ambiguity blocking.
    primary = [match for match in unique if not _inside_nearby_context(text, match[2])]
    if primary:
        best_priority = relation_priority.get(primary[0][0].relation, 9)
        if sum(relation_priority.get(item.relation, 9) == best_priority for item, _alias, _position in primary) > 1:
            flags.append("ambiguous_market_location")
    return keys, displays, evidence, flags


_NEARBY_PROJECT_CONTEXT = _NEARBY_LOCATION_CONTEXT

_FALSE_EXPLICIT_PROJECT_VALUES = {"物业费", "网络", "房间保洁", "保洁", "停车位"}


def _project_identity_text(text: str) -> str:
    """Remove nearby/amenity tails before selecting the listing's project.

    Nearby landmarks remain available to the location extractor, but they must
    not compete at project-identity confidence with the listing headline.
    """
    return _NEARBY_PROJECT_CONTEXT.sub("", str(text or ""))


def _extract_project(text: str) -> tuple[str | None, str | None, str | None, str | None, str | None, list[dict[str, Any]], list[str]]:
    project_text = _project_identity_text(text)
    matches: list[tuple[ProjectIdentity, str, int]] = []
    for item in PROJECT_IDENTITIES:
        hit = _find_alias(project_text, item.aliases)
        if hit:
            alias, position = hit
            matches.append((item, alias, position))
    matches.sort(key=lambda item: (item[2], -len(item[1])))
    project_matches = [item for item in matches if item[0].kind == "project"]
    brand_matches = [item for item in matches if item[0].kind == "brand"]
    evidence = [
        _evidence(
            _PARSER_COMPAT_PROJECT_KEYS.get(item.key, item.key) if item.kind == "project" else item.key,
            f"raw_{item.kind}_alias",
            "high",
            alias,
        )
        for item, alias, _position in matches
    ]
    unique_projects = {item.key for item, _alias, _position in project_matches}
    if len(unique_projects) > 1:
        return None, None, None, None, None, evidence, ["ambiguous_project"]
    project = project_matches[0][0] if project_matches else None
    brand = brand_matches[0][0] if brand_matches else None
    project_alias = None
    if project:
        alias_hits = [alias for item, alias, _position in matches if item.key == project.key]
        latin = next((alias for alias in alias_hits if re.search(r"[A-Za-z]", alias)), None)
        project_alias = latin or (alias_hits[0] if alias_hits else None)
    project_key = _PARSER_COMPAT_PROJECT_KEYS.get(project.key, project.key) if project else None
    project_name = project.display if project else None

    # Registry V3 is the sole PROJECT identity resolver. Family/market aliases
    # may return ambiguity, but never silently become a concrete project.
    if not project:
        resolved = resolve_project_v3(project_text)
        if resolved.project_entity_id and not resolved.ambiguity:
            project_key = resolved.project_key
            project_name = resolved.project_name
            project_alias = resolved.match_alias or clean_text(project_text)
            evidence.append(_evidence(project_key, "registry_v3_project_alias", "high", project_alias))
        elif resolved.ambiguity:
            evidence.append(_evidence(resolved.family_id or "project_family", "registry_v3_ambiguous_family", "medium", resolved.match_alias or project_text))

    if not project_key:
        explicit = re.search(
            r"(?<![A-Za-z0-9\u4e00-\u9fff])(?:项目(?:名称)?|楼盘|小区|社区|公寓名)"
            r"\s*[:：]\s*([^\n，,；;｜|]{2,60})",
            project_text, flags=re.I,
        )
        if explicit:
            candidate = clean_text(explicit.group(1))
            candidate = re.sub(
                r"(?:出租|招租|for\s+rent|公寓|住宅|别墅|排屋)\s*$", "", candidate,
                flags=re.I,
            ).strip(" -｜|·•,，:：")
            if (
                candidate
                and candidate not in {"金边", "房源", "出租", "公寓", "住宅"}
                and candidate not in _FALSE_EXPLICIT_PROJECT_VALUES
                and not all(
                    part.strip() in _FALSE_EXPLICIT_PROJECT_VALUES
                    for part in re.split(r"[、,/，\s]+", candidate)
                    if part.strip()
                )
            ):
                slug = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", candidate.lower()).strip("_")[:40]
                project_key = f"project:{slug}" if slug else None
                project_name = candidate
                evidence.append(_evidence(candidate, "raw_explicit_project", "high", explicit.group(0)))
    return (
        project_key,
        project_name,
        project_alias,
        brand.key if brand else None,
        brand.display if brand else None,
        evidence,
        ["project_brand_only"] if brand and not project_key else [],
    )


_INVENTORY_MENU_MARKER = re.compile(
    r"(?:户型选择|房型选择|可选户型|可选房型|户型可选|房型可选|库存户型|其他户型|更多户型|大量房源|房源充足)",
    flags=re.I,
)


def _strip_inventory_menu_segments(text: str) -> tuple[str, list[str]]:
    current: list[str] = []
    inventory: list[str] = []
    for raw_line in str(text or "").splitlines() or [str(text or "")]:
        line = clean_text(raw_line)
        if not line:
            continue
        marker = _INVENTORY_MENU_MARKER.search(line)
        if marker:
            prefix = line[:marker.start()].strip(" ，,；;｜|")
            if prefix:
                current.append(prefix)
            inventory.append(line[marker.start():])
            continue
        choice_list = bool(
            re.search(r"(?:单间|studio|\d{1,2}\s*房)\s*[／/|、,，]\s*(?:单间|studio|\d{1,2}\s*房)", line, flags=re.I)
            and re.search(r"(?:都有|均有|可选|任选|选择)", line, flags=re.I)
        )
        if choice_list:
            inventory.append(line)
            continue
        current.append(line)
    return "\n".join(current), inventory


def _property_matches(text: str) -> list[tuple[PropertyRule, str, int]]:
    matches: list[tuple[PropertyRule, str, int]] = []
    for rule in PROPERTY_RULES:
        hit = _find_alias(text, rule.aliases)
        if hit:
            alias, position = hit
            matches.append((rule, alias, position))
    return matches


def _resolve_property_matches(
    matches: list[tuple[PropertyRule, str, int]],
    *,
    evidence_source: str = "raw_property_alias",
    confidence: str = "high",
) -> tuple[str, str | None, str, str, list[dict[str, Any]], list[str]]:
    if not matches:
        return "未知", None, "未知", "unknown", [], ["unknown_property_type"]
    # A generic alias embedded in a longer explicit type (e.g. "别墅" inside
    # "联排别墅") must not manufacture a contradiction. Retain independent
    # current-listing type mentions such as "排屋/别墅" as an ambiguity.
    specific_hits = [item for item in matches if len(item[1]) > 2]
    filtered: list[tuple[PropertyRule, str, int]] = []
    for candidate in matches:
        rule, alias, _position = candidate
        if len(alias) <= 2 and any(
            alias in other_alias and rule.family != other_rule.family
            for other_rule, other_alias, _ in specific_hits
        ):
            continue
        filtered.append(candidate)
    matches = filtered
    families = {rule.family for rule, _alias, _position in matches}
    evidence = [
        _evidence(rule.display, evidence_source, confidence, alias)
        for rule, alias, _position in matches
    ]
    if len(families) > 1:
        return "未知", None, "未知", "ambiguous", evidence, ["ambiguous_property_type"]
    matches.sort(key=lambda item: (-len(item[1]), item[2]))
    rule = matches[0][0]
    return rule.family, rule.subtype or None, rule.display, "confirmed", evidence, []


def _extract_property(text: str) -> tuple[str, str | None, str, str, list[dict[str, Any]], list[str]]:
    current_text, inventory_segments = _strip_inventory_menu_segments(text)

    # Explicit current-listing type fields are authoritative over descriptive
    # mentions elsewhere in the post. Inventory/menu lines are excluded first.
    explicit_scopes = [
        line for line in current_text.splitlines()
        if re.search(r"(?:房源类型|物业类型|房屋类型|房产类型)\s*[:：]", line, flags=re.I)
    ]
    explicit_matches = _property_matches("\n".join(explicit_scopes))
    if explicit_matches:
        return _resolve_property_matches(
            explicit_matches, evidence_source="raw_explicit_property_type", confidence="high"
        )

    current_matches = _property_matches(current_text)
    if current_matches:
        return _resolve_property_matches(current_matches)

    # A type seen only in a stock/menu list is not the current listing's type.
    # Preserve evidence for review, but never turn it into a confirmed fact.
    inventory_matches = _property_matches("\n".join(inventory_segments))
    if inventory_matches:
        evidence = [
            _evidence(rule.display, "raw_inventory_property_alias", "medium", alias)
            for rule, alias, _position in inventory_matches
        ]
        return "未知", None, "未知", "unknown", evidence, [
            "unknown_property_type", "property_type_only_in_inventory"
        ]
    return "未知", None, "未知", "unknown", [], ["unknown_property_type"]


def classify_listing_taxonomy(raw_text: str) -> TaxonomyResult:
    text = str(raw_text or "")
    area_key, area_display, area_level, area_status, area_evidence, area_flags = _extract_physical_area(text)
    market_keys, market_displays, market_evidence, market_flags = _extract_markets(text)
    project_key, project_name, project_alias, brand_key, brand_name, project_evidence, project_flags = _extract_project(text)
    registry_project = legacy_project_by_key(project_key)
    if (
        registry_project is not None
        and registry_project.kind == "project"
        and registry_project.location_verified
        and registry_project.canonical_area_key
        and registry_project.canonical_area_display
    ):
        area_key = registry_project.canonical_area_key
        area_display = registry_project.canonical_area_display
        area_level = registry_project.canonical_area_level or None
        area_status = "confirmed"
        area_evidence = [
            _evidence(
                registry_project.canonical_area_key,
                "registry_project_geo",
                "high",
                registry_project.canonical_project_name,
            )
        ]
    family, subtype, property_display, property_status, property_evidence, property_flags = _extract_property(text)
    if family == "未知" and project_key:
        project_meta = next((item for item in PROJECT_IDENTITIES if item.key == project_key), None)
        if project_meta and project_meta.kind == "project" and project_meta.property_family:
            family = project_meta.property_family
            subtype = None
            property_display = project_meta.property_family
            property_status = "inferred"
            property_evidence = [
                _evidence(project_meta.property_family, "project_property_metadata", "high", project_name or project_key)
            ]
            property_flags = [flag for flag in property_flags if flag != "unknown_property_type"]
    project_alias_evidence = ([_evidence(project_alias, "raw_project_alias", "high", project_alias)] if project_alias else [])
    return TaxonomyResult(
        canonical_area_key=area_key,
        canonical_area_display=area_display,
        canonical_area_level=area_level,
        area_status=area_status,
        market_location_keys=market_keys,
        market_location_displays=market_displays,
        project_key=project_key,
        project_name=project_name,
        project_alias=project_alias,
        project_brand_key=brand_key,
        project_brand=brand_name,
        property_type=family,
        property_subtype=subtype,
        property_type_display=property_display,
        property_type_status=property_status,
        evidence={
            "canonical_area_key": area_evidence,
            "market_location_keys": market_evidence,
            "project": project_evidence,
            "project_alias": project_alias_evidence,
            "property_type": property_evidence,
        },
        flags=list(dict.fromkeys(area_flags + market_flags + project_flags + property_flags)),
    )


def public_location_from_fields(
    *,
    canonical_area_key: object = None,
    canonical_area_display: object = None,
    area_status: object = None,
    market_location_keys: object = None,
    market_location_displays: object = None,
    project_key: object = None,
    project_name: object = None,
) -> tuple[str | None, str | None, str]:
    """Resolve public location without ever using the project name as GEO."""
    del project_name
    registry_project = legacy_project_by_key(project_key)
    if (
        registry_project is not None
        and registry_project.kind == "project"
        and registry_project.location_verified
        and clean_text(registry_project.public_location_key)
        and clean_text(registry_project.public_location_display)
    ):
        level = (
            "level_2_physical_confirmed"
            if registry_project.canonical_area_key
            else "level_1_registry_confirmed"
        )
        return (
            clean_text(registry_project.public_location_key),
            clean_text(registry_project.public_location_display),
            level,
        )

    area_key = clean_text(canonical_area_key)
    area_display = clean_text(canonical_area_display)
    if area_status == "confirmed" and area_key and area_display:
        return area_key, area_display, "level_2_physical_confirmed"

    market_keys = [clean_text(value) for value in (market_location_keys or []) if clean_text(value)]
    market_displays = [clean_text(value) for value in (market_location_displays or []) if clean_text(value)]
    if market_keys and market_displays:
        return market_keys[0], market_displays[0], "level_1_market_confirmed"
    return None, None, "unknown"


def public_location(taxonomy: TaxonomyResult) -> tuple[str | None, str | None, str]:
    """Return the safe, externally displayable location and its evidence level."""
    return public_location_from_fields(
        canonical_area_key=taxonomy.canonical_area_key,
        canonical_area_display=taxonomy.canonical_area_display,
        area_status=taxonomy.area_status,
        market_location_keys=taxonomy.market_location_keys,
        market_location_displays=taxonomy.market_location_displays,
        project_key=taxonomy.project_key,
        project_name=taxonomy.project_name,
    )


__all__ = [
    "LocationResolution",
    "MANUAL_MARKET_ALIASES",
    "MARKET_LOCATIONS",
    "PHYSICAL_AREAS",
    "PROJECT_IDENTITIES",
    "TaxonomyResult",
    "classify_listing_taxonomy",
    "market_location_by_key",
    "physical_area_by_key",
    "public_location",
    "public_location_from_fields",
    "resolve_location_alias",
]
