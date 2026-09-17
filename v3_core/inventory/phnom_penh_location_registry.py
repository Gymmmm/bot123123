"""Authoritative Phnom Penh project/location registry.

PROJECT identity, physical GEO, and renter-facing MARKET aliases are deliberately
separate. A project alias can help recognition/search without becoming a
location. Exact project GEO is populated only when it is backed by repository
evidence or an explicit product rule.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable


def clean_registry_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\u00a0", " ")).strip()


@dataclass(frozen=True)
class PhysicalAreaRecord:
    key: str
    display: str
    level: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class MarketLocationRecord:
    key: str
    display: str
    relation: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class ProjectRegistryEntry:
    key: str
    canonical_project_name: str
    project_aliases: tuple[str, ...]
    project_name_en: str = ""
    canonical_area_key: str = ""
    canonical_area_display: str = ""
    canonical_area_level: str = ""
    district: str = ""
    sangkat: str = ""
    road_name: str = ""
    road_aliases: tuple[str, ...] = ()
    market_area_aliases: tuple[str, ...] = ()
    search_aliases: tuple[str, ...] = ()
    public_location_key: str = ""
    public_location_display: str = ""
    property_family: str | None = None
    location_verified: bool = False
    kind: str = "project"

    @property
    def display(self) -> str:
        return self.canonical_project_name

    @property
    def aliases(self) -> tuple[str, ...]:
        return self.project_aliases


PHYSICAL_AREAS: tuple[PhysicalAreaRecord, ...] = (
    PhysicalAreaRecord("BKK1", "BKK1", "sangkat", ("boeung keng kang 1", "bkk1", "bkk 1", "bkk-1", "万景岗1", "万景岗一区")),
    PhysicalAreaRecord("BKK2", "BKK2", "sangkat", ("boeung keng kang 2", "bkk2", "bkk 2", "bkk-2", "万景岗2", "万景岗二区")),
    PhysicalAreaRecord("BKK3", "BKK3", "sangkat", ("boeung keng kang 3", "bkk3", "bkk 3", "bkk-3", "万景岗3", "万景岗三区")),
    PhysicalAreaRecord("百色河", "永旺1附近（百色河区）", "sangkat", ("tonle bassac", "tonle basak", "百色河", "百色河区")),
    PhysicalAreaRecord("钻石岛", "钻石岛", "neighborhood", ("钻石岛", "钻岛", "koh pich", "diamond island")),
    PhysicalAreaRecord("TK/7月区", "堆谷（TK）", "khan", ("tuol kork", "toul kork", "堆谷", "堆谷区", "7月区", "七月区")),
    PhysicalAreaRecord("森速", "森速（永旺2一带）", "khan", ("sen sok", "sensok", "森速", "森速区")),
    PhysicalAreaRecord("水净华", "水净华半岛", "khan", ("chroy changvar", "chroy changva", "水净华", "水静华")),
    PhysicalAreaRecord("桑园", "桑园", "khan", ("chamkarmon", "chamkar mon", "桑园", "桑园区")),
    PhysicalAreaRecord("隆边", "隆边", "khan", ("daun penh", "隆边", "隆边区")),
    PhysicalAreaRecord("铁桥头", "铁桥头", "khan", ("chbar ampov", "chbar ampeou", "铁桥头", "铁桥头区")),
)


MARKET_LOCATIONS: tuple[MarketLocationRecord, ...] = (
    MarketLocationRecord("BKK1", "BKK1", "district", ("bkk1", "bkk 1", "bkk-1", "万景岗1", "万景岗一区")),
    MarketLocationRecord("BKK2", "BKK2", "district", ("bkk2", "bkk 2", "bkk-2", "万景岗2", "万景岗二区")),
    MarketLocationRecord("BKK3", "BKK3", "district", ("bkk3", "bkk 3", "bkk-3", "万景岗3", "万景岗三区")),
    MarketLocationRecord("BKK", "BKK", "district", ("bkk", "万景岗", "bkk附近", "boeung keng kang")),
    MarketLocationRecord("钻石岛", "钻石岛", "district", ("钻石岛", "钻石岛附近", "钻岛", "koh pich", "diamond island")),
    MarketLocationRecord("百色河", "永旺1附近（百色河区）", "district", ("百色河", "百色河区", "tonle bassac", "tonle basak", "bassac")),
    MarketLocationRecord("俄罗斯市场", "俄罗斯市场附近", "nearby", ("俄罗斯市场", "俄罗斯市场附近", "俄市", "russian market", "ttp", "toul tom poung", "toul tompoung")),
    MarketLocationRecord("TK/7月区", "堆谷（TK）", "district", ("堆谷", "堆谷区", "堆谷（TK）", "tk", "tuol kork", "toul kork", "7月区", "七月区")),
    MarketLocationRecord("洪森大道", "洪森大道", "corridor", ("洪森大道", "60米", "60米方向", "60米大道", "60米路", "hun sen boulevard", "hun sen blvd", "ph60m", "60米炳发")),
    MarketLocationRecord("一号路", "一号路附近", "corridor", ("一号路附近", "一号路", "1号路", "一号公路", "1号公路", "national road 1", "nr1", "one road", "一号路炳发城", "1号路炳发")),
    MarketLocationRecord("莫尼旺大道", "莫尼旺大道", "corridor", ("莫尼旺大道", "莫尼旺", "monivong", "preah monivong blvd", "preah monivong boulevard")),
    MarketLocationRecord("毛泽东大道", "毛泽东大道", "corridor", ("毛泽东大道", "毛泽东", "mao tse toung blvd", "mao tse toung boulevard", "mao tse tung")),
    MarketLocationRecord("俄罗斯大道", "俄罗斯大道", "corridor", ("俄罗斯大道", "russian federation blvd", "russian federation boulevard", "russian boulevard")),
    MarketLocationRecord("诺罗敦大道", "诺罗敦大道", "corridor", ("诺罗敦大道", "norodom", "norodom blvd", "norodom boulevard")),
    MarketLocationRecord("271路", "271路", "corridor", ("271路", "271", "street 271", "st 271", "st. 271")),
    MarketLocationRecord("598路", "598路附近", "corridor", ("598路附近", "598路", "598公路")),
    MarketLocationRecord("50米路", "50米路附近", "corridor", ("50米路附近", "50米路", "50米大道")),
    MarketLocationRecord("集茂", "集茂", "nearby", ("598路集茂", "集茂", "chip mong", "chipmong")),
    MarketLocationRecord("永旺商圈", "永旺1附近", "nearby", ("永旺1", "永旺1附近", "永旺一", "aeon1", "aeon 1")),
    MarketLocationRecord("永旺2", "永旺2附近", "nearby", ("永旺2", "永旺2附近", "永旺二", "aeon2", "aeon 2")),
    MarketLocationRecord("永旺3", "永旺3附近", "nearby", ("永旺3", "永旺3附近", "永旺三", "aeon3", "aeon 3", "aeon mean chey")),
    MarketLocationRecord("森速", "森速（永旺2一带）", "district", ("森速", "森速区", "森速（永旺2一带）", "sen sok", "sensok")),
    MarketLocationRecord("桑园", "桑园", "district", ("桑园", "桑园区", "chamkarmon", "chamkar mon")),
    MarketLocationRecord("隆边", "隆边", "district", ("隆边", "隆边区", "daun penh")),
    MarketLocationRecord("铁桥头", "铁桥头", "district", ("铁桥头", "铁桥头区", "chbar ampov", "chbar ampeou")),
    MarketLocationRecord("水净华", "水净华半岛", "district", ("水净华", "水净华区", "水净华半岛", "水静华", "chroy changvar", "chroy changva")),
    MarketLocationRecord("河边", "河边", "corridor", ("河边", "河畔", "riverside")),
    MarketLocationRecord("金街", "金街附近", "nearby", ("金街", "金街附近", "金街中国城")),
    MarketLocationRecord("Naga", "金界附近", "nearby", ("金界", "金界附近", "naga", "naga world", "nagaworld")),
    MarketLocationRecord("Boeung Snor", "Boeung Snor", "nearby", ("boeung snor", "boeung snor附近", "boeung snor炳发")),
    MarketLocationRecord("Norea", "Norea", "nearby", ("norea", "norea附近", "norea炳发")),
    MarketLocationRecord("德崇机场", "德崇机场方向", "nearby", ("新机场", "新机场附近", "德崇机场", "techo international airport")),
    MarketLocationRecord("机场附近", "机场附近", "nearby", ("机场路", "老机场", "旧机场")),
    MarketLocationRecord("2004路", "2004路附近", "corridor", ("2004路", "street 2004", "st 2004", "st. 2004")),
    MarketLocationRecord("独立碑", "独立碑附近", "nearby", ("独立碑", "独立纪念碑", "independence monument")),
    MarketLocationRecord("中央市场", "中央市场", "nearby", ("中央市场", "新街市", "central market", "phsar thmei")),
    MarketLocationRecord("奥林匹克", "奥林匹克", "nearby", ("奥林匹克", "奥林匹亚", "olympic", "olympia")),
)


PROJECT_IDENTITIES: tuple[ProjectRegistryEntry, ...] = (
    ProjectRegistryEntry(
        "agile_sky_residence", "雅居乐",
        ("雅居乐", "雅居乐公寓", "雅居乐天悦", "Agile", "Agile Sky", "Agile Sky Residence"),
        project_name_en="Agile Sky Residence",
        canonical_area_key="BKK3", canonical_area_display="BKK3", canonical_area_level="sangkat",
        district="BKK / Boeung Keng Kang", sangkat="BKK3",
        road_name="莫尼旺大道", road_aliases=("Monivong", "Preah Monivong Blvd", "Preah Monivong Boulevard"),
        market_area_aliases=("BKK", "BKK附近", "BKK1附近", "莫尼旺", "莫尼旺大道"),
        search_aliases=("BKK1 雅居乐", "BKK 雅居乐", "莫尼旺 雅居乐"),
        public_location_key="BKK3", public_location_display="BKK3 · 莫尼旺大道",
        location_verified=True,
    ),
    ProjectRegistryEntry(
        "the_bridge", "桥牌",
        ("桥牌", "桥牌公寓", "The Bridge", "Bridge", "世桥"),
        project_name_en="The Bridge",
        canonical_area_key="百色河", canonical_area_display="百色河", canonical_area_level="sangkat",
        public_location_key="百色河", public_location_display="百色河",
        market_area_aliases=("金街", "金街附近", "永旺1附近"),
        property_family="公寓", location_verified=True,
    ),
    ProjectRegistryEntry(
        "the_peak", "The Peak 香格里拉",
        ("香格里拉", "香格里拉公寓", "The Peak", "Peak", "THE•PEAK"),
        project_name_en="The Peak",
        canonical_area_key="钻石岛", canonical_area_display="钻石岛", canonical_area_level="neighborhood",
        public_location_key="钻石岛", public_location_display="钻石岛",
        market_area_aliases=("钻石岛", "钻石岛附近"),
        location_verified=True,
    ),
    ProjectRegistryEntry(
        "rf_city", "富力城",
        ("富力城", "富力中心城", "富力金边中心城", "R&F City", "R&F", "RF City", "R F City", "金边中心城"),
        project_name_en="R&F City",
        road_name="洪森大道", road_aliases=("Hun Sen Boulevard", "Hun Sen Blvd", "60米", "60米大道"),
        market_area_aliases=("60米", "60米方向", "洪森大道", "永旺3附近"),
        public_location_key="洪森大道", public_location_display="洪森大道",
        location_verified=True,
    ),
    ProjectRegistryEntry("rose_apple", "玫瑰滨江园", ("玫瑰滨江园", "玫瑰滨江", "Rose Apple"), project_name_en="Rose Apple"),
    ProjectRegistryEntry("the_pinnacle", "The Pinnacle 幸福广场", ("太子幸福", "太子幸福广场", "幸福广场", "The Pinnacle", "Prince Happiness Plaza"), project_name_en="The Pinnacle", property_family="公寓"),
    ProjectRegistryEntry("prince_international_plaza", "太子国际广场", ("太子国际", "太子国际广场")),
    ProjectRegistryEntry("prince_huan_yu_center", "太子·寰宇中心", ("太子寰宇", "太子寰宇中心", "太子·寰宇中心", "金边寰宇中心", "寰宇中心", "寰宇", "Prince Huan Yu Center", "Prince Huanyu Center", "Prince Huan Yu", "Huan Yu Center")),
    ProjectRegistryEntry("prince_modern_plaza", "太子现代广场", ("太子现代广场",)),
    ProjectRegistryEntry("prince_central_plaza", "Prince Central Plaza 太子中央广场", ("太子中央广场", "Prince Central Plaza"), project_name_en="Prince Central Plaza"),
    ProjectRegistryEntry("wealth_mansion", "财富大厦", ("财富大厦", "Wealth Mansion"), project_name_en="Wealth Mansion"),
    ProjectRegistryEntry("capital_gold", "首都国金", ("首都国金",)),
    ProjectRegistryEntry("royal_one", "皇家一号", ("皇家一号", "Royal One"), project_name_en="Royal One"),
    ProjectRegistryEntry("picasso_city_garden", "Picasso City Garden 毕加索", ("毕加索", "Picasso", "毕加索城市花园", "毕加索一期", "Picasso City Garden", "Picasso Garden City", "Picasso Garden"), project_name_en="Picasso City Garden"),
    ProjectRegistryEntry("morgan", "摩根", ("摩根", "Morgan"), project_name_en="Morgan"),
    ProjectRegistryEntry("the_gateway", "The Gateway", ("The Gateway", "Gateway"), project_name_en="The Gateway"),
    ProjectRegistryEntry("urban_village_2", "首都·国金 Urban Village Phase 2", ("Urban Village", "Urban Village Phase 2", "首都国金二期"), project_name_en="Urban Village Phase 2"),
    ProjectRegistryEntry("peng_huoth_city", "炳发城", ("炳发城",)),
    ProjectRegistryEntry("vila_town", "Vila Town", ("Vila Town",), project_name_en="Vila Town"),
    ProjectRegistryEntry("j_tower_2", "J Tower 2", ("J Tower 2", "J Tower2", "JTower 2"), project_name_en="J Tower 2"),
    ProjectRegistryEntry("le_conde_bkk1", "王府·观邸 Le Condé BKK1", ("Le Conde", "Le Condé", "王府观邸", "王府·观邸"), project_name_en="Le Condé BKK1"),
    ProjectRegistryEntry("time_square_9", "Time Square 9", ("Time Square 9", "Times Square 9"), project_name_en="Time Square 9"),
    ProjectRegistryEntry("sky_villa", "Sky Villa 天空别墅", ("Sky Villa", "金边Sky Villa豪宅", "天空别墅"), project_name_en="Sky Villa"),
    ProjectRegistryEntry("chip_mong", "Chip Mong", ("Chip Mong Land", "Chip Mong", "ChipMong", "集茂"), kind="brand"),
    ProjectRegistryEntry("peng_huoth", "Peng Huoth", ("Borey Peng Huoth", "Peng Huoth", "炳发"), kind="brand"),
)

MANUAL_MARKET_ALIASES: dict[str, tuple[str, ...]] = {}


def normalize_project_alias(value: object) -> str:
    return clean_registry_text(value).casefold()


def project_aliases(item: ProjectRegistryEntry) -> tuple[str, ...]:
    return tuple(
        value for value in (
            item.canonical_project_name,
            item.project_name_en,
            *item.project_aliases,
            *item.search_aliases,
        )
        if clean_registry_text(value)
    )


def duplicate_project_aliases() -> dict[str, tuple[str, ...]]:
    owners: dict[str, set[str]] = {}
    for item in PROJECT_IDENTITIES:
        if item.kind != "project":
            continue
        for alias in project_aliases(item):
            normalized = normalize_project_alias(alias)
            owners.setdefault(normalized, set()).add(item.key)
    return {
        alias: tuple(sorted(keys))
        for alias, keys in owners.items()
        if len(keys) > 1
    }


def resolve_project_alias(value: object) -> ProjectRegistryEntry | None:
    normalized = normalize_project_alias(value)
    if not normalized:
        return None
    matches = [
        item for item in PROJECT_IDENTITIES
        if item.kind == "project"
        and any(normalize_project_alias(alias) == normalized for alias in project_aliases(item))
    ]
    return matches[0] if len(matches) == 1 else None


def project_by_key(key: object) -> ProjectRegistryEntry | None:
    wanted = clean_registry_text(key).casefold()
    return next((item for item in PROJECT_IDENTITIES if item.key.casefold() == wanted), None)


def _contains_alias(text: str, alias: str) -> bool:
    haystack = clean_registry_text(text).casefold()
    needle = clean_registry_text(alias).casefold()
    if not haystack or not needle:
        return False
    if re.search(r"[a-z0-9]", needle):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack))
    return needle in haystack


def projects_in_text(text: object) -> tuple[ProjectRegistryEntry, ...]:
    matches: list[ProjectRegistryEntry] = []
    for item in PROJECT_IDENTITIES:
        if item.kind != "project":
            continue
        aliases = (item.canonical_project_name, item.project_name_en, *item.project_aliases, *item.search_aliases)
        if any(alias and _contains_alias(str(text or ""), alias) for alias in aliases):
            matches.append(item)
    return tuple(matches)


def project_search_terms(text: object) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for item in projects_in_text(text):
        for value in (item.canonical_project_name, item.project_name_en, *item.project_aliases):
            cleaned = clean_registry_text(value)
            folded = cleaned.casefold()
            if cleaned and folded not in seen:
                seen.add(folded)
                values.append(cleaned)
    return tuple(values)


def unresolved_project_names() -> tuple[str, ...]:
    return tuple(
        item.canonical_project_name
        for item in PROJECT_IDENTITIES
        if item.kind == "project" and not item.location_verified
    )


def registry_project_count() -> int:
    return sum(1 for item in PROJECT_IDENTITIES if item.kind == "project")


def registry_alias_count() -> int:
    aliases: set[str] = set()

    def add(values: Iterable[str]) -> None:
        for value in values:
            cleaned = clean_registry_text(value).casefold()
            if cleaned:
                aliases.add(cleaned)

    for item in PHYSICAL_AREAS:
        add((item.key, item.display, *item.aliases))
    for item in MARKET_LOCATIONS:
        add((item.key, item.display, *item.aliases))
    for item in PROJECT_IDENTITIES:
        add((item.canonical_project_name, item.project_name_en, *item.project_aliases, *item.road_aliases, *item.market_area_aliases, *item.search_aliases))
    return len(aliases)


__all__ = [
    "MANUAL_MARKET_ALIASES", "MARKET_LOCATIONS", "PHYSICAL_AREAS", "PROJECT_IDENTITIES",
    "MarketLocationRecord", "PhysicalAreaRecord", "ProjectRegistryEntry",
    "clean_registry_text", "duplicate_project_aliases", "normalize_project_alias", "project_aliases", "project_by_key", "project_search_terms", "projects_in_text", "resolve_project_alias",
    "registry_alias_count", "registry_project_count", "unresolved_project_names",
]
