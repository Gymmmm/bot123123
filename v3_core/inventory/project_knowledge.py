"""Project-level reference knowledge for Phnom Penh listings.

The knowledge bundle is additive. It never overwrites source-backed unit facts.
A recognized project may inherit reference metadata (location, developer,
amenities, common fees, nearby landmarks, evidence and conflicts), but
OWNER_SPECIFIC / conflicting values remain reference-only.
"""
from __future__ import annotations

import csv
import io
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

KNOWLEDGE_VERSION = "pp_project_knowledge_v1_20260929"
_DATA_DIR = Path(__file__).with_name("data") / "project_knowledge"
_VALID_STATUSES = {"VERIFIED", "PARTIAL", "NEEDS_VERIFICATION", "UNKNOWN"}

# Taxonomy keys that intentionally differ from the research registry.
_TAXONOMY_TO_KNOWLEDGE = {
    "prince_huan_yu_center": "prince-universe",
    "phnom_penh_galaxy_garden": "xinghe-garden",
    # Existing taxonomy key historically names 威尔斯公馆. Do not attach the
    # separate 财富大厦 / Wealth Mansion profile to it.
    "wealth_mansion": "wealth-mansion",
    **{f"time_square_{n}": f"times-square-{n}" for n in (1, 2, 3, 5, 7, 8, 9, 11)},
}
_KNOWLEDGE_TO_TAXONOMY = {value: key for key, value in _TAXONOMY_TO_KNOWLEDGE.items()}

_WEB_VERIFIED_PROJECTS: dict[str, dict[str, Any]] = {
    "prince_central_plaza": {
        "knowledge_key": "web:prince-central-plaza",
        "reference_kind": "web_verified_project",
        "display_name": "Prince Central Plaza 太子中央广场",
        "developer": "Prince Real Estate (Cambodia) Group",
        "project_type": "公寓 / 商业综合体",
        "location": "Tonle Bassac · Norodom Boulevard",
        "completion_year": "2017",
        "nearby": ["独立碑", "永旺1", "河滨", "使馆区"],
        "source_urls": [
            "https://www.princerealestate.com/Project/info.aspx?itemid=588",
            "https://www.asia.villas/projects/cambodia/phnom-penh/chamkar-mon/tonle-basak/prince-central-plaza",
        ],
    },
    "prince_international_plaza": {
        "knowledge_key": "web:prince-international-plaza",
        "reference_kind": "web_verified_project",
        "display_name": "Prince International Plaza 太子国际广场",
        "developer": "Prince Real Estate Group",
        "project_type": "商业 / 办公 / 住宅综合体",
        "location": "Sen Sok · Tuol Kork · Meanchey交界方向",
        "building_profile": "Prince Group公开资料称项目约200,000㎡，包含商业与住宅用途。",
        "source_urls": [
            "https://www.princeholdinggroup.com/featured_development/prince-square/",
        ],
    },
    "casa_meridian": {
        "knowledge_key": "web:casa-by-meridian",
        "reference_kind": "web_verified_project",
        "display_name": "CASA by Meridian",
        "developer": "Meridian International / M.D H.K Property (Cambodia) Ltd",
        "project_type": "公寓",
        "location": "钻石岛 / Koh Pich",
        "amenities": ["泳池", "健身房", "会所", "儿童区", "24小时安保"],
        "nearby": ["永旺1", "NagaWorld", "Sofitel", "Canadian International School"],
        "source_urls": [
            "https://www.realestate.com.kh/new-developments/casa-by-meridian/",
            "https://www.landscope.com/overseas-properties/1777cc",
        ],
    },
    "sky_villa": {
        "knowledge_key": "web:sky-villa",
        "reference_kind": "web_verified_project",
        "display_name": "Sky Villa",
        "developer": "Greatview Investment（公开项目资料口径）",
        "project_type": "高端公寓 / 服务式住宅",
        "location": "7 Makara · Veal Vong · Street 163",
        "address": "No. 192, Street 163, Sangkat Veal Vong, Khan 7 Makara, Phnom Penh",
        "building_profile": "两栋35层住宅塔楼，公开项目资料记录256个住宅单位。",
        "completion_year": "2020",
        "amenities": ["泳池", "健身房", "桑拿", "Jacuzzi", "花园", "儿童区", "停车", "24小时接待", "视频安保"],
        "nearby": ["奥林匹克体育场"],
        "source_urls": [
            "https://www.realestate.com.kh/new-developments/sky-villa/",
            "https://data.opendevelopmentcambodia.net/en/dataset/webpage-capture-on-the-location-of-sky-villa-phnom-penh",
        ],
    },
    "le_conde_bkk1": {
        "knowledge_key": "web:le-conde-bkk1",
        "reference_kind": "web_verified_project",
        "display_name": "Le Condé BKK1",
        "developer": "Wangfu International",
        "project_type": "43层综合开发项目",
        "location": "BKK1, Phnom Penh",
        "nearby": ["独立碑", "皇宫", "永旺1"],
        "source_urls": ["https://www.leconde.com/"],
    },
    "la_vista_one": {
        "knowledge_key": "web:la-vista-one",
        "reference_kind": "web_verified_project",
        "display_name": "La Vista ONE 紫晶壹号",
        "developer": "YIN YI VENTURE CO., LTD / 柬埔寨银翼创投有限公司",
        "project_type": "公寓",
        "location": "水净华 · Mekong Road · Sokha Hotel北侧",
        "address": "No.3 Mekong Road, Chroy Changvar District, Phnom Penh",
        "source_urls": ["https://www.lavistaone.com/"],
    },
    "picasso_city_garden": {
        "knowledge_key": "web:picasso-city-garden",
        "reference_kind": "web_verified_project",
        "display_name": "Picasso City Garden 毕加索城市花园",
        "developer": "Picasso City Garden Development Plc.",
        "project_type": "高端公寓",
        "location": "BKK1 · Street 322",
        "address": "No. 41, Street 322, Village 7, Sangkat Boeng Keng Kang 1, Phnom Penh",
        "source_urls": [
            "https://pcgdevelopmentplc.com.kh/en/about-us/",
            "https://pcgdevelopmentplc.com.kh/en/contact-us/",
        ],
    },
    "diamond_one": {
        "knowledge_key": "web:diamond-one",
        "reference_kind": "web_verified_project",
        "display_name": "Diamond One",
        "project_type": "公寓 / 联排住宅",
        "location": "钻石岛 / Koh Pich",
        "completion_year": "2019",
        "amenities": ["泳池", "健身房", "停车", "花园", "儿童区", "备用发电", "24小时接待", "视频安保"],
        "nearby": ["永旺1", "Canadian International School", "会展中心", "Sofitel"],
        "source_urls": [
            "https://www.realestate.com.kh/new-developments/diamond-one/",
            "https://construction-property.com/diamond-one-opens-sales-center/",
        ],
    },
    "the_penthouse_residence": {
        "knowledge_key": "web:the-penthouse-residence",
        "reference_kind": "web_verified_project",
        "display_name": "The Penthouse Residence",
        "project_type": "公寓 / 服务式住宅",
        "location": "Tonle Bassac · Sothearos Boulevard",
        "address": "No. 83B, Sothearos Boulevard, Phnom Penh",
        "amenities": ["泳池", "健身房", "停车", "Sky Bar", "Spa", "花园"],
        "building_profile": "官方资料同时出现36层与43层口径；系统保留冲突，不把总楼层自动继承到房源。",
        "nearby": ["永旺1", "Sofitel", "iCan British International School"],
        "notes": "楼层口径在公开资料中存在差异，系统不把总楼层作为自动继承事实。",
        "source_urls": [
            "https://thepenthouseresidence.com/our-facilities/",
            "https://www.realestate.com.kh/new-developments/the-penthouse-residence-51335/2-bed-1-bath-condo-268415/",
        ],
    },
    "bali_3": {
        "knowledge_key": "web:bali-3",
        "reference_kind": "web_verified_project",
        "display_name": "Bali 3 Condominium",
        "project_type": "公寓",
        "location": "Chroy Changvar, Phnom Penh",
        "amenities": ["泳池", "健身房", "停车", "安保"],
        "notes": "管理费/停车是否包含随具体租约变化，不作为项目统一收费自动继承。",
        "source_urls": [
            "https://camrealtyservice.com/building/bali-3-resort-and-hotel/",
            "https://www.realtor.com/international/kh/tonle-bassac-chamkarmon-phnom-penh-360107136896/",
        ],
    },
    "peninsula_private_residence": {
        "knowledge_key": "web:peninsula-private-residences",
        "reference_kind": "web_verified_project",
        "display_name": "The Peninsula Private Residences",
        "project_type": "公寓 / 商业",
        "location": "Chroy Changvar · 日本桥附近",
        "developer": "CC Peninsula Co., Ltd.（National 6A Investment + SUN & MOON Group + Saturn Investment 合资）",
        "building_profile": "约25层、161个住宅单位，含Studio/1房/2房/3房。",
        "amenities": ["泳池", "健身房", "桑拿", "花园"],
        "source_urls": [
            "https://www.sunandmoongroup.com.kh/our-companies/service-apartment.html",
            "https://www.realestate.com.kh/new-developments/the-peninsula-private-residences/offices-222379/",
        ],
    },
    "borey_angkor": {
        "knowledge_key": "web:borey-angkor-phnom-penh",
        "reference_kind": "web_verified_project",
        "display_name": "Borey Angkor Phnom Penh",
        "project_type": "Borey / 别墅住宅社区",
        "location": "Russey Keo · Angkor Boulevard",
        "developer": "Angkor Continent Group Co., Ltd.",
        "property_types": ["排屋", "双拼", "别墅"],
        "source_urls": ["https://www.boreyapp.com/"],
    },
    "yuetai_ecc": {
        "knowledge_key": "web:yuetai-ecc",
        "reference_kind": "web_verified_project",
        "display_name": "YUETAI ECC",
        "project_type": "综合楼 / 公寓办公用途",
        "location": "Norodom Boulevard · Tonle Bassac, Chamkarmon",
        "notes": "当前公开证据足以确认建筑与位置；统一住宅收费/配套规则仍按具体房源核验。",
        "source_urls": [
            "https://www.ppcbank.com.kh/atm-branches/ppcbank-atm-yuetai-ecc-building/",
        ],
    },
}


def _clean_row(row: dict[str, Any]) -> dict[str, str]:
    clean = {
        str(key): str(value or "").strip()
        for key, value in row.items()
        if key not in {None, "index"} and str(value or "").strip()
    }
    # Two late registry rows were exported without search_aliases_cn, shifting
    # status/source columns one place left. Repair only this unmistakable shape.
    status = clean.get("verification_status", "")
    shifted_status = clean.get("search_aliases_cn", "")
    if status.startswith(("http://", "https://")) and shifted_status in _VALID_STATUSES:
        source_1 = status
        source_2 = clean.get("source_1", "")
        source_3 = clean.get("source_2", "")
        clean["verification_status"] = shifted_status
        clean.pop("search_aliases_cn", None)
        clean["source_1"] = source_1
        if source_2:
            clean["source_2"] = source_2
        if source_3:
            clean["source_3"] = source_3
    return clean


def _rows(prefix: str) -> Iterable[dict[str, str]]:
    for path in sorted(_DATA_DIR.glob(f"{prefix}_*.csv")):
        text = path.read_text(encoding="utf-8-sig")
        # Library CSV materialization may preserve a two-line sheet wrapper.
        # Accept it deterministically so the checked-in research snapshot stays
        # traceable to the original source export.
        if text.startswith("<PARSED TEXT FOR SHEET:"):
            lines = text.splitlines()
            if len(lines) >= 2 and ">" in lines[1]:
                lines[1] = lines[1].split(">", 1)[1]
                text = "\n".join(lines[1:])
        for row in csv.DictReader(io.StringIO(text)):
            yield _clean_row(row)


def _entity_key(value: str) -> str:
    value = str(value or "").strip()
    return value.split(":", 1)[-1] if ":" in value else value


@lru_cache(maxsize=1)
def _bundle() -> dict[str, dict[str, Any]]:
    projects: dict[str, dict[str, Any]] = {}

    for row in _rows("registry"):
        key = row.get("canonical_key", "")
        if key:
            projects.setdefault(key, {})["registry"] = row

    for row in _rows("living"):
        key = _entity_key(row.get("project_entity_id", ""))
        if key:
            projects.setdefault(key, {})["living"] = row

    for row in _rows("v5"):
        identity = str(row.get("canonical project identity", "") or "").strip()
        if identity.startswith("project:new:"):
            key = identity[len("project:new:"):]
        elif identity.startswith("project:"):
            key = identity[len("project:"):]
        else:
            key = ""
        if key:
            projects.setdefault(key, {})["v5_profile"] = row

    for row in _rows("aliases"):
        identity = str(row.get("canonical_identity", "") or "").strip()
        if identity.startswith("project:new:"):
            key = identity[len("project:new:"):]
        elif identity.startswith("project:"):
            key = identity[len("project:"):]
        else:
            key = ""
        if key:
            projects.setdefault(key, {}).setdefault("aliases", []).append(row)

    for prefix, field in (
        ("relations", "relations"),
        ("evidence", "evidence"),
        ("conflicts", "conflicts"),
    ):
        for row in _rows(prefix):
            key = _entity_key(row.get("project_entity_id", ""))
            if key:
                projects.setdefault(key, {}).setdefault(field, []).append(row)

    return projects


def knowledge_key_for_taxonomy(project_key: object) -> str | None:
    key = str(project_key or "").strip()
    if not key:
        return None
    if key in _TAXONOMY_TO_KNOWLEDGE:
        return _TAXONOMY_TO_KNOWLEDGE[key]
    candidate = key.replace("_", "-")
    return candidate if candidate in _bundle() else None


def taxonomy_key_for_knowledge(knowledge_key: object) -> str:
    key = str(knowledge_key or "").strip()
    if key in _KNOWLEDGE_TO_TAXONOMY:
        return _KNOWLEDGE_TO_TAXONOMY[key]
    return key.replace("-", "_")


def _peng_huoth_family_reference() -> dict[str, Any]:
    family_rows = [
        row for row in _rows("aliases")
        if str(row.get("canonical_identity") or "") == "family:peng-huoth"
    ]
    child_projects = sorted(
        key for key, payload in _bundle().items()
        if key.startswith(("peng-huoth-", "the-star-"))
        or any(
            str(row.get("target_display_name") or "").startswith("炳发")
            for row in payload.get("aliases") or []
        )
    )
    return {
        "knowledge_key": "family:peng-huoth",
        "knowledge_version": KNOWLEDGE_VERSION,
        "reference_kind": "project_family",
        "reference_only": True,
        "requires_specific_project": True,
        "display_name": "炳发 / Borey Peng Huoth",
        "developer": "Borey Peng Huoth / Peng Huoth Group",
        "developer_founded_year": "2005",
        "project_type": "大型住宅社区项目 family",
        "known_corridors": [
            "1号路 / National Road 1",
            "598路 / Chea Sophara",
            "60米大道 / Samdech Techo Hun Sen Blvd",
            "50米路 / Ring Road 2",
            "6A路 / National Road 6A",
            "217路 / Monireth Blvd",
            "371路",
            "1928路 / Oknha Mong Reth Thy",
            "Chamkar Dong",
            "Veng Sreng",
        ],
        "property_types": ["排屋 / Link House", "商铺屋 / Shop House", "独栋别墅 / Single Villa", "双拼 / Twin Villa", "公寓 / Condominium（部分子项目）"],
        "resolution_hint": "先按道路/地标缩小炳发 family，再解析具体子项目；裸炳发/炳发城不能继承某个子项目的收费、泳池或其他配套。",
        "source_urls": [
            "https://boreypenghuoth.com/borey/en/about-us/who-we-are/",
            "https://boreypenghuoth.com/borey/en/project-listing/",
            "https://boreypenghuoth.com/borey/en/our-properties/",
        ],
        "aliases": family_rows,
        "child_project_keys": child_projects,
        "inheritance_policy": {
            "listing_fact_precedence": True,
            "owner_specific_not_project_default": True,
            "conflicts_require_confirmation": True,
            "needs_verification_not_asserted": True,
        },
    }


def project_reference_for_key(project_key: object) -> dict[str, Any] | None:
    taxonomy_key = str(project_key or "").strip()
    if taxonomy_key == "peng_huoth_city":
        return _peng_huoth_family_reference()
    knowledge_key = knowledge_key_for_taxonomy(taxonomy_key)
    if not knowledge_key:
        web_profile = _WEB_VERIFIED_PROJECTS.get(taxonomy_key)
        if not web_profile:
            return None
        result = deepcopy(web_profile)
        result["knowledge_version"] = KNOWLEDGE_VERSION
        result["reference_only"] = True
        result["inheritance_policy"] = {
            "listing_fact_precedence": True,
            "owner_specific_not_project_default": True,
            "conflicts_require_confirmation": True,
            "needs_verification_not_asserted": True,
        }
        return result
    payload = _bundle().get(knowledge_key)
    if not payload:
        return None
    result = deepcopy(payload)
    web_profile = _WEB_VERIFIED_PROJECTS.get(taxonomy_key)
    if web_profile:
        result["web_verified"] = deepcopy(web_profile)
        for field in (
            "display_name", "developer", "project_type", "location", "address",
            "amenities", "nearby", "building_profile", "completion_year", "notes",
            "source_urls",
        ):
            if web_profile.get(field) not in (None, "", [], {}):
                result.setdefault(field, deepcopy(web_profile[field]))
    result["knowledge_key"] = knowledge_key
    result["knowledge_version"] = KNOWLEDGE_VERSION
    result["reference_only"] = True
    result["inheritance_policy"] = {
        "listing_fact_precedence": True,
        "owner_specific_not_project_default": True,
        "conflicts_require_confirmation": True,
        "needs_verification_not_asserted": True,
    }
    return result


def project_reference_location(project_key: object) -> dict[str, str] | None:
    """Return a safe read-time location fallback for an exact project.

    This never mutates canonical listing facts. Only VERIFIED project-level
    research or a web-verified project profile may supply the fallback.
    """
    reference = project_reference_for_key(project_key)
    if not reference or reference.get("reference_kind") == "project_family":
        return None

    v5 = reference.get("v5_profile") or {}
    if str(v5.get("verification_status") or "").strip().upper() == "VERIFIED":
        display = str(v5.get("中文位置展示") or "").strip()
        if display:
            return {"display": display, "source": "project_v5_verified", "confidence": "verified"}

    registry = reference.get("registry") or {}
    if str(registry.get("verification_status") or "").strip().upper() == "VERIFIED":
        display = str(
            registry.get("public_location_display_cn")
            or registry.get("canonical_geo_display")
            or ""
        ).strip()
        if display:
            return {"display": display, "source": "project_registry_verified", "confidence": "verified"}

    web = reference.get("web_verified") or {}
    display = str(web.get("location") or reference.get("location") or "").strip()
    if display and (
        web
        or reference.get("reference_kind") == "web_verified_project"
    ):
        return {"display": display, "source": "project_web_verified", "confidence": "verified"}
    return None


def enrich_project_reference(facts: dict[str, Any]) -> dict[str, Any]:
    enriched = deepcopy(facts)
    reference = project_reference_for_key(enriched.get("project_key"))
    if reference:
        enriched["project_reference"] = reference
        enriched["project_reference_version"] = KNOWLEDGE_VERSION
    return enriched


def project_knowledge_stats() -> dict[str, int]:
    projects = _bundle()
    return {
        "projects": len(projects),
        "registry_profiles": sum(1 for value in projects.values() if value.get("registry")),
        "living_profiles": sum(1 for value in projects.values() if value.get("living")),
        "v5_profiles": sum(1 for value in projects.values() if value.get("v5_profile")),
        "relations": sum(len(value.get("relations") or []) for value in projects.values()),
        "evidence_rows": sum(len(value.get("evidence") or []) for value in projects.values()),
        "conflicts": sum(len(value.get("conflicts") or []) for value in projects.values()),
        "alias_rows": sum(len(value.get("aliases") or []) for value in projects.values()),
    }


def _project_reference_name(reference: dict[str, Any]) -> str:
    registry = reference.get("registry") or {}
    v5 = reference.get("v5_profile") or {}
    return str(
        registry.get("canonical_name_cn")
        or v5.get("中文常用名")
        or reference.get("display_name")
        or reference.get("knowledge_key")
        or "该项目"
    ).strip()


def _range_text(living: dict[str, Any], prefix: str, unit_key: str) -> str | None:
    low = str(living.get(f"{prefix}_min") or "").strip()
    high = str(living.get(f"{prefix}_max") or "").strip()
    if not low and not high:
        return None
    unit = str(living.get(unit_key) or "").strip()
    value = low or high
    if low and high and low != high:
        value = f"{low}–{high}"
    return f"{value} {unit}".strip()


def project_reference_summary(project_key: object) -> str | None:
    """Build a compact but information-dense project profile for read-time UI."""
    reference = project_reference_for_key(project_key)
    if not reference:
        return None
    if reference.get("reference_kind") == "project_family":
        corridors = "、".join(reference.get("known_corridors") or [])
        types = "、".join(reference.get("property_types") or [])
        parts = [
            str(reference.get("display_name") or "").strip(),
            f"开发商：{reference['developer']}" if reference.get("developer") else "",
            f"常见项目分布：{corridors}" if corridors else "",
            f"住宅类型：{types}" if types else "",
            str(reference.get("resolution_hint") or "").strip(),
        ]
        return "\n".join(part for part in parts if part)

    registry = reference.get("registry") or {}
    v5 = reference.get("v5_profile") or {}
    living = reference.get("living") or {}
    name = _project_reference_name(reference)
    location = project_reference_location(project_key)
    developer = registry.get("developer") or reference.get("developer")
    project_type = registry.get("project_type") or v5.get("项目类型") or reference.get("project_type")
    building = reference.get("building_profile")
    nearby = reference.get("nearby") or []
    if not nearby:
        raw_nearby = str(v5.get("附近地标") or "").strip()
        nearby = [item.strip() for item in raw_nearby.replace("；", "、").split("、") if item.strip()]
    amenities = list(reference.get("amenities") or [])
    if not amenities:
        amenity_fields = (
            ("泳池", "pool"), ("健身房", "gym"), ("桑拿", "sauna"),
            ("蒸汽房", "steam_room"), ("儿童区", "kids_playground"),
            ("花园", "garden"), ("屋顶", "rooftop"), ("24小时安保", "security_24h"),
            ("门禁", "access_card"), ("CCTV", "cctv"), ("备用发电", "generator"),
        )
        amenities = [
            label for label, field in amenity_fields
            if str(living.get(field) or "").strip().upper() in {"YES", "TRUE", "1"}
        ]
    parts = [name]
    if developer:
        parts.append(f"开发商：{developer}")
    if project_type:
        parts.append(f"项目类型：{project_type}")
    if location:
        parts.append(f"位置：{location['display']}")
    address = str(reference.get("address") or "").strip()
    if address and (not location or address not in location["display"]):
        parts.append(f"核实地址：{address}")
    completion_year = str(reference.get("completion_year") or living.get("building_year") or "").strip()
    if completion_year:
        parts.append(f"交付/完工资料：{completion_year}")
    if building:
        parts.append(f"项目规模：{building}")
    if amenities:
        parts.append(f"公区/配套：{'、'.join(str(item) for item in amenities)}")
    if nearby:
        parts.append(f"周边：{'、'.join(str(item) for item in nearby)}")
    living_summary = str(living.get("living_summary") or "").strip()
    if living_summary:
        parts.append(f"租住参考：{living_summary}")
    parts.append("具体房源的租金、水电、管理费、停车、押金和开放规则，以该套房源及当期物业/合同为准。")
    return "\n".join(parts)


def answer_project_question(project_key: object, question: object) -> str | None:
    """Answer deterministic project questions from the reference layer only.

    Returns None when the knowledge bundle cannot support the answer. Listing
    facts always take precedence; fee ranges are explicitly labelled as market
    practice/owner-specific when the source says so.
    """
    reference = project_reference_for_key(project_key)
    q = str(question or "").strip().casefold()
    if not reference or not q:
        return None
    if reference.get("reference_kind") == "project_family":
        if any(token in q for token in ("开发商", "谁开发", "开发公司")) and reference.get("developer"):
            return f"{reference['display_name']}开发商：{reference['developer']}。"
        if any(token in q for token in ("位置", "地址", "哪里", "在哪")):
            corridors = "、".join(reference.get("known_corridors") or [])
            return f"{reference['display_name']}不是单一地址，已知项目分布包括：{corridors}。{reference['resolution_hint']}"
        if any(token in q for token in ("户型", "房型", "类型", "排屋", "别墅", "公寓")):
            types = "、".join(reference.get("property_types") or [])
            return f"{reference['display_name']} family 包含：{types}。具体以子项目为准。"
        if any(token in q for token in ("项目", "配套", "泳池", "健身", "水费", "电费", "物业", "管理费", "停车")):
            return str(reference.get("resolution_hint") or "").strip() or None
        return None

    name = _project_reference_name(reference)
    registry = reference.get("registry") or {}
    v5 = reference.get("v5_profile") or {}
    living = reference.get("living") or {}

    if any(token in q for token in ("位置", "地址", "哪里", "在哪")):
        locations: list[str] = []
        for value in (
            v5.get("中文位置展示"),
            registry.get("public_location_display_cn"),
            registry.get("canonical_geo_display"),
            reference.get("location"),
            reference.get("address"),
        ):
            clean = str(value or "").strip()
            if clean and clean not in locations:
                locations.append(clean)
        if locations:
            return f"{name}：{'；'.join(locations)}。"

    if any(token in q for token in ("开发商", "谁开发", "开发公司")):
        developer = registry.get("developer") or reference.get("developer")
        if developer:
            return f"{name}开发商：{developer}。"

    fee_specs = (
        (("电费", "电价"), "electricity_rate", "electricity_unit", "electricity_scope"),
        (("水费", "水价"), "water_rate", "water_unit", "water_scope"),
        (("物业费", "管理费"), "management_fee", "management_fee_unit", "management_fee_scope"),
        (("汽车停车", "车位费", "停车费"), "car_parking_fee", "car_parking_fee_unit", "car_parking_fee_scope"),
        (("摩托停车", "摩托车位"), "motorbike_parking_fee", "motorbike_parking_fee_unit", "motorbike_parking_fee_scope"),
    )
    for tokens, prefix, unit_key, scope_key in fee_specs:
        if any(token in q for token in tokens):
            value = _range_text(living, prefix, unit_key)
            if value:
                scope = str(living.get(scope_key) or "").strip().upper()
                if scope == "OWNER_SPECIFIC":
                    caveat = "这是现有房源样本，不是项目统一收费；具体这套以房东/合同为准。"
                else:
                    caveat = "这是项目资料中的常见/参考口径；具体这套以房东和合同为准。"
                return f"{name}：{value}。{caveat}"

    amenity_tokens = {
        "泳池": "pool",
        "健身": "gym",
        "桑拿": "sauna",
        "蒸汽": "steam_room",
        "按摩池": "jacuzzi",
        "儿童": "kids_playground",
        "花园": "garden",
        "天台": "rooftop",
        "安保": "security_24h",
        "门禁": "access_card",
        "监控": "cctv",
        "发电机": "generator",
        "备用电": "backup_power",
    }
    asked = [(label, field) for label, field in amenity_tokens.items() if label in q]
    if asked:
        yes = [
            label for label, field in asked
            if str(living.get(field) or "").strip().upper() in {"YES", "TRUE", "1"}
        ]
        web_amenities = {str(item).strip() for item in reference.get("amenities") or []}
        yes.extend(label for label, _field in asked if any(label in item for item in web_amenities) and label not in yes)
        if yes:
            return f"{name}项目资料显示有：{'、'.join(yes)}。具体开放/收费规则以物业当期为准。"
        return None

    if any(token in q for token in ("楼龄", "哪年", "建成", "交付")) and living.get("building_year"):
        return f"{name}项目资料记录年份：{living['building_year']}。"
    if any(token in q for token in ("多少层", "总楼层", "几层")) and living.get("total_floors"):
        return f"{name}项目资料记录总楼层：{living['total_floors']}。"
    if any(token in q for token in ("多少户", "户数", "多少套")) and living.get("unit_count"):
        return f"{name}项目资料记录单位数：{living['unit_count']}。"

    if any(token in q for token in ("项目资料", "项目怎么样", "项目介绍", "配套", "详细资料", "楼盘资料")):
        return project_reference_summary(project_key)

    return None


def registry_project_identities() -> tuple[dict[str, Any], ...]:
    """Return research-backed identities safe to add to project recognition.

    VERIFIED/PARTIAL controls identity recognition only. No registry location or
    property type is promoted into authoritative listing facts here.
    """
    identities: list[dict[str, Any]] = []
    for knowledge_key, payload in _bundle().items():
        row = payload.get("registry") or {}
        status = row.get("verification_status", "")
        aliases: list[str] = []
        market_aliases = {
            alias.strip().casefold()
            for alias in str(row.get("market_aliases_cn", "") or "").split(";")
            if alias.strip()
        }
        if status in {"VERIFIED", "PARTIAL"}:
            for field, value in (
                ("canonical_name_cn", row.get("canonical_name_cn", "")),
                ("canonical_name_en", row.get("canonical_name_en", "")),
                ("search_aliases_cn", row.get("search_aliases_cn", "")),
            ):
                for alias in str(value or "").split(";"):
                    alias = alias.strip()
                    # Location-qualified market handles such as「一号路炳发」are
                    # search/navigation aliases, not safe project identities.
                    if field == "search_aliases_cn" and alias.casefold() in market_aliases:
                        continue
                    if alias and alias not in aliases:
                        aliases.append(alias)

        verified_alias_rows = [
            item for item in payload.get("aliases") or []
            if item.get("entity_level") == "PROJECT"
            and item.get("verification_status") == "VERIFIED"
            and item.get("resolution_action") == "DIRECT_RESOLVE"
            and str(item.get("search_only", "")).casefold() != "true"
            and str(item.get("conflict_flag", "")).casefold() != "true"
        ]
        for item in verified_alias_rows:
            alias = str(item.get("raw_alias", "") or "").strip()
            if alias and alias not in aliases:
                aliases.append(alias)

        if not aliases:
            continue
        display = (
            row.get("canonical_name_cn")
            or row.get("canonical_name_en")
            or next(
                (str(item.get("target_display_name") or "").strip() for item in verified_alias_rows if item.get("target_display_name")),
                knowledge_key,
            )
        )
        identities.append(
            {
                "taxonomy_key": taxonomy_key_for_knowledge(knowledge_key),
                "knowledge_key": knowledge_key,
                "display": display,
                "aliases": tuple(aliases),
                "verification_status": status or ("VERIFIED" if verified_alias_rows else "UNKNOWN"),
            }
        )
    return tuple(identities)


__all__ = [
    "KNOWLEDGE_VERSION",
    "enrich_project_reference",
    "knowledge_key_for_taxonomy",
    "project_reference_for_key",
    "project_reference_location",
    "project_reference_summary",
    "project_knowledge_stats",
    "answer_project_question",
    "registry_project_identities",
    "taxonomy_key_for_knowledge",
]
