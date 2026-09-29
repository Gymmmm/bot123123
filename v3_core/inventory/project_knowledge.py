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
    "the_penthouse_residence": {
        "knowledge_key": "web:the-penthouse-residence",
        "reference_kind": "web_verified_project",
        "display_name": "The Penthouse Residence",
        "project_type": "公寓 / 服务式住宅",
        "location": "Tonle Bassac · Sothearos Boulevard",
        "address": "No. 83B, Sothearos Boulevard, Phnom Penh",
        "amenities": ["泳池", "健身房", "停车", "Sky Bar"],
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
        "building_profile": "公开项目资料称约25层、161个住宅单位，含Studio/1房/2房/3房。",
        "source_urls": [
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
        "resolution_hint": "先按道路/地标缩小炳发 family，再解析具体项目；裸炳发/炳发城不能继承某个子项目收费或配套。",
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
    "project_knowledge_stats",
    "registry_project_identities",
    "taxonomy_key_for_knowledge",
]
