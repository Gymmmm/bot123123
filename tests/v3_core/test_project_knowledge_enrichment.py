from __future__ import annotations

from v3_core.inventory.canonical_facts import canonicalize_source, draft_projection
from v3_core.inventory.project_knowledge import (
    project_knowledge_stats,
    registry_project_identities,
)


def test_project_knowledge_bundle_has_full_research_coverage():
    assert project_knowledge_stats() == {
        "projects": 114,
        "registry_profiles": 107,
        "living_profiles": 107,
        "v5_profiles": 113,
        "relations": 39,
        "evidence_rows": 53,
        "conflicts": 12,
        "alias_rows": 476,
    }


def test_bare_known_project_gets_detailed_reference_without_source_location():
    facts = canonicalize_source("The Palms别墅出租\n4房\n租金$2500/月")
    assert facts["project_key"] == "the_palms"
    assert facts["public_location_display"] is None

    reference = facts["project_reference"]
    assert reference["knowledge_key"] == "the-palms"
    assert reference["reference_only"] is True
    assert reference["registry"]["canonical_geo_display"] == "铁桥头 · 一号路 · 湄公河边"
    assert reference["registry"]["developer"] == "Oxley Worldbridge"
    assert "公共设施与安保较明确" in reference["living"]["living_summary"]
    assert reference["inheritance_policy"]["listing_fact_precedence"] is True


def test_project_reference_is_exposed_in_draft_projection():
    facts = canonicalize_source("The Peak 香格里拉\n2房\n租金$1400/月")
    projected = draft_projection(facts)
    assert projected["project_reference"]["knowledge_key"] == "the-peak"
    assert projected["project_reference"]["living"]["pool"] == "YES"
    assert projected["project_reference"]["evidence"]


def test_listing_specific_fee_is_not_overwritten_by_project_market_reference():
    facts = canonicalize_source("雅居乐天悦\n1房\n租金$900/月\n电费$0.35/度")
    assert facts["electric_rate"] == "$0.35/度"
    reference = facts["project_reference"]
    assert reference["knowledge_key"] == "agile-sky-residence"
    assert reference["living"]["electricity_rate_min"] == "0.25"
    assert reference["living"]["electricity_rate_max"] == "0.27"


def test_wealth_mansion_legacy_split_is_reconciled_to_one_verified_identity():
    wells = canonicalize_source("威尔斯公馆出租\n2房\n租金$1200/月")
    wealth = canonicalize_source("财富大厦出租\n2房\n租金$1200/月")
    assert wells["project_key"] == "wealth_mansion"
    assert wealth["project_key"] == "wealth_mansion"
    assert wells["project_reference"]["knowledge_key"] == "wealth-mansion"
    assert wealth["project_reference"]["knowledge_key"] == "wealth-mansion"
    assert wells["project_reference"]["v5_profile"]["中文常用名"] == "威尔斯公馆"


def test_market_handle_does_not_become_specific_borey_project():
    facts = canonicalize_source("铁桥头一号路炳发\n联排出租\n4房\n租金$2000")
    assert facts["project_key"] is None
    assert facts["project_brand"] == "Peng Huoth"
    assert facts["public_location_display"] == "铁桥头 · 一号路炳发"


def test_registry_expands_recognition_without_inheriting_authoritative_location():
    identities = {item["taxonomy_key"] for item in registry_project_identities()}
    assert "the_palms" in identities
    assert "chip_mong_park_land_598" in identities

    facts = canonicalize_source("Park Land 598别墅出租\n4房\n租金$1800/月")
    assert facts["project_key"] == "chip_mong_park_land_598"
    assert facts["project_reference"]["knowledge_key"] == "chip-mong-park-land-598"
    # Registry location is available as reference, but not promoted to an
    # authoritative listing location without an explicit safe location rule.
    assert facts["public_location_display"] is None
    assert facts["project_reference"]["registry"]["public_location_display_cn"] == "598路 · Chip Mong"


def test_projectless_listing_does_not_receive_project_reference():
    facts = canonicalize_source("BKK1两房出租\n租金$800/月")
    assert "project_reference" not in facts


def test_verified_alias_registry_adds_new_projects_without_family_false_positive():
    ts6 = canonicalize_source("Time Square 6出租\n1房\n租金$700/月")
    assert ts6["project_key"] == "times_square_6"
    assert ts6["project_reference"]["knowledge_key"] == "times-square-6"

    royal_condo = canonicalize_source("奥凯德皇家公寓出租\n2房\n租金$900/月")
    assert royal_condo["project_key"] == "orkide_the_royal_condominium"
    assert royal_condo["project_reference"]["knowledge_key"] == "orkide-the-royal-condominium"

    royal_villa = canonicalize_source("Orkide The Royal 别墅社区\n4房\n租金$2500/月")
    assert royal_villa["project_key"] == "orkide_the_royal"

    generic = canonicalize_source("时代广场出租\n1房\n租金$700/月")
    assert generic["project_key"] is None
