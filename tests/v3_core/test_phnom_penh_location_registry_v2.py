from __future__ import annotations

import pytest

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import public_location_from_fields
from v3_core.inventory.phnom_penh_location_registry import (
    duplicate_project_aliases,
    project_search_terms,
    resolve_project_alias,
    registry_alias_count,
    registry_project_count,
    unresolved_project_names,
)


def _canonical(project_text: str):
    return canonicalize_source(f"{project_text} 公寓出租\n2房1厅\n租金 $800/月")


@pytest.mark.parametrize("alias", ["雅居乐", "Agile", "Agile Sky", "Agile Sky Residence"])
def test_agile_aliases_resolve_to_one_project_and_registry_geo(alias):
    facts = _canonical(f"BKK1 {alias}")
    assert facts["project_key"] == "agile_sky_residence"
    assert facts["project_name"] == "雅居乐"
    assert facts["canonical_area_key"] == "BKK3"
    assert facts["canonical_area_display"] == "BKK3"
    assert facts["public_location_key"] == "BKK3"
    assert facts["public_location_display"] == "BKK3 · 莫尼旺大道"
    assert "BKK1" in facts["market_location_keys"]


@pytest.mark.parametrize("alias", ["The Bridge", "Bridge", "桥牌", "桥牌公寓", "世桥"])
def test_bridge_aliases_resolve_to_one_project(alias):
    facts = _canonical(alias)
    assert facts["project_key"] == "the_bridge"
    assert facts["project_name"] == "桥牌"
    assert facts["canonical_area_key"] == "百色河"
    assert facts["public_location_display"] == "百色河"


@pytest.mark.parametrize("alias", ["The Peak", "Peak", "香格里拉", "香格里拉公寓"])
def test_peak_aliases_resolve_to_one_project(alias):
    facts = _canonical(alias)
    assert facts["project_key"] == "the_peak"
    assert facts["project_name"] == "The Peak 香格里拉"
    assert facts["canonical_area_key"] == "钻石岛"
    assert facts["public_location_display"] == "钻石岛"


@pytest.mark.parametrize("alias", ["R&F City", "R&F", "富力城"])
def test_rf_city_aliases_resolve_to_one_project(alias):
    facts = _canonical(alias)
    assert facts["project_key"] == "rf_city"
    assert facts["project_name"] == "富力城"
    assert facts["canonical_area_key"] is None
    assert facts["public_location_key"] == "洪森大道"
    assert facts["public_location_display"] == "洪森大道"


def test_project_name_is_never_public_location_fallback():
    assert public_location_from_fields(
        project_key="project:unknown",
        project_name="某项目",
    ) == (None, None, "unknown")


def test_bkk1_agile_search_terms_keep_project_identity_separate_from_market_alias():
    terms = project_search_terms("BKK1 雅居乐")
    assert "雅居乐" in terms
    assert "Agile Sky Residence" in terms
    assert "BKK1" not in terms


def test_registry_exposes_counts_and_unverified_projects_without_guessing_geo():
    assert registry_project_count() == 19
    assert registry_alias_count() > registry_project_count()
    needs = set(unresolved_project_names())
    assert {"玫瑰滨江园", "The Pinnacle 幸福广场", "太子国际广场", "太子·寰宇中心", "财富大厦", "Picasso City Garden 毕加索", "首都·国金 Urban Village Phase 2", "炳发城"}.issubset(needs)
    assert {"雅居乐", "桥牌", "The Peak 香格里拉", "富力城"}.isdisjoint(needs)


@pytest.mark.parametrize(
    "alias,expected_key",
    [
        ("雅居乐", "agile_sky_residence"),
        ("雅居乐公寓", "agile_sky_residence"),
        ("Agile", "agile_sky_residence"),
        ("Agile Sky", "agile_sky_residence"),
        ("Agile Sky Residence", "agile_sky_residence"),
        ("The Bridge", "the_bridge"),
        ("Bridge", "the_bridge"),
        ("桥牌", "the_bridge"),
        ("桥牌公寓", "the_bridge"),
        ("世桥", "the_bridge"),
        ("R&F City", "rf_city"),
        ("R&F", "rf_city"),
        ("富力城", "rf_city"),
        ("The Peak", "the_peak"),
        ("Peak", "the_peak"),
        ("香格里拉", "the_peak"),
        ("香格里拉公寓", "the_peak"),
        ("毕加索", "picasso_city_garden"),
        ("太子寰宇", "prince_huan_yu_center"),
    ],
)
def test_project_alias_resolves_to_at_most_one_canonical_identity(alias, expected_key):
    resolved = resolve_project_alias(alias)
    assert resolved is not None
    assert resolved.key == expected_key


def test_no_duplicate_normalized_project_aliases():
    duplicates = duplicate_project_aliases()
    assert duplicates == {}, "\n".join(
        f"{alias}: {', '.join(keys)}"
        for alias, keys in sorted(duplicates.items())
    )


@pytest.mark.parametrize(
    "landmark",
    ["永旺1", "永旺2", "永旺3", "金街", "金界"],
)
def test_market_landmark_never_becomes_project_name(landmark):
    facts = _canonical(landmark)
    assert facts["project_key"] is None
    assert facts["project_name"] is None


def test_aeon1_is_market_landmark_not_project():
    facts = _canonical("AEON Mall Phnom Penh 永旺1附近")
    assert facts["project_key"] is None
    assert facts["project_name"] is None
    assert "永旺商圈" in facts["market_location_keys"]
