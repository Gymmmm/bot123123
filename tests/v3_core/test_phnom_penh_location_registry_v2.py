from __future__ import annotations

import pytest

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import public_location_from_fields
from v3_core.inventory.phnom_penh_location_registry import (
    project_search_terms,
    registry_alias_count,
    registry_project_count,
    unresolved_project_names,
)


def _canonical(project_text: str):
    return canonicalize_source(f"{project_text} 公寓出租\n2房1厅\n租金 $800/月")


@pytest.mark.parametrize("alias", ["雅居乐", "Agile", "Agile Sky", "Agile Sky Residence"])
def test_agile_aliases_resolve_to_one_project_and_registry_geo(alias):
    facts = _canonical(f"BKK1 {alias}")
    assert facts["project_key"] == "agile_sky"
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
    assert facts["project_name"] == "香格里拉"
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
    assert registry_project_count() >= 19
    assert registry_alias_count() > registry_project_count()
    needs = set(unresolved_project_names())
    assert {"玫瑰滨江园", "太子幸福广场", "太子国际广场", "太子寰宇", "财富大厦", "首都国金", "皇家一号", "毕加索", "摩根", "The Gateway", "Urban Village", "炳发城"}.issubset(needs)
    assert {"雅居乐", "桥牌", "香格里拉", "富力城"}.isdisjoint(needs)
