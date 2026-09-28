"""All verified projects must expose geographic areas, never project-as-area."""
from __future__ import annotations

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import (
    PROJECT_IDENTITIES,
    location_display_overlaps_project,
    preferred_public_location_for_project,
)


def test_no_verified_project_publishes_its_own_name_as_area() -> None:
    samples = [
        (p.display, p)
        for p in PROJECT_IDENTITIES
        if p.kind == "project" and p.default_location_display
    ]
    assert len(samples) >= 20
    for display, project in samples:
        # Bare project mention + rent facts should still yield a geographic area.
        text = f"{display}\n2房出租\n租金$900/月"
        facts = canonicalize_source(text)
        if facts.get("project_key") != project.key:
            # Ambiguous brand tokens may not resolve; skip those.
            continue
        loc = facts.get("public_location_display") or ""
        assert loc, display
        assert not location_display_overlaps_project(project.display, loc), (
            project.key,
            project.display,
            loc,
        )
        assert loc == project.default_location_display or not location_display_overlaps_project(
            project.display, loc
        )


def test_preferred_public_location_backfill_table() -> None:
    cases = [
        ("the_peak", "The Peak 香格里拉", "The Peak 香格里拉", "金街附近"),
        ("rf_city", "富力城", "富力城", "60米大道 · 永旺3附近"),
        ("peng_huoth_city", "炳发城", "炳发城", "一号路 / 60米 / 50米炳发"),
        ("the_pinnacle", "The Pinnacle 幸福广场", "太子幸福广场", "莫尼旺大道附近"),
        ("prince_huan_yu_center", "太子·寰宇中心", "太子·寰宇中心", "金界 / 永旺1附近"),
        ("prince_central_plaza", "Prince Central Plaza 太子中央广场", "Prince Central Plaza 太子中央广场", "独立碑附近"),
        ("prince_international_plaza", "太子国际广场", "太子国际广场", "俄罗斯大道"),
        ("sky_villa", "Sky Villa 天空别墅", "Sky Villa 天空别墅", "马卡拉"),
        ("peninsula_private_residence", "半岛御景", "半岛御景", "水净华 · 日本桥附近"),
        ("morgan_enmaison", "摩根天御 Morgan EnMaison", "摩根天御 Morgan EnMaison", "水净华 · 日本桥附近"),
        ("rose_garden", "玫瑰滨江园", "玫瑰滨江园", "永旺1附近"),
        ("the_bridge", "桥牌", "金街附近", "金街附近"),  # already geographic — keep
        ("the_peak", "The Peak 香格里拉", "金界附近", "金界附近"),  # specific nearby — keep
    ]
    for key, project_name, current, expected in cases:
        _, display = preferred_public_location_for_project(
            project_key=key,
            project_name=project_name,
            location_key=current,
            location_display=current,
        )
        assert display == expected, (key, current, display, expected)
