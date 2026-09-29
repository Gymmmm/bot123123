from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import classify_listing_taxonomy


def test_decorative_emoji_inside_verified_chinese_project_name_is_ignored() -> None:
    result = classify_listing_taxonomy("☀️金边 首都💕国金 单间户型 360$/月")
    assert result.project_key == "urban_village_1"
    assert result.project_name
    facts = canonicalize_source("☀️金边 首都💕国金 单间户型 360$/月")
    assert facts["public_location_key"] == "洪森大道"
    assert "ambiguous_project" not in result.flags


def test_specific_project_alias_beats_nested_generic_project_alias() -> None:
    result = classify_listing_taxonomy("#紫晶壹号 两房一厅 河景公寓")
    assert result.project_key == "la_vista_one"
    assert result.project_name
    facts = canonicalize_source("#紫晶壹号 两房一厅 河景公寓")
    assert facts["public_location_key"] == "水净华"
    assert "ambiguous_project" not in result.flags


def test_bare_time_square_remains_unresolved_when_phase_is_unknown() -> None:
    result = classify_listing_taxonomy("#时代广场 两房公寓出租")
    assert result.project_key is None


def test_explicit_time_square_phase_still_resolves() -> None:
    result = classify_listing_taxonomy("#时代广场1 两房公寓出租")
    assert result.project_key == "time_square_1"
