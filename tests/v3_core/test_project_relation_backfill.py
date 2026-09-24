"""VERIFIED project → location / property_type Canonical backfill."""
from __future__ import annotations

from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.inventory.listing_taxonomy import (
    PROJECT_IDENTITIES,
    classify_listing_taxonomy,
    project_identity_by_key,
    public_location_from_fields,
)


def test_pinnacle_bare_text_gets_verified_location_and_single_type():
    facts = canonicalize_source("The Pinnacle\n2房\n租金$850/月\n39楼")
    assert facts["project_key"] == "the_pinnacle"
    assert facts["project_name"] == "The Pinnacle 幸福广场"
    assert facts["public_location_key"] == "百色河"
    assert facts["public_location_display"] == "莫尼旺大道附近"
    assert facts["public_location_display"] != facts["project_name"]
    assert facts["publication_location_level"] == "level_1_market_confirmed"
    assert facts["property_type"] == "公寓"
    assert facts["property_type_status"] == "inferred"


def test_pinnacle_chinese_alias_uses_geographic_area_not_project_name():
    facts = canonicalize_source("太子幸福广场\n2房2厅\n租金$850/月\n39楼")
    assert facts["project_key"] == "the_pinnacle"
    assert facts["public_location_display"] == "莫尼旺大道附近"
    assert "幸福广场" not in (facts["public_location_display"] or "")


def test_rf_city_mixed_does_not_infer_apartment():
    facts = canonicalize_source("富力城出租\n2房\n租金$800/月")
    assert facts["project_key"] == "rf_city"
    assert facts["public_location_display"] == "60米大道 · 永旺3附近"
    assert facts["property_type"] == "未知"
    assert facts["property_type_status"] == "unknown"


def test_rf_city_explicit_apartment_text_still_wins():
    facts = canonicalize_source("富力城公寓出租\n2房\n租金$800/月")
    assert facts["project_key"] == "rf_city"
    assert facts["property_type"] == "公寓"
    assert facts["property_type_status"] == "confirmed"


def test_public_location_never_equals_project_name_fallback():
    key, display, level = public_location_from_fields(
        project_key="mystery_project",
        project_name="Mystery Tower",
    )
    assert key is None
    assert display is None
    assert level == "unknown"


def test_verified_project_default_location_lookup_without_markets():
    project = project_identity_by_key("the_bridge")
    assert project is not None
    assert project.default_location_key == "百色河"
    key, display, level = public_location_from_fields(project_key="the_bridge")
    assert key == "百色河"
    assert display == "金街附近"
    assert level == "level_1_market_confirmed"


def test_source_location_beats_project_default():
    tax = classify_listing_taxonomy("The Pinnacle 幸福广场\n位置：BKK1\n2房\n租金$850/月")
    # BKK1 is extracted as market/district; project default must not replace it.
    assert "BKK1" in tax.market_location_keys or tax.canonical_area_key == "BKK1"
    facts = canonicalize_source("The Pinnacle 幸福广场\n位置：BKK1\n2房\n租金$850/月")
    assert facts["public_location_display"] == "BKK1"
    assert facts["project_name"] == "The Pinnacle 幸福广场"


def test_batch1_peak_uses_canonical_geo_not_market_alias():
    peak = canonicalize_source("香格里拉公寓出租\n2房\n租金$999/月")
    assert peak["project_key"] == "the_peak"
    # Living-facts: canonical GEO = 百色河; 金界/钻石岛 are market-only.
    assert peak["public_location_key"] == "百色河"
    assert peak["public_location_display"] == "金街附近"
    assert peak["public_location_display"] != peak["project_name"]


def test_j_tower_phases_do_not_share_one_campus_location():
    jt1 = canonicalize_source("J Tower 1出租\n2房\n租金$900/月")
    jt3 = canonicalize_source("J Tower 3出租\n2房\n租金$900/月")
    assert jt1["project_key"] == "j_tower_1"
    assert jt3["project_key"] == "j_tower_3"
    assert jt1["public_location_key"] == "BKK1"
    assert jt3["public_location_key"] == "百色河"


def test_v5_agile_and_time_square_phase_locations():
    agile = canonicalize_source("雅居乐天悦出租\n2房\n租金$800/月")
    assert agile["project_key"] == "agile_sky_residence"
    assert agile["public_location_key"] == "BKK3"
    assert agile["public_location_display"] == "BKK3 · 莫尼旺大道"

    ts1 = canonicalize_source("Time Square 1出租\n1房\n租金$700/月")
    ts2 = canonicalize_source("Time Square 2出租\n1房\n租金$700/月")
    ts8 = canonicalize_source("Time Square 8出租\n1房\n租金$700/月")
    assert ts1["project_key"] == "time_square_1"
    assert ts2["project_key"] == "time_square_2"
    assert ts8["project_key"] == "time_square_8"
    assert ts1["public_location_key"] == "BKK1"
    assert ts2["public_location_key"] == "TK/7月区"
    assert ts8["public_location_key"] == "俄罗斯市场"


def test_prince_cluster_customer_facing_locations():
    central = canonicalize_source("太子中央广场出租\n2房\n租金$850/月")
    modern = canonicalize_source("太子现代广场出租\n1房\n租金$600/月")
    assert central["project_key"] == "prince_central_plaza"
    assert modern["project_key"] == "prince_modern_plaza"
    assert central["public_location_display"] == "独立碑附近"
    assert modern["public_location_display"] == "诺罗敦大道"
    assert "百色河" not in (central["public_location_display"] or "")


def test_web_verified_remaining_project_locations():
    cases = [
        ("M Residence出租\n1房\n租金$900/月", "m_residence", "BKK1"),
        ("Odom Living出租\n2房\n租金$1200/月", "odom_living", "独立碑附近"),
        ("半岛御景出租\n2房\n租金$1000/月", "peninsula_private_residence", "水净华 · 日本桥附近"),
        ("ONE 70出租\n1房\n租金$700/月", "one_70", "隆边 · PPCC附近"),
        ("Chief Tower出租\n2房\n租金$1100/月", "chief_tower", "BKK1 · 莫尼旺大道"),
        ("皇家花园公寓出租\n2房\n租金$800/月", "royal_park", "堆谷（TK）"),
    ]
    for text, key, display in cases:
        facts = canonicalize_source(text)
        assert facts["project_key"] == key, text
        assert facts["public_location_display"] == display, text
        assert "百色河" not in (facts["public_location_display"] or "")


def test_core_verified_projects_declare_mode_and_location_contract():
    required = {"the_pinnacle", "rf_city", "the_bridge", "aeon1", "peng_huoth_city"}
    found = {item.key: item for item in PROJECT_IDENTITIES if item.key in required}
    assert set(found) == required
    assert found["the_pinnacle"].property_type_mode == "single"
    assert found["the_pinnacle"].default_location_key == "百色河"
    assert found["the_bridge"].default_location_key == "百色河"
    assert found["rf_city"].property_type_mode == "mixed"
    assert found["rf_city"].property_family is None
    assert found["peng_huoth_city"].property_type_mode == "mixed"


def test_projectless_villa_posts_resolve_road_corridors():
    """排屋/别墅帖常无具体楼盘名，靠路名/走廊词定位。"""
    cases = [
        ("炳发城出租\n联排\n租金$1800", None, "炳发城", "一号路 / 60米 / 50米炳发"),
        ("铁桥头一号路炳发\n联排出租\n4房\n租金$2000", "Peng Huoth", "一号路", "铁桥头 · 一号路炳发"),
        ("一号路炳发\n别墅\n租金$2500", "Peng Huoth", "一号路", "一号路炳发"),
        ("60米炳发联排\n租金$2200", "Peng Huoth", "洪森大道", "60米大道 · 炳发"),
        ("50米炳发城\n排屋出租\n租金$1500", None, "50米路", "50米炳发"),
        ("洪森大道别墅\n4房5卫\n租金$2500", None, "洪森大道", "60米大道"),
        ("6号路炳发排屋\n租金$1700", "Peng Huoth", "6号路", "6号路炳发"),
    ]
    for text, brand, loc_key, loc_display in cases:
        facts = canonicalize_source(text)
        assert facts["public_location_key"] == loc_key, text
        assert facts["public_location_display"] == loc_display, text
        if brand:
            assert facts["project_brand"] == brand, text
        # Corridor beats district when both appear (铁桥头 + 一号路).
        if "铁桥头" in text and "一号路" in text:
            assert facts["market_location_keys"][0] == "一号路", text
            assert "铁桥头" in facts["market_location_keys"]

