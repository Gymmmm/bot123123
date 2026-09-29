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


def test_verified_bali3_chinese_alias_gets_chroy_changvar_default() -> None:
    facts = canonicalize_source("巴厘3，一房一厅 租金300-350 押一付一，合同一年。")
    assert facts["project_key"] == "bali_3"
    assert facts["public_location_key"] == "水净华"


def test_verified_yuetai_ecc_alias_gets_tonle_bassac_default() -> None:
    facts = canonicalize_source("#粤泰ECC 小单间：$200/月 家具齐全")
    assert facts["project_key"] == "yuetai_ecc"
    assert facts["public_location_key"] == "百色河"
    assert facts["public_location_display"] == "诺罗敦大道 · 永旺1附近"


def test_bare_yuetai_is_not_forced_to_ecc() -> None:
    result = classify_listing_taxonomy("#粤泰 三房公寓出租")
    assert result.project_key is None


def test_spaced_aeon2_is_safe_nearby_location() -> None:
    facts = canonicalize_source("永旺 2 附近独栋别墅 出租价格：2800 房间7+2")
    assert facts["public_location_key"] == "永旺2"


def test_picasso_bkk1_does_not_gain_one_letter_daun_penh_false_positive() -> None:
    facts = canonicalize_source("#Picasso City Garden BKK1 两房出租 $1200/月")
    assert facts["project_key"] == "picasso_city_garden"
    assert facts["public_location_key"] == "BKK1"
    assert "隆边" not in facts["market_location_keys"]
    assert "ambiguous_market_location" not in facts["candidate_flags"]


def test_nearby_landmarks_do_not_override_verified_project_location() -> None:
    facts = canonicalize_source(
        "#香格里拉 两房出租 $1400/月\n📍周边：金界｜金街｜永旺1"
    )
    assert facts["project_key"] == "the_peak"
    assert facts["public_location_key"] == "百色河"
    assert facts["public_location_display"] == "金街附近"
    assert "ambiguous_market_location" not in facts["candidate_flags"]


def test_conflicting_corridors_remain_ambiguous() -> None:
    facts = canonicalize_source("#洪森大道50米路 双拼别墅出租 $1450/月")
    assert "ambiguous_market_location" in facts["candidate_flags"]


def test_bare_yuetai_market_shorthand_resolves_to_yuetai_ecc():
    facts = canonicalize_source("#粤泰\n一房一厅出租\n$500/月")
    assert facts["project_key"] == "yuetai_ecc"
    assert facts["public_location_display"] == "诺罗敦大道 · 永旺1附近"


def test_yuetai_apartment_market_shorthand_resolves_to_yuetai_ecc():
    facts = canonicalize_source("#粤泰公寓\n两房出租")
    assert facts["project_key"] == "yuetai_ecc"


def test_phnom_penh_central_plaza_market_name_resolves_to_prince_central():
    facts = canonicalize_source("#金边中央广场\n一房出租")
    assert facts["project_key"] == "prince_central_plaza"
    assert facts["public_location_display"] == "独立碑附近"


def test_prince_times_square_is_not_megakim_time_square_series():
    facts = canonicalize_source("#太子时代广场\n两房出租")
    assert facts["project_key"] == "prince_times_square"
    assert facts["public_location_display"] == "诺罗敦大道南段"


def test_bare_time_square_remains_phase_ambiguous():
    facts = canonicalize_source("#时代广场\n两房出租")
    assert facts.get("project_key") in (None, "")
