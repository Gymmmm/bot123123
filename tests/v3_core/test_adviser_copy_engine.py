from __future__ import annotations

from v3_core.adviser_copy import adviser_tags_from_facts, generate_adviser_lines, generate_adviser_text
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts


def _parse(text: str):
    base = canonicalize_source(text)
    return enrich_authoritative_facts(text, base)


def test_scanner_extracts_explicit_adviser_signals_without_changing_core_facts():
    raw = (
        "BKK1 公寓出租\n2房1厅\n租金 $800/月\n面积95㎡ 19楼\n"
        "每周保洁两次，物业费包含，宽带已经装好，可养宠物，带阳台，河景"
    )
    facts = _parse(raw)
    assert facts["monthly_rent_usd"] == 800
    assert facts["layout"] == "2房1厅"
    signals = set(facts["adviser_signals"])
    assert {"cleaning_2x", "management_wifi_ready", "pet_allowed", "balcony", "river_view"}.issubset(signals)


def test_admin_supplement_reparse_adds_adviser_signals():
    original = "BKK1 公寓出租\n2房1厅\n租金 $800/月\n面积95㎡"
    first = _parse(original)
    assert "cleaning_2x" not in first.get("adviser_signals", [])
    supplemented = original + "\n补充：每周保洁2次，物业费已含，网络费也包含。"
    second = _parse(supplemented)
    assert {"cleaning_2x", "management_wifi"}.issubset(set(second["adviser_signals"]))
    tags = adviser_tags_from_facts(second)
    assert tags[0] == "cleaning_2x"
    assert "management_wifi" in tags


def test_negative_feature_wording_is_not_promoted():
    facts = _parse(
        "BKK1 公寓出租 2房1厅 租金$800/月 面积95㎡\n"
        "不包物业费，不包网络费，无阳台，不可养宠物。"
    )
    signals = set(facts.get("adviser_signals") or [])
    assert "management_included" not in signals
    assert "wifi_included" not in signals
    assert "management_wifi" not in signals
    assert "balcony" not in signals
    assert "pet_allowed" not in signals


def test_v2_is_single_point_and_deterministic_even_with_many_signals():
    facts = {
        "layout": "2房1厅",
        "bedrooms": 2,
        "adviser_signals": ["management_wifi", "pool", "gym", "never_lived", "new_condition"],
    }
    tags = adviser_tags_from_facts(facts)
    assert "management_wifi" in tags
    assert "pool_gym" in tags
    assert "pool" not in tags
    assert "gym" not in tags
    assert "never_lived" in tags
    assert "new_condition" not in tags
    first = generate_adviser_text(facts, seed="QL-RF-A2B3|hash", max_points=99)
    assert first
    assert "\n" not in first
    assert generate_adviser_lines(facts, seed="QL-RF-A2B3|hash", max_points=99) == [first]
    assert all(generate_adviser_text(facts, seed="QL-RF-A2B3|hash", max_points=99) == first for _ in range(100))


def test_no_fallback_when_no_useful_fact_exists():
    assert generate_adviser_text({}, seed="empty", max_points=1, allow_fallback=True) == ""
    assert generate_adviser_lines({}) == []


def test_property_type_pool_gym_and_furniture_alone_are_silent():
    for facts in (
        {"property_type": "公寓"},
        {"amenities": ["泳池"]},
        {"amenities": ["健身房"]},
        {"house": {"furnished": True}},
    ):
        assert generate_adviser_lines(facts, seed="ordinary-only") == []


def test_room_specific_copy_never_crosses_layouts():
    one = generate_adviser_text({"layout": "1房1厅", "bedrooms": 1}, seed="one")
    two = generate_adviser_text({"layout": "2房1厅", "bedrooms": 2}, seed="two")
    three = generate_adviser_text({"layout": "3房2厅", "bedrooms": 3}, seed="three")
    assert "一房" in one and "两房" not in one and "三房" not in one
    assert "两房" in two and "三房" not in two
    assert "三房" in three and "两房" not in three


def test_verified_condition_pet_view_and_floor_can_create_one_useful_judgement():
    assert "全新未入住" in generate_adviser_text({"never_lived": True})
    assert "宠物" in generate_adviser_text({"pet_allowed": True})
    view = generate_adviser_text({"floor": "19", "river_view": True})
    assert "河景" in view and "遮挡" in view
    assert len(generate_adviser_lines({"floor": "19", "river_view": True, "pet_allowed": True}, max_points=99)) == 1


def test_wifi_included_and_wifi_ready_keep_distinct_semantics():
    fee_line = generate_adviser_text({"adviser_signals": ["wifi_included"]}, seed="fee")
    ready_line = generate_adviser_text({"adviser_signals": ["wifi_ready"]}, seed="ready")
    assert "包含" in fee_line and "套餐" in fee_line
    assert "已就绪" in ready_line and "实际可用状态" in ready_line


def test_generic_cleaning_included_does_not_guess_frequency():
    tags = adviser_tags_from_facts({"services": {"cleaning": "包含"}})
    assert tags == ["cleaning_included"]
    line = generate_adviser_text({"services": {"cleaning": "包含"}})
    assert "保洁服务" in line
    assert not any(token in line for token in ("每周一次", "每周两次", "每周三次"))


def test_villa_with_real_useful_facts_keeps_real_facts_only_and_single_point():
    facts = {"property_type": "别墅", "adviser_signals": ["management_wifi", "cleaning_2x"]}
    tags = adviser_tags_from_facts(facts)
    assert "villa" not in tags
    assert "management_wifi" in tags
    assert "cleaning_2x" in tags
    lines = generate_adviser_lines(facts, seed="same", max_points=99)
    assert len(lines) == 1
    assert lines == generate_adviser_lines(facts, seed="same", max_points=99)
