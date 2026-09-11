from __future__ import annotations

from v3_core.inventory.qiaolian_say import (
    enrich_qiaolian_facts,
    extract_qiaolian_tags,
    qiaolian_notes_from_facts,
)


def _facts(**overrides):
    base = {
        "schema_version": "canonical_facts.v1",
        "canonical_facts_hash": "old-hash",
        "property_type": "公寓",
        "layout": "2房1厅",
        "bedrooms": 2,
        "size_sqm": 95,
        "floor": "18",
        "quality": {"score": 100, "all_flags": []},
    }
    base.update(overrides)
    return base


def test_scanner_prefers_combined_management_wifi_and_exact_cleaning_frequency():
    facts = _facts(management_fee="包含", internet_fee="包含")
    tags = extract_qiaolian_tags("每周固定两次保洁上门", facts)

    assert "management_wifi" in tags
    assert "management_included" not in tags
    assert "wifi_included" not in tags
    assert "cleaning_2x" in tags
    assert "cleaning_1x" not in tags
    assert "cleaning_3x" not in tags


def test_never_lived_private_pool_and_river_view_suppress_weaker_variants():
    tags = extract_qiaolian_tags(
        "全新未入住，装修很新，私人泳池，泳池健身房都有，江景和城市景观都能看到",
        _facts(),
    )

    assert "never_lived" in tags
    assert "new_condition" not in tags
    assert "private_pool" in tags
    assert "pool" not in tags
    assert "pool_gym" not in tags
    assert "river_view" in tags
    assert "city_view" not in tags
    assert "gym" in tags


def test_pet_negative_wins_over_positive_noise():
    tags = extract_qiaolian_tags("有人问可养宠物吗？这套不允许养宠物。", _facts())
    assert "pet_allowed" not in tags


def test_large_layout_and_floor_are_rule_based_without_extra_claims():
    tags = extract_qiaolian_tags("", _facts(size_sqm=108, floor="22"))
    assert "large_layout" in tags
    assert "high_floor" in tags
    notes = qiaolian_notes_from_facts(
        {**_facts(size_sqm=108, floor="22"), "qiaolian_tags": tags},
        seed="QL-TEST-1",
    )
    assert len(notes) == 2
    assert all("安静" not in note and "不潮" not in note for note in notes)


def test_manual_qiaolian_say_replaces_auto_and_is_limited_to_two_lines():
    enriched = enrich_qiaolian_facts(
        "物业费含，网络费含\n侨联说重置\n侨联说：第一句。\n侨联说：第二句。\n侨联说：第三句。",
        _facts(),
    )

    assert enriched["qiaolian_say"] == ["第二句。", "第三句。"]
    assert qiaolian_notes_from_facts(enriched, seed="QL-TEST-2") == ["第二句。", "第三句。"]
    assert enriched["canonical_facts_hash"] != "old-hash"


def test_manual_reset_without_copy_restores_auto_generation():
    enriched = enrich_qiaolian_facts(
        "物业费含，网络费含\n侨联说：旧句。\n侨联说重置",
        _facts(management_fee="包含", internet_fee="包含"),
    )

    assert "qiaolian_say" not in enriched
    assert enriched["qiaolian_tags"][0] == "management_wifi"
    assert qiaolian_notes_from_facts(enriched, seed="QL-TEST-3")


def test_manual_disable_suppresses_everything():
    enriched = enrich_qiaolian_facts(
        "物业费含，网络费含\n侨联说重置\n侨联说关闭",
        _facts(management_fee="包含", internet_fee="包含"),
    )

    assert enriched["qiaolian_say_disabled"] is True
    assert qiaolian_notes_from_facts(enriched, seed="QL-TEST-4") == []


def test_fallback_is_deterministic_and_not_more_than_one_when_no_tags():
    facts = enrich_qiaolian_facts("普通房源", _facts(size_sqm=80, floor="10"))
    first = qiaolian_notes_from_facts(facts, seed="QL-TEST-5")
    second = qiaolian_notes_from_facts(facts, seed="QL-TEST-5")

    assert first == second
    assert len(first) == 1
