from __future__ import annotations

from v3_core.user_bot.search_query import (
    detect_location_keys,
    detect_property_type,
    detect_room_type,
    parse_budget_range,
    parse_search_criteria,
)


def test_budget_range_matches_locked_production_behavior():
    assert parse_budget_range("预算800以内") == (None, 800)
    assert parse_budget_range("800以上") == (800, None)
    assert parse_budget_range("500-800") == (500, 800)
    assert parse_budget_range("预算800") == (600, 1000)
    assert parse_budget_range("没写预算") == (None, None)


def test_property_and_room_detection_match_locked_keywords():
    assert detect_property_type("想找 villa") == "别墅"
    assert detect_property_type("BKK1 apartment studio") == "公寓"
    assert detect_property_type("办公室 1000以内") == "办公室"
    assert detect_room_type("BKK1 一房 800内") == "1房"
    assert detect_room_type("想找2br") == "2房"
    assert detect_room_type("studio") == "studio"


def test_location_detection_is_derived_from_canonical_taxonomy():
    assert detect_location_keys("BKK1 一房 800以内") == ("BKK1",)
    assert "富力城" in detect_location_keys("富力城两房")
    assert "钻石岛" in detect_location_keys("diamond island apartment")
    assert detect_location_keys("BKK2 / BKK3") == ("BKK2", "BKK3")
    assert detect_location_keys("不限区域") == ()


def test_parse_search_criteria_records_room_type_without_promoting_it_to_property_filter():
    criteria = parse_search_criteria("BKK1 一房 800以内")

    assert criteria.location_keys == ("BKK1",)
    assert criteria.room_type == "1房"
    assert criteria.property_type == ""
    assert criteria.budget_min is None
    assert criteria.budget_max == 800
    assert criteria.has_filter


def test_studio_keeps_production_dual_signal_property_and_room_type():
    criteria = parse_search_criteria("钻石岛 studio 600")

    assert criteria.property_type == "公寓"
    assert criteria.room_type == "studio"
    assert "钻石岛" in criteria.location_keys
    assert criteria.budget_min == 400
    assert criteria.budget_max == 800
