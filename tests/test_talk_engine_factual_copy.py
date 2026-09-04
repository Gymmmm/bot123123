from __future__ import annotations

from qiaolian_dual.talk_engine import generate_talk


def test_known_costs_are_spoken_in_plain_language_without_inventing_values():
    listing = {
        "listing_id": "QC0350",
        "normalized_data": {
            "management_fee": "包含",
            "internet_fee": "包含",
            "electric_rate": "$0.25/度",
            "water_rate": "$0.50/吨",
        },
    }
    text = generate_talk(listing, max_points=2)
    assert "物业包了" in text
    assert "网络包了" in text
    assert "电费$0.25/度" in text
    assert "水费$0.50/吨" in text
    assert "停车" not in text


def test_service_fact_uses_fixed_plain_copy():
    listing = {
        "listing_id": "QC0351",
        "normalized_data": {
            "services": {"pest_control": "包含", "cleaning": "每周2次"},
        },
    }
    text = generate_talk(listing, max_points=2)
    assert "灭虫" in text
    assert "保洁" in text
    assert "值得" not in text
    assert "性价比" not in text


def test_fee_summary_leaves_room_for_one_real_service_point():
    listing = {
        "listing_id": "QC0355",
        "normalized_data": {
            "management_fee": "包含",
            "electric_rate": "$0.25/度",
            "services": {"pest_control": "包含", "cleaning": "每周2次"},
        },
    }
    lines = generate_talk(listing, max_points=2).splitlines()
    assert len(lines) == 2
    assert "灭虫" in lines[0]
    assert "保洁" in lines[1]
    assert "电费$0.25/度" not in lines[0]


def test_no_supported_fact_means_no_automatic_talk():
    listing = {"listing_id": "QC0352", "project": "测试项目", "price": 800}
    assert generate_talk(listing, max_points=2, allow_empty=True) == ""


def test_unknown_costs_are_not_rendered_as_facts():
    listing = {
        "listing_id": "QC0353",
        "normalized_data": {
            "management_fee": "待确认",
            "internet_fee": "未知",
            "electric_rate": "",
            "water_rate": None,
        },
    }
    assert generate_talk(listing, max_points=2, allow_empty=True) == ""


def test_manual_talk_still_wins():
    listing = {
        "listing_id": "QC0354",
        "qiaolian_talk": "房东确认周六下午可以看房。",
        "normalized_data": {"management_fee": "包含"},
    }
    assert generate_talk(listing, max_points=2) == "房东确认周六下午可以看房。"
