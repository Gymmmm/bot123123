from __future__ import annotations

import pytest

from qiaolian_dual.canonical_fact_projection import package_gate
from qiaolian_dual.canonical_facts import PARSER_REVISION, canonicalize_source


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("出租价格：$1,500/月", 1500),
        ("出租价格：$850/月", 850),
        ("租金520$包物业", 520),
        ("特价出租600$", 600),
        ("出租情况：850$", 850),
        ("💰租金：7000美元每月", 7000),
    ],
)
def test_verified_rent_formats_from_review_corpus(line: str, expected: int) -> None:
    facts = canonicalize_source(f"区域：BKK1\n公寓出租\n1房1卫\n{line}")
    assert facts["parser_revision"] == PARSER_REVISION == "v1.2"
    assert facts["monthly_rent_usd"] == expected
    assert facts["price_status"] == "confirmed"
    assert "missing_price" not in facts["quality"]["hard_flags"]


def test_sale_only_keeps_sale_price_without_fake_missing_rent() -> None:
    facts = canonicalize_source(
        "区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡"
    )
    assert facts["deal_type"] == "sale"
    assert facts["sale_price_usd"] == 90000
    assert facts["monthly_rent_usd"] is None
    assert "missing_price" not in facts["quality"]["hard_flags"]
    assert "non_rental_source" in facts["quality"]["hard_flags"]
    assert package_gate(facts, 4)["ok"] is False


def test_mixed_listing_keeps_rent_and_sale_as_separate_facts() -> None:
    facts = canonicalize_source(
        "区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000"
    )
    assert facts["deal_type"] == "mixed"
    assert facts["monthly_rent_usd"] == 680
    assert facts["sale_price_usd"] == 100000
    assert "mixed_sale_rent_terms" in facts["quality"]["review_flags"]
    assert package_gate(facts, 4)["ok"] is False


def test_deposit_utility_and_sale_amounts_never_become_rent() -> None:
    facts = canonicalize_source(
        "区域：BKK1\n公寓出租，也可出售\n1房1卫\n押金：$1000\n电费：$0.25/度\n售价：$100000"
    )
    assert facts["monthly_rent_usd"] is None
    assert facts["sale_price_usd"] == 100000
    assert "missing_price" in facts["quality"]["hard_flags"]


def test_compound_one_road_and_peng_huoth_city_are_split() -> None:
    facts = canonicalize_source(
        "一号公路炳发城 别墅出租\n4房5卫\n租金：$2300/月"
    )
    assert "一号路" in facts["market_location_keys"]
    assert "炳发城" in facts["market_location_keys"]
    assert facts["project_name"] == "炳发城"
    assert facts["project_brand"] == "Peng Huoth"
    assert facts["public_location_key"] == "一号路"


def test_yongwang2_and_598_road_are_independent_tokens() -> None:
    facts = canonicalize_source(
        "永旺2附近598路 公寓出租\n1房1卫\n租金：$650/月"
    )
    assert "598路" in facts["market_location_keys"]
    assert "永旺2" in facts["market_location_keys"]
    assert facts["public_location_key"] == "598路"


def test_50m_road_is_a_conservative_market_location() -> None:
    facts = canonicalize_source("50米路 公寓出租\n1房1卫\n租金：$500/月")
    assert facts["public_location_key"] == "50米路"
    assert facts["public_location_display"] == "50米路附近"
    assert facts["canonical_area_key"] is None


def test_inventory_room_menu_cannot_manufacture_current_property_type() -> None:
    facts = canonicalize_source(
        "【两房两卫出租】\n区域：BKK1\n房间户型：2房2卫\n"
        "户型选择：单间公寓／1房公寓／2房公寓均有\n出租价格：$850/月"
    )
    assert facts["layout"] == "2房2卫"
    assert facts["property_type"] == "未知"
    assert "property_type_only_in_inventory" in facts["candidate_flags"]


def test_explicit_current_property_type_beats_inventory_menu() -> None:
    facts = canonicalize_source(
        "公寓两房出租\n区域：BKK1\n房间户型：2房2卫\n"
        "户型选择：单间公寓／别墅／排屋均有\n出租价格：$850/月"
    )
    assert facts["property_type"] == "公寓"
    assert facts["property_type_status"] == "confirmed"
    assert "ambiguous_property_type" not in facts["candidate_flags"]


def test_unapproved_compound_project_token_is_not_invented() -> None:
    facts = canonicalize_source("BKK1白金湾 公寓出租\n1房1卫\n租金：$520/月")
    assert facts["public_location_key"] == "BKK1"
    assert facts["project_name"] is None
    assert facts["project_key"] is None
