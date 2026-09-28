from copy import deepcopy

import pytest

from parser_v2_safe import assert_v1_1_preserved, enrich_v2


def base_v1():
    return {
        "parser_version": "v1_1",
        "location": "钻石岛",
        "price": 680,
        "property_type": "公寓",
        "layout": "2房2卫",
        "rental": {
            "deposit": "押2付1",
            "payment": "月付",
            "lease": "1年",
            "available_date": None,
        },
        "house": {
            "furniture": "部分家具",
            "features": ["河景"],
        },
        "amenities": ["健身房"],
    }


def test_v1_1_nonempty_values_are_authoritative():
    v1 = base_v1()
    before = deepcopy(v1)
    out = enrich_v2(
        "雅居乐 $999/月 3房1卫 家具家电齐全 长租 房间保洁 管家服务 游泳池",
        v1,
    )
    assert v1 == before
    assert out["location"] == "钻石岛"
    assert out["price"] == 680
    assert out["property_type"] == "公寓"
    assert out["layout"] == "2房2卫"
    assert out["rental"]["lease"] == "1年"
    assert out["house"]["furniture"] == "部分家具"
    assert out["parser_version"] == "v1_1"


def test_v2_adds_safe_services_features_and_amenities():
    out = enrich_v2(
        "房间保洁每周2次，管家服务，灭虫，更换床品每周1次。"
        "家具家电齐全，精装修，拎包入住，采光好，独立院子，"
        "游泳池、健身房、儿童乐园。包物业，包含Wi-Fi。",
        {},
    )
    assert out["services"]["cleaning"] == "每周2次"
    assert out["services"]["concierge"] == "包含"
    assert out["services"]["pest_control"] == "包含"
    assert out["services"]["linen_change"] == "每周1次"
    assert out["house"]["furniture"] == "家具齐全"
    assert out["house"]["appliances"] == "家电齐全"
    assert out["house"]["decoration"] == "精装修"
    assert "拎包入住" in out["house"]["features"]
    assert "采光好" in out["house"]["features"]
    assert "独立院子" in out["house"]["features"]
    assert "游泳池" in out["amenities"]
    assert "健身房" in out["amenities"]
    assert "儿童乐园" in out["amenities"]
    assert "物业费" in out["included"]
    assert "Wi-Fi" in out["included"]


def test_money_is_never_parsed_or_guessed_by_v2():
    v1 = {"price": None, "sale_price": None, "rental": {"lease": None}}
    out = enrich_v2(
        "出租价格：$1,800/月，售价31万美元，押金$500，水电押金$200",
        v1,
    )
    assert out["price"] is None
    assert out["sale_price"] is None
    assert "monthly_rent_usd" not in out
    assert "sale_price_usd" not in out


def test_pending_project_is_review_only():
    v1 = {"project": None}
    out = enrich_v2("Picasso City Garden 两房出租", v1)
    assert out["project"] is None
    assert "Picasso City Garden" in out["review"]["possible_projects"]
    assert "Picasso" in out["review"]["possible_projects"]


def test_unlabelled_dimension_goes_to_review_only():
    v1 = {"size": None}
    out = enrich_v2("土地很好，尺寸 12.5 x 20 米", v1)
    assert out["size"] is None
    assert out["review"]["unrecognized_terms"]
    assert not any(key in out for key in ("area_sqm", "land_size_sqm", "building_size_sqm"))


def test_lease_only_fills_empty_value():
    empty = enrich_v2("适合长租", {"rental": {"lease": None}})
    fixed = enrich_v2("半年或1年", {"rental": {"lease": "2年"}})
    assert empty["rental"]["lease"] == "长租"
    assert fixed["rental"]["lease"] == "2年"


def test_bare_free_is_not_ambiguous():
    out = enrich_v2("免费", {})
    assert out["review"]["ambiguous_terms"] == []


def test_existing_additive_lists_keep_original_prefix():
    v1 = {
        "amenities": ["健身房"],
        "included": ["物业费"],
        "house": {"features": ["河景"]},
        "review": {"possible_projects": ["旧候选"]},
    }
    out = enrich_v2("游泳池，拎包入住，雅居乐，包网络", v1)
    assert out["amenities"][0] == "健身房"
    assert out["included"][0] == "物业费"
    assert out["house"]["features"][0] == "河景"
    assert out["review"]["possible_projects"][0] == "旧候选"


def test_guard_rejects_any_nonempty_v1_overwrite():
    before = {"price": 680, "rental": {"lease": "1年"}}
    bad = {"price": 700, "rental": {"lease": "1年"}}
    with pytest.raises(AssertionError):
        assert_v1_1_preserved(before, bad)


def test_parser_metadata_is_additive_not_replaced():
    out = enrich_v2("配套齐全", {"parser_version": "v1_1"})
    assert out["parser_version"] == "v1_1"
    assert out["enrichment_version"] == "v2_safe"
    assert out["parser_chain"] == "v1_1+v2_safe"
    assert "配套齐全" in out["review"]["ambiguous_terms"]
