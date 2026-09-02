from qiaolian_dual.canonical_enrichment_v12 import canonicalize_source_v12


def parse(text: str):
    return canonicalize_source_v12(raw_text=text, sanitized_text=text)


def test_compound_road_and_project_are_split():
    facts = parse(
        "🏠【排屋出租】#一号路炳发城\n"
        "💰出租价格：$1,000/月\n"
        "🏠房间数量：4房5卫\n"
        "🏢楼层情况：3层\n"
        "📄合同情况：1年\n"
        "🔐押金情况：押2付1"
    )
    assert facts["road"] == "一号路"
    assert facts["project_name"] == "炳发城"
    assert facts["project_group"] == "炳发城"
    assert facts["monthly_rent_usd"] == 1000
    assert facts["floor"] == "3"


def test_one_road_alias_is_normalized_without_whole_phrase_matching():
    facts = parse("双拼别墅出租 #一号公路炳发城\n出租价格：$1,700/月\n4房5卫")
    assert facts["road"] == "一号路"
    assert facts["project_name"] == "炳发城"
    assert facts["monthly_rent_usd"] == 1700


def test_price_fallback_accepts_comma_and_special_rent_shape():
    comma = parse("双拼别墅出租\n出租价格：$1,500/月\n4房5卫")
    special = parse("Agile 雅居乐一房一厅特价出租600$\n面积：57平方米")
    assert comma["monthly_rent_usd"] == 1500
    assert special["monthly_rent_usd"] == 600
    assert special["project_name"] == "雅居乐"


def test_mixed_listing_keeps_rent_and_sale_price_separate():
    facts = parse(
        "【双拼别墅出租/出售】#炳发城50米路\n"
        "出售价格：$260,000\n"
        "出租价格：$1,500/月\n"
        "4房5卫"
    )
    assert facts["deal_type"] == "mixed"
    assert facts["monthly_rent_usd"] == 1500
    assert facts["sale_price_usd"] == 260000
    assert facts["road_tokens"] == ["50米路"]
    assert facts["project_name"] == "炳发城"


def test_sale_only_price_is_preserved_but_stays_non_rental():
    facts = parse("炳发城 B户型双拼\n售价：31万美元\n4房5卫")
    assert facts["deal_type"] == "sale"
    assert facts["sale_price_usd"] == 310000
    assert "non_rental_source" in facts["quality"]["hard_flags"]
    assert "missing_price" not in facts["quality"]["hard_flags"]


def test_current_listing_type_is_not_taken_from_inventory_menu():
    facts = parse(
        "【精装两房出租】#金边威尔斯公馆\n"
        "出租价格：$850/月\n"
        "房间户型：2房1厅\n"
        "户型选择：单间/1房/2房/3房都有"
    )
    assert facts["project_name"] == "威尔斯"
    assert facts["layout"] == "2房1厅"
    assert facts["property_type_display"] != "Studio"


def test_explicit_property_type_can_fill_unknown_type():
    facts = parse("香格里拉高端公寓出租\n出租价格：$1,050/月\n3房1厅")
    assert facts["project_name"] == "香格里拉"
    assert facts["property_type"] == "公寓"
    assert facts["monthly_rent_usd"] == 1050


def test_nearby_wording_is_preserved():
    facts = parse("金街附近漂亮两房公寓出租\n租金 $650/月\n2房2卫")
    assert facts["location_anchor"] == "金街"
    assert facts["nearby"] is True
    assert "金街" in facts["market_location_keys"]
    idx = facts["market_location_keys"].index("金街")
    assert facts["market_location_displays"][idx] == "金街附近"


def test_plus_room_is_not_arithmetic_sum():
    facts = parse("洪森大道独栋别墅出租\n出租价格：$1500/月\n房间数量：4+1房6卫")
    assert facts["bedrooms"] == 4
    assert facts["extra_rooms"] == 1
    assert "4+1" in facts["layout"]


def test_move_in_ready_and_furniture_are_independent():
    ready_only = parse("公寓出租 $650/月\n2房2卫\n拎包入住")
    furnished = parse("公寓出租 $650/月\n2房2卫\n家具齐全，拎包入住")
    assert ready_only["move_in_ready"] is True
    assert ready_only.get("furniture_status") is None
    assert furnished["move_in_ready"] is True
    assert furnished["furniture_status"] == "家具齐全"


def test_long_term_is_not_fabricated_as_one_year():
    facts = parse("公寓出租\n租金 $600/月\n1房1卫\n长租")
    assert facts["contract_term_display"] == "长租"
    assert facts.get("contract_term_months") is None


def test_pet_negotiable_is_not_pet_allowed():
    facts = parse("公寓出租\n租金 $600/月\n1房1卫\n宠物可谈")
    assert facts["pet_policy"] == "宠物可谈"


def test_unknown_project_is_not_invented():
    facts = parse("神秘花园两房出租\n出租价格：$700/月\n2房1卫")
    assert facts.get("project_name") is None
