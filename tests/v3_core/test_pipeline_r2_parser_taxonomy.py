from v3_core.inventory.canonical_facts import PARSER_REVISION, canonicalize_source
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts


def _parse(raw: str):
    return enrich_authoritative_facts(raw, canonicalize_source(raw))


def test_r2_advances_parser_revision():
    assert PARSER_REVISION == "v1.4"


def test_real_l267_l268_compact_labelled_layouts_are_extracted_without_guesses():
    l267 = """【香格里拉公寓出租】
房型：2+1
楼层：48（T1河景）
租金：1100💵
押金：押一付一
合同：1年
两室一厅"""
    l268 = """【香格里拉公寓出租】
房型：2+1
楼层：48（T1河景）
租金：1100💵
押金：押一付一
合同：1年"""
    for raw in (l267, l268):
        facts = _parse(raw)
        assert facts["project_name"] == "The Peak 香格里拉"
        assert facts["monthly_rent_usd"] == 1100
        assert facts["layout"] == "2+1"
        assert facts["bedrooms"] is None
        assert facts["bathrooms"] is None
        assert facts["helper_rooms"] is None
        assert "missing_layout" not in facts["quality"]["blocking_flags"]


def test_real_l264_chinese_room_words_are_normalized():
    raw = """公寓：#雅居乐
户型：#两室一厅
地址：莫尼旺大道
楼层：22/33楼
租金：800$
面积：76平米
押金：1個月
租期：1年起租"""
    facts = _parse(raw)
    assert facts["layout"] == "2房1厅"
    assert facts["bedrooms"] == 2
    assert facts["living_rooms"] == 1
    assert facts["bathrooms"] is None
    assert "missing_layout" not in facts["quality"]["blocking_flags"]


def test_labelled_compact_layout_accepts_ascii_colon_and_spaces_but_bare_expression_does_not():
    assert _parse("富力城 公寓出租\n户型: 2 + 1\n租金 $800/月")["layout"] == "2+1"
    assert _parse("富力城 公寓出租\n两房一厅\n租金 $800/月")["layout"] == "2房1厅"
    assert _parse("富力城 公寓出租\n方案 2+1\n租金 $800/月")["layout"] is None


def test_existing_layout_formats_do_not_regress():
    cases = {
        "2房": "2房",
        "2房1厅": "2房1厅",
        "2房2卫": "2房2卫",
        "2+1房": "2+1房",
        "Studio": "Studio",
        "2 bedrooms / 2 bathrooms": "2房2卫",
    }
    for raw_layout, expected in cases.items():
        facts = _parse(f"富力城 公寓出租\n{raw_layout}\n租金 $800/月")
        assert facts["layout"] == expected


def test_included_amenities_cannot_become_explicit_project():
    for value in ("物业费", "网络", "房间保洁", "保洁", "停车位"):
        facts = _parse(f"【两房公寓出租】\n租金 $800/月\n2房1厅\n包含项目：{value}")
        assert facts["project_name"] is None


def test_real_taxonomy_regressions_keep_headline_project_and_treat_nearby_as_evidence():
    cases = {
        "l_195": ("""345🌳【特价复式公寓出租】#太子寰宇
💰出租价格：$600/月
🏠房间户型：复式1房1厅
📍周边配套：金界、金街美食广场、苏豪夜市、永旺1""", "太子·寰宇中心"),
        "l_197": ("""343🌳【特价高端公寓出租】#香格里拉
💰出租价格：$700/月
🏠房间户型：1房1厅
📍周边配套：金界、金街美食广场、苏豪夜市、永旺1""", "The Peak 香格里拉"),
        "l_204": ("""336🌳【特价三房出租】#太子国际广场
💰出租价格：$800/月
🏠房间户型：3房""", "太子国际广场"),
        "l_212": ("""327🌳【复式四房首次出租】#金边寰宇中心
💰出租价格：$2000/月
🏠房间户型：复式4房
🎁包含项目：网络、房间保洁""", "太子·寰宇中心"),
        "l_213": ("""326🌳【高端公寓出租】#香格里拉
💰出租价格：$1,050/月
🏠房间户型：3房1厅
📍周边配套：金界、金街美食广场""", "The Peak 香格里拉"),
        "l_214": ("""325🌳【精装三房出租】#香格里拉The Peak
💰出租价格：$999/月
🏠房间户型：3房
🎁包含项目：物业费、房间保洁""", "The Peak 香格里拉"),
        "l_217": ("""322🌳【特价两房出租】#金边Sky Villa豪宅
💰出租价格：$2,950/月
🏠房间户型：2房1厅（大平层）
🎁包含项目：网络、保洁、停车位""", "Sky Villa 天空别墅"),
        "l_225": ("""312🏠【精选一房出租】#Picasso City Garden BKK1
💰出租价格：$1,100/月
🏠房间户型：1房1卫""", "Picasso City Garden 毕加索"),
        "l_255": ("""273🌵香格里拉 🇨🇳 城市景观
🏠户型：2房+1房｜1厅2卫
💰租金：1400美元每月
📍周边：金界｜金街｜苏豪｜永旺""", "The Peak 香格里拉"),
        "l_262": ("""260香格里拉 🇨🇳 一房一厅
🏠户型：1房1厅
💰租金：850美元每月
📍周边：金界｜金街｜苏豪｜永旺""", "The Peak 香格里拉"),
    }
    for listing_id, (raw, expected_project) in cases.items():
        facts = _parse(raw)
        assert facts["project_name"] == expected_project, listing_id
        assert "ambiguous_project" not in facts["quality"]["blocking_flags"], listing_id
        assert "ambiguous_market_location" not in facts["quality"]["blocking_flags"], listing_id


def test_l221_stays_blocked_after_false_project_is_removed():
    raw = """318🌳【两房公寓出租】#时代广场
💰出租价格：$1,200/月
🏠房间户型：2房1厅
🎁包含项目：物业费
🏊公共配套：免费泳池、健身房"""
    facts = _parse(raw)
    assert facts["project_name"] is None
    assert facts["public_location_display"] is None
    assert "missing_public_location" in facts["quality"]["blocking_flags"]


def test_overlapping_new_airport_aliases_are_one_location_not_ambiguous_l216():
    raw = """323🌳【独栋别墅出租】#新机场附近
💰出租价格：$1,800/月（可谈）
🏠房间数量：5房6卫
📐建筑面积：7.7米×21米
📐土地面积：16米×25米
🛋️配套情况：家具家电齐全"""
    facts = _parse(raw)
    assert facts["public_location_display"] == "德崇机场方向"
    assert "ambiguous_market_location" not in facts["quality"]["blocking_flags"]
