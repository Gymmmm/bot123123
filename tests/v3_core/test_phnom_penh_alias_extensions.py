from v3_core.inventory.listing_taxonomy import classify_listing_taxonomy


def test_common_phnom_penh_projects_normalize():
    cases = {
        "BKK1 J Tower 2 两房出租": "J Tower 2",
        "毕加索城市花园 一房 $800": "Picasso City Garden 毕加索",
        "Le Conde BKK1 1BR": "王府·观邸 Le Condé BKK1",
        "富力金边中心城 两房出租": "富力城",
        "太子中央广场 2房": "Prince Central Plaza 太子中央广场",
        "Time Square 9 BKK1": "Time Square 9",
        "Urban Village Phase 2 2BR": "首都·国金 Urban Village Phase 2",
    }
    for text, expected in cases.items():
        assert classify_listing_taxonomy(text).project_name == expected


def test_common_phnom_penh_location_shorthand_normalize():
    cases = {
        "60米大道 两房出租": "洪森大道",
        "Monivong Blvd apartment": "莫尼旺大道",
        "Norodom Boulevard condo": "诺罗敦大道",
        "毛泽东路附近公寓": "毛泽东大道",
        "Street 2004 studio": "2004路",
        "独立纪念碑附近": "独立碑",
        "Naga World附近": "Naga",
    }
    for text, expected in cases.items():
        assert expected in classify_listing_taxonomy(text).market_location_keys


def test_developer_brand_does_not_become_specific_project():
    chip = classify_listing_taxonomy("Chip Mong Land villa for rent")
    assert chip.project_name is None
    assert chip.project_brand == "Chip Mong"

    peng = classify_listing_taxonomy("Borey Peng Huoth villa for rent")
    assert peng.project_name is None
    assert peng.project_brand == "Peng Huoth"


def test_market_aliases_never_include_blank_tokens():
    from v3_core.inventory import listing_taxonomy as taxonomy

    assert all(
        taxonomy.clean_text(alias)
        for item in taxonomy.MARKET_LOCATIONS
        for alias in item.aliases
    )
