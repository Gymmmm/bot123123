from __future__ import annotations

import pytest

from qiaolian_v3.parser.canonical import canonicalize_source


@pytest.mark.parametrize(
    ('text', 'layout', 'bedrooms', 'living_rooms', 'bathrooms', 'helper_rooms'),
    [
        ('公寓出租\n2房1办公2卫\n租金：$800/月', '2房1办公2卫', 2, None, 2, None),
        ('公寓出租\n3房+1保姆房 4卫\n租金：$1200/月', '3房+1保姆房4卫', 3, None, 4, 1),
        # Locked V2.2 regex ordering intentionally matches the helper-room
        # substring first for this compound form; preserve that exact behavior.
        ('公寓出租\n2+1房+1佣人房 1厅 3卫\n租金：$1500/月', '1房+1佣人房', 1, None, None, 1),
        ('公寓出租\n2 Bedrooms / 2 Bathrooms\n租金：$900/月', '2房2卫', 2, None, 2, None),
    ],
)
def test_locked_complex_layout_rules(text, layout, bedrooms, living_rooms, bathrooms, helper_rooms):
    facts = canonicalize_source('区域：BKK1\n' + text)
    assert facts['layout'] == layout
    assert facts['bedrooms'] == bedrooms
    assert facts['living_rooms'] == living_rooms
    assert facts['bathrooms'] == bathrooms
    assert facts['helper_rooms'] == helper_rooms


def test_locked_generic_payment_terms_are_preserved_without_fake_deposit_split():
    facts = canonicalize_source('区域：BKK1\n公寓出租\n1房1卫\n租金：$600/月\n付款方式：2付1')
    assert facts['deposit_payment_terms'] == '2付1'
    assert facts['deposit_months'] is None
    assert facts['prepay_months'] is None


def test_locked_listing_detail_fields_are_preserved():
    facts = canonicalize_source(
        '区域：BKK1\n公寓出租\n2房2卫\n租金：$800/月\n'
        '入住时间：2026-10-01\n物业费：已含\n网络：$20/月\n水费：$0.5/m3\n'
        '电费：$0.25/度\n停车费：$50/月\n看房时间：每天10:00-18:00\n视频看房：可安排'
    )
    assert facts['available_date'] == '2026-10-01'
    assert facts['management_fee'] == '已含'
    assert facts['internet_fee'] == '$20/月'
    assert facts['water_rate'] == '$0.5/m3'
    assert facts['electric_rate'] == '$0.25/度'
    assert facts['parking_fee'] == '$50/月'
    assert facts['viewing_time'] == '每天10:00-18:00'
    assert facts['video_viewing'] == '可安排'


def test_locked_land_building_and_primary_size_rules_are_preserved():
    facts = canonicalize_source(
        '区域：BKK1\n别墅出租\n4房5卫\n租金：$2300/月\n'
        '土地尺寸：10m×20m\n建筑面积：150㎡\n面积：88㎡'
    )
    assert facts['land_dimension'] == '10m×20m'
    assert facts['building_size_sqm'] == 150
    assert facts['size_sqm'] == 88


def test_locked_unlabelled_dimension_stays_separate_from_area():
    facts = canonicalize_source('区域：BKK1\n公寓出租\n1房1卫\n租金：$500/月\n尺寸：8m×12m')
    assert facts['size_sqm'] is None
    assert facts['unlabelled_dimension'] == '8m×12m'


def test_locked_discounted_rent_uses_explicit_current_and_original_labels():
    facts = canonicalize_source('区域：BKK1\n公寓出租\n1房1卫\n原租金：$800/月\n现租金：$650/月')
    assert facts['monthly_rent_usd'] == 650
    assert facts['original_monthly_rent_usd'] == 800
    assert facts['price_status'] == 'confirmed'


def test_locked_compound_one_road_and_peng_huoth_city_are_split():
    facts = canonicalize_source('一号公路炳发城 别墅出租\n4房5卫\n租金：$2300/月')
    assert '一号路' in facts['market_location_keys']
    assert '炳发城' in facts['market_location_keys']
    assert facts['project_name'] == '炳发城'
    assert facts['project_brand'] == 'Peng Huoth'
    assert facts['public_location_key'] == '一号路'


def test_locked_yongwang2_and_598_road_are_independent_tokens():
    facts = canonicalize_source('永旺2附近598路 公寓出租\n1房1卫\n租金：$650/月')
    assert '598路' in facts['market_location_keys']
    assert '永旺2' in facts['market_location_keys']
    assert facts['public_location_key'] == '598路'


def test_locked_50m_road_remains_conservative_market_location():
    facts = canonicalize_source('50米路 公寓出租\n1房1卫\n租金：$500/月')
    assert facts['public_location_key'] == '50米路'
    assert facts['public_location_display'] == '50米路附近'
    assert facts['canonical_area_key'] is None


def test_locked_inventory_room_menu_cannot_manufacture_current_property_type():
    facts = canonicalize_source(
        '【两房两卫出租】\n区域：BKK1\n房间户型：2房2卫\n'
        '户型选择：单间公寓／1房公寓／2房公寓均有\n出租价格：$850/月'
    )
    assert facts['layout'] == '2房2卫'
    assert facts['property_type'] == '未知'
    assert 'property_type_only_in_inventory' in facts['candidate_flags']


def test_locked_explicit_current_property_type_beats_inventory_menu():
    facts = canonicalize_source(
        '公寓两房出租\n区域：BKK1\n房间户型：2房2卫\n'
        '户型选择：单间公寓／别墅／排屋均有\n出租价格：$850/月'
    )
    assert facts['property_type'] == '公寓'
    assert facts['property_type_status'] == 'confirmed'
    assert 'ambiguous_property_type' not in facts['candidate_flags']


def test_locked_unapproved_compound_project_token_is_not_invented():
    facts = canonicalize_source('BKK1白金湾 公寓出租\n1房1卫\n租金：$520/月')
    assert facts['public_location_key'] == 'BKK1'
    assert facts['project_name'] is None
    assert facts['project_key'] is None
