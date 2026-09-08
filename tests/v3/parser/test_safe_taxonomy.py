from __future__ import annotations

from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.parser.safe_enrichment import enrich_safe


def test_safe_enrichment_is_additive_and_never_parses_money():
    base = {'monthly_rent_usd': 680, 'sale_price_usd': None, 'services': {}, 'included': [], 'amenities': [], 'house': {'features': []}}
    out = enrich_safe(
        '出租价格：$1800/月，售价31万美元。房间保洁每周2次，管家服务，灭虫，更换床品每周1次。家具家电齐全，精装修，拎包入住，游泳池、健身房。包物业，包含Wi-Fi。',
        base,
    )
    assert out['monthly_rent_usd'] == 680
    assert out['sale_price_usd'] is None
    assert out['services']['cleaning'] == '每周2次'
    assert out['services']['concierge'] == '包含'
    assert out['services']['pest_control'] == '包含'
    assert out['services']['linen_change'] == '每周1次'
    assert out['house']['furniture'] == '家具齐全'
    assert out['house']['appliances'] == '家电齐全'
    assert out['house']['decoration'] == '精装修'
    assert '拎包入住' in out['house']['features']
    assert '游泳池' in out['amenities'] and '健身房' in out['amenities']
    assert '物业费' in out['included'] and 'Wi-Fi' in out['included']


def test_pending_project_and_unlabelled_dimension_are_review_only():
    facts = canonicalize_source('BKK1白金湾 公寓出租\n1房1卫\n尺寸 12.5 x 20 米\n租金：$520/月')
    assert facts['project_name'] is None
    assert facts['project_key'] is None
    assert '白金湾' in facts['review']['possible_projects']
    assert facts['size_sqm'] is None
    assert facts['review']['unrecognized_terms']


def test_inventory_menu_does_not_manufacture_current_property_type():
    facts = canonicalize_source(
        '【两房两卫出租】\n区域：BKK1\n房间户型：2房2卫\n户型选择：单间公寓／1房公寓／2房公寓均有\n出租价格：$850/月'
    )
    assert facts['layout'] == '2房2卫'
    assert facts['property_type'] == '未知'
    assert 'property_type_only_in_inventory' in facts['candidate_flags']


def test_compound_market_tokens_remain_conservative():
    road = canonicalize_source('一号公路炳发城 别墅出租\n4房5卫\n租金：$2300/月')
    assert '一号路' in road['market_location_keys']
    assert '炳发城' in road['market_location_keys']
    assert road['project_name'] == '炳发城'
    assert road['project_brand'] == 'Peng Huoth'
    assert road['public_location_key'] == '一号路'

    fifty = canonicalize_source('50米路 公寓出租\n1房1卫\n租金：$500/月')
    assert fifty['public_location_key'] == '50米路'
    assert fifty['canonical_area_key'] is None
