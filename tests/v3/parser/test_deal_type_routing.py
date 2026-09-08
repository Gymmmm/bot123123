from __future__ import annotations

from qiaolian_v3.parser.canonical import canonicalize_source


def test_rent_sale_unknown_are_the_only_persistable_deal_states():
    rent = canonicalize_source('区域：BKK1\n公寓出租\n1房1卫\n租金：$800/月')
    sale = canonicalize_source('区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡')
    unknown = canonicalize_source('区域：BKK1\n公寓\n1房1卫')
    assert rent['deal_type'] == 'rent'
    assert sale['deal_type'] == 'sale'
    assert unknown['deal_type'] == 'unknown'
    assert {rent['deal_type'], sale['deal_type'], unknown['deal_type']} == {'rent', 'sale', 'unknown'}


def test_rent_and_sale_intent_becomes_unresolved_unknown_with_conflict_review():
    facts = canonicalize_source(
        '区域：BKK1\n公寓可出租，也可出售\n2房2卫\n租金：$680/月\n售价：$100,000'
    )
    assert facts['deal_type'] == 'unknown'
    assert facts['deal_type_candidates'] == ['rent', 'sale']
    assert facts['monthly_rent_usd'] == 680
    assert facts['sale_price_usd'] == 100000
    assert 'mixed' not in {facts['deal_type'], *facts['deal_type_candidates']}
    assert 'conflicting_deal_type' in facts['review_flags']
    assert 'conflicting_deal_type' in facts['quality']['review_flags']
    assert 'ambiguous_deal_type' not in facts['review_flags']
    assert 'ambiguous_deal_type' not in facts['candidate_flags']


def test_sale_is_valid_canonical_data_not_non_rental_reject():
    facts = canonicalize_source('区域：BKK1\n商铺出售\n售价：$90,000\n面积：120㎡')
    assert facts['deal_type'] == 'sale'
    assert facts['sale_price_usd'] == 90000
    assert facts['monthly_rent_usd'] is None
    assert 'non_rental_source' not in facts.get('hard_flags', [])
    assert 'non_rental_source' not in facts['quality']['hard_flags']
    assert 'skipped_non_rental' not in facts.get('processing_flags', [])
