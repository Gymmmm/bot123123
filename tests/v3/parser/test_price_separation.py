from __future__ import annotations

import pytest

from qiaolian_v3.parser.canonical import canonicalize_source


@pytest.mark.parametrize(('line','expected'), [
    ('出租价格：$1,500/月', 1500),
    ('出租价格：$850/月', 850),
    ('租金520$包物业', 520),
    ('特价出租600$', 600),
    ('出租情况：850$', 850),
    ('💰租金：7000美元每月', 7000),
])
def test_locked_v12_rent_regressions(line: str, expected: int):
    facts = canonicalize_source(f'区域：BKK1\n公寓出租\n1房1卫\n{line}')
    assert facts['monthly_rent_usd'] == expected
    assert facts['sale_price_usd'] is None


def test_deposit_utility_and_sale_amounts_never_become_rent():
    facts = canonicalize_source(
        '区域：BKK1\n公寓出售\n1房1卫\n押金：$1000\n电费：$0.25/度\n售价：$100000'
    )
    assert facts['monthly_rent_usd'] is None
    assert facts['sale_price_usd'] == 100000


def test_conflicting_rent_values_are_unknown_fact_not_guessed():
    facts = canonicalize_source('区域：BKK1\n公寓出租\n1房1卫\n租金：$680/月\n租金：$750/月')
    assert facts['monthly_rent_usd'] is None
    assert facts['price_status'] == 'conflict'
    assert 'conflicting_rental_price' in facts['hard_flags']
