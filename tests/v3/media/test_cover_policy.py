from __future__ import annotations

from qiaolian_v3.media.cover_policy import select_cover_style


def test_normal_listing_defaults_classic_blue():
    result = select_cover_style(property_type='apartment', monthly_rent_usd=800)
    assert result.style == 'classic_blue'
    assert result.source == 'automatic'


def test_villa_or_high_rent_routes_black_gold():
    assert select_cover_style(property_type='villa', monthly_rent_usd=900).style == 'black_gold'
    assert select_cover_style(property_type='apartment', monthly_rent_usd=1200).style == 'black_gold'


def test_right_price_can_only_come_from_manual_selection():
    automatic = select_cover_style(property_type='apartment', monthly_rent_usd=800)
    assert automatic.style != 'right_price'
    manual = select_cover_style(property_type='apartment', monthly_rent_usd=800, manual_style='right_price')
    assert manual.style == 'right_price'
    assert manual.source == 'manual'
