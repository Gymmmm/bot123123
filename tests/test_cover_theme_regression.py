from PIL import Image
from cover_generator import (
    COVER_BOTTOM_PANEL_ALPHA,
    COVER_BOTTOM_PANEL_BLUR,
    COVER_LEFT_SHADE_ALPHA,
    COVER_LEFT_SHADE_WIDTH_RATIO,
    COVER_LOGO_WIDTH_RATIO,
    COVER_PRICE_PANEL_ALPHA,
    COVER_W,
    COVER_H,
    choose_cover_theme,
)


def test_cover_geometry_and_opacity_contract():
    assert (COVER_W, COVER_H) == (1280, 960)
    assert 0.14 <= COVER_LOGO_WIDTH_RATIO <= 0.16
    assert 0.38 <= COVER_LEFT_SHADE_WIDTH_RATIO <= 0.42
    assert 128 <= COVER_LEFT_SHADE_ALPHA <= 141
    assert 148 <= COVER_BOTTOM_PANEL_ALPHA <= 166
    assert 6 <= COVER_BOTTOM_PANEL_BLUR <= 10
    assert 209 <= COVER_PRICE_PANEL_ALPHA <= 225


def test_theme_selection_is_deterministic_for_reference_profiles():
    assert choose_cover_theme(Image.new('RGB', (100,100), (65,75,90))) == 'black_gold'
    assert choose_cover_theme(Image.new('RGB', (100,100), (210,215,220))) == 'navy_gold'
    assert choose_cover_theme(Image.new('RGB', (100,100), (190,150,105))) == 'warm_champagne'
