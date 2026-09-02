from pathlib import Path

from publication_package import COVER_TEMPLATE_MAP, classify
from qiaolian_dual.cover_styles import (
    FINAL_COVER_STYLES,
    cover_template_path,
    normalize_cover_style,
)


def test_only_three_horizontal_cover_styles_are_active():
    assert FINAL_COVER_STYLES == ("classic_blue", "right_price", "black_gold")
    assert set(COVER_TEMPLATE_MAP) == {*FINAL_COVER_STYLES, "video_vertical"}
    for style in FINAL_COVER_STYLES:
        assert cover_template_path(style).is_file()


def test_legacy_names_collapse_into_final_styles():
    assert normalize_cover_style("blue_banner") == "classic_blue"
    assert normalize_cover_style("left_info") == "classic_blue"
    assert normalize_cover_style("price_tag") == "right_price"
    assert normalize_cover_style("villa_premium") == "black_gold"
    assert normalize_cover_style("unknown") == "classic_blue"


def test_still_listings_have_one_deterministic_default():
    for property_type, price in (("公寓", 600), ("排屋", 1200), ("别墅", 5000)):
        routed = classify(
            source_type="telegram",
            source_name="collector",
            property_type=property_type,
            project="示例房源",
            price=price,
        )
        assert routed["cover_template"] == "classic_blue"


def test_admin_publish_shape_is_fixed_to_cover_and_buttons():
    admin = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    package = Path("publication_package.py").read_text(encoding="utf-8")
    assert "_publish_options_from_note" not in admin
    assert "_save_publish_layout_for_draft" not in admin
    for callback in ("ap:pb:", "ap:pl:", "ap:pc:", "ap:pn:"):
        assert callback not in admin
    assert 'snapshot["publish_layout"] = "buttons"' in package
    assert 'snapshot["publish_cover"] = "cover"' in package
    assert 'snapshot["publish_actions"] = "buttons"' in package


def test_preview_and_generated_assets_share_production_renderer():
    generator = Path("cover_generator.py").read_text(encoding="utf-8")
    admin = Path("autopilot_publish_bot.py").read_text(encoding="utf-8")
    assert "from publication_package import render_cover_preview" in generator
    assert "from publication_package import render_cover_preview" in admin
    assert "ImageDraw" not in generator
    assert "hero_collage" not in generator


def test_final_templates_opt_in_to_overflow_autofit():
    renderer = Path("html_cover_renderer.py").read_text(encoding="utf-8")
    assert "root.querySelectorAll('[data-autofit]')" in renderer
    assert "el.scrollWidth > el.clientWidth" in renderer
    for style in FINAL_COVER_STYLES:
        template = cover_template_path(style).read_text(encoding="utf-8")
        assert "data-autofit" in template
