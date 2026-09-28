from pathlib import Path

from publication_package import COVER_TEMPLATE_MAP, classify
from qiaolian_dual.cover_styles import (
    COVER_CANVAS,
    FINAL_COVER_STYLES,
    PORTRAIT_COVER_STYLES,
    VIDEO_COVER_STYLES,
    cover_orientation,
    cover_style_family,
    cover_template_path,
    normalize_cover_style,
    resolve_cover_style_for_source,
)


def test_only_four_horizontal_cover_styles_are_active():
    assert FINAL_COVER_STYLES == ("classic_blue", "right_price", "black_gold", "premium_photo")
    assert COVER_CANVAS["landscape"] == (1080, 864)
    assert COVER_CANVAS["portrait"] == (1200, 1500)
    assert set(COVER_TEMPLATE_MAP) == {
        *FINAL_COVER_STYLES,
        *PORTRAIT_COVER_STYLES,
        *VIDEO_COVER_STYLES,
        "video_vertical",
    }
    for style in FINAL_COVER_STYLES:
        assert cover_template_path(style).is_file()
    for style in PORTRAIT_COVER_STYLES:
        assert cover_template_path(style).is_file()
        assert cover_orientation(style) == "portrait"
        assert cover_style_family(style) == style.removesuffix("_portrait")
    for style in VIDEO_COVER_STYLES:
        assert cover_template_path(style, allow_video=True).is_file()


def test_cover_style_follows_main_photo_orientation(tmp_path: Path):
    from PIL import Image

    landscape = tmp_path / "land.jpg"
    portrait = tmp_path / "port.jpg"
    Image.new("RGB", (1200, 900), (10, 20, 30)).save(landscape)
    Image.new("RGB", (900, 1200), (10, 20, 30)).save(portrait)

    assert resolve_cover_style_for_source("premium_photo", landscape) == "premium_photo"
    assert resolve_cover_style_for_source("premium_photo", portrait) == "premium_photo_portrait"
    assert resolve_cover_style_for_source("classic_blue_portrait", landscape) == "classic_blue"
    assert resolve_cover_style_for_source("black_gold", portrait) == "black_gold_portrait"


def test_legacy_names_collapse_into_final_styles():
    assert normalize_cover_style("blue_banner") == "classic_blue"
    assert normalize_cover_style("left_info") == "classic_blue"
    assert normalize_cover_style("price_tag") == "right_price"
    assert normalize_cover_style("villa_premium") == "black_gold"
    assert normalize_cover_style("unknown") == "classic_blue"
    assert normalize_cover_style("vertical", allow_video=True) == "video_portrait"
    assert normalize_cover_style("video_vertical", allow_video=True) == "video_portrait"


def test_launch_default_cover_routing_is_deterministic():
    cases = (
        ("公寓", 600, "premium_photo"),
        ("公寓", 1199, "premium_photo"),
        ("公寓", 1200, "black_gold"),
        ("排屋", 600, "black_gold"),
        ("别墅", 5000, "black_gold"),
    )
    for property_type, price, expected in cases:
        routed = classify(
            source_type="telegram",
            source_name="collector",
            property_type=property_type,
            project="示例房源",
            price=price,
        )
        assert routed["cover_template"] == expected


def test_video_routing_uses_portrait_video_template():
    routed = classify(
        source_type="telegram",
        source_name="collector",
        property_type="公寓",
        project="示例房源",
        media_type="video",
    )
    assert routed["cover_template"] == "video_portrait"
    assert routed["media_type"] == "video"


def test_right_price_is_manual_only():
    routed = classify(
        source_type="telegram",
        source_name="collector",
        property_type="公寓",
        project="特价房源",
        price=600,
        is_special=True,
    )
    assert routed["cover_template"] == "premium_photo"
    assert normalize_cover_style("right_price") == "right_price"
    assert normalize_cover_style("premium_photo") == "premium_photo"


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
        assert 'id="brandLogo"' in template
        assert "1080px" in template and "864px" in template
    portrait = cover_template_path("premium_photo_portrait").read_text(encoding="utf-8")
    assert "data-autofit" in portrait
    assert 'id="brandLogo"' in portrait
    assert "1200px" in portrait and "1500px" in portrait
    for style in VIDEO_COVER_STYLES:
        template = cover_template_path(style, allow_video=True).read_text(encoding="utf-8")
        assert "data-autofit" in template
        assert 'id="brandLogo"' in template
