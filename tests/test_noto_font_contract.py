from pathlib import Path

from qiaolian_dual.cover_styles import (
    FINAL_COVER_STYLES,
    PORTRAIT_COVER_STYLES,
    VIDEO_COVER_STYLES,
    cover_template_path,
)


def test_all_active_property_templates_use_noto_first():
    styles = (*FINAL_COVER_STYLES, *PORTRAIT_COVER_STYLES, *VIDEO_COVER_STYLES)
    for style in styles:
        path = cover_template_path(style, allow_video=True)
        text = path.read_text(encoding="utf-8")
        assert "Noto Sans CJK SC" in text or "Noto Sans SC" in text, path.name
        first_family = text.find("font-family")
        assert first_family >= 0, path.name
        declaration = text[first_family : first_family + 180]
        noto_at = min(
            [
                pos
                for pos in (declaration.find("Noto Sans CJK SC"), declaration.find("Noto Sans SC"))
                if pos >= 0
            ],
            default=-1,
        )
        pingfang_at = declaration.find("PingFang SC")
        assert noto_at >= 0, path.name
        assert pingfang_at < 0 or noto_at < pingfang_at, path.name


def test_html_cover_renderer_forces_noto_on_poster_tree():
    text = Path("html_cover_renderer.py").read_text(encoding="utf-8")
    assert ".poster, .poster *" in text
    assert 'font-family: "Noto Sans CJK SC", "Noto Sans SC", sans-serif !important;' in text
    forced_block = text[text.index(".poster, .poster *") :]
    assert 'font-family: "PingFang SC"' not in forced_block


def test_classic_blue_matches_approved_structure():
    text = Path("templates/property/01_经典蓝卡模板.html").read_text(encoding="utf-8")
    assert "width: 1080px" in text or "width:1080px" in text
    assert "height: 864px" in text or "height:864px" in text
    assert 'id="brandLogo"' in text
    assert "brand-logo" in text
    assert ".card" in text
    assert 'id="project"' in text
    assert 'id="layout"' in text
    assert 'id="price"' in text
    for token in ("{{PROJECT}}", "{{LAYOUT}}", "{{SIZE}}", "{{FLOOR}}", "{{PRICE}}", "{{H1}}", "{{H2}}", "{{H3}}"):
        assert token in text


def test_standard_canvas_templates_exist_for_image_and_video():
    portrait = cover_template_path("premium_photo_portrait").read_text(encoding="utf-8")
    assert "1200px" in portrait and "1500px" in portrait
    assert 'id="brandLogo"' in portrait
    landscape_video = cover_template_path("video_landscape", allow_video=True).read_text(encoding="utf-8")
    assert "1600px" in landscape_video and "900px" in landscape_video
    portrait_video = cover_template_path("video_portrait", allow_video=True).read_text(encoding="utf-8")
    assert "1080px" in portrait_video and "1920px" in portrait_video
