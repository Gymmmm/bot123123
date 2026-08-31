from pathlib import Path


ACTIVE_PROPERTY_TEMPLATES = [
    "templates/property/01_经典蓝卡模板.html",
    "templates/property/02_极简白条模板.html",
    "templates/property/03_右侧价格牌模板.html",
    "templates/property/04_黑金高级感_右侧价格牌模板.html",
    "templates/property/04_竖版视频封面模板.html",
    "templates/property/05_editorial_mobile.html",
    "templates/property/06_mono_editorial.html",
    "templates/property/07_split_editorial.html",
    "templates/property/08_mobile_bold.html",
    "templates/property/09_readable_card.html",
]


def test_all_active_property_templates_use_noto_first():
    for name in ACTIVE_PROPERTY_TEMPLATES:
        text = Path(name).read_text(encoding="utf-8")
        assert "Noto Sans CJK SC" in text or "Noto Sans SC" in text, name
        first_family = text.find("font-family")
        assert first_family >= 0, name
        declaration = text[first_family:first_family + 180]
        noto_at = min(
            [pos for pos in (declaration.find("Noto Sans CJK SC"), declaration.find("Noto Sans SC")) if pos >= 0],
            default=-1,
        )
        pingfang_at = declaration.find("PingFang SC")
        assert noto_at >= 0, name
        assert pingfang_at < 0 or noto_at < pingfang_at, name


def test_html_cover_renderer_forces_noto_on_poster_tree():
    text = Path("html_cover_renderer.py").read_text(encoding="utf-8")
    assert '.poster, .poster *' in text
    assert 'font-family: "Noto Sans CJK SC", "Noto Sans SC", sans-serif !important;' in text
    forced_block = text[text.index('.poster, .poster *'):]
    assert 'font-family: "PingFang SC"' not in forced_block


def test_canonical_pillow_renderer_never_falls_back_to_non_noto_font():
    text = Path("qiaolian_dual/image_renderer.py").read_text(encoding="utf-8")
    font_block = text[text.index("def _font"):text.index("def _open_source")]
    assert "NotoSansCJK" in font_block
    assert "QIAOLIAN_NOTO_BOLD_FONT" in font_block
    assert "QIAOLIAN_NOTO_REGULAR_FONT" in font_block
    for forbidden in ("PingFang", "STHeiti", "DejaVu", "Microsoft YaHei", "load_default"):
        assert forbidden not in font_block
    assert "noto_sans_cjk_sc_font_missing" in font_block
