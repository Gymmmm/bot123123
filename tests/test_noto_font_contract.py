from pathlib import Path


ACTIVE_PROPERTY_TEMPLATES = [
    "templates/property/01_经典蓝卡模板.html",
    "templates/property/03_右侧价格牌模板.html",
    "templates/property/04_黑金高级感_右侧价格牌模板.html",
    "templates/property/04_竖版视频封面模板.html",
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


def test_classic_blue_matches_approved_structure():
    text = Path("templates/property/01_经典蓝卡模板.html").read_text(encoding="utf-8")
    assert 'width:1600px;height:1200px' in text
    assert '.top-brand' in text
    assert '.brand-inner' in text
    assert '.bottom-card' in text
    assert 'width:885px' in text
    assert 'height:305px' in text
    assert 'border-radius:38px' in text
    assert 'linear-gradient(104deg,rgba(20,57,148,.93)' in text
    assert '#f5d56b' in text
    for token in ('{{PROJECT}}', '{{LAYOUT}}', '{{SIZE}}', '{{FLOOR}}', '{{PRICE}}', '{{H1}}', '{{H2}}', '{{H3}}'):
        assert token in text
    assert 'id="t1"' in text and 'id="t8"' in text
