from __future__ import annotations

from hashlib import sha256
import inspect
from pathlib import Path

from PIL import Image, ImageFont

import v3_core.media.cover_generator as cover_generator
from v3_core.media.cover_generator import (
    BRAND_CN,
    BRAND_EN,
    CoverRenderData,
    _fit_font,
    _format_layout,
    _text_width,
    generate_cover,
)
from v3_core.media.cover_styles import cover_template_path


def _fake_font(size: int, bold: bool = False):
    del bold
    return ImageFont.load_default(size=size)


def test_three_visual_styles_generate_distinct_outputs_and_keep_existing_sizes(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(cover_generator, "_font", _fake_font)
    source = tmp_path / "same_source.jpg"
    Image.new("RGB", (1600, 1200), (112, 136, 154)).save(source)
    data = CoverRenderData(
        public_listing_id="QL-VISUAL",
        project="富力城",
        layout="3房1厅2卫",
        area="富力城",
        price="1600",
    )
    expected_sizes = {
        "classic_blue": (1200, 900),
        "right_price": (1200, 900),
        "black_gold": (1280, 720),
    }
    hashes = set()
    for style, expected_size in expected_sizes.items():
        output = tmp_path / f"{style}.jpg"
        generate_cover(style=style, source_image=str(source), output_path=str(output), data=data)
        with Image.open(output) as image:
            assert image.size == expected_size
        hashes.add(sha256(output.read_bytes()).hexdigest())
    assert len(hashes) == 3


def test_long_project_price_and_layout_fit_their_safe_widths(monkeypatch):
    monkeypatch.setattr(cover_generator, "_font", _fake_font)
    project = "钻石岛玫瑰滨江"
    layout = _format_layout("4房2厅3卫")
    price = "$10000/月"
    assert layout == "4房2厅｜3卫"

    cases = [
        (project, 620, 50, 34, True),
        (project, 500, 54, 34, True),
        (project, 530, 68, 42, True),
        (layout, 610, 29, 22, True),
        (layout, 500, 25, 19, False),
        (layout, 450, 27, 20, True),
        (price, 340, 50, 34, True),
        (price, 268, 38, 28, True),
        (price, 272, 46, 32, True),
    ]
    for text, max_width, preferred, minimum, bold in cases:
        font = _fit_font(text, max_width=max_width, preferred=preferred, minimum=minimum, bold=bold)
        assert _text_width(font, text) <= max_width


def test_brand_and_font_contract_are_identical_across_three_templates():
    assert BRAND_CN == "侨联地产"
    assert BRAND_EN == "QIAOLIAN REALTY"
    for style in ("classic_blue", "right_price", "black_gold"):
        html = cover_template_path(style, allow_video=False).read_text(encoding="utf-8")
        assert BRAND_CN in html
        assert BRAND_EN in html
        assert "{{PROJECT}}" in html
        assert "{{LAYOUT}}" in html
        assert "{{AREA}}" in html
        assert "{{PRICE_LINE}}" in html
        assert "📍" not in html
        assert "QIAO LIAN PROPERTY" not in html
        assert ">QIAO LIAN<" not in html

    renderer_source = inspect.getsource(cover_generator)
    assert "📍" not in renderer_source
    assert "NotoSansCJK" in renderer_source
    assert "PingFang.ttc" in renderer_source


def test_cover_crop_never_stretches_source(tmp_path: Path):
    source = tmp_path / "wide.jpg"
    Image.new("RGB", (1800, 900), (90, 110, 130)).save(source)
    for size in ((1200, 900), (1280, 720)):
        cropped = cover_generator._cover_crop(source, size)
        assert cropped.size == size
