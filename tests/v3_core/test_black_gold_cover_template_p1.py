from hashlib import sha256
from pathlib import Path

from PIL import Image, ImageFont

from v3_core.media.cover_generator import CoverRenderData, generate_cover
from v3_core.media.cover_styles import cover_template_path


def test_black_gold_uses_new_1280x720_layout_and_existing_fields(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        "v3_core.media.cover_generator._font",
        lambda size, bold=False: ImageFont.load_default(size=size),
    )
    source = tmp_path / "source.jpg"
    Image.new("RGB", (1600, 1200), (96, 118, 140)).save(source)
    data = CoverRenderData(
        public_listing_id="QL-TEST",
        project="富力城",
        layout="3房1厅2卫",
        area="森速区",
        price="1200",
    )

    black_gold = tmp_path / "black_gold.jpg"
    right_price = tmp_path / "right_price.jpg"
    generate_cover(style="black_gold", source_image=str(source), output_path=str(black_gold), data=data)
    generate_cover(style="right_price", source_image=str(source), output_path=str(right_price), data=data)

    with Image.open(black_gold) as image:
        assert image.size == (1280, 720)
    with Image.open(right_price) as image:
        assert image.size == (1200, 900)
    assert sha256(black_gold.read_bytes()).hexdigest() != sha256(right_price.read_bytes()).hexdigest()

    html = cover_template_path("black_gold", allow_video=False).read_text(encoding="utf-8")
    for placeholder in ("{{BG_SRC}}", "{{AREA}}", "{{PROJECT}}", "{{LAYOUT}}", "{{PRICE_LINE}}"):
        assert placeholder in html
    assert "QIAOLIAN REALTY" in html
    assert "width: 1280px" in html
    assert "height: 720px" in html
