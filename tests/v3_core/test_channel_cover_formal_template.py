from pathlib import Path
from PIL import Image

from v3_core.media.cover_renderer import CoverRenderData, render_cover


def test_formal_channel_cover_is_1080x1350_white_footer_and_comma_price(tmp_path):
    photos = []
    for i in range(4):
        p = tmp_path / f"p{i}.jpg"
        Image.new("RGB", (900 + i * 40, 700 + i * 20), (80 + i * 20, 100, 120)).save(p)
        photos.append(str(p))
    out = tmp_path / "cover.png"
    data = CoverRenderData(
        public_listing_id="QL-PP-M7T7",
        property_type="别墅",
        layout="4房",
        area="永旺3附近",
        price="1200",
    )
    result = render_cover(
        style="anything",
        source_image=photos[0],
        source_images=photos[1:],
        output_path=str(out),
        data=data,
    )
    assert data.price_line() == "$1,200/月"
    with Image.open(result) as image:
        assert image.size == (1080, 1350)
        # Locked formal template footer is white, not legacy blue.
        assert image.convert("RGB").getpixel((10, 1320)) == (255, 255, 255)
