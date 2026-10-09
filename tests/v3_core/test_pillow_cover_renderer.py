from pathlib import Path
from PIL import Image
import pytest
from v3_core.media.cover_renderer import CoverRenderData, render_cover

def make(path: Path, size=(1600, 900), value=120):
    Image.new("RGB", size, (value, min(255, value+30), min(255, value+60))).save(path)
    return str(path)

@pytest.mark.parametrize("count", [1, 2, 4, 10])
def test_pillow_cover_counts_and_exact_size(tmp_path, count):
    paths=[]
    for i in range(count):
        size=(1600,900) if i % 2 == 0 else (700,1400)
        paths.append(make(tmp_path / f"{i}.jpg", size=size, value=40+i*15))
    out=tmp_path/"cover.png"
    render_cover(style="classic_blue", source_image=paths[0], source_images=paths[1:],
                 source_labels=("客厅", "卧室", "厨房"), output_path=str(out),
                 data=CoverRenderData(public_listing_id="QL-PP-T1", project="富力城", layout="1房", price="650"))
    with Image.open(out) as im:
        assert im.size == (1080,1423)
        assert im.format == "PNG"
        # Footer is always a clean white brand strip, independent of photo colors.
        assert all(channel >= 248 for channel in im.convert("RGB").getpixel((540, 1402)))

def test_price_chinese_and_bad_image_are_tolerated(tmp_path):
    good=make(tmp_path/"good.png", size=(900,1600))
    bad=tmp_path/"bad.jpg"; bad.write_bytes(b"not-an-image")
    out=tmp_path/"cover.jpg"
    data=CoverRenderData(public_listing_id="QL-PP-T2", project="太子中央广场", area="金边", price="650")
    assert data.price_line() == "$650/月"
    render_cover(style="anything", source_image=str(bad), source_images=[good], output_path=str(out), data=data)
    with Image.open(out) as im:
        assert im.size == (1080,1423)
        assert im.format == "JPEG"

def test_price_uses_thousands_separator_and_never_duplicates_month_suffix():
    assert CoverRenderData(public_listing_id="QL", price="1500").price_line() == "$1,500/月"
    assert CoverRenderData(public_listing_id="QL", price="$1,500/月").price_line() == "$1,500/月"
    assert CoverRenderData(public_listing_id="QL", price="1500", deal_type="sale").price_line() == "$1,500"

def test_cover_subtitle_prefers_layout_without_repeating_property_type():
    from v3_core.media.cover_renderer import _cover_subtitle
    assert _cover_subtitle(CoverRenderData(
        public_listing_id="QL", layout="双拼别墅", property_type="双拼别墅"
    )) == "双拼别墅"
    assert _cover_subtitle(CoverRenderData(
        public_listing_id="QL", layout="", property_type="公寓"
    )) == "公寓"

def test_apartment_uses_same_renderer_with_shorter_hero(tmp_path):
    paths = [make(tmp_path / f"apartment-{index}.jpg", value=80 + index * 20) for index in range(4)]
    out=tmp_path/"apartment-cover.png"
    render_cover(style="premium_photo", source_image=paths[0], source_images=paths[1:],
                 source_labels=("客厅", "卧室", "厨房"), output_path=str(out),
                 data=CoverRenderData(public_listing_id="QL", property_type="公寓", project="太子国际广场", layout="3房", price="800"))
    with Image.open(out) as im:
        assert im.size == (1080,1203)

def test_font_missing_falls_back(tmp_path, monkeypatch):
    import v3_core.media.cover_renderer as renderer
    monkeypatch.setattr(renderer, "FONT_CANDIDATES", ("/missing/font.ttf",))
    src=make(tmp_path/"one.jpg")
    out=tmp_path/"cover.png"
    render_cover(style="x", source_image=src, output_path=str(out), data=CoverRenderData(public_listing_id="QL", project="侨联地产", price="650"))
    assert out.is_file()

def test_fit_crops_without_stretching():
    from v3_core.media.cover_renderer import _fit
    src=Image.new("RGB",(2000,500))
    fitted=_fit(src,(352,492))
    assert fitted.size == (352,492)

def test_all_bad_images_follow_business_failure_not_worker_crash(tmp_path):
    bad=tmp_path/"bad.jpg"; bad.write_bytes(b"bad")
    with pytest.raises(ValueError, match="cover_no_usable_images"):
        render_cover(style="x", source_image=str(bad), output_path=str(tmp_path/"x.png"), data=CoverRenderData(public_listing_id="QL"))


@pytest.mark.parametrize(
    "property_type,expected_size,hero_height",
    [
        ("独栋别墅", (1080, 1423), 810),
        ("双拼别墅", (1080, 1423), 810),
        ("联排别墅", (1080, 1423), 810),
        ("villa", (1080, 1423), 810),
        ("公寓", (1080, 1203), 590),
        ("服务式公寓", (1080, 1203), 590),
        ("服务式", (1080, 1203), 590),
        ("Condo", (1080, 1203), 590),
        ("studio", (1080, 1203), 590),
        ("apartment", (1080, 1203), 590),
    ],
)
def test_cover_size_per_property_type(tmp_path, property_type, expected_size, hero_height):
    from v3_core.media.cover_renderer import FOOTER_HEIGHT, GALLERY_HEIGHT, INFO_HEIGHT, _layout_geometry
    data = CoverRenderData(public_listing_id="QL", property_type=property_type, project="BKK1", layout="3房", price="4000")
    canvas, hero, gallery_top, gallery_h, footer_top = _layout_geometry(data)
    assert canvas == expected_size
    assert hero == hero_height
    assert (gallery_top, gallery_h, footer_top) == (hero + INFO_HEIGHT, GALLERY_HEIGHT, hero + INFO_HEIGHT + GALLERY_HEIGHT)
    assert canvas[1] - footer_top == FOOTER_HEIGHT
    paths = [make(tmp_path / f"{i}.jpg", value=60 + i * 30) for i in range(4)]
    out = tmp_path / "cover.jpg"
    render_cover(style="premium_photo", source_image=paths[0], source_images=paths[1:],
                 source_labels=("客厅", "卧室", "厨房"), output_path=str(out), data=data)
    with Image.open(out) as im:
        assert im.size == expected_size
        rgb = im.convert("RGB")
        # Dark info bar sits directly under the hero, left of the price card.
        assert max(rgb.getpixel((20, hero + INFO_HEIGHT // 2))) < 60


def test_hero_is_4_3_for_villa_and_wide_for_apartment():
    from v3_core.media.cover_renderer import APARTMENT_HERO_HEIGHT, TALL_HERO_HEIGHT
    assert 1080 * 3 == TALL_HERO_HEIGHT * 4
    assert 1.8 < 1080 / APARTMENT_HERO_HEIGHT < 1.85


def test_long_title_and_price_stay_inside_info_bar(tmp_path):
    from PIL import ImageDraw
    from v3_core.media.cover_renderer import _draw_dark_price_pill, INFO_HEIGHT
    layer = Image.new("RGBA", (1080, 400), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    left = _draw_dark_price_pill(draw, CoverRenderData(public_listing_id="QL", price="12500"),
                                 right=1044, top=100, bottom=100 + INFO_HEIGHT, max_width=340)
    assert left is not None and 1044 - left <= 340
    bbox = layer.getchannel("A").getbbox()
    assert bbox[1] >= 100 and bbox[3] <= 100 + INFO_HEIGHT
    paths = [make(tmp_path / f"{i}.jpg") for i in range(4)]
    out = tmp_path / "long.jpg"
    render_cover(style="x", source_image=paths[0], source_images=paths[1:], output_path=str(out),
                 data=CoverRenderData(public_listing_id="QL", property_type="公寓",
                                      project="非常非常长的项目名称测试太子国际广场豪华服务式公寓三期",
                                      layout="3房2卫 · 高层 · 全新精装 · 拎包入住", price="12500"))
    with Image.open(out) as im:
        assert im.size == (1080, 1203)
