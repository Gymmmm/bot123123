"""Villa hero: text in the open sky, building kept visible, V-A / V-B geometry."""
from __future__ import annotations

import numpy as np
from PIL import Image, ImageDraw, ImageOps

from v3_core.media import hero_layout
from v3_core.media.cover_renderer import CoverRenderData, _layout_geometry, render_cover
from v3_core.media.service import villa_template_from_env

DATA = CoverRenderData(public_listing_id="QL-X", project="BKK1", property_type="独栋别墅", layout="4+2房", price="4000")


def _villa(size=(1200, 1000), roof=0.40, sky=(232, 236, 240)):
    """Smooth bright sky over a busy textured building starting at ``roof`` × height."""
    w, h = size
    img = Image.new("RGB", size, sky)
    rng = np.random.default_rng(0)
    top = int(h * roof)
    block = rng.integers(40, 200, (h - top, w, 3), dtype=np.uint8)
    img.paste(Image.fromarray(block), (0, top))
    draw = ImageDraw.Draw(img)
    for x in range(0, w, 60):
        draw.line((x, top, x, h), fill=(20, 20, 20), width=6)
    return img


def _busy(size=(1200, 1000)):
    rng = np.random.default_rng(1)
    return Image.fromarray(rng.integers(0, 255, (size[1], size[0], 3), dtype=np.uint8))


def test_building_top_and_slot_lands_in_the_sky():
    img = _villa()
    assert abs(hero_layout.building_top(img) - 0.40) < 0.05
    hero = ImageOps.fit(img, (1080, 878))
    slot = hero_layout.text_slot(hero, (360, 120), margin=40)
    assert slot is not None and slot.align == "center"
    assert slot.y + slot.height <= 0.40 * 878 + 5          # entirely above the roofline
    assert slot.luminance > 200


def test_no_sky_means_no_slot():
    hero = ImageOps.fit(_busy(), (1080, 700))
    assert hero_layout.text_slot(hero, (360, 120), margin=40) is None


def test_frame_keeps_roof_and_text_room_and_cuts_foreground_first():
    img = _villa(size=(1080, 1500), roof=0.62)              # short building, tall sky
    _, cy = hero_layout.frame_offset(img, (1080, 700), text_room=200)
    top = cy * (1500 - 700)
    assert top <= 0.62 * 1500 - 200 + 2                     # text room above the roof stays
    assert top + 700 >= 0.94 * 1500 - 2                     # building down to its base stays
    assert top + 700 < 1500                                 # surplus came out of the foreground


def test_frame_centres_on_the_building_when_it_cannot_fit():
    img = _villa(size=(1080, 1600), roof=0.0)
    _, cy = hero_layout.frame_offset(img, (1080, 700), text_room=200)
    assert 0.3 < cy < 0.7                                   # not top-anchored: entrance is not cut away


def test_villa_compact_geometry_is_1080x1200_hero_700_thumbs_430_footer_64():
    geo = _layout_geometry(DATA, "villa_compact")
    assert geo["canvas"] == (1080, 1200) and geo["hero"] == (1080, 700)
    assert geo["footer_height"] == 64 and geo["footer_top"] == 1136
    assert {box[3] for box in geo["thumbs"]} <= {426, 427, 428, 429, 430}
    va = _layout_geometry(DATA, "villa_4x5")
    assert va["canvas"] == (1080, 1350) and va["hero"] == (1080, 878)


def _thumbs(tmp_path):
    paths = []
    for i in range(3):
        p = tmp_path / f"t{i}.jpg"
        Image.new("RGB", (800, 600), (90 + 30 * i, 120, 150)).save(p)
        paths.append(str(p))
    return paths


def test_title_is_drawn_in_the_sky_with_dark_ink_on_bright_sky(tmp_path):
    hero = tmp_path / "hero.jpg"
    _villa(size=(1200, 1000), roof=0.45).save(hero)
    for template, size in (("villa_4x5", (1080, 1350)), ("villa_compact", (1080, 1200))):
        out = render_cover(style="x", source_image=str(hero), source_images=_thumbs(tmp_path),
                           source_labels=("客厅", "卧室", "厨房"), output_path=str(tmp_path / f"{template}.jpg"),
                           data=DATA, layout=template)
        with Image.open(out) as im:
            assert im.size == size
            gray = np.asarray(im.convert("L"), dtype=np.int16)
            band = gray[20:300, 340:740]                      # centred sky band
            assert (band < 90).sum() > 400                    # dark title ink present in the sky
            assert gray[40:120, 30:150].min() > 150           # legacy top-left corner left empty


def test_without_sky_the_legacy_top_left_title_is_used(tmp_path):
    hero = tmp_path / "busy.jpg"
    _busy().save(hero)
    out = render_cover(style="x", source_image=str(hero), source_images=_thumbs(tmp_path),
                       source_labels=("", "", ""), output_path=str(tmp_path / "b.jpg"), data=DATA, layout="villa_compact")
    with Image.open(out) as im:
        assert im.size == (1080, 1200)


def test_villa_template_env(monkeypatch):
    monkeypatch.delenv("V3_VILLA_COVER_TEMPLATE", raising=False)
    assert villa_template_from_env() == "villa_black_gold"
    for name in ("villa_compact", "villa_4x5", "villa_black_gold"):
        monkeypatch.setenv("V3_VILLA_COVER_TEMPLATE", name)
        assert villa_template_from_env() == name
    monkeypatch.setenv("V3_VILLA_COVER_TEMPLATE", "bogus")
    assert villa_template_from_env() == "villa_black_gold"
