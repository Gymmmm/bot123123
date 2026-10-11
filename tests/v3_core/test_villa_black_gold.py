"""Villa cover in Gym's black-gold reference layout (villa_black_gold, default villa template)."""
from __future__ import annotations

import numpy as np
from PIL import Image

from v3_core.media import villa_card
from v3_core.media.cover_renderer import CoverRenderData, _layout_geometry, render_cover
from v3_core.media.cover_service import cover_terms

DATA = CoverRenderData(public_listing_id="QL-X", area="一号路附近", property_type="独栋别墅", layout="5房6卫",
                       price="5000", terms="押2付1 · 1年")


def _photos(tmp_path, count=4):
    paths = []
    for i in range(count):
        p = tmp_path / f"p{i}.jpg"
        Image.new("RGB", (1600, 1100), (60 + 40 * i, 110, 160 - 20 * i)).save(p)
        paths.append(str(p))
    return paths


def _render(tmp_path, data=DATA, labels=("客厅", "卧室", "厨房"), count=4, name="c.png"):
    paths = _photos(tmp_path, count)
    out = render_cover(style="x", source_image=paths[0], source_images=paths[1:], source_labels=labels,
                       output_path=str(tmp_path / name), data=data)
    return np.asarray(Image.open(out).convert("RGB")).astype(int)


def test_reference_geometry_is_the_default_villa_layout(monkeypatch):
    monkeypatch.delenv("V3_VILLA_COVER_TEMPLATE", raising=False)
    geo = _layout_geometry(DATA)
    assert geo["template"] == "villa_black_gold"
    assert geo["canvas"] == (1080, 1388)                   # reference 1080x1388 (~4:5)
    assert geo["hero"] == (1080, 700) and geo["bar"] == (700, 868)
    assert geo["thumbs"] == [(14, 868, 346, 384), (367, 868, 346, 384), (720, 868, 346, 384)]
    assert geo["footer_top"] == 1252 and geo["footer_height"] == 136
    assert geo["price_card"] == (560, 708, 1044, 845)


def test_env_switch_keeps_the_older_villa_templates(monkeypatch):
    monkeypatch.setenv("V3_VILLA_COVER_TEMPLATE", "villa_4x5")
    assert _layout_geometry(DATA)["canvas"] == (1080, 1350)
    monkeypatch.setenv("V3_VILLA_COVER_TEMPLATE", "villa_compact")
    assert _layout_geometry(DATA)["canvas"] == (1080, 1200)


def test_render_bar_thumbs_footer(tmp_path):
    px = _render(tmp_path)
    assert px.shape[:2] == (1388, 1080)
    assert np.abs(px[860, 530] - villa_card.BAR_BG).max() <= 6          # black-gold bar
    rule = px[776, 200]
    assert rule[0] - rule[2] > 40 and rule[0] > 150                     # thin gold rule under the title
    assert px[869, 15].min() >= 240                                      # rounded thumb corner shows light bg
    assert px[1060, 190].min() < 235                                     # thumbnail photo
    assert px[1380, 5].min() >= 245                                     # white footer
    assert np.abs(px[1322, 1030] - villa_card.PILL_BG).max() <= 8       # 出租房源 pill
    line = px[1321, 600]
    assert line[0] - line[2] > 30                                        # thin gold line in the middle


def _gold_ink_columns(px, box):
    x0, y0, x1, y1 = box
    region = px[y0:y1, x0:x1]
    gold = (region[..., 0] > 120) & (region[..., 0] - region[..., 2] > 40)
    cols = np.nonzero(gold.any(0))[0]
    return x0 + cols.min(), x0 + cols.max()


def test_price_group_is_optically_centred_in_the_card(tmp_path):
    px = _render(tmp_path)
    cx0, cy0, cx1, cy1 = villa_card.GEOMETRY.price_card
    left, right = _gold_ink_columns(px, (cx0 + 8, cy0 + 50, cx1 - 8, cy1 - 8))   # below the label, inside the outline
    assert abs((left + right) / 2 - (cx0 + cx1) / 2) <= 6


def test_missing_fields_are_omitted_not_invented():
    assert villa_card.info_parts(DATA) == ["独栋别墅", "5房", "押2付1 · 1年"]
    bare = CoverRenderData(public_listing_id="QL", property_type="独栋别墅")
    assert villa_card.info_parts(bare) == ["独栋别墅"]
    assert villa_card.rooms_text("4+2房5卫") == "4+2房" and villa_card.rooms_text("3房1厅｜2卫") == "3房"
    assert villa_card.rooms_text("") == "" and villa_card.rooms_text("精装") == ""
    assert cover_terms({"offer_type": "rent", "payment_terms": "押2付1", "contract_term": "合同：1年"}) == "押2付1 · 1年"
    assert cover_terms({"offer_type": "rent", "deposit_terms": "押1付1", "contract_term": ""}) == "押1付1"
    assert cover_terms({"offer_type": "rent", "payment_terms": "未知"}) == ""
    assert cover_terms({"offer_type": "sale", "payment_terms": "押2付1"}) == ""


def test_no_price_draws_no_card(tmp_path):
    data = CoverRenderData(public_listing_id="QL", area="BKK1", property_type="独栋别墅")
    px = _render(tmp_path, data=data)
    cx0, cy0, cx1, cy1 = villa_card.GEOMETRY.price_card
    assert np.abs(px[cy0 + 40, cx0 + 1] - villa_card.BAR_BG).max() <= 8          # no gold outline


def test_room_tags_only_for_confident_labels(tmp_path):
    with_tag = _render(tmp_path, labels=("客厅", "", ""), name="a.png")
    no_tag = _render(tmp_path, labels=("", "", ""), name="b.png")
    assert np.abs(with_tag[892, 40] - no_tag[892, 40]).max() > 40              # tag drawn on thumb 1
    assert np.abs(with_tag[892, 393] - no_tag[892, 393]).max() <= 3            # thumb 2: no guessed label


def test_fewer_photos(tmp_path):
    assert villa_card.thumb_columns(3) == [(14, 346), (367, 346), (720, 346)]
    (x0, w0), (x1, w1) = villa_card.thumb_columns(2)
    assert x0 == 14 and x1 + w1 <= 1066 and w0 == w1
    two = _render(tmp_path, count=3, name="two.png")
    assert two.shape[:2] == (1388, 1080)
    one = _render(tmp_path, count=1, name="one.png")
    assert one.shape[:2] == (1350, 1080)                                       # no detail photos → full hero


def test_footer_uses_our_lockup_not_the_reference_mark():
    lockup = villa_card.gold_lockup(68)
    assert lockup is not None and lockup.height == 68
    rgba = np.asarray(lockup).astype(int)
    house = rgba[:, : int(lockup.width * 0.25)]
    solid = house[house[..., 3] > 200]
    assert len(solid) and abs(solid[:, :3].mean(0) - villa_card.HOUSE_GOLD).max() <= 4   # gold house mark
