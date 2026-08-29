from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from PIL import Image

from qiaolian_dual import image_renderer
from qiaolian_dual.cover_templates import CANVAS_SIZE, COVER_TEXT_BOX, SAFE_INSET, WHITE_BORDER


def _facts(**changes):
    facts = {
        "project_name": "The Peak",
        "public_location_display": "BKK1",
        "property_type": "apartment",
        "property_type_display": "公寓",
        "monthly_rent_usd": 1280,
        "layout": "2房2卫",
        "size_sqm": 86,
        "floor": "18",
    }
    facts.update(changes)
    return facts


def _image(path: Path, color=(75, 105, 135), size=(1400, 900), mode="RGB") -> Path:
    Image.new(mode, size, color).save(path)
    return path


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_output_size_ratio_white_border_and_safe_text_box(tmp_path):
    source = _image(tmp_path / "source.jpg")
    result = image_renderer.render_listing_images(property_id="QC100", canonical_facts=_facts(), source_images=[source], output_root=tmp_path / "rendered")
    assert result.ok
    with Image.open(result.outputs[0]).convert("RGB") as rendered:
        assert rendered.size == CANVAS_SIZE == (1200, 900)
        assert rendered.width / rendered.height == pytest.approx(4 / 3)
        assert min(rendered.getpixel((4, 4))) > 245
    x1, y1, x2, y2 = COVER_TEXT_BOX
    assert x1 >= SAFE_INSET and x2 <= CANVAS_SIZE[0] - SAFE_INSET
    assert y1 >= SAFE_INSET and y2 <= CANVAS_SIZE[1] - SAFE_INSET
    assert WHITE_BORDER == 36


def test_four_templates_share_geometry_but_change_style(tmp_path):
    source = _image(tmp_path / "source.jpg")
    digests = []
    for key in "ABCD":
        result = image_renderer.render_listing_images(property_id=f"QC{key}", canonical_facts=_facts(), source_images=[source], output_root=tmp_path / "rendered", template=key)
        with Image.open(result.outputs[0]) as rendered:
            assert rendered.size == CANVAS_SIZE
        digests.append(_hash(result.outputs[0]))
    assert len(set(digests)) == 4


def test_missing_fields_are_hidden_without_placeholder_copy(tmp_path):
    source = _image(tmp_path / "source.jpg")
    facts = _facts(project_name=None, public_location_display=None, monthly_rent_usd=None, layout=None, size_sqm=None, floor=None, property_type=None, property_type_display=None)
    data = image_renderer.renderer_listing_data(facts, "QCEMPTY")
    assert not image_renderer._price(data)
    assert all(image_renderer._clean(value) == "" for key, value in data.items() if key not in {"property_id", "currency"})
    result = image_renderer.render_listing_images(property_id="QCEMPTY", canonical_facts=facts, source_images=[source], output_root=tmp_path / "rendered")
    assert result.ok


def test_dark_and_light_logo_are_selected_from_image_luminance(tmp_path):
    bright = _image(tmp_path / "bright.jpg", (235, 235, 235))
    dark = _image(tmp_path / "dark.jpg", (22, 22, 22))
    result = image_renderer.render_listing_images(property_id="QCLOGO", canonical_facts=_facts(), source_images=[bright, dark], output_root=tmp_path / "rendered")
    assert result.logo_variants == ["dark", "light"]


def test_multiple_images_keep_order_and_source_hashes(tmp_path):
    sources = [_image(tmp_path / f"{index}.jpg", (index * 30, 80, 150)) for index in range(1, 4)]
    before = [_hash(path) for path in sources]
    result = image_renderer.render_listing_images(property_id="QCORDER", canonical_facts=_facts(), source_images=sources, output_root=tmp_path / "rendered")
    assert [path.name for path in result.outputs] == ["cover.jpg", "photo_01.jpg", "photo_02.jpg"]
    assert [_hash(path) for path in sources] == before
    with Image.open(result.outputs[1]).convert("RGB") as first_detail, Image.open(result.outputs[2]).convert("RGB") as second_detail:
        assert first_detail.getpixel((600, 450))[0] < second_detail.getpixel((600, 450))[0]


def test_output_cannot_overlap_input(tmp_path):
    output_root = tmp_path / "rendered"
    source = _image(tmp_path / "source.jpg")
    with pytest.raises(ValueError, match="output_overlaps_input"):
        image_renderer.render_listing_images(property_id="QCX", canonical_facts=_facts(), source_images=[output_root / "QCX" / "cover.jpg"], output_root=output_root)
    assert source.exists()
    with pytest.raises(ValueError, match="unsafe_property_id"):
        image_renderer.render_listing_images(property_id="../source", canonical_facts=_facts(), source_images=[source], output_root=output_root)


def test_bad_small_transparent_and_single_failures_are_explicit_and_isolated(tmp_path):
    corrupt = tmp_path / "corrupt.jpg"; corrupt.write_bytes(b"not an image")
    small = _image(tmp_path / "small.jpg", size=(100, 100))
    transparent = _image(tmp_path / "transparent.png", (0, 0, 0, 0), mode="RGBA")
    valid = _image(tmp_path / "valid.jpg", (110, 130, 150))
    before = {path.name: _hash(path) for path in (corrupt, small, transparent, valid)}
    result = image_renderer.render_listing_images(property_id="QCBAD", canonical_facts=_facts(), source_images=[corrupt, valid, small, transparent], output_root=tmp_path / "rendered")
    assert [path.name for path in result.outputs] == ["cover.jpg"]
    assert [issue.code for issue in result.issues] == ["unrecognized_image", "image_too_small", "transparent_image"]
    assert all(issue.message.startswith("图片 ") for issue in result.issues)
    assert {path.name: _hash(path) for path in (corrupt, small, transparent, valid)} == before


def test_adapter_uses_existing_canonical_projection_once(monkeypatch):
    calls = []
    def projection(facts, listing_id, source_post_url=""):
        calls.append((facts, listing_id))
        return {"project": "唯一项目", "area": "BKK1", "layout": "1房", "price": 700, "currency": "USD", "size_sqm": 45, "floor": "8", "property_type_display": "公寓"}
    monkeypatch.setattr(image_renderer, "listing_projection", projection)
    data = image_renderer.renderer_listing_data({"canonical": True}, "QCONE")
    assert calls == [({"canonical": True}, "QCONE")]
    assert data["location"] == "唯一项目" and data["price"] == 700

