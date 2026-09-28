from __future__ import annotations

from pathlib import Path

from PIL import Image

from media_pipeline_v1_1 import _resolve_ranked_path, get_source_order_files
from photo_formatter_v1_1 import format_gallery_photo, ordered_source_files
from publication_package import _finalize_detail_image


def _make(path: Path, size: tuple[int, int], color=(24, 32, 48)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color).save(path, "JPEG", quality=96)


def test_gallery_orientation_presets_keep_complete_source(tmp_path: Path):
    cases = [
        ((1600, 900), (1200, 900), "landscape"),
        ((900, 1600), (900, 1200), "portrait"),
        ((1000, 1000), (1080, 1080), "square"),
    ]
    for index, (source_size, expected_canvas, expected_orientation) in enumerate(cases, start=1):
        source = tmp_path / f"source_{index}.jpg"
        output = tmp_path / f"out_{index}.jpg"
        _make(source, source_size)
        info = format_gallery_photo(source, output, add_logo=False)
        assert info["orientation"] == expected_orientation
        assert (info["canvas"]["width"], info["canvas"]["height"]) == expected_canvas
        with Image.open(output) as rendered:
            assert rendered.size == expected_canvas
        box = info["image_box"]
        assert box["x"] >= 0 and box["y"] >= 0
        assert box["x"] + box["width"] <= expected_canvas[0]
        assert box["y"] + box["height"] <= expected_canvas[1]


def test_logo_is_anchored_inside_actual_photo_not_white_border(tmp_path: Path):
    source = tmp_path / "portrait.jpg"
    logo = tmp_path / "logo.png"
    output = tmp_path / "portrait_out.jpg"
    _make(source, (600, 1600), color=(35, 45, 60))
    Image.new("RGBA", (220, 60), (255, 255, 255, 235)).save(logo)

    info = format_gallery_photo(source, output, logo_path=logo, logo_position="top_left")
    image_box = info["image_box"]
    logo_box = info["logo_box"]
    assert logo_box is not None
    assert logo_box["x"] >= image_box["x"]
    assert logo_box["y"] >= image_box["y"]
    assert logo_box["x"] + logo_box["width"] <= image_box["x"] + image_box["width"]
    assert logo_box["y"] + logo_box["height"] <= image_box["y"] + image_box["height"]


def test_gallery_logo_is_light_brand_mark_not_large_overlay(tmp_path: Path):
    source = tmp_path / "landscape.jpg"
    logo = tmp_path / "logo.png"
    output = tmp_path / "landscape_out.jpg"
    _make(source, (1600, 900), color=(35, 45, 60))
    Image.new("RGBA", (120, 40), (255, 255, 255, 235)).save(logo)

    info = format_gallery_photo(source, output, logo_path=logo, logo_position="top_left")
    image_box = info["image_box"]
    logo_box = info["logo_box"]
    assert logo_box is not None
    assert logo_box["width"] >= 180
    assert logo_box["width"] >= int(image_box["width"] * 0.16)
    assert logo_box["width"] <= int(image_box["width"] * 0.32) + 1
    assert logo_box["height"] <= int(image_box["height"] * 0.15) + 1


def test_logo_can_switch_to_top_right_without_leaving_photo(tmp_path: Path):
    source = tmp_path / "wide.jpg"
    logo = tmp_path / "logo.png"
    output = tmp_path / "wide_out.jpg"
    _make(source, (1800, 900), color=(35, 45, 60))
    Image.new("RGBA", (240, 70), (255, 255, 255, 235)).save(logo)
    info = format_gallery_photo(source, output, logo_path=logo, logo_position="top_right")
    image_box = info["image_box"]
    logo_box = info["logo_box"]
    assert logo_box["x"] > image_box["x"] + image_box["width"] // 2
    assert logo_box["x"] + logo_box["width"] <= image_box["x"] + image_box["width"]


def test_natural_filename_fallback_is_1_2_10_not_1_10_2(tmp_path: Path):
    for name in ("1.jpg", "10.jpg", "2.jpg"):
        _make(tmp_path / name, (800, 600))
    files = ordered_source_files(tmp_path)
    assert [path.name for path in files] == ["1.jpg", "2.jpg", "10.jpg"]


def test_explicit_collector_order_beats_filename_sort(tmp_path: Path):
    for name in ("1.jpg", "2.jpg", "10.jpg"):
        _make(tmp_path / name, (800, 600))
    files = get_source_order_files(tmp_path, source_order=["10.jpg", "1.jpg", "2.jpg"])
    assert [path.name for path in files] == ["10.jpg", "1.jpg", "2.jpg"]


def test_manifest_raw_images_json_order_is_authoritative(tmp_path: Path):
    for name in ("1.jpg", "2.jpg", "10.jpg"):
        _make(tmp_path / name, (800, 600))
    manifest = tmp_path / "raw.json"
    manifest.write_text(
        '{"raw_images_json":[{"local_path":"2.jpg"},{"local_path":"10.jpg"},{"local_path":"1.jpg"}]}',
        encoding="utf-8",
    )
    files = ordered_source_files(tmp_path, source_manifest=manifest)
    assert [path.name for path in files] == ["2.jpg", "10.jpg", "1.jpg"]


def test_ranker_relative_file_resolves_against_listing_folder(tmp_path: Path):
    source = tmp_path / "2.jpg"
    _make(source, (800, 600))
    resolved = _resolve_ranked_path("2.jpg", tmp_path)
    assert resolved == source.resolve()


def test_formal_publication_package_uses_v11_orientation_canvases(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("QIAOLIAN_GALLERY_LOGO_POSITION", "top_left")
    cases = [
        ((1600, 900), (1200, 900)),
        ((900, 1600), (900, 1200)),
        ((1000, 1000), (1080, 1080)),
    ]
    for index, (source_size, expected) in enumerate(cases, start=1):
        source = tmp_path / f"formal_source_{index}.jpg"
        target = tmp_path / f"formal_target_{index}.jpg"
        _make(source, source_size, color=(40, 54, 72))
        returned = _finalize_detail_image(str(source), str(target))
        assert returned == str(target)
        with Image.open(target) as rendered:
            assert rendered.size == expected


def test_formal_publication_logo_position_rejects_invalid_env(tmp_path: Path, monkeypatch):
    source = tmp_path / "formal_portrait.jpg"
    target = tmp_path / "formal_portrait_out.jpg"
    _make(source, (700, 1500), color=(40, 54, 72))
    monkeypatch.setenv("QIAOLIAN_GALLERY_LOGO_POSITION", "not-a-position")
    _finalize_detail_image(str(source), str(target))
    with Image.open(target) as rendered:
        assert rendered.size == (900, 1200)
