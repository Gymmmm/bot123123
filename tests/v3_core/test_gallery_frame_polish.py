from pathlib import Path

from PIL import Image, ImageDraw

from v3_core.media.photo_formatter import (
    CANVAS_PRESETS,
    FRAME_BORDER,
    cover_frame_image,
    format_gallery_photo,
)


def _room(path: Path, size: tuple[int, int], tone: int = 80):
    image = Image.new("RGB", size, (tone + 40, tone + 30, tone + 20))
    draw = ImageDraw.Draw(image)
    draw.rectangle((40, 40, size[0] - 40, size[1] - 40), outline=(220, 220, 210), width=6)
    draw.rectangle((90, 100, size[0] // 2, size[1] // 2), fill=(170, 195, 210))
    image.save(path, "JPEG", quality=92)
    return path


def test_gallery_cover_fill_uses_thin_white_frame_and_shared_canvas(tmp_path: Path):
    landscape = _room(tmp_path / "land.jpg", (1600, 900), 70)
    portrait = _room(tmp_path / "port.jpg", (900, 1400), 90)
    out_a = tmp_path / "a.jpg"
    out_b = tmp_path / "b.jpg"

    info_a = format_gallery_photo(landscape, out_a, add_logo=False, force_orientation="landscape")
    info_b = format_gallery_photo(portrait, out_b, add_logo=False, force_orientation="landscape")

    assert info_a["canvas"] == {"width": 1080, "height": 864}
    assert info_b["canvas"] == {"width": 1080, "height": 864}
    assert info_a["border"] == FRAME_BORDER
    assert info_a["enhanced"] is True

    with Image.open(out_a) as img:
        assert img.size == (1080, 864)
        # Outer rim stays white; inner photo is not washed into a fat letterbox.
        assert img.getpixel((0, 0)) == (255, 255, 255)
        assert img.getpixel((FRAME_BORDER, FRAME_BORDER)) != (255, 255, 255)
        assert img.getpixel((540, 432)) != (255, 255, 255)


def test_portrait_force_orientation_uses_1200x1500(tmp_path: Path):
    source = _room(tmp_path / "mixed.jpg", (1200, 900), 60)
    output = tmp_path / "port.jpg"
    info = format_gallery_photo(source, output, add_logo=False, force_orientation="portrait")
    assert info["canvas"] == {"width": 1200, "height": 1500}
    assert info["orientation"] == "portrait"
    with Image.open(output) as img:
        assert img.size == CANVAS_PRESETS["portrait"]["size"]


def test_cover_frame_image_fills_inner_box():
    src = Image.new("RGB", (400, 800), (30, 80, 120))
    canvas, box = cover_frame_image(src, (1080, 864), border=10)
    assert canvas.size == (1080, 864)
    assert box == (10, 10, 1060, 844)
    assert canvas.getpixel((0, 0)) == (255, 255, 255)
    assert canvas.getpixel((10, 10)) == (30, 80, 120)
