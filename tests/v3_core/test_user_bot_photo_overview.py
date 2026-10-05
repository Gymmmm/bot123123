from pathlib import Path
from PIL import Image

from v3_core.user_bot.photo_overview import MAX_PREVIEW, render_photo_overview


def test_photo_overview_is_fixed_4x5_and_caps_at_eight(tmp_path: Path):
    photos = []
    for i in range(10):
        p = tmp_path / f"p{i}.jpg"
        Image.new("RGB", (900 + i * 10, 600 + i * 7), (30 + i, 60, 90)).save(p)
        photos.append(str(p))
    out = tmp_path / "overview.jpg"
    rendered = render_photo_overview(tuple(photos), output_path=out)
    assert rendered == str(out.resolve())
    assert MAX_PREVIEW == 8
    with Image.open(out) as image:
        assert image.size == (1080, 1350)

def test_overview_renderer_skips_broken_images(tmp_path: Path):
    broken = tmp_path / "broken.jpg"
    broken.write_bytes(b"not-an-image")
    valid = tmp_path / "valid.jpg"
    Image.new("RGB", (800, 600), (20, 40, 60)).save(valid)
    out = tmp_path / "safe.jpg"
    assert render_photo_overview((str(broken), str(valid)), output_path=out) == str(out.resolve())
    assert out.is_file()

def test_overview_is_single_composed_image_for_eight_plus_sources(tmp_path: Path):
    photos = []
    for i in range(9):
        p = tmp_path / f"room_{i}.jpg"
        Image.new("RGB", (720 + i * 5, 540), (50 + i, 80, 110)).save(p)
        photos.append(str(p))
    out = tmp_path / "overview-nine.jpg"
    render_photo_overview(tuple(photos), output_path=out)
    assert out.is_file()
    with Image.open(out) as image:
        assert image.size == (1080, 1350)