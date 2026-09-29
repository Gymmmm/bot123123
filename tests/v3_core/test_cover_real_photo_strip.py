from pathlib import Path

from PIL import Image

from v3_core.media.cover_renderer import append_real_photo_strip


def test_cover_thumbnail_uses_only_available_distinct_real_photos(tmp_path: Path):
    poster = tmp_path / "poster.jpg"
    Image.new("RGB", (1200, 1500), "black").save(poster)
    colors = ("red", "green", "blue")
    paths = []
    for i, color in enumerate(colors):
        path = tmp_path / f"room{i}.jpg"
        Image.new("RGB", (800, 600), color).save(path)
        paths.append(str(path))
    append_real_photo_strip(poster, tuple(paths))
    with Image.open(poster) as result:
        assert result.size == (1080, 1350)
        assert result.getpixel((180, 1100))[0] > 150  # room one: red
        assert result.getpixel((540, 1100))[1] > 75   # room two: green
        assert result.getpixel((900, 1100))[2] > 150  # room three: blue

    one = tmp_path / "single.jpg"
    Image.new("RGB", (1200, 1500), "black").save(one)
    append_real_photo_strip(one, (paths[0],))
    with Image.open(one) as result:
        assert result.size == (1080, 1350)
        assert result.getpixel((900, 1100))[0] > 150  # no repeated padding
