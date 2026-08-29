import io
from PIL import Image

from meihua_publisher import (
    CHANNEL_ALBUM_SIZE,
    CHANNEL_MAIN_ALBUM_MAX,
    _normalize_for_album_slot,
    normalize_album_image,
    split_album_for_channel,
)


def _jpeg(size, color=(180, 120, 80)):
    im = Image.new("RGB", size, color)
    buf = io.BytesIO()
    im.save(buf, "JPEG")
    return buf.getvalue()


def test_grouped_media_outputs_1080x1350_for_landscape_square_and_portrait():
    for source_size in ((1920, 1080), (1200, 1200), (900, 1600)):
        out = normalize_album_image(_jpeg(source_size))
        with Image.open(io.BytesIO(out)) as im:
            assert im.size == (1080, 1350)


def test_landscape_subject_is_contained_not_hard_cropped():
    src = Image.new("RGB", (1600, 900), "white")
    for x in range(0, 120):
        for y in range(900):
            src.putpixel((x, y), (255, 0, 0))
    for x in range(1480, 1600):
        for y in range(900):
            src.putpixel((x, y), (0, 255, 0))
    buf = io.BytesIO(); src.save(buf, "JPEG", quality=100)
    out = normalize_album_image(buf.getvalue())
    with Image.open(io.BytesIO(out)).convert("RGB") as im:
        y = im.height // 2
        left = im.getpixel((10, y))
        right = im.getpixel((im.width - 11, y))
        assert left[0] > left[1] * 1.4
        assert right[1] > right[0] * 1.4


def test_every_album_slot_uses_same_45_ratio():
    src = _jpeg((1920, 1080))
    for idx in range(4):
        out = _normalize_for_album_slot(src, index=idx, total=4)
        with Image.open(io.BytesIO(out)) as im:
            assert im.size == CHANNEL_ALBUM_SIZE


def test_channel_main_album_stays_four_and_extra_goes_to_discussion():
    assert CHANNEL_MAIN_ALBUM_MAX == 4
    paths = [f"image_{i}.jpg" for i in range(1, 9)]
    main, extra = split_album_for_channel(paths)
    assert len(main) == 4
    assert len(extra) == 4
    assert set(main).isdisjoint(extra)
    assert set(main + extra) == set(paths)
