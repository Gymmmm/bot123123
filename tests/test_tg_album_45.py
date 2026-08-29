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


def test_grouped_media_outputs_clean_1200x900_for_all_source_shapes():
    for source_size in ((1920, 1080), (1200, 1200), (900, 1600)):
        out = normalize_album_image(_jpeg(source_size))
        with Image.open(io.BytesIO(out)).convert("RGB") as im:
            assert im.size == (1200, 900) == CHANNEL_ALBUM_SIZE
            assert min(im.getpixel((4, 4))) > 235


def test_landscape_subject_is_contained_inside_white_card_not_hard_cropped():
    src = Image.new("RGB", (1800, 900), "white")
    for x in range(0, 150):
        for y in range(900):
            src.putpixel((x, y), (255, 0, 0))
    for x in range(1650, 1800):
        for y in range(900):
            src.putpixel((x, y), (0, 255, 0))
    buf = io.BytesIO(); src.save(buf, "JPEG", quality=100)
    out = normalize_album_image(buf.getvalue())
    with Image.open(io.BytesIO(out)).convert("RGB") as im:
        y = im.height // 2
        scan = [im.getpixel((x, y)) for x in range(20, im.width - 20)]
        assert any(r > g * 1.4 and r > b * 1.4 for r, g, b in scan)
        assert any(g > r * 1.4 and g > b * 1.4 for r, g, b in scan)
        assert min(im.getpixel((4, 4))) > 235


def test_every_album_slot_uses_same_clean_4_3_card():
    src = _jpeg((1920, 1080))
    for idx in range(6):
        out = _normalize_for_album_slot(src, index=idx, total=6)
        with Image.open(io.BytesIO(out)) as im:
            assert im.size == CHANNEL_ALBUM_SIZE == (1200, 900)


def test_default_channel_helper_stays_four_and_extra_goes_to_discussion():
    assert CHANNEL_MAIN_ALBUM_MAX == 4
    paths = [f"image_{i}.jpg" for i in range(1, 9)]
    main, extra = split_album_for_channel(paths)
    assert len(main) == 4
    assert len(extra) == 4
    assert set(main).isdisjoint(extra)
    assert set(main + extra) == set(paths)
