from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

from qiaolian_v3.media.gallery import build_media_selection


def _image(path: Path, size=(1200, 800), value=140):
    image = Image.new('RGB', size, (value, value, value))
    draw = ImageDraw.Draw(image)
    width, height = size
    offset = max(10, value % max(20, width // 4))
    draw.rectangle((offset, offset, max(offset + 10, width // 2), max(offset + 10, height // 3)), fill=(255 - value, value // 2, (value * 3) % 255))
    draw.line((0, height - 1 - offset % max(1, height), width - 1, offset % max(1, height)), fill=(value // 2, 255 - value, value), width=max(1, width // 100))
    image.save(path)
    return str(path)


def test_bad_images_are_filtered_and_four_usable_images_are_required(tmp_path):
    good = [_image(tmp_path / f'g{i}.jpg', value=100 + i * 23) for i in range(4)]
    bad = _image(tmp_path / 'tiny.jpg', size=(320, 240), value=211)
    result = build_media_selection(good + [bad])
    assert result.usable_count == 4
    assert str(Path(bad).resolve()) in result.rejected_paths
    assert result.meets_photo_minimum is True


def test_manual_cover_has_precedence_when_usable(tmp_path):
    paths = [_image(tmp_path / f'{i}.jpg', value=100 + i * 23) for i in range(4)]
    result = build_media_selection(paths, manual_cover_path=paths[-1])
    assert result.cover_path == str(Path(paths[-1]).resolve())
    assert result.cover_source == 'manual'


def test_right_price_is_manual_only(tmp_path):
    paths = [_image(tmp_path / f'{i}.jpg', value=100 + i * 23) for i in range(4)]
    automatic = build_media_selection(paths, requested_cover_style='right_price')
    assert automatic.cover_style == 'classic_blue'
    assert 'right_price_requires_manual_selection' in automatic.warnings

    manual = build_media_selection(
        paths,
        manual_cover_path=paths[0],
        requested_cover_style='right_price',
        manual_style_selected=True,
    )
    assert manual.cover_style == 'right_price'
