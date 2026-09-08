from __future__ import annotations

from pathlib import Path

from PIL import Image

from qiaolian_v3.media.gallery import build_media_selection


def _image(path: Path, size=(1200, 800), value=140):
    Image.new('RGB', size, (value, value, value)).save(path)
    return str(path)


def test_bad_images_are_filtered_and_four_usable_images_are_required(tmp_path):
    good = [_image(tmp_path / f'g{i}.jpg', value=100 + i * 10) for i in range(4)]
    bad = _image(tmp_path / 'tiny.jpg', size=(320, 240))
    result = build_media_selection(good + [bad])
    assert result.usable_count == 4
    assert bad in result.rejected_paths
    assert result.meets_photo_minimum is True


def test_manual_cover_has_precedence_when_usable(tmp_path):
    paths = [_image(tmp_path / f'{i}.jpg', value=100 + i * 10) for i in range(4)]
    result = build_media_selection(paths, manual_cover_path=paths[-1])
    assert result.cover_path == str(Path(paths[-1]).resolve())
    assert result.cover_source == 'manual'


def test_right_price_is_manual_only(tmp_path):
    paths = [_image(tmp_path / f'{i}.jpg', value=100 + i * 10) for i in range(4)]
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
