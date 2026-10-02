from pathlib import Path

import pytest
from PIL import Image

from v3_core.user_bot.photos_collage import (
    render_listing_preview_collage,
    render_photo_page_collage,
)


def _photos(tmp_path: Path, count: int) -> list[str]:
    result = []
    for index in range(count):
        path = tmp_path / f"p{index}.jpg"
        Image.new("RGB", (900 + index * 20, 700), (30 + index * 20, 70, 110)).save(path)
        result.append(str(path))
    return result


def test_apartment_preview_accepts_three_selected_photos(tmp_path):
    paths = _photos(tmp_path, 3)
    output = render_listing_preview_collage(
        paths,
        property_type="公寓",
        public_listing_id="QL-PP-TEST",
        out_path=tmp_path / "apartment.jpg",
    )
    with Image.open(output) as image:
        assert image.size == (1280, 900)


def test_villa_preview_requires_four_selected_photos(tmp_path):
    paths = _photos(tmp_path, 4)
    output = render_listing_preview_collage(
        paths,
        property_type="别墅",
        public_listing_id="QL-PP-TEST",
        out_path=tmp_path / "villa.jpg",
    )
    with Image.open(output) as image:
        assert image.size == (1280, 900)
    with pytest.raises(ValueError):
        render_listing_preview_collage(
            paths[:3],
            property_type="别墅",
            public_listing_id="QL-PP-TEST",
            out_path=tmp_path / "bad.jpg",
        )


def test_photo_page_is_single_large_four_photo_canvas(tmp_path):
    paths = _photos(tmp_path, 4)
    output = render_photo_page_collage(
        paths,
        public_listing_id="QL-PP-TEST",
        page=1,
        out_path=tmp_path / "page.jpg",
    )
    with Image.open(output) as image:
        assert image.size == (1280, 1280)
