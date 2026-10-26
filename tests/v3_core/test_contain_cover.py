from PIL import Image
import pytest
from v3_core.media.contain_cover import _frame
from v3_core.media.cover_renderer import CoverRenderData, render_cover


@pytest.mark.parametrize('style', ['ql_frame_blue', 'ql_frame_cream', 'ql_frame_green', 'ql_frame_white'])
@pytest.mark.parametrize('count', [1, 2, 3, 4])
def test_no_crop_templates(tmp_path, style, count):
    photos = []
    for i in range(count):
        path = tmp_path / f'{i}.png'
        Image.new('RGB', (960 if i % 2 else 500, 500 if i % 2 else 1000), (30 + 35*i, 70, 120)).save(path)
        photos.append(str(path))
    target = tmp_path / 'cover.jpg'
    render_cover(style='default', source_image=photos[0], source_images=photos[1:],
                 output_path=str(target), layout=style,
                 data=CoverRenderData(public_listing_id='QL-TEST', project='测试房源', price='3700'))
    with Image.open(target) as result:
        assert result.size == (1080, 1350)


def test_frame_preserves_source_edges():
    photo = Image.new('RGB', (600, 300), 'white')
    for x, color in [(0, (255, 0, 0)), (599, (0, 0, 255))]:
        for y in range(300):
            photo.putpixel((x, y), color)
    canvas = Image.new('RGB', (700, 700), 'white')
    _frame(canvas, photo, (0, 0, 700, 700), '#EDEEEB')
    assert (255, 0, 0) in canvas.get_flattened_data()
    assert (0, 0, 255) in canvas.get_flattened_data()
