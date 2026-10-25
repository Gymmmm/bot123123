from PIL import Image
import pytest
from v3_core.media.cover_renderer import CoverRenderData, render_cover

@pytest.mark.parametrize('template,expected',[
    ('ql_optimized_villa',(1080,1350)),
    ('ql_optimized_apartment',(1200,800)),
    ('ql_optimized_premium',(1080,1350)),
])
def test_optimized_templates_have_expected_size(tmp_path,template,expected):
    files=[]
    for i in range(4):
        p=tmp_path/f'{i}.png'
        Image.new('RGB',(640,920),(30+i*45,70,110)).save(p)
        files.append(str(p))
    output=tmp_path/'out.jpg'
    render_cover(style='default',source_image=files[0],source_images=files[1:],
                 output_path=str(output),layout=template,
                 data=CoverRenderData(public_listing_id='QL-X',project='炳发城',property_type='别墅',price='3700'))
    with Image.open(output) as cover:
        assert cover.size == expected
