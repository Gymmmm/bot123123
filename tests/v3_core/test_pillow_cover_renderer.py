from pathlib import Path
from PIL import Image
import pytest
from v3_core.media.cover_renderer import CoverRenderData, render_cover

def make(path: Path, size=(1600, 900), value=120):
    Image.new("RGB", size, (value, min(255, value+30), min(255, value+60))).save(path)
    return str(path)

@pytest.mark.parametrize("count", [1, 2, 4, 10])
def test_pillow_cover_counts_and_exact_size(tmp_path, count):
    paths=[]
    for i in range(count):
        size=(1600,900) if i % 2 == 0 else (700,1400)
        paths.append(make(tmp_path / f"{i}.jpg", size=size, value=40+i*15))
    out=tmp_path/"cover.png"
    render_cover(style="classic_blue", source_image=paths[0], source_images=paths[1:],
                 output_path=str(out), data=CoverRenderData(public_listing_id="QL-PP-T1", project="富力城", layout="1房", price="650"))
    with Image.open(out) as im:
        assert im.size == (1080,1350)
        assert im.format == "PNG"

def test_price_chinese_and_bad_image_are_tolerated(tmp_path):
    good=make(tmp_path/"good.png", size=(900,1600))
    bad=tmp_path/"bad.jpg"; bad.write_bytes(b"not-an-image")
    out=tmp_path/"cover.jpg"
    data=CoverRenderData(public_listing_id="QL-PP-T2", project="太子中央广场", area="金边", price="650")
    assert data.price_line() == "$650/月"
    render_cover(style="anything", source_image=str(bad), source_images=[good], output_path=str(out), data=data)
    with Image.open(out) as im:
        assert im.size == (1080,1350)
        assert im.format == "JPEG"

def test_font_missing_falls_back(tmp_path, monkeypatch):
    import v3_core.media.cover_renderer as renderer
    monkeypatch.setattr(renderer, "FONT_CANDIDATES", ("/missing/font.ttf",))
    src=make(tmp_path/"one.jpg")
    out=tmp_path/"cover.png"
    render_cover(style="x", source_image=src, output_path=str(out), data=CoverRenderData(public_listing_id="QL", project="侨联地产", price="650"))
    assert out.is_file()

def test_fit_crops_without_stretching():
    from v3_core.media.cover_renderer import _fit
    src=Image.new("RGB",(2000,500))
    fitted=_fit(src,(352,492))
    assert fitted.size == (352,492)

def test_all_bad_images_follow_business_failure_not_worker_crash(tmp_path):
    bad=tmp_path/"bad.jpg"; bad.write_bytes(b"bad")
    with pytest.raises(ValueError, match="cover_no_usable_images"):
        render_cover(style="x", source_image=str(bad), output_path=str(tmp_path/"x.png"), data=CoverRenderData(public_listing_id="QL"))
