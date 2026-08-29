from pathlib import Path
import re

p = Path('meihua_publisher.py')
text = p.read_text(encoding='utf-8')

old_constants = '''# 6 张相册比例：landscape=横向 3:2（不少客户端更接近「3 列×2 行」观感）；square=1:1 方图（常为 2 列×3 行）
CHANNEL_ALBUM_SIX_ASPECT = os.getenv("CHANNEL_ALBUM_SIX_ASPECT", "landscape").strip().lower()
# 组图排版：one_three=首张横图+后三张方图循环（Telegram 常见「上一横、下三格」）；classic=按张数统一方图/原逻辑
CHANNEL_ALBUM_LAYOUT = os.getenv("CHANNEL_ALBUM_LAYOUT", "one_three").strip().lower()
# 1+3 主图比例 16:9；方图边长
ONE_THREE_HERO_BOX = (1280, 720)
ONE_THREE_TILE = int(os.getenv("ONE_THREE_TILE", "1080"))
'''
new_constants = '''# Telegram 手机端 grouped media 统一使用 4:5 竖图派生稿。
# 原始素材永不覆盖；横图/方图使用模糊延展背景完整保留主体，不做硬裁切。
CHANNEL_ALBUM_SIZE = (1080, 1350)
CHANNEL_ALBUM_LAYOUT = "portrait_4_5"
'''
if old_constants not in text:
    raise SystemExit('album constants block not found')
text = text.replace(old_constants, new_constants, 1)

start = text.index('def normalize_album_image(')
end = text.index('\n\n# ── 文案构造', start)
new_block = r'''def _fit_to_45_canvas(
    image: Image.Image,
    *,
    canvas_size: tuple[int, int] = CHANNEL_ALBUM_SIZE,
) -> Image.Image:
    """生成 Telegram 4:5 发布派生图，同时完整保留原图主体。

    横图/方图不做 ImageOps.fit 硬裁。背景使用同图放大、轻模糊和轻压暗，
    前景按 contain 方式完整置中。这样手机端统一 1080x1350，同时保留房源证据。
    """
    src = image.convert("RGB")
    cw, ch = canvas_size

    # 先生成满版背景。背景允许裁切，因为它只承担延展，不承担证据内容。
    bg = ImageOps.fit(src, (cw, ch), method=Image.Resampling.LANCZOS)
    bg = bg.filter(ImageFilter.GaussianBlur(radius=max(18, int(min(cw, ch) * 0.025))))
    bg = ImageEnhance.Brightness(bg).enhance(0.78)
    bg = ImageEnhance.Color(bg).enhance(0.92)

    # 主体图严格 contain，不裁掉左右/上下内容。
    fg = src.copy()
    fg.thumbnail((cw, ch), Image.Resampling.LANCZOS)
    x = (cw - fg.width) // 2
    y = (ch - fg.height) // 2

    canvas = bg.convert("RGBA")
    # 给主体一个非常轻的阴影/分层，避免横图边界显得突兀。
    shadow_pad = max(6, int(min(cw, ch) * 0.008))
    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle(
        (x - shadow_pad, y - shadow_pad, x + fg.width + shadow_pad, y + fg.height + shadow_pad),
        radius=max(10, int(min(cw, ch) * 0.012)),
        fill=(0, 0, 0, 42),
    )
    canvas = Image.alpha_composite(canvas, shadow)
    canvas.paste(fg, (x, y))
    return canvas.convert("RGB")


def normalize_album_image(
    image_bytes: bytes,
    *,
    target_size: int = 1280,
    force_square: bool = False,
    fit_box: tuple[int, int] | None = None,
) -> bytes:
    """统一 Telegram grouped media 为 1080x1350 (4:5) 派生图。

    旧参数保留仅用于回调兼容；新发布不再生成 16:9 hero / 1:1 tile。
    """
    im = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    im = _fit_to_45_canvas(im)
    out = io.BytesIO()
    im.save(out, "JPEG", quality=91, optimize=True)
    return out.getvalue()


def _album_layout_is_one_three() -> bool:
    """兼容旧调用；新发布布局固定为 portrait_4_5。"""
    return False


def _normalize_for_album_slot(image_bytes: bytes, *, index: int, total: int) -> bytes:
    """所有频道主相册和评论区相册统一 4:5，不按槽位改变比例。"""
    return normalize_album_image(image_bytes)
'''
text = text[:start] + new_block + text[end:]
p.write_text(text, encoding='utf-8')

# Add focused regression tests.
t = Path('tests/test_tg_album_45.py')
t.write_text(r'''import io
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
    # 红/绿左右边缘都必须仍存在于前景主体；硬裁 4:5 会丢掉其中一侧。
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
        # 前景横图宽度会占满 1080；在其垂直中段检查左右证据边缘。
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
''', encoding='utf-8')

# Extend permanent CI.
w = Path('.github/workflows/qiaolian-ui-check.yml')
wt = w.read_text(encoding='utf-8')
wt = wt.replace(
    'tests/test_daily_broadcast_rotation.py',
    'tests/test_daily_broadcast_rotation.py tests/test_tg_album_45.py',
)
w.write_text(wt, encoding='utf-8')
