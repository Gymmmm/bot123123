"""Opt-in, no-crop 4:5 Qiaolian covers. Every photograph is fully contained."""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps

SIZE = (1080, 1350)
STYLES = {
    'ql_frame_blue': {'bg': '#EAF2F8', 'mat': '#DDE8F0', 'ink': '#173B63', 'muted': '#536B80'},
    'ql_frame_cream': {'bg': '#F8F1E8', 'mat': '#EEE4D8', 'ink': '#644833', 'muted': '#8B7561'},
    'ql_frame_green': {'bg': '#F0F5EB', 'mat': '#DFE9D8', 'ink': '#315B37', 'muted': '#66826B'},
    'ql_frame_white': {'bg': '#FAFAF8', 'mat': '#EDEEEB', 'ink': '#282D33', 'muted': '#7A7E82'},
}


def _font(size: int, bold: bool = False):
    root = Path(__file__).resolve().parents[2] / 'assets' / 'fonts'
    for path in (root / ('NotoSansSC-Bold.otf' if bold else 'NotoSansSC-Medium.otf'),
                 Path('/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc')):
        try:
            return ImageFont.truetype(str(path), size)
        except OSError:
            continue
    return ImageFont.load_default()


def _fitted_text(draw, value, max_width, size, minimum=20, bold=False):
    value = str(value or '').strip()
    for point in range(size, minimum - 1, -1):
        font = _font(point, bold)
        if draw.textlength(value, font=font) <= max_width:
            return value, font
    font = _font(minimum, bold)
    while value and draw.textlength(value + '…', font=font) > max_width:
        value = value[:-1]
    return (value + '…' if value else ''), font


def _frame(canvas, photo, box, mat, inset=7):
    """Contain the ENTIRE source image: never crop, stretch or use a blurred fill."""
    x, y, width, height = box
    draw = ImageDraw.Draw(canvas)
    draw.rectangle((x, y, x + width - 1, y + height - 1), fill=mat)
    inner = (max(1, width - inset * 2), max(1, height - inset * 2))
    photo = ImageOps.exif_transpose(photo).convert('RGB')
    scaled = ImageOps.contain(photo, inner, method=Image.Resampling.LANCZOS)
    canvas.paste(scaled, (x + (width - scaled.width) // 2, y + (height - scaled.height) // 2))



def _photo_boxes(count: int, *, photo_top: int = 139, photo_bottom: int = 1115,
                 gap: int = 10) -> list[tuple[int, int, int, int]]:
    """One unified geometry for every palette: 4 side, 3 top, 2 stacked, 1 full.

    Photo ordering comes from the existing cover planner: hero first.
    """
    if not 1 <= count <= 4:
        raise ValueError('photo_count_must_be_1_to_4')
    height = photo_bottom - photo_top
    if count == 1:
        return [(24, photo_top, 1032, height)]
    if count == 2:
        first = (height - gap) // 2
        return [(24, photo_top, 1032, first),
                (24, photo_top + first + gap, 1032, height - first - gap)]
    if count == 3:
        hero_h = 635
        bottom_h = height - hero_h - gap
        left_w = (1032 - gap) // 2
        return [(24, photo_top, 1032, hero_h),
                (24, photo_top + hero_h + gap, left_w, bottom_h),
                (24 + left_w + gap, photo_top + hero_h + gap, 1032 - left_w - gap, bottom_h)]
    hero_w = 652
    right_x = 24 + hero_w + gap
    right_w = 1032 - hero_w - gap
    each = (height - 2 * gap) // 3
    return [(24, photo_top, hero_w, height),
            (right_x, photo_top, right_w, each),
            (right_x, photo_top + each + gap, right_w, each),
            (right_x, photo_top + 2 * (each + gap), right_w, height - 2 * (each + gap))]


def render_contain_cover(*, images, data, output_path: str, layout: str, logo=None):
    if layout not in STYLES:
        raise ValueError('unknown_contain_layout')
    if not images:
        raise ValueError('cover_no_usable_images')
    spec = STYLES[layout]
    canvas = Image.new('RGB', SIZE, spec['bg'])
    draw = ImageDraw.Draw(canvas)
    # All four variants use precisely the same canvas and information hierarchy.
    # The layout adapts to 1/2/3/4 actual photographs without repeating media.
    draw.text((32, 27), '精选房源 · 实拍展示', fill=spec['ink'], font=_font(42, True))
    draw.text((34, 82), 'PROPERTY COLLECTION', fill=spec['muted'], font=_font(18))
    draw.line((32, 119, 1048, 119), fill=spec['mat'], width=3)
    photo_top, photo_bottom, gap = 139, 1115, 10
    count = min(len(images), 4)
    boxes = _photo_boxes(count, photo_top=photo_top, photo_bottom=photo_bottom, gap=gap)
    for photo, box in zip(images[:count], boxes):
        _frame(canvas, photo, box, spec['mat'])

    draw.rectangle((24, 1133, 1056, 1324), fill='#FFFFFF')
    title = data.project or data.project_alias or data.area or '房源详情'
    subtitle = ' · '.join(str(x).strip() for x in (data.property_type, data.layout) if x)
    title, title_font = _fitted_text(draw, title, 590, 43, 29, True)
    draw.text((50, 1152), title, font=title_font, fill=spec['ink'])
    subtitle, subtitle_font = _fitted_text(draw, subtitle, 580, 27, 21)
    draw.text((52, 1221), subtitle, font=subtitle_font, fill=spec['muted'])
    price = data.price_line()
    if price:
        price, price_font = _fitted_text(draw, price, 365, 56, 30, True)
        draw.text((1027, 1192), price, anchor='ra', font=price_font, fill=spec['ink'])
    if logo is not None:
        mark = logo.convert('RGBA')
        mark.thumbnail((165, 45), Image.Resampling.LANCZOS)
        canvas.paste(mark, (1025 - mark.width, 1265), mark)
    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(target, format='JPEG' if target.suffix.lower() in ('.jpg', '.jpeg') else 'PNG', quality=94)
    return str(target)
