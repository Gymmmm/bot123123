"""Opt-in, no-crop 4:5 Qiaolian covers. Every photograph is fully contained."""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageOps

SIZE = (1080, 1350)
STYLES = {
    'ql_frame_blue': {'bg': '#EAF2F8', 'mat': '#DDE8F0', 'ink': '#173B63', 'muted': '#536B80', 'layout': 'side'},
    'ql_frame_cream': {'bg': '#F8F1E8', 'mat': '#EEE4D8', 'ink': '#644833', 'muted': '#8B7561', 'layout': 'top'},
    'ql_frame_green': {'bg': '#F0F5EB', 'mat': '#DFE9D8', 'ink': '#315B37', 'muted': '#66826B', 'layout': 'side'},
    'ql_frame_white': {'bg': '#FAFAF8', 'mat': '#EDEEEB', 'ink': '#282D33', 'muted': '#7A7E82', 'layout': 'top'},
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
    photo_h = photo_bottom - photo_top
    boxes = []
    if count == 1:
        boxes = [(24, photo_top, 1032, photo_h)]
    elif count == 2:
        if spec['layout'] == 'side':
            boxes = [(24, photo_top, 632, photo_h), (666, photo_top, 390, photo_h)]
        else:
            h = (photo_h - gap) // 2
            boxes = [(24, photo_top, 1032, h), (24, photo_top + h + gap, 1032, photo_h - h - gap)]
    elif spec['layout'] == 'side':
        hero_w = 652
        side_x, side_w = 24 + hero_w + gap, 1032 - hero_w - gap
        side_count = count - 1
        each = (photo_h - gap * (side_count - 1)) // side_count
        boxes = [(24, photo_top, hero_w, photo_h)]
        for i in range(side_count):
            y = photo_top + i * (each + gap)
            h = each if i < side_count - 1 else photo_bottom - y
            boxes.append((side_x, y, side_w, h))
    else:
        hero_h = 625 if count == 4 else 670
        remaining = photo_h - hero_h - gap
        side_count = count - 1
        cell_w = (1032 - gap * (side_count - 1)) // side_count
        boxes = [(24, photo_top, 1032, hero_h)]
        for i in range(side_count):
            x = 24 + i * (cell_w + gap)
            w = cell_w if i < side_count - 1 else 1056 - x
            boxes.append((x, photo_top + hero_h + gap, w, remaining))
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
