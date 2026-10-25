"""Approved Qiaolian C-style editorial covers for three or four photos."""
from __future__ import annotations

import hashlib
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

W, H, GAP, FOOTER = 1080, 1350, 9, 176
GOLD = (231, 190, 116)
BLACK = (20, 23, 25)
CREAM = (244, 239, 229)


def _font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    base = Path(__file__).resolve().parents[2] / "assets" / "fonts"
    path = base / ("NotoSansSC-Bold.otf" if bold else "NotoSansSC-Medium.otf")
    try:
        return ImageFont.truetype(str(path), size)
    except OSError:
        return ImageFont.truetype("DejaVuSans.ttf", size)


def _fit(photo: Image.Image, size: tuple[int, int], *, preserve_tall_exterior: bool = False) -> Image.Image:
    """Crop normally, but never distort or cut the roof off a tall villa hero."""
    photo_width, photo_height = photo.size
    slot_width, slot_height = size
    if preserve_tall_exterior and photo_height > photo_width * 1.18 and slot_width > slot_height * 0.75:
        background = ImageOps.fit(photo, size, method=Image.Resampling.LANCZOS)
        background = background.filter(ImageFilter.GaussianBlur(22))
        foreground = ImageOps.contain(photo, size, method=Image.Resampling.LANCZOS)
        background.paste(foreground, ((slot_width - foreground.width) // 2, 0))
        return background
    return ImageOps.fit(photo, size, method=Image.Resampling.LANCZOS)


def _clip(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, width: int) -> str:
    if draw.textlength(text, font=font) <= width:
        return text
    suffix = "…"
    while text and draw.textlength(text.rstrip() + suffix, font=font) > width:
        text = text[:-1]
    return text.rstrip() + suffix if text else ""


def _scaled_logo(logo: Image.Image, *, max_width: int, max_height: int) -> Image.Image:
    factor = min(max_width / logo.width, max_height / logo.height)
    return logo.resize(
        (max(1, round(logo.width * factor)), max(1, round(logo.height * factor))),
        Image.Resampling.LANCZOS,
    )


def _watermark(logo: Image.Image, *, max_width: int) -> Image.Image:
    """Turn the official lockup into the restrained ivory photo watermark."""
    mark = _scaled_logo(logo, max_width=max_width, max_height=72)
    alpha = mark.getchannel("A").point(lambda value: round(value * 0.78))
    ink = Image.new("RGBA", mark.size, (255, 248, 224, 0))
    ink.putalpha(alpha)
    return ink


def _paste_rounded(canvas: Image.Image, image: Image.Image, box: tuple[int, int, int, int], radius: int = 12) -> None:
    x, y, width, height = box
    mask = Image.new("L", (width, height), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, width, height), radius=radius, fill=255)
    canvas.paste(image, (x, y), mask)


def _pin(size: int = 31) -> Image.Image:
    scale = 4
    icon = Image.new("RGBA", (size * scale, size * scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(icon)
    cx, cy, radius = icon.width // 2, round(icon.height * 0.38), round(icon.width * 0.27)
    draw.polygon(
        ((cx - radius, cy + radius // 2), (cx + radius, cy + radius // 2), (cx, icon.height - 4)),
        fill=(*GOLD, 255),
    )
    draw.ellipse((cx - radius, cy - radius, cx + radius, cy + radius), fill=(*GOLD, 255))
    hole = round(radius * 0.35)
    draw.ellipse((cx - hole, cy - hole, cx + hole, cy + hole), fill=(*BLACK, 255))
    return icon.resize((size, size), Image.Resampling.LANCZOS)


def render_editorial_cover(*, images: list[Image.Image], data, output_path: str, layout: str,
                           logo: Image.Image | None = None, polished: bool = False) -> str:
    """Render exactly three or four distinct approved photos."""
    if len(images) not in {3, 4}:
        raise ValueError("editorial_cover_requires_3_or_4_photos")
    signatures = [hashlib.sha256(image.tobytes()).digest() for image in images]
    if len(set(signatures)) != len(signatures):
        raise ValueError("cover_duplicate_photos")

    footer_height = 132 if polished else FOOTER
    photo_bottom = H - footer_height
    count = len(images)
    # For villas, a generous vertical exterior is the left hero.
    # Premium apartments keep a wide top hero and large detail cells beneath.
    if polished and layout == "ql_optimized_villa":
        left = round(W * .60)
        side_w = W - left - GAP
        each_h = (photo_bottom - (count - 2) * GAP) // (count - 1)
        rectangles = [(0, 0, left, photo_bottom)]
        y = 0
        for i in range(count - 1):
            height = each_h if i < count - 2 else photo_bottom - y
            rectangles.append((left + GAP, y, side_w, height))
            y += height + GAP
    else:
        hero_height = 870 if polished else 826
        rectangles = [(0, 0, W, hero_height)]
        y = hero_height + GAP
        available_width = W - GAP * (count - 2)
        widths = [available_width // (count - 1)] * (count - 1)
        widths[-1] += available_width - sum(widths)
        x = 0
        for width in widths:
            rectangles.append((x, y, width, photo_bottom - y))
            x += width + GAP

    canvas = Image.new("RGB", (W, H), CREAM)
    for index, (image, (x, y, width, height)) in enumerate(zip(images, rectangles)):
        fitted = _fit(
            image,
            (width, height),
            preserve_tall_exterior=index == 0 and layout in {"ql_editorial_side", "ql_optimized_villa"},
        )
        _paste_rounded(canvas, fitted, (x, y, width, height))

        if logo is not None and not polished:
            mark = _watermark(logo, max_width=185 if index == 0 else 128)
            photo_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            photo_layer.alpha_composite(mark, (x + 18, y + 16))
            canvas.paste(photo_layer, (0, 0), photo_layer)

    draw = ImageDraw.Draw(canvas)
    if polished:
        # Keep the photo area quiet and the footer secondary to the property.
        draw.rectangle((0, photo_bottom, W, H), fill=(250, 248, 245))
        title = str(data.project or data.project_alias or data.area or "精选房源").strip()
        subtitle = " · ".join(str(v).strip() for v in (data.property_type, data.layout) if v)
        draw.text((28, photo_bottom + 18), _clip(draw, title, _font(38, True), 455),
                  font=_font(38, True), fill=(42, 42, 39))
        draw.text((30, photo_bottom + 77), _clip(draw, subtitle, _font(24), 450),
                  font=_font(24), fill=(87, 84, 77))
        draw.text((528, photo_bottom + 51), _clip(draw, data.price_line(), _font(48, True), 330),
                  font=_font(48, True), fill=(133, 92, 46))
        if logo is not None:
            mark = _scaled_logo(logo, max_width=160, max_height=43)
            canvas.paste(mark, (W - mark.width - 24, photo_bottom + 53), mark)
        target = Path(output_path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        canvas.save(target, format="JPEG" if target.suffix.lower() in {".jpg", ".jpeg"} else "PNG", quality=94)
        return str(target)
    draw.rounded_rectangle((0, photo_bottom - 12, W, H), radius=14, fill=BLACK)
    draw.rectangle((0, photo_bottom, W, H), fill=BLACK)
    draw.rectangle((0, photo_bottom, W, photo_bottom + 3), fill=GOLD)
    title = str(data.project or data.project_alias or data.area or "精选房源").strip()
    subtitle = " · ".join(
        value for value in (
            str(data.property_type or "").strip(),
            str(data.layout or "").strip(),
        ) if value
    )
    title_font = _font(38, True)
    subtitle_font = _font(25)
    price_label_font = _font(22)
    price_font = _font(50, True)
    pin = _pin(31)
    canvas.paste(pin, (28, photo_bottom + 38), pin)
    draw.text((70, photo_bottom + 28), _clip(draw, title, title_font, 348), font=title_font, fill="white")
    draw.text((70, photo_bottom + 97), _clip(draw, subtitle.replace(" · ", "  |  "), subtitle_font, 350),
              font=subtitle_font, fill=(225, 225, 225))
    draw.line((445, photo_bottom + 32, 445, H - 32), fill=(166, 143, 102), width=2)
    price = _clip(draw, data.price_line(), price_font, 330)
    draw.text((477, photo_bottom + 38), "月租金" if price.endswith("/月") else "售价", font=price_label_font,
              fill=(226, 217, 200))
    draw.text((477, photo_bottom + 76), price, font=price_font, fill=GOLD)

    if logo is not None:
        mark = _scaled_logo(logo, max_width=205, max_height=90)
        canvas.paste(mark, (W - mark.width - 28, photo_bottom + (FOOTER - mark.height) // 2), mark)

    rounded = Image.new("RGB", (W, H), CREAM)
    outer_mask = Image.new("L", (W, H), 0)
    ImageDraw.Draw(outer_mask).rounded_rectangle((0, 0, W - 1, H - 1), radius=18, fill=255)
    rounded.paste(canvas, (0, 0), outer_mask)
    canvas = rounded

    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(target, format="JPEG", quality=94, optimize=True)
    else:
        canvas.save(target, format="PNG", optimize=True)
    return str(target)


def render_wide_polished_cover(*, images: list[Image.Image], data, output_path: str,
                               logo: Image.Image | None = None) -> str:
    """3:2 apartment cover: 64% living-room hero, right supporting rooms."""
    if len(images) not in (3, 4):
        raise ValueError("wide_cover_requires_3_or_4_photos")
    if len({hashlib.sha256(im.tobytes()).digest() for im in images}) != len(images):
        raise ValueError("cover_duplicate_photos")
    width, height, gap, footer = 1200, 800, 6, 95
    photo_h = height - footer
    hero_w = round(width * .64)
    canvas = Image.new("RGB", (width, height), (250, 248, 245))
    boxes = [(0, 0, hero_w, photo_h)]
    y = 0
    thumb_h = (photo_h - gap * (len(images) - 2)) // (len(images) - 1)
    for index in range(len(images)-1):
        h = thumb_h if index < len(images)-2 else photo_h-y
        boxes.append((hero_w+gap, y, width-hero_w-gap, h))
        y += h+gap
    for photo, (x,y,w,h) in zip(images, boxes):
        _paste_rounded(canvas, _fit(photo,(w,h)), (x,y,w,h), radius=9)
    draw = ImageDraw.Draw(canvas)
    title = str(data.project or data.project_alias or data.area or "精选房源").strip()
    subtitle = " · ".join(str(v).strip() for v in (data.property_type,data.layout) if v)
    draw.text((24,photo_h+12),_clip(draw,title,_font(32,True),410),font=_font(32,True),fill=(37,40,41))
    draw.text((25,photo_h+57),_clip(draw,subtitle,_font(20),420),font=_font(20),fill=(84,84,80))
    draw.text((550,photo_h+28),_clip(draw,data.price_line(),_font(40,True),400),font=_font(40,True),fill=(133,92,46))
    if logo is not None:
        mark=_scaled_logo(logo,max_width=145,max_height=35)
        canvas.paste(mark,(width-mark.width-24,photo_h+33),mark)
    target=Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True,exist_ok=True)
    canvas.save(target,format="JPEG" if target.suffix.lower() in {".jpg",".jpeg"} else "PNG",quality=94)
    return str(target)


__all__ = ["render_editorial_cover", "render_wide_polished_cover"]
