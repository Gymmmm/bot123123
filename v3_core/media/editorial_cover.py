"""Approved Qiaolian C-style editorial covers for three or four photos."""
from __future__ import annotations

import hashlib
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

W, H, GAP, FOOTER = 1200, 850, 7, 106
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
                           logo: Image.Image | None = None) -> str:
    """Render exactly three or four distinct approved photos."""
    if len(images) not in {3, 4}:
        raise ValueError("editorial_cover_requires_3_or_4_photos")
    signatures = [hashlib.sha256(image.tobytes()).digest() for image in images]
    if len(set(signatures)) != len(signatures):
        raise ValueError("cover_duplicate_photos")

    photo_bottom = H - FOOTER
    count = len(images)
    if layout == "ql_editorial_side":
        hero_width = 752
        rectangles = [(0, 0, hero_width, photo_bottom)]
        x = hero_width + GAP
        available_height = photo_bottom - GAP * (count - 2)
        heights = [available_height // (count - 1)] * (count - 1)
        heights[-1] += available_height - sum(heights)
        y = 0
        for height in heights:
            rectangles.append((x, y, W - x, height))
            y += height + GAP
    else:
        hero_height = round(photo_bottom * 0.665)
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
            preserve_tall_exterior=index == 0 and layout == "ql_editorial_side",
        )
        _paste_rounded(canvas, fitted, (x, y, width, height))

        if logo is not None:
            mark = _watermark(logo, max_width=185 if index == 0 else 128)
            photo_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            photo_layer.alpha_composite(mark, (x + 18, y + 16))
            canvas.paste(photo_layer, (0, 0), photo_layer)

    draw = ImageDraw.Draw(canvas)
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
    title_font = _font(36, True)
    subtitle_font = _font(22)
    price_label_font = _font(16)
    price_font = _font(45, True)
    pin = _pin(31)
    canvas.paste(pin, (29, photo_bottom + 22), pin)
    draw.text((70, photo_bottom + 15), _clip(draw, title, title_font, 350), font=title_font, fill="white")
    draw.text((70, photo_bottom + 62), _clip(draw, subtitle.replace(" · ", "  |  "), subtitle_font, 350),
              font=subtitle_font, fill=(225, 225, 225))
    draw.line((452, photo_bottom + 18, 452, H - 18), fill=(166, 143, 102), width=2)
    price = _clip(draw, data.price_line(), price_font, 395)
    draw.text((493, photo_bottom + 14), "月租金" if price.endswith("/月") else "售价", font=price_label_font,
              fill=(226, 217, 200))
    draw.text((493, photo_bottom + 36), price, font=price_font, fill=GOLD)

    if logo is not None:
        mark = _scaled_logo(logo, max_width=270, max_height=72)
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


__all__ = ["render_editorial_cover"]
