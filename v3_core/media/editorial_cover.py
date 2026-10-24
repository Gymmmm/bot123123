"""Approved Qiaolian C-style editorial covers for three or four photos."""
from __future__ import annotations

import hashlib
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

W, H, GAP, FOOTER = 1200, 850, 7, 106
GOLD = (231, 190, 116)
BLACK = (20, 23, 25)


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

    canvas = Image.new("RGB", (W, H), "white")
    for index, (image, (x, y, width, height)) in enumerate(zip(images, rectangles)):
        fitted = _fit(
            image,
            (width, height),
            preserve_tall_exterior=index == 0 and layout == "ql_editorial_side",
        )
        canvas.paste(fitted, (x, y))

    draw = ImageDraw.Draw(canvas)
    draw.rectangle((0, photo_bottom, W, H), fill=BLACK)
    draw.rectangle((0, photo_bottom, W, photo_bottom + 3), fill=GOLD)
    title = str(data.project or data.project_alias or data.area or "精选房源").strip()
    subtitle = " · ".join(
        value for value in (
            str(data.property_type or "").strip(),
            str(data.layout or "").strip(),
        ) if value
    )
    title_font = _font(40, True)
    subtitle_font = _font(23)
    price_font = _font(49, True)
    draw.text((32, photo_bottom + 18), _clip(draw, title, title_font, 405), font=title_font, fill="white")
    draw.text((34, photo_bottom + 67), _clip(draw, subtitle, subtitle_font, 385),
              font=subtitle_font, fill=(225, 225, 225))
    price = _clip(draw, data.price_line(), price_font, 395)
    draw.text((475, photo_bottom + 24), price, font=price_font, fill=GOLD)

    if logo is not None:
        max_width, max_height = 258, 73
        factor = min(max_width / logo.width, max_height / logo.height)
        mark = logo.resize(
            (max(1, int(logo.width * factor)), max(1, int(logo.height * factor))),
            Image.Resampling.LANCZOS,
        )
        backing = Image.new("RGBA", (mark.width + 16, mark.height + 12), (255, 255, 255, 245))
        backing.alpha_composite(mark, (8, 6))
        canvas.paste(backing.convert("RGB"), (W - backing.width - 24, photo_bottom + 13))

    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(target, format="JPEG", quality=94, optimize=True)
    else:
        canvas.save(target, format="PNG", optimize=True)
    return str(target)


__all__ = ["render_editorial_cover"]
