"""Fast Pillow-based cover generator for V3 Publisher.

The three production styles share one restrained brand system while keeping
three distinct layout languages. Rendering is local Pillow-only and consumes
resolved V3 listing facts without changing publication semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import os
from pathlib import Path
import re

from PIL import Image, ImageDraw, ImageFont


BRAND_CN = "侨联地产"
BRAND_EN = "QIAOLIAN REALTY"
BRAND_BLUE = (18, 57, 103)
BRAND_BLUE_SOFT = (40, 89, 142)
BRAND_GOLD = (211, 179, 98)
BRAND_GOLD_SOFT = (236, 214, 156)
WARM_WHITE = (249, 247, 240)
TEXT_LIGHT = (231, 236, 243)
TEXT_MUTED = (182, 194, 208)

STYLE_LAYOUTS = {
    "classic_blue": {
        "size": (1200, 900),
        "safe": 48,
        "brand_box": (48, 42, 324, 118),
        "card": (48, 642, 1152, 858),
        "card_radius": 18,
    },
    "right_price": {
        "size": (1200, 900),
        "safe": 48,
        "gradient_width": 520,
        "price_card": (850, 722, 1156, 856),
        "price_radius": 18,
    },
    "black_gold": {
        "size": (1280, 720),
        "safe": 44,
        "mask_width": 520,
        "price_card": (938, 574, 1236, 674),
        "price_radius": 12,
    },
}


@dataclass(frozen=True)
class CoverRenderData:
    public_listing_id: str
    project: str = ""
    project_alias: str = ""
    property_type: str = ""
    deal_type: str = "rent"
    layout: str = ""
    area: str = ""
    size: str = ""
    floor: str = ""
    price: str = ""
    highlight_1: str = ""
    highlight_2: str = ""
    highlight_3: str = ""


def _font_path(*, bold: bool) -> str:
    env_key = "QIAOLIAN_COVER_FONT_BOLD" if bold else "QIAOLIAN_COVER_FONT_REGULAR"
    configured = str(os.getenv(env_key) or "").strip()
    candidates = [
        configured,
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if bold else "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for value in candidates:
        if value and Path(value).is_file():
            return value
    raise FileNotFoundError("cover_generator_cjk_font_not_found")


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Load Simplified Chinese Noto CJK when available, never emoji fonts."""
    path = _font_path(bold=bold)
    fallback: ImageFont.FreeTypeFont | None = None
    for index in range(32):
        try:
            font = ImageFont.truetype(path, size, index=index)
        except OSError:
            break
        try:
            family, style = font.getname()
        except Exception:
            family, style = "", ""
        family_l = str(family).lower()
        style_l = str(style).lower()
        if fallback is None and "cjk sc" in family_l:
            fallback = font
        if "noto sans cjk sc" in family_l and "mono" not in family_l:
            if bold and "bold" not in style_l:
                continue
            return font
    if fallback is not None:
        return fallback
    return ImageFont.truetype(path, size)


def _text_width(font: ImageFont.FreeTypeFont, text: str) -> int:
    bbox = font.getbbox(str(text or ""))
    return max(0, int(bbox[2] - bbox[0]))


def _fit_font(text: str, *, max_width: int, preferred: int, minimum: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Shrink one single-line label just enough to stay inside its safe area."""
    value = str(text or "")
    for size in range(preferred, minimum - 1, -2):
        font = _font(size, bold=bold)
        if _text_width(font, value) <= max_width:
            return font
    return _font(minimum, bold=bold)


def _format_layout(value: str) -> str:
    """Visual-only layout normalization: 3房1厅2卫 -> 3房1厅｜2卫."""
    text = re.sub(r"\s+", "", str(value or "").strip())
    if "｜" not in text:
        match = re.search(r"(\d+卫)$", text)
        if match and match.start() > 0:
            text = f"{text[:match.start()]}｜{match.group(1)}"
    return text


def _clean_cover_location(value: object) -> str:
    """Return plain location text; never substitute a project name for GEO."""
    text = str(value or "").strip()
    while text and not (text[0].isalnum() or "\u4e00" <= text[0] <= "\u9fff"):
        text = text[1:].lstrip()
    return text or "金边"


def _cover_price(data: CoverRenderData) -> str:
    raw = str(data.price or "").strip()
    if not raw:
        return "价格待确认"
    if raw in {"售价面议", "租金面议", "价格面议", "面议"}:
        return raw
    if not raw.startswith("$"):
        raw = f"${raw}"
    if str(data.deal_type or "rent").lower() == "rent" and "/月" not in raw:
        raw += "/月"
    return raw


def _cover_crop(source: Path, size: tuple[int, int]) -> Image.Image:
    target_width, target_height = size
    bg = Image.open(source).convert("RGBA")
    bg_ratio = bg.width / bg.height
    target_ratio = target_width / target_height
    if bg_ratio > target_ratio:
        new_height = target_height
        new_width = int(new_height * bg_ratio)
    else:
        new_width = target_width
        new_height = int(new_width / bg_ratio)
    bg = bg.resize((new_width, new_height), Image.Resampling.LANCZOS)
    left = (new_width - target_width) // 2
    top = (new_height - target_height) // 2
    return bg.crop((left, top, left + target_width, top + target_height))


def _rounded_card_with_shadow(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    bbox: tuple[int, int, int, int],
    *,
    radius: int,
    fill: tuple[int, int, int, int],
) -> None:
    x1, y1, x2, y2 = bbox
    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    for offset, alpha in ((5, 12), (3, 20), (1, 28)):
        shadow_draw.rounded_rectangle([x1, y1 + offset, x2, y2 + offset], radius=radius, fill=(0, 0, 0, alpha))
    canvas.alpha_composite(shadow)
    draw.rounded_rectangle(bbox, radius=radius, fill=fill)


def _draw_brand(
    draw: ImageDraw.ImageDraw,
    *,
    x: int,
    y: int,
    accent: tuple[int, int, int],
    cn_fill: tuple[int, int, int] = (255, 255, 255),
    en_fill: tuple[int, int, int] | None = None,
    compact: bool = False,
) -> None:
    en_fill = en_fill or accent
    bar_h = 34 if compact else 40
    draw.rounded_rectangle([x, y + 2, x + 3, y + 2 + bar_h], radius=2, fill=accent)
    cn_size = 20 if compact else 24
    en_size = 10 if compact else 11
    draw.text((x + 16, y), BRAND_CN, fill=cn_fill, font=_font(cn_size, bold=True))
    draw.text((x + 16, y + cn_size + 7), BRAND_EN, fill=en_fill, font=_font(en_size, bold=True))


def _render_classic_blue(bg: Image.Image, *, title: str, layout: str, location: str, price: str) -> Image.Image:
    cfg = STYLE_LAYOUTS["classic_blue"]
    canvas = bg.copy()
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    bx1, by1, bx2, by2 = cfg["brand_box"]
    od.rounded_rectangle([bx1, by1, bx2, by2], radius=14, fill=(10, 39, 80, 210))
    cx1, cy1, cx2, cy2 = cfg["card"]
    od.rounded_rectangle([cx1, cy1, cx2, cy2], radius=cfg["card_radius"], fill=(11, 46, 91, 207))
    od.rectangle([cx1 + 18, cy1, cx2 - 18, cy1 + 2], fill=(*BRAND_GOLD, 180))
    canvas = Image.alpha_composite(canvas, overlay)
    draw = ImageDraw.Draw(canvas)

    _draw_brand(draw, x=66, y=57, accent=BRAND_GOLD, en_fill=(218, 228, 240), compact=True)

    title_font = _fit_font(title, max_width=620, preferred=50, minimum=34, bold=True)
    layout_font = _fit_font(layout, max_width=610, preferred=29, minimum=22, bold=True)
    location_font = _fit_font(location, max_width=610, preferred=21, minimum=17)
    price_font = _fit_font(price, max_width=340, preferred=50, minimum=34, bold=True)

    draw.text((72, 669), title, fill=WARM_WHITE, font=title_font)
    draw.text((72, 729), layout, fill=TEXT_LIGHT, font=layout_font)
    draw.text((72, 779), location, fill=TEXT_MUTED, font=location_font)

    price_width = _text_width(price_font, price)
    price_x = 1118 - price_width
    draw.text((price_x, 711), price, fill=BRAND_GOLD_SOFT, font=price_font)
    return canvas


def _render_black_gold(bg: Image.Image, *, title: str, layout: str, location: str, price: str, price_label: str) -> Image.Image:
    cfg = STYLE_LAYOUTS["black_gold"]
    width, height = cfg["size"]
    canvas = bg.copy()
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)

    mask_width = cfg["mask_width"]
    for x in range(mask_width):
        progress = x / max(1, mask_width - 1)
        alpha = int(210 * math.pow(1 - progress, 1.35))
        od.line([(x, 0), (x, height)], fill=(9, 9, 10, alpha))
    for y in range(110):
        alpha = int(78 * (1 - y / 109))
        od.line([(0, y), (width, y)], fill=(0, 0, 0, alpha))

    canvas = Image.alpha_composite(canvas, overlay)
    draw = ImageDraw.Draw(canvas)
    _draw_brand(draw, x=44, y=38, accent=BRAND_GOLD, en_fill=BRAND_GOLD, compact=True)

    title_font = _fit_font(title, max_width=500, preferred=54, minimum=34, bold=True)
    layout_font = _fit_font(layout, max_width=500, preferred=25, minimum=19)
    location_font = _fit_font(location, max_width=500, preferred=18, minimum=15)

    draw.text((44, 438), title, fill=WARM_WHITE, font=title_font)
    draw.text((44, 504), layout, fill=TEXT_LIGHT, font=layout_font)
    draw.line([(44, 544), (86, 544)], fill=BRAND_GOLD, width=1)
    draw.text((44, 555), location, fill=BRAND_GOLD_SOFT, font=location_font)

    x1, y1, x2, y2 = cfg["price_card"]
    draw.rounded_rectangle([x1, y1, x2, y2], radius=cfg["price_radius"], fill=(12, 12, 13, 205), outline=BRAND_GOLD, width=2)
    label_font = _font(14, bold=True)
    value_font = _fit_font(price, max_width=(x2 - x1) - 30, preferred=38, minimum=28, bold=True)
    draw.text((x1 + 18, y1 + 14), price_label, fill=(185, 167, 125), font=label_font)
    value_width = _text_width(value_font, price)
    draw.text((x2 - 18 - value_width, y1 + 42), price, fill=BRAND_GOLD, font=value_font)
    return canvas


def _render_right_price(bg: Image.Image, *, title: str, layout: str, location: str, price: str, price_label: str) -> Image.Image:
    cfg = STYLE_LAYOUTS["right_price"]
    width, height = cfg["size"]
    mask = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    md = ImageDraw.Draw(mask)
    gradient_w = cfg["gradient_width"]
    for x in range(gradient_w):
        progress = x / max(1, gradient_w - 1)
        alpha = int(180 * math.pow(1 - progress, 1.45))
        md.line([(x, 0), (x, height)], fill=(7, 16, 31, alpha))
    canvas = Image.alpha_composite(bg, mask)
    draw = ImageDraw.Draw(canvas)

    _draw_brand(draw, x=52, y=46, accent=BRAND_BLUE_SOFT, en_fill=(220, 229, 240), compact=False)

    title_font = _fit_font(title, max_width=530, preferred=68, minimum=42, bold=True)
    draw.text((54, 282), title, fill="white", font=title_font)
    title_box = title_font.getbbox(title)
    title_h = max(1, title_box[3] - title_box[1])

    pill_y = 282 + title_h + 26
    layout_font = _fit_font(layout, max_width=450, preferred=27, minimum=20, bold=True)
    layout_w = _text_width(layout_font, layout)
    pill_w = min(500, layout_w + 36)
    draw.rounded_rectangle([54, pill_y, 54 + pill_w, pill_y + 44], radius=18, fill=(255, 255, 255, 238))
    draw.text((72, pill_y + 7), layout, fill=(26, 39, 57), font=layout_font)

    location_font = _fit_font(location, max_width=500, preferred=23, minimum=18)
    draw.text((54, pill_y + 72), location, fill=TEXT_LIGHT, font=location_font)

    x1, y1, x2, y2 = cfg["price_card"]
    _rounded_card_with_shadow(canvas, draw, (x1, y1, x2, y2), radius=cfg["price_radius"], fill=(253, 254, 255, 246))
    label_font = _font(17)
    value_font = _fit_font(price, max_width=(x2 - x1) - 34, preferred=46, minimum=32, bold=True)
    label_width = _text_width(label_font, price_label)
    value_width = _text_width(value_font, price)
    draw.text((x1 + (x2 - x1 - label_width) // 2, y1 + 18), price_label, fill=(120, 137, 157), font=label_font)
    draw.text((x1 + (x2 - x1 - value_width) // 2, y1 + 55), price, fill=BRAND_BLUE, font=value_font)
    return canvas


def generate_cover(
    *,
    style: str = "pillow_v1",
    source_image: str,
    output_path: str,
    data: CoverRenderData,
) -> str:
    """Generate one production listing cover with the style's existing ratio."""
    style = str(style or "right_price").strip().lower()
    if style not in STYLE_LAYOUTS:
        style = "right_price"
    source = Path(source_image).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source}")
    output.parent.mkdir(parents=True, exist_ok=True)

    bg = _cover_crop(source, STYLE_LAYOUTS[style]["size"])
    title = str(data.project or data.property_type or "优质房源").strip()
    layout = _format_layout(data.layout or data.property_type or "房源")
    location = _clean_cover_location(data.area)
    price = _cover_price(data)
    price_label = "租金" if str(data.deal_type or "rent").lower() == "rent" else "售价"

    if style == "classic_blue":
        canvas = _render_classic_blue(bg, title=title, layout=layout, location=location, price=price)
    elif style == "black_gold":
        canvas = _render_black_gold(bg, title=title, layout=layout, location=location, price=price, price_label=price_label)
    else:
        canvas = _render_right_price(bg, title=title, layout=layout, location=location, price=price, price_label=price_label)

    if output.suffix.lower() not in {".jpg", ".jpeg"}:
        output = output.with_suffix(".jpg")
    canvas.convert("RGB").save(output, "JPEG", quality=95, optimize=True)
    return str(output)


def generate_property_cover(
    bg_path: str,
    output_path: str,
    *,
    title: str = "桥牌",
    tag: str = "单间",
    location: str = "金街附近",
    price: str = "$300/月",
    price_label: str = "租金",
    brand_title: str = BRAND_CN,
    brand_sub: str = BRAND_EN,
) -> str:
    """Compatibility wrapper for the historical standalone generator."""
    del price_label, brand_title, brand_sub
    raw_price = str(price or "").replace("/月", "").replace("$", "").strip()
    return generate_cover(
        source_image=bg_path,
        output_path=output_path,
        data=CoverRenderData(
            public_listing_id="manual",
            project=title,
            layout=tag,
            area=location,
            price=raw_price,
            deal_type="rent",
        ),
    )


__all__ = [
    "BRAND_CN",
    "BRAND_EN",
    "STYLE_LAYOUTS",
    "CoverRenderData",
    "generate_cover",
    "generate_property_cover",
]
