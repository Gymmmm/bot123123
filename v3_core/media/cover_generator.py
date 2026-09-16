"""Fast Pillow-based cover generator for V3 Publisher.

This is the production cover path for channel listings. It intentionally avoids
HTML/Playwright and renders directly from resolved V3 listing facts.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


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
    """Load the Simplified-Chinese face from a TTC instead of relying on index 0."""
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


def _rounded_card_with_shadow(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    bbox: list[int],
    *,
    radius: int,
    fill: tuple[int, int, int, int],
) -> None:
    x1, y1, x2, y2 = bbox
    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    for offset, alpha in ((6, 15), (4, 25), (2, 35)):
        shadow_draw.rounded_rectangle(
            [x1, y1 + offset, x2, y2 + offset],
            radius=radius,
            fill=(0, 0, 0, alpha),
        )
    canvas.alpha_composite(shadow)
    draw.rounded_rectangle(bbox, radius=radius, fill=fill, outline=(255, 255, 255, 180), width=2)


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


def generate_cover(
    *,
    style: str = "pillow_v1",
    source_image: str,
    output_path: str,
    data: CoverRenderData,
) -> str:
    """Generate one 1200x900 listing cover using Pillow only."""
    del style  # retained only for the existing service/package contract
    source = Path(source_image).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source}")
    output.parent.mkdir(parents=True, exist_ok=True)

    target_width, target_height = 1200, 900
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
    bg = bg.crop((left, top, left + target_width, top + target_height))

    mask = Image.new("RGBA", (target_width, target_height), (0, 0, 0, 0))
    mask_draw = ImageDraw.Draw(mask)
    gradient_w = int(target_width * 0.48)
    for x in range(gradient_w):
        progress = x / gradient_w
        alpha = int(220 * (1 - math.pow(progress, 1.3)))
        mask_draw.line([(x, 0), (x, target_height)], fill=(5, 12, 24, alpha))

    canvas = Image.alpha_composite(bg, mask)
    draw = ImageDraw.Draw(canvas)

    font_brand = _font(36, bold=True)
    font_brand_sub = _font(18)
    font_title = _font(76, bold=True)
    font_tag = _font(32, bold=True)
    font_location = _font(32)
    font_price_label = _font(24)
    font_price_value = _font(60, bold=True)

    x_offset = 60
    curr_y = 50
    draw.line([(x_offset, curr_y + 4), (x_offset, curr_y + 54)], fill="white", width=4)
    logo_x = x_offset + 20
    draw.text((logo_x, curr_y), "侨联地产", fill="white", font=font_brand)
    draw.text((logo_x, curr_y + 42), "QIAO LIAN", fill=(210, 215, 225), font=font_brand_sub)

    title = str(data.project or data.property_type or "优质房源").strip()
    tag = str(data.layout or data.property_type or "房源").strip()
    location = str(data.area or "位置待确认").strip()
    price = _cover_price(data)

    curr_y = 260
    draw.text((x_offset, curr_y), title, fill="white", font=font_title)
    title_bbox = font_title.getbbox(title)
    curr_y += (title_bbox[3] - title_bbox[1]) + 30

    tag_bbox = font_tag.getbbox(tag)
    tag_w = tag_bbox[2] - tag_bbox[0] + 44
    tag_h = 52
    draw.rounded_rectangle(
        [x_offset, curr_y, x_offset + tag_w, curr_y + tag_h],
        radius=26,
        fill="white",
    )
    text_w = tag_bbox[2] - tag_bbox[0]
    draw.text((x_offset + (tag_w - text_w) // 2, curr_y + 8), tag, fill="#111827", font=font_tag)
    curr_y += tag_h + 35
    draw.text((x_offset, curr_y), f"📍  {location}", fill="white", font=font_location)

    card_w, card_h = 360, 160
    card_x2 = target_width - 50
    card_y2 = target_height - 50
    card_x1 = card_x2 - card_w
    card_y1 = card_y2 - card_h
    _rounded_card_with_shadow(
        canvas,
        draw,
        [card_x1, card_y1, card_x2, card_y2],
        radius=24,
        fill=(252, 253, 255, 248),
    )

    price_label = "租金" if str(data.deal_type or "rent").lower() == "rent" else "售价"
    label_bbox = font_price_label.getbbox(price_label)
    label_w = label_bbox[2] - label_bbox[0]
    draw.text(
        (card_x1 + (card_w - label_w) // 2, card_y1 + 24),
        price_label,
        fill="#64748B",
        font=font_price_label,
    )
    price_bbox = font_price_value.getbbox(price)
    price_w = price_bbox[2] - price_bbox[0]
    draw.text(
        (card_x1 + (card_w - price_w) // 2, card_y1 + 64),
        price,
        fill="#0F4C81",
        font=font_price_value,
    )

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
    brand_title: str = "侨联地产",
    brand_sub: str = "QIAO LIAN",
) -> str:
    """Compatibility wrapper matching the standalone generator supplied by the owner."""
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


__all__ = ["CoverRenderData", "generate_cover", "generate_property_cover"]
