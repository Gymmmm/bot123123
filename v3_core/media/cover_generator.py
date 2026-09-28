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
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc" if bold else "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
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
    """Generate one listing cover using Pillow only.

    Landscape image styles render at 1080×864. Portrait / video style keys
    fall back to their visual family on the landscape canvas so callers never
    crash; production HTML renderer owns true portrait/video canvases.
    """
    from .cover_styles import cover_style_family, normalize_cover_style

    style = cover_style_family(normalize_cover_style(style, allow_video=True))
    if style not in {"classic_blue", "right_price", "black_gold", "premium_photo"}:
        style = "right_price"
    source = Path(source_image).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"cover_source_not_found:{source}")
    output.parent.mkdir(parents=True, exist_ok=True)

    target_width, target_height = 1080, 864
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

    title = str(data.project or data.property_type or "优质房源").strip()
    tag = str(data.layout or data.property_type or "房源").strip()
    location = str(data.area or "位置待确认").strip()
    price = _cover_price(data)
    price_label = "租金" if str(data.deal_type or "rent").lower() == "rent" else "售价"

    if style == "classic_blue":
        canvas = bg.copy()
        overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        od.rectangle([0, 0, 430, 150], fill=(10, 47, 122, 238))
        od.rounded_rectangle([28, 590, 860, 870], radius=28, fill=(17, 71, 178, 232))
        canvas = Image.alpha_composite(canvas, overlay)
        draw = ImageDraw.Draw(canvas)
        draw.text((34, 28), "侨联地产", fill="white", font=_font(48, bold=True))
        draw.text((36, 88), "QIAO LIAN PROPERTY", fill=(230, 238, 255), font=_font(20))
        draw.text((58, 620), title, fill="white", font=_font(62, bold=True))
        draw.text((58, 706), tag, fill=(236, 244, 255), font=_font(34, bold=True))
        draw.text((58, 770), location, fill=(236, 244, 255), font=_font(28))
        draw.text((500, 745), price, fill=(246, 201, 72), font=_font(58, bold=True))
    elif style == "black_gold":
        shade = Image.new("RGBA", (target_width, target_height), (0, 0, 0, 0))
        sd = ImageDraw.Draw(shade)
        for x in range(int(target_width * 0.58)):
            progress = x / max(1, int(target_width * 0.58))
            alpha = int(225 * (1 - progress))
            sd.line([(x, 0), (x, target_height)], fill=(4, 4, 4, alpha))
        canvas = Image.alpha_composite(bg, shade)
        draw = ImageDraw.Draw(canvas)
        gold=(242, 207, 121)
        draw.text((48, 42), "侨联地产", fill=gold, font=_font(44, bold=True))
        draw.text((50, 96), "QIAO LIAN", fill=(219, 194, 142), font=_font(18))
        draw.text((48, 280), title, fill=(255, 248, 232), font=_font(72, bold=True))
        tag_box=[48, 390, 48+max(190, len(tag)*54), 458]
        draw.rounded_rectangle(tag_box, radius=34, fill=(24,24,24,230), outline=gold, width=3)
        draw.text((70, 400), tag, fill=gold, font=_font(36, bold=True))
        draw.text((48, 510), f"区域 · {location}", fill=(245,233,204), font=_font(30, bold=True))
        card=[780, 680, 1150, 850]
        draw.rounded_rectangle(card, radius=28, fill=(14,14,14,225), outline=gold, width=3)
        draw.text((805, 704), price_label, fill=(219,194,142), font=_font(26, bold=True))
        draw.text((805, 752), price, fill=gold, font=_font(50, bold=True))
    elif style == "premium_photo":
        # Photo-first: soft bottom gradient + white type (matches HTML 13_ template).
        shade = Image.new("RGBA", (target_width, target_height), (0, 0, 0, 0))
        sd = ImageDraw.Draw(shade)
        band = int(target_height * 0.42)
        for y in range(band):
            progress = y / max(1, band - 1)
            alpha = int(200 * (progress ** 1.6))
            sd.line([(0, target_height - band + y), (target_width, target_height - band + y)], fill=(0, 0, 0, alpha))
        for y in range(int(target_height * 0.22)):
            progress = 1 - (y / max(1, int(target_height * 0.22) - 1))
            alpha = int(90 * (progress ** 1.4))
            sd.line([(0, y), (int(target_width * 0.55), y)], fill=(0, 0, 0, alpha))
        canvas = Image.alpha_composite(bg, shade)
        draw = ImageDraw.Draw(canvas)
        gold = (244, 221, 176)
        draw.text((48, 40), "侨联地产", fill=gold, font=_font(44, bold=True))
        draw.text((50, 94), "QIAO LIAN", fill=(198, 154, 67), font=_font(16, bold=True))
        meta_bits = [p for p in (location, str(data.size or "").strip(), str(data.floor or "").strip()) if p]
        if meta_bits:
            draw.text((48, target_height - 150), "  ·  ".join(meta_bits), fill=(235, 235, 235), font=_font(24))
        highlights = " · ".join(
            part for part in (str(data.highlight_1 or "").strip(), str(data.highlight_2 or "").strip(), str(data.highlight_3 or "").strip()) if part
        )
        if highlights:
            draw.text((48, target_height - 110), highlights, fill=(235, 235, 235), font=_font(22))
        right_title = title if not tag else f"{title} · {tag}"
        title_font = _font(40, bold=True)
        price_font = _font(64, bold=True)
        tb = title_font.getbbox(right_title)
        pb = price_font.getbbox(price)
        draw.text((target_width - 52 - (tb[2] - tb[0]), target_height - 150), right_title, fill=(245, 245, 245), font=title_font)
        draw.text((target_width - 52 - (pb[2] - pb[0]), target_height - 100), price, fill="white", font=price_font)
    else:
        mask = Image.new("RGBA", (target_width, target_height), (0, 0, 0, 0))
        mask_draw = ImageDraw.Draw(mask)
        gradient_w = int(target_width * 0.48)
        for x in range(gradient_w):
            progress = x / gradient_w
            alpha = int(220 * (1 - math.pow(progress, 1.3)))
            mask_draw.line([(x, 0), (x, target_height)], fill=(5, 12, 24, alpha))
        canvas = Image.alpha_composite(bg, mask)
        draw = ImageDraw.Draw(canvas)
        x_offset = 60
        curr_y = 50
        draw.line([(x_offset, curr_y + 4), (x_offset, curr_y + 54)], fill="white", width=4)
        logo_x = x_offset + 20
        draw.text((logo_x, curr_y), "侨联地产", fill="white", font=_font(36, bold=True))
        draw.text((logo_x, curr_y + 42), "QIAO LIAN", fill=(210, 215, 225), font=_font(18))
        curr_y = 260
        title_font = _font(76, bold=True)
        draw.text((x_offset, curr_y), title, fill="white", font=title_font)
        title_bbox = title_font.getbbox(title)
        curr_y += (title_bbox[3] - title_bbox[1]) + 30
        tag_font = _font(32, bold=True)
        tag_bbox = tag_font.getbbox(tag)
        tag_w = tag_bbox[2] - tag_bbox[0] + 44
        draw.rounded_rectangle([x_offset, curr_y, x_offset + tag_w, curr_y + 52], radius=26, fill="white")
        draw.text((x_offset + 22, curr_y + 8), tag, fill="#111827", font=tag_font)
        curr_y += 87
        draw.text((x_offset, curr_y), f"📍  {location}", fill="white", font=_font(32))
        card_w, card_h = 360, 160
        card_x2, card_y2 = target_width - 50, target_height - 50
        card_x1, card_y1 = card_x2 - card_w, card_y2 - card_h
        _rounded_card_with_shadow(canvas, draw, [card_x1, card_y1, card_x2, card_y2], radius=24, fill=(252, 253, 255, 248))
        label_font = _font(24)
        value_font = _font(60, bold=True)
        label_bbox = label_font.getbbox(price_label)
        draw.text((card_x1 + (card_w - (label_bbox[2]-label_bbox[0])) // 2, card_y1 + 24), price_label, fill="#64748B", font=label_font)
        price_bbox = value_font.getbbox(price)
        draw.text((card_x1 + (card_w - (price_bbox[2]-price_bbox[0])) // 2, card_y1 + 64), price, fill="#0F4C81", font=value_font)

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
