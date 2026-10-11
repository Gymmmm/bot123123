"""Villa cover in Gym's black-gold reference layout (2026-10-11, "一号路炳发 $5,000").

Geometry measured from the 1080x1388 reference (≈ 4:5.1):

* hero exterior   y 0–700 (photo fades into the bar from y 560)
* black-gold bar  y 700–868: 📍 title + thin gold rule + "类型 | N房 | 押付·租期" left,
  gold-outlined price card (560,708)–(1044,845) right ("租金 / RENT", big $amount, /月)
* thumbnails      y 868–1252: three 346x384 rounded (r=18) photos at x 14 / 367 / 720 on
                  a light background, room tags only when the label is confident
* footer          y 1252–1388 white: 侨联地产 / QIAO LIAN PROPERTY gold house lockup,
                  thin gold line in the middle, 「出租房源」 pill right

Only real fields are drawn: an empty type / room count / terms is omitted (no guesses).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import TYPE_CHECKING, Iterable, Sequence

from PIL import Image, ImageDraw, ImageFilter, ImageOps

from .hero_layout import frame_offset

if TYPE_CHECKING:  # pragma: no cover
    from .cover_renderer import CoverRenderData


@dataclass(frozen=True)
class VillaCardGeometry:
    canvas: tuple[int, int] = (1080, 1388)
    hero_height: int = 700           # photo area (bar starts here)
    fade_start: int = 560            # photo → bar gradient
    bar_bottom: int = 868
    thumb_top: int = 868
    thumb_bottom: int = 1252
    thumb_x: tuple[int, ...] = (14, 367, 720)
    thumb_width: int = 346
    thumb_radius: int = 18
    margin: int = 36
    title_size: int = 62
    info_size: int = 33
    price_card: tuple[int, int, int, int] = (560, 708, 1044, 845)
    price_size: int = 86
    footer_top: int = 1252


GEOMETRY = VillaCardGeometry()
BAR_BG = (22, 17, 13)
CARD_BG = (15, 11, 8)
GOLD = (212, 183, 107)
GOLD_TOP = (236, 214, 156)
GOLD_BOTTOM = (184, 143, 63)
GOLD_RULE = (200, 168, 108)
LABEL_GOLD = (187, 171, 137)
LIGHT_BG = (252, 251, 248)
FOOTER_BG = (254, 254, 253)
PILL_BG = (240, 226, 197)
INK = (34, 27, 20)
HOUSE_GOLD = (194, 156, 88)
LOCKUP_SUB = (96, 88, 78)


def rooms_text(layout: str) -> str:
    """'4+2房5卫' → '4+2房', '5房6卫' → '5房', '3房1厅｜2卫' → '3房'. Empty when no room count."""
    value = re.sub(r"\s+", "", str(layout or "")).replace("＋", "+")
    match = re.match(r"^(\d+(?:\+\d+)?)房", value)
    if match:
        return f"{match.group(1)}房"
    return "单间" if value in {"单间", "studio", "开间"} else ""


def info_parts(data: "CoverRenderData") -> list[str]:
    parts = [str(data.property_type or "").strip(), rooms_text(data.layout), str(getattr(data, "terms", "") or "").strip()]
    return [p for p in parts if p]


def _gradient_text(size: tuple[int, int], mask: Image.Image, top: tuple[int, int, int], bottom: tuple[int, int, int]) -> Image.Image:
    w, h = size
    grad = Image.new("RGBA", (1, h))
    for y in range(h):
        t = y / max(1, h - 1)
        grad.putpixel((0, y), tuple(int(top[i] + (bottom[i] - top[i]) * t) for i in range(3)) + (255,))
    grad = grad.resize((w, h))
    grad.putalpha(mask)
    return grad


def _rounded_mask(size: tuple[int, int], radius: int) -> Image.Image:
    scale = 4
    big = Image.new("L", (size[0] * scale, size[1] * scale), 0)
    ImageDraw.Draw(big).rounded_rectangle((0, 0, big.width - 1, big.height - 1), radius=radius * scale, fill=255)
    return big.resize(size, Image.Resampling.LANCZOS)


def gold_lockup(height: int) -> Image.Image | None:
    """Our 侨联地产 / QIAO LIAN PROPERTY house lockup, recoloured for a white footer:
    gold house, dark Chinese wordmark, warm-grey English line (same shapes as the asset)."""
    path = Path(__file__).resolve().parents[2] / "media_pipeline" / "assets" / "qiaolian_logo_black_gold_lockup.png"
    try:
        with Image.open(path) as raw:
            alpha = raw.convert("RGBA").getchannel("A")
    except (OSError, ValueError):
        return None
    bbox = alpha.getbbox()
    if not bbox:
        return None
    alpha = alpha.crop(bbox)
    w, h = alpha.size
    colour = Image.new("RGBA", (w, h), (*INK, 255))
    house_right = int(w * 0.255)          # house mark ≈ x 0–220 of 967
    english_top = int(h * 0.80)           # English line ≈ y 212–259 of 259
    draw = ImageDraw.Draw(colour)
    draw.rectangle((0, 0, house_right, h), fill=(*HOUSE_GOLD, 255))
    draw.rectangle((house_right + 1, english_top, w, h), fill=(*LOCKUP_SUB, 255))
    colour.putalpha(alpha)
    width = max(1, round(w * height / h))
    return colour.resize((width, height), Image.Resampling.LANCZOS)


def thumb_columns(count: int, g: VillaCardGeometry = GEOMETRY) -> list[tuple[int, int]]:
    """(x, width) per thumbnail: the reference 14/367/720 x 346 for three, evenly re-split for fewer."""
    if count >= len(g.thumb_x):
        return [(x, g.thumb_width) for x in g.thumb_x]
    if count <= 0:
        return []
    left, right, gap = g.thumb_x[0], g.thumb_x[-1] + g.thumb_width, g.thumb_x[1] - g.thumb_x[0] - g.thumb_width
    width = (right - left - gap * (count - 1)) // count
    return [(left + i * (width + gap), width) for i in range(count)]


def _ink_box(draw: ImageDraw.ImageDraw, text: str, font) -> tuple[int, int, int, int]:
    return draw.textbbox((0, 0), text, font=font, anchor="ls")


def render_villa_card(*, data: "CoverRenderData", images: Sequence[Image.Image], labels: Iterable[str],
                      output: Path, geometry: VillaCardGeometry = GEOMETRY) -> Path:
    from .cover_renderer import _fit, _fit_line, _font, _pin_icon  # shared helpers (lazy: avoid cycle)
    from .cover_renderer import _cover_title

    g = geometry
    W, H = g.canvas
    canvas = Image.new("RGB", g.canvas, LIGHT_BG)
    # ---- hero: building kept, surplus cut from the foreground; fades into the bar
    hero_box = (W, g.bar_bottom)                         # photo runs under the bar's fade zone
    centering = frame_offset(images[0], (W, g.hero_height), text_room=24)
    hero = ImageOps.fit(images[0], (W, g.hero_height), method=Image.Resampling.LANCZOS, centering=centering)
    canvas.paste(hero, (0, 0))
    bar = Image.new("RGBA", (W, g.bar_bottom - g.fade_start), (*BAR_BG, 0))
    span = g.hero_height - g.fade_start
    alpha = Image.new("L", (1, bar.height))
    for y in range(bar.height):
        t = min(1.0, y / span)
        alpha.putpixel((0, y), int(255 * (t * t * (3 - 2 * t))))   # smoothstep
    bar.putalpha(alpha.resize(bar.size))
    canvas.paste(bar, (0, g.fade_start), bar)
    del hero_box
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    m = g.margin
    # ---- left: 📍 title, gold rule, info line
    card = g.price_card
    left_max = card[0] - 40 - m
    pin = _pin_icon(66, GOLD)
    pin_bbox = pin.getchannel("A").getbbox() or (0, 0, 66, 66)
    pin = pin.crop(pin_bbox)
    layer.alpha_composite(pin, (m, 702))
    title_x = m + pin.width + 18
    title, tfont = _fit_line(draw, _cover_title(data), max_width=card[0] - 24 - title_x, start_size=g.title_size, min_size=34)
    ink = _ink_box(draw, title, tfont)
    draw.text((title_x - ink[0], 712 - ink[1]), title, font=tfont, fill=(255, 255, 255, 255), anchor="ls")
    rule_right = min(left_max + m, max(466, title_x + (ink[2] - ink[0]) + 24))   # reference: x 36–466
    draw.rectangle((m, 776, rule_right, 777), fill=(*GOLD_RULE, 255))
    parts = info_parts(data)
    if parts:
        size = g.info_size
        while size > 22:
            font = _font(size, bold=False)
            total = sum(draw.textlength(p, font=font) for p in parts) + (len(parts) - 1) * 34
            if total <= left_max:
                break
            size -= 1
        font = _font(size, bold=False)
        x = m + 4
        baseline = 846
        ref = _ink_box(draw, "押房", font)
        for i, part in enumerate(parts):
            if i:
                x += 16
                draw.rectangle((x, baseline + ref[1] - 2, x + 1, baseline + 2), fill=(*GOLD_RULE, 255))
                x += 18
            draw.text((x, baseline), part, font=font, fill=(255, 255, 255, 255), anchor="ls")
            x += int(draw.textlength(part, font=font))
    # ---- right: gold-outlined price card, price optically centred
    price = data.price_line()
    amount, per = (price[:-2], "/月") if price.endswith("/月") else (price, "")
    if amount:
        cx0, cy0, cx1, cy1 = card
        draw.rounded_rectangle(card, radius=22, fill=(*CARD_BG, 255), outline=(*GOLD, 255), width=3)
        label_font = _font(25, bold=False)
        inner_w = cx1 - cx0 - 56
        size = g.price_size if amount.startswith("$") else 64
        while True:
            pfont = _font(size, bold=True)
            per_font = _font(max(26, int(size * 0.4)), bold=False)
            a_ink = _ink_box(draw, amount, pfont)
            per_w = int(draw.textlength(per, font=per_font)) + (10 if per else 0)
            if (a_ink[2] - a_ink[0]) + per_w <= inner_w or size <= 40:
                break
            size -= 2
        group_w = (a_ink[2] - a_ink[0]) + per_w
        gx = cx0 + (cx1 - cx0 - group_w) // 2
        label = "租金 / RENT" if str(data.deal_type or "rent").lower() == "rent" else "售价 / PRICE"
        l_ink = _ink_box(draw, label, label_font)
        label_top = cy0 + 15
        draw.text((gx - l_ink[0], label_top - l_ink[1]), label, font=label_font, fill=(*LABEL_GOLD, 255), anchor="ls")
        # digits: cap-height centred between label and card bottom (the "$" descender is ignored)
        digits = _ink_box(draw, "0", pfont)
        free_top, free_bottom = label_top + (l_ink[3] - l_ink[1]) + 6, cy1 - 6
        baseline = (free_top + free_bottom) // 2 + (digits[3] - digits[1]) // 2 - digits[3]
        mask = Image.new("L", layer.size, 0)
        ImageDraw.Draw(mask).text((gx - a_ink[0], baseline), amount, font=pfont, fill=255, anchor="ls")
        box = mask.getbbox()
        if box:
            region = mask.crop(box)
            layer.alpha_composite(_gradient_text(region.size, region, GOLD_TOP, GOLD_BOTTOM), box[:2])
        if per:
            draw.text((gx + (a_ink[2] - a_ink[0]) + 10, baseline), per, font=per_font, fill=(*GOLD, 255), anchor="ls")
    canvas.paste(layer, (0, 0), layer)
    # ---- thumbnails (rounded, light background), tags only for confident labels
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    labels = list(labels or ())
    th = g.thumb_bottom - g.thumb_top
    for index, (image, (x, tw)) in enumerate(zip(images[1:4], thumb_columns(len(images[1:4]), g))):
        canvas.paste(_fit(image, (tw, th)), (x, g.thumb_top), _rounded_mask((tw, th), g.thumb_radius))
        label = labels[index] if index < len(labels) else ""
        if label:
            tdraw = ImageDraw.Draw(layer)
            label, font = _fit_line(tdraw, label, max_width=tw - 64, start_size=24, min_size=16, bold=False)
            ink = _ink_box(tdraw, label, font)
            box = (x + 14, g.thumb_top + 12, x + 14 + (ink[2] - ink[0]) + 36, g.thumb_top + 12 + 40)
            tdraw.rounded_rectangle(box, radius=20, fill=(18, 14, 11, 225), outline=(*GOLD, 255), width=2)
            tx = box[0] + (box[2] - box[0] - (ink[2] - ink[0])) // 2 - ink[0]
            ty = box[1] + (box[3] - box[1] - (ink[3] - ink[1])) // 2 - ink[1]
            tdraw.text((tx, ty), label, font=font, fill=(255, 255, 255, 255), anchor="ls")
    # ---- footer
    fdraw = ImageDraw.Draw(layer)
    fdraw.rectangle((0, g.footer_top, W, H), fill=(*FOOTER_BG, 255))
    mid = (g.footer_top + H) // 2 + 2
    x_after = 43
    lockup = gold_lockup(68)
    if lockup is not None:
        layer.alpha_composite(lockup, (43, mid - lockup.height // 2))
        x_after = 43 + lockup.width
    pill = (778, mid - 30, 1045, mid + 29)
    fdraw.rounded_rectangle(pill, radius=30, fill=(*PILL_BG, 255))
    pfont = _font(31, bold=True)
    ink = _ink_box(fdraw, "出租房源", pfont)
    fdraw.text((pill[0] + (pill[2] - pill[0] - (ink[2] - ink[0])) // 2 - ink[0],
                pill[1] + (pill[3] - pill[1] - (ink[3] - ink[1])) // 2 - ink[1]), "出租房源", font=pfont,
               fill=(*INK, 255), anchor="ls")
    fdraw.rectangle((max(x_after + 36, 465), mid - 1, pill[0] - 18, mid + 1), fill=(*GOLD_RULE, 255))
    canvas.paste(layer, (0, 0), layer)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".jpg", ".jpeg"}:
        canvas.save(output, format="JPEG", quality=92, optimize=True)
    else:
        canvas.save(output, format="PNG", optimize=True)
    return output


__all__ = ["GEOMETRY", "VillaCardGeometry", "gold_lockup", "info_parts", "render_villa_card", "rooms_text", "thumb_columns"]
