#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter

W, H = 1600, 1200
GOLD = "#E8D5A8"
WHITE = "#FFFFFF"
MUTED_WHITE = "#E9EDF2"

FONT_CANDIDATES_BOLD = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.otf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
]
FONT_CANDIDATES_REGULAR = [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.otf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
]


def find_font(candidates):
    for p in candidates:
        if Path(p).exists():
            return p
    raise RuntimeError("未找到中文字体。Debian/Ubuntu 可执行：apt install -y fonts-noto-cjk")


FONT_BOLD = find_font(FONT_CANDIDATES_BOLD)
FONT_REGULAR = find_font(FONT_CANDIDATES_REGULAR)
NOTO_SC_INDEX = 2


def font_bold(size):
    return ImageFont.truetype(FONT_BOLD, size, index=NOTO_SC_INDEX)


def font_regular(size):
    return ImageFont.truetype(FONT_REGULAR, size, index=NOTO_SC_INDEX)


def cover_crop(image: Image.Image, width: int, height: int):
    image = image.convert("RGB")
    iw, ih = image.size
    scale = max(width / iw, height / ih)
    nw, nh = int(iw * scale), int(ih * scale)
    image = image.resize((nw, nh), Image.Resampling.LANCZOS)
    left, top = (nw - width) // 2, (nh - height) // 2
    return image.crop((left, top, left + width, top + height))


def fit_font(text, max_width, start_size, min_size=30, bold=True):
    dummy = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(dummy)
    for size in range(start_size, min_size - 1, -2):
        f = font_bold(size) if bold else font_regular(size)
        box = draw.textbbox((0, 0), str(text), font=f)
        if box[2] - box[0] <= max_width:
            return f
    return font_bold(min_size) if bold else font_regular(min_size)


def overlay_vertical_gradient(base, y0, y1, start_alpha, end_alpha=0, reverse=False):
    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    span = max(1, y1 - y0)
    for y in range(y0, y1):
        t = (y - y0) / span
        if reverse:
            a = int(end_alpha + (start_alpha - end_alpha) * t)
        else:
            a = int(start_alpha + (end_alpha - start_alpha) * t)
        d.line((0, y, W, y), fill=(0, 0, 0, max(0, min(255, a))))
    return Image.alpha_composite(base, layer)


def glass_panel(base, box, radius, fill=(15, 17, 23, 128), blur=14,
                outline=(232, 213, 168, 46), width=2, shadow=True):
    x1, y1, x2, y2 = map(int, box)
    result = base.copy()
    crop = base.crop((x1, y1, x2, y2)).filter(ImageFilter.GaussianBlur(blur))
    mask = Image.new("L", (x2 - x1, y2 - y1), 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle((0, 0, x2 - x1 - 1, y2 - y1 - 1), radius=radius, fill=255)
    if shadow:
        shadow_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
        sd = ImageDraw.Draw(shadow_layer)
        sd.rounded_rectangle((x1, y1 + 10, x2, y2 + 10), radius=radius, fill=(0, 0, 0, 55))
        result = Image.alpha_composite(result, shadow_layer)
    result.paste(crop, (x1, y1), mask)
    tint = Image.new("RGBA", base.size, (0, 0, 0, 0))
    td = ImageDraw.Draw(tint)
    td.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=fill, outline=outline, width=width)
    return Image.alpha_composite(result, tint)


def generate_cover(image_path: str, output_path: str, *, title: str, rent,
                   property_type: str, location: str, area: str = "", floor: str = "",
                   tags=None, logo_path: str | None = None):
    tags = tags or []
    bg = cover_crop(Image.open(image_path), W, H).convert("RGBA")

    # CSS-equivalent softened fades: top 280px, bottom 320px.
    bg = overlay_vertical_gradient(bg, 0, 280, 115, 0)
    bg = overlay_vertical_gradient(bg, H - 320, H, 0, 153, reverse=True)

    draw = ImageDraw.Draw(bg)
    draw.line((48, 42, 1552, 42), fill=(232, 213, 168, 170), width=2)

    # Brand block.
    logo_x, logo_y = 48, 40
    if logo_path and Path(logo_path).exists():
        logo = Image.open(logo_path).convert("RGBA")
        ratio = min(420 / logo.width, 112 / logo.height)
        logo = logo.resize((int(logo.width * ratio), int(logo.height * ratio)), Image.Resampling.LANCZOS)
        bg.alpha_composite(logo, (logo_x, logo_y + 24))
    else:
        draw.text((48, 72), "侨联地产", font=font_bold(52), fill=GOLD)
        draw.text((50, 137), "QIAO LIAN PROPERTY", font=font_regular(18), fill=MUTED_WHITE)

    # Price glass card, right 48/top 48, ~48% opacity.
    rent_box = (1080, 110, 1552, 295)
    bg = glass_panel(bg, rent_box, 26, fill=(18, 20, 26, 122), blur=12,
                     outline=(232, 213, 168, 51), width=2)
    draw = ImageDraw.Draw(bg)
    rent_label = "Monthly Rent"
    lf = font_regular(30)
    b = draw.textbbox((0, 0), rent_label, font=lf)
    draw.text((rent_box[0] + (rent_box[2] - rent_box[0] - (b[2]-b[0]))/2, 137), rent_label, font=lf, fill=(255,255,255,230))
    rent_text = f"${rent}/月"
    rf = fit_font(rent_text, 410, 72, 56, True)
    b = draw.textbbox((0,0), rent_text, font=rf)
    draw.text((rent_box[0] + (rent_box[2]-rent_box[0]-(b[2]-b[0]))/2, 184), rent_text, font=rf, fill=GOLD)

    # Main title / property pill.
    title_font = fit_font(title, 930, 78, 54, True)
    draw.text((48, 280), str(title), font=title_font, fill=WHITE,
              stroke_width=2, stroke_fill=(0,0,0,115))

    pf = font_bold(34)
    pb = draw.textbbox((0,0), str(property_type), font=pf)
    pw = pb[2] - pb[0]
    pill = (48, 432, 48 + pw + 52, 500)
    bg = glass_panel(bg, pill, 34, fill=(20,22,28,133), blur=8,
                     outline=(255,255,255,31), width=1, shadow=False)
    draw = ImageDraw.Draw(bg)
    draw.text((74, 444), str(property_type), font=pf, fill=WHITE)

    # Bottom cards: 1.4 / .8 / .8 ratios, 18px gap, 48px inset.
    bottom_y, bottom_h = 997, 155
    usable = W - 96 - 36
    unit = usable / 3.0
    w1, w2, w3 = int(unit * 1.4), int(unit * 0.8), int(unit * 0.8)
    # normalize exact width to available space
    total = w1 + w2 + w3
    scale = usable / total
    w1, w2 = int(w1*scale), int(w2*scale)
    w3 = usable - w1 - w2
    x1 = 48
    card1 = (x1, bottom_y, x1+w1, bottom_y+bottom_h)
    x2 = card1[2] + 18
    card2 = (x2, bottom_y, x2+w2, bottom_y+bottom_h)
    x3 = card2[2] + 18
    card3 = (x3, bottom_y, 1552, bottom_y+bottom_h)

    for card in (card1, card2, card3):
        bg = glass_panel(bg, card, 22, fill=(15,17,23,128), blur=14,
                         outline=(232,213,168,46), width=2)
    draw = ImageDraw.Draw(bg)
    labf, valf = font_regular(22), font_bold(44)

    def card_text(card, label, value, maxw=None):
        x, y = card[0] + 26, card[1] + 18
        draw.text((x, y), label, font=labf, fill=(255,255,255,190))
        vf = fit_font(value or "—", maxw or (card[2]-card[0]-52), 44, 28, True)
        draw.text((x, y+42), value or "—", font=vf, fill=WHITE)

    card_text(card1, "位置", location, card1[2]-card1[0]-52)
    card_text(card2, "面积", area or "—")
    card_text(card3, "楼层", floor or "—")

    if tags:
        tx, ty = card1[0] + 26, card1[1] + 105
        tf = font_bold(22)
        for tag in tags[:3]:
            b = draw.textbbox((0,0), str(tag), font=tf)
            tw = b[2]-b[0]
            if tx + tw + 34 > card1[2]-20:
                break
            pill = (tx, ty, tx+tw+34, ty+38)
            bg = glass_panel(bg, pill, 19, fill=(32,36,44,153), blur=5,
                             outline=(255,255,255,26), width=1, shadow=False)
            draw = ImageDraw.Draw(bg)
            draw.text((tx+17, ty+5), str(tag), font=tf, fill=(240,230,204,255))
            tx += tw + 44

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    bg.convert("RGB").save(output_path, "JPEG", quality=94, optimize=True, progressive=True)
    return output_path
