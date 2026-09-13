#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter

W, H = 1600, 1200
GOLD = "#E8D5A8"
WHITE = "#FFFFFF"
MUTED = "#E7E8EA"

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
    raise RuntimeError("未找到中文字体")


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
    left = (nw - width) // 2
    top = (nh - height) // 2
    return image.crop((left, top, left + width, top + height))


def fit_font(text, max_width, start_size, min_size=28, bold=True):
    dummy = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(dummy)
    for size in range(start_size, min_size - 1, -2):
        f = font_bold(size) if bold else font_regular(size)
        box = draw.textbbox((0, 0), str(text), font=f)
        if box[2] - box[0] <= max_width:
            return f
    return font_bold(min_size) if bold else font_regular(min_size)


def glass_panel(base, box, radius=24, fill=(11, 14, 20, 120), blur=16, outline=(232, 213, 168, 85), width=1):
    x1, y1, x2, y2 = map(int, box)
    region = base.crop((x1, y1, x2, y2)).filter(ImageFilter.GaussianBlur(blur))
    mask = Image.new("L", (x2 - x1, y2 - y1), 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle((0, 0, x2 - x1 - 1, y2 - y1 - 1), radius=radius, fill=255)
    base.paste(region, (x1, y1), mask)
    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    d.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)
    return Image.alpha_composite(base, layer)


def generate_cover(image_path: str, output_path: str, *, title: str, rent, property_type: str,
                   location: str, area: str = "", floor: str = "", tags=None, logo_path: str | None = None):
    tags = tags or []
    bg = cover_crop(Image.open(image_path), W, H).convert("RGBA")

    # 只做很轻的整体压暗，保留实拍本身。
    bg = Image.alpha_composite(bg, Image.new("RGBA", (W, H), (4, 8, 14, 24)))

    # 顶部轻渐变，仅服务 logo / 租金可读性。
    top = Image.new("RGBA", (W, 230), (0, 0, 0, 0))
    td = ImageDraw.Draw(top)
    for y in range(230):
        a = int(82 * (1 - y / 230))
        td.line((0, y, W, y), fill=(0, 0, 0, a))
    bg.alpha_composite(top, (0, 0))

    # 底部只留一层很短的渐变，不再铺整条黑带。
    bottom = Image.new("RGBA", (W, 360), (0, 0, 0, 0))
    bd = ImageDraw.Draw(bottom)
    for y in range(360):
        t = y / 359
        a = int(12 + 125 * (t ** 1.9))
        bd.line((0, y, W, y), fill=(0, 0, 0, a))
    bg.alpha_composite(bottom, (0, H - 360))

    draw = ImageDraw.Draw(bg)
    draw.line((48, 42, 1552, 42), fill=(232, 213, 168, 130), width=2)

    # Logo 缩小，减少与标题抢视觉。
    if logo_path and Path(logo_path).exists():
        logo = Image.open(logo_path).convert("RGBA")
        ratio = min(315 / logo.width, 92 / logo.height)
        logo = logo.resize((int(logo.width * ratio), int(logo.height * ratio)), Image.Resampling.LANCZOS)
        bg.alpha_composite(logo, (48, 66))

    # 右上价格：更窄、更轻、信息层级更清楚。
    price_box = (1110, 72, 1552, 224)
    bg = glass_panel(bg, price_box, radius=28, fill=(10, 14, 18, 112), blur=18, outline=(232, 213, 168, 95))
    draw = ImageDraw.Draw(bg)
    lab = "月租"
    lf = font_regular(24)
    lw = draw.textbbox((0, 0), lab, font=lf)[2]
    draw.text((price_box[0] + (price_box[2]-price_box[0]-lw)/2, 92), lab, font=lf, fill=(255,255,255,220))
    price = f"${rent}/月"
    pf = fit_font(price, 390, 66, 48, True)
    pw = draw.textbbox((0, 0), price, font=pf)[2]
    draw.text((price_box[0] + (price_box[2]-price_box[0]-pw)/2, 126), price, font=pf, fill=GOLD)

    # 主标题下移到画面下半部，让主体照片完整展示。
    title_y = 770
    title_font = fit_font(title, 1010, 76, 48, True)
    draw.text((52, title_y), title, font=title_font, fill=WHITE, stroke_width=2, stroke_fill=(0,0,0,110))

    # 户型 pill 改成真正的小标签，不再是大块。
    pill_font = font_bold(30)
    pb = draw.textbbox((0, 0), property_type, font=pill_font)
    pw = pb[2] - pb[0]
    pill = (54, title_y + 102, 54 + pw + 46, title_y + 158)
    bg = glass_panel(bg, pill, radius=28, fill=(15, 17, 22, 105), blur=10, outline=(255,255,255,55))
    draw = ImageDraw.Draw(bg)
    draw.text((77, title_y + 112), property_type, font=pill_font, fill=WHITE)

    # 底部信息改成一整条轻量信息栏，中间用竖线分隔，取消三个笨重盒子。
    bar = (48, 990, 1552, 1140)
    bg = glass_panel(bg, bar, radius=26, fill=(12, 15, 20, 105), blur=18, outline=(232,213,168,70))
    draw = ImageDraw.Draw(bg)
    xcuts = [590, 1050]
    for x in xcuts:
        draw.line((x, 1016, x, 1115), fill=(232,213,168,70), width=1)

    label_f = font_regular(20)
    value_f = font_bold(36)

    def info(x, label, value, width):
        draw.text((x, 1014), label, font=label_f, fill=(232,213,168,220))
        vf = fit_font(value or "—", width, 36, 28, True)
        draw.text((x, 1052), value or "—", font=vf, fill=WHITE)

    info(76, "位置", location or "—", 470)
    info(625, "面积", area or "—", 355)
    info(1085, "楼层", floor or "—", 360)

    # 可选标签紧贴信息栏上方；没有就不占空间。
    if tags:
        tx, ty = 54, 934
        tf = font_regular(19)
        for tag in tags[:3]:
            tw = draw.textbbox((0,0), tag, font=tf)[2]
            box = (tx, ty, tx + tw + 28, ty + 38)
            bg = glass_panel(bg, box, radius=19, fill=(22,26,32,95), blur=8, outline=(255,255,255,35))
            draw = ImageDraw.Draw(bg)
            draw.text((tx+14, ty+7), tag, font=tf, fill=(244,236,216,235))
            tx += tw + 40

    out = bg.convert("RGB")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    out.save(output_path, "JPEG", quality=94, optimize=True, progressive=True)
    return output_path
