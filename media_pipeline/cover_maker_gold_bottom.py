#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter


# ============================================================
# 侨联地产 Telegram 封面
# 尺寸：1600 × 1200
# 风格：实拍大图 + 金色品牌 + 黑色租金卡 + 底部参数卡
# ============================================================

W = 1600
H = 1200

GOLD = "#F1D176"
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
    raise RuntimeError(
        "未找到中文字体。Debian/Ubuntu 可执行：apt install -y fonts-noto-cjk"
    )


FONT_BOLD = find_font(FONT_CANDIDATES_BOLD)
FONT_REGULAR = find_font(FONT_CANDIDATES_REGULAR)


# NotoSansCJK-*.ttc 集合：0=JP 1=KR 2=SC 3=TC 4=HK — 侨联统一锁 SC
NOTO_SC_INDEX = 2


def font_bold(size):
    return ImageFont.truetype(FONT_BOLD, size, index=NOTO_SC_INDEX)


def font_regular(size):
    return ImageFont.truetype(FONT_REGULAR, size, index=NOTO_SC_INDEX)


def cover_crop(image: Image.Image, width: int, height: int):
    """等比铺满，不拉伸。"""
    image = image.convert("RGB")
    iw, ih = image.size

    scale = max(width / iw, height / ih)
    nw = int(iw * scale)
    nh = int(ih * scale)

    image = image.resize((nw, nh), Image.Resampling.LANCZOS)

    left = (nw - width) // 2
    top = (nh - height) // 2

    return image.crop((left, top, left + width, top + height))


def fit_font(text, max_width, start_size, min_size=30, bold=True):
    """文字太长时自动缩小字号。"""
    dummy = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(dummy)

    for size in range(start_size, min_size - 1, -2):
        f = font_bold(size) if bold else font_regular(size)
        box = draw.textbbox((0, 0), text, font=f)
        if box[2] - box[0] <= max_width:
            return f

    return font_bold(min_size) if bold else font_regular(min_size)


def rounded_panel(base, box, radius, fill):
    """带透明度的圆角面板。"""
    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    d.rounded_rectangle(box, radius=radius, fill=fill)
    return Image.alpha_composite(base, layer)


def draw_house_icon(draw, x, y, size=48, color=GOLD, width=7):
    """没有 Logo PNG 时使用简化房子图标。"""
    roof_y = y + size * 0.38
    left = x
    right = x + size
    center = x + size / 2

    draw.line(
        [
            (left, roof_y),
            (center, y),
            (right, roof_y),
        ],
        fill=color,
        width=width,
        joint="curve",
    )

    draw.line(
        [
            (x + size * 0.15, roof_y - 2),
            (x + size * 0.15, y + size * 0.88),
            (x + size * 0.85, y + size * 0.88),
            (x + size * 0.85, roof_y - 2),
        ],
        fill=color,
        width=width,
    )

    draw.rectangle(
        (
            int(x + size * 0.42),
            int(y + size * 0.58),
            int(x + size * 0.62),
            int(y + size * 0.88),
        ),
        outline=color,
        width=max(3, width - 2),
    )


def generate_cover(
    image_path: str,
    output_path: str,
    *,
    title: str,
    rent,
    property_type: str,
    location: str,
    area: str = "",
    floor: str = "",
    tags=None,
    logo_path: str | None = None,
):
    tags = tags or []

    bg = Image.open(image_path)
    bg = cover_crop(bg, W, H).convert("RGBA")

    shade = Image.new("RGBA", (W, H), (3, 10, 20, 50))
    bg = Image.alpha_composite(bg, shade)

    gradient = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gp = gradient.load()

    for y in range(H):
        if y < 650:
            alpha = int(38 * (1 - y / 650))
        else:
            alpha = 0

        for x in range(W):
            gp[x, y] = (0, 0, 0, alpha)

    bg = Image.alpha_composite(bg, gradient)

    bottom = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    bp = bottom.load()

    for y in range(850, H):
        alpha = int(min(135, (y - 850) / 350 * 135))
        for x in range(W):
            bp[x, y] = (0, 0, 0, alpha)

    bg = Image.alpha_composite(bg, bottom)

    draw = ImageDraw.Draw(bg)

    draw.line((55, 48, 1545, 48), fill=(241, 209, 118, 145), width=2)

    logo_x = 58
    logo_y = 82

    if logo_path and Path(logo_path).exists():
        logo = Image.open(logo_path).convert("RGBA")

        max_logo_w = 350
        max_logo_h = 105

        ratio = min(
            max_logo_w / logo.width,
            max_logo_h / logo.height
        )

        logo = logo.resize(
            (int(logo.width * ratio), int(logo.height * ratio)),
            Image.Resampling.LANCZOS
        )

        bg.alpha_composite(logo, (logo_x, logo_y))

    else:
        draw_house_icon(draw, logo_x, logo_y + 14, 54)

        draw.text(
            (132, 72),
            "侨联地产",
            font=font_bold(54),
            fill=GOLD,
        )

        draw.text(
            (134, 138),
            "QIAO LIAN PROPERTY",
            font=font_regular(19),
            fill=MUTED_WHITE,
        )

    rent_box = (1095, 126, 1542, 298)

    bg = rounded_panel(
        bg,
        rent_box,
        28,
        (7, 12, 18, 220),
    )

    draw = ImageDraw.Draw(bg)

    rent_label = "Monthly Rent"
    label_font = font_regular(32)
    bbox = draw.textbbox((0, 0), rent_label, font=label_font)
    label_w = bbox[2] - bbox[0]

    draw.text(
        (rent_box[0] + (rent_box[2] - rent_box[0] - label_w) / 2, 145),
        rent_label,
        font=label_font,
        fill=WHITE,
    )

    rent_text = f"${rent}/月"

    rent_font = fit_font(
        rent_text,
        max_width=410,
        start_size=76,
        min_size=58,
        bold=True,
    )

    bbox = draw.textbbox((0, 0), rent_text, font=rent_font)
    rent_w = bbox[2] - bbox[0]

    draw.text(
        (
            rent_box[0] + (rent_box[2] - rent_box[0] - rent_w) / 2,
            190,
        ),
        rent_text,
        font=rent_font,
        fill=GOLD,
    )

    title_font = fit_font(
        title,
        max_width=1010,
        start_size=79,
        min_size=56,
        bold=True,
    )

    draw.text(
        (57, 292),
        title,
        font=title_font,
        fill=WHITE,
        stroke_width=1,
        stroke_fill=(0, 0, 0, 50),
    )

    type_font = font_bold(48)

    type_bbox = draw.textbbox(
        (0, 0),
        property_type,
        font=type_font,
    )

    type_w = type_bbox[2] - type_bbox[0]

    chip_box = (
        57,
        514,
        57 + type_w + 50,
        596,
    )

    bg = rounded_panel(
        bg,
        chip_box,
        16,
        (20, 29, 46, 220),
    )

    draw = ImageDraw.Draw(bg)

    draw.text(
        (82, 525),
        property_type,
        font=type_font,
        fill=WHITE,
    )

    bottom_y = 1030
    bottom_h = 145

    card1 = (57, bottom_y, 655, bottom_y + bottom_h)
    card2 = (667, bottom_y, 1096, bottom_y + bottom_h)
    card3 = (1108, bottom_y, 1543, bottom_y + bottom_h)

    for card in [card1, card2, card3]:
        bg = rounded_panel(
            bg,
            card,
            13,
            (12, 20, 33, 220),
        )

    draw = ImageDraw.Draw(bg)

    label_font = font_regular(25)
    value_font = font_bold(38)

    draw.text(
        (77, bottom_y + 14),
        "位置",
        font=label_font,
        fill=GOLD,
    )

    loc_font = fit_font(
        location,
        520,
        40,
        28,
        bold=True,
    )

    draw.text(
        (77, bottom_y + 52),
        location,
        font=loc_font,
        fill=WHITE,
    )

    draw.text(
        (690, bottom_y + 14),
        "面积",
        font=label_font,
        fill=GOLD,
    )

    draw.text(
        (690, bottom_y + 52),
        area or "—",
        font=value_font,
        fill=WHITE,
    )

    draw.text(
        (1132, bottom_y + 14),
        "楼层",
        font=label_font,
        fill=GOLD,
    )

    draw.text(
        (1132, bottom_y + 52),
        floor or "—",
        font=value_font,
        fill=WHITE,
    )

    if tags:
        tag_x = 77
        tag_y = 1137

        tag_font = font_regular(21)

        for tag in tags[:3]:
            bbox = draw.textbbox((0, 0), tag, font=tag_font)
            tw = bbox[2] - bbox[0]

            pill_w = tw + 30
            pill_h = 35

            if tag_x + pill_w > card1[2] - 20:
                break

            tag_layer = Image.new(
                "RGBA",
                bg.size,
                (0, 0, 0, 0),
            )

            td = ImageDraw.Draw(tag_layer)

            td.rounded_rectangle(
                (
                    tag_x,
                    tag_y,
                    tag_x + pill_w,
                    tag_y + pill_h,
                ),
                radius=17,
                fill=(25, 30, 37, 225),
                outline=(241, 209, 118, 130),
                width=1,
            )

            bg = Image.alpha_composite(bg, tag_layer)
            draw = ImageDraw.Draw(bg)

            draw.text(
                (tag_x + 15, tag_y + 4),
                tag,
                font=tag_font,
                fill=WHITE,
            )

            tag_x += pill_w + 9

    out = bg.convert("RGB")

    Path(output_path).parent.mkdir(
        parents=True,
        exist_ok=True
    )

    out.save(
        output_path,
        "JPEG",
        quality=94,
        optimize=True,
        progressive=True,
    )

    return output_path
