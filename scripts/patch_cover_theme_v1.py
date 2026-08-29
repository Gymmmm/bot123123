from pathlib import Path

p = Path('cover_generator.py')
text = p.read_text(encoding='utf-8')
start = text.index('def _draw_new_cover(')
end = text.index('\n\n# ── 主生成函数', start)

new_block = r'''# ── 首页封面视觉规范：固定布局，按底图自动配色 ─────────────────
COVER_W, COVER_H = 1280, 960
COVER_LOGO_WIDTH_RATIO = 0.15
COVER_LEFT_SHADE_WIDTH_RATIO = 0.40
COVER_LEFT_SHADE_ALPHA = 138       # 54%
COVER_BOTTOM_PANEL_ALPHA = 158     # 62%
COVER_PRICE_PANEL_ALPHA = 222      # 87%
COVER_BOTTOM_PANEL_BLUR = 8

COVER_THEMES = {
    "black_gold": {
        "accent": (232, 190, 94),
        "text": (255, 250, 239),
        "panel": (10, 12, 16),
        "price": (13, 14, 17),
    },
    "navy_gold": {
        "accent": (225, 188, 98),
        "text": (248, 251, 255),
        "panel": (11, 28, 54),
        "price": (9, 24, 48),
    },
    "warm_champagne": {
        "accent": (224, 187, 122),
        "text": (255, 248, 236),
        "panel": (35, 24, 18),
        "price": (31, 22, 17),
    },
}


def _cover_image_stats(image: Image.Image) -> tuple[float, float, float]:
    thumb = image.convert("RGB").resize((64, 64), Image.Resampling.LANCZOS)
    px = list(thumb.getdata())
    brightness = sum((r + g + b) / 3 for r, g, b in px) / len(px)
    warmth = sum((r - b) for r, g, b in px) / len(px)
    saturation = 0.0
    for r, g, b in px:
        mx, mn = max(r, g, b), min(r, g, b)
        saturation += 0 if mx == 0 else (mx - mn) / mx
    saturation /= len(px)
    return brightness, warmth, saturation


def choose_cover_theme(image: Image.Image) -> str:
    """按首图自动选择主题；只变配色，不改变布局。"""
    brightness, warmth, saturation = _cover_image_stats(image)
    if warmth >= 10 and brightness >= 95:
        return "warm_champagne"
    if brightness >= 158 and saturation <= 0.34:
        return "navy_gold"
    return "black_gold"


def _cover_contain_4_3(src: Image.Image, size: tuple[int, int]) -> Image.Image:
    """封面保持 4:3；主体完整优先，必要时用同图轻模糊延展。"""
    cw, ch = size
    src = src.convert("RGB")
    ratio_diff = abs((src.width / max(1, src.height)) - (cw / ch))
    if ratio_diff <= 0.16:
        return ImageOps.fit(src, (cw, ch), method=Image.Resampling.LANCZOS)
    bg = ImageOps.fit(src, (cw, ch), method=Image.Resampling.LANCZOS)
    bg = bg.filter(ImageFilter.GaussianBlur(18))
    bg = ImageEnhance.Brightness(bg).enhance(0.88)
    fg = src.copy()
    fg.thumbnail((cw, ch), Image.Resampling.LANCZOS)
    bg.paste(fg, ((cw - fg.width) // 2, (ch - fg.height) // 2))
    return bg


def _draw_new_cover(
    output_path: str,
    project: str,
    layout: str,
    area: str,
    price,
    size: str,
    floor: str,
    highlights: list,
    base_image_path: Optional[str] = None,
    source_type: str = "",
    source_name: str = "",
) -> None:
    """生产首页封面：左上品牌、左侧标题、右上价格、底部位置/面积/楼层。"""
    W, H = COVER_W, COVER_H
    project = str(project or "").strip()
    layout = str(layout or "").strip()
    area = str(area or "").strip()
    size = str(size or "").strip().replace("m2", "㎡").replace("M2", "㎡")
    if size and re.fullmatch(r"\d+(?:\.\d+)?", size):
        size += "㎡"
    floor = str(floor or "").strip()
    if floor and re.fullmatch(r"\d+", floor):
        floor += "楼"

    price_text = ""
    if price is not None and str(price).strip():
        raw = str(price).strip()
        clean = raw.replace("$", "").replace(",", "")
        try:
            value = float(clean.replace("/月", ""))
            price_text = f"${int(value):,}/月" if value.is_integer() else f"${value:,.0f}/月"
        except Exception:
            price_text = raw if "月" in raw else f"{raw}/月"

    if base_image_path and os.path.isfile(base_image_path):
        try:
            src = Image.open(base_image_path).convert("RGB")
            bg = _cover_contain_4_3(src, (W, H))
        except Exception as exc:
            log.warning("封面底图读取失败: %s", exc)
            bg = Image.new("RGB", (W, H), (42, 42, 42))
    else:
        bg = Image.new("RGB", (W, H), (42, 42, 42))

    # 只做轻度 P 图，不把房屋主体压黑。
    brightness, _, _ = _cover_image_stats(bg)
    if brightness < 112:
        bg = ImageEnhance.Brightness(bg).enhance(1.10)
    elif brightness > 205:
        bg = ImageEnhance.Brightness(bg).enhance(0.96)
    bg = ImageEnhance.Contrast(bg).enhance(1.035)
    bg = ImageEnhance.Color(bg).enhance(1.025)

    theme_name = choose_cover_theme(bg)
    theme = COVER_THEMES[theme_name]
    accent = theme["accent"]
    text_color = theme["text"]
    panel_rgb = theme["panel"]
    price_rgb = theme["price"]
    img = bg.convert("RGBA")

    # 左侧渐变：最大约 54% 黑度，覆盖 40% 宽度。
    shade = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shade)
    shade_w = int(W * COVER_LEFT_SHADE_WIDTH_RATIO)
    for x in range(shade_w):
        t = x / max(1, shade_w - 1)
        alpha = int(COVER_LEFT_SHADE_ALPHA * ((1 - t) ** 1.45))
        sd.line((x, 0, x, H), fill=(8, 10, 13, alpha))
    img = Image.alpha_composite(img, shade)

    # 底部半透明磨砂栏；仍能看见房屋。
    panel_h = int(H * 0.19)
    panel_box = (26, H - panel_h - 22, W - 26, H - 22)
    img = _apply_frosted_panel(
        img,
        panel_box,
        radius=22,
        blur_radius=COVER_BOTTOM_PANEL_BLUR,
        tint_rgb=panel_rgb,
        tint_alpha=COVER_BOTTOM_PANEL_ALPHA,
        outline=(*accent, 78),
    )
    draw = ImageDraw.Draw(img)

    # 品牌块：固定视觉宽度约 15% 画布，不随模板漂移。
    logo_visual_w = int(W * COVER_LOGO_WIDTH_RATIO)
    icon_size = max(30, int(logo_visual_w * 0.20))
    brand_x, brand_y = 48, 42
    _draw_house_outline_mark(
        draw, x=brand_x, y=brand_y + 4, size=icon_size,
        fill=(*accent, 255), shadow=(0, 0, 0, 90),
    )
    brand_font = _font(max(30, int(logo_visual_w * 0.22)), True)
    brand_sub_font = _font(max(10, int(logo_visual_w * 0.075)), False)
    text_x = brand_x + icon_size + 12
    draw.text((text_x, brand_y), "侨联地产", font=brand_font, fill=(*accent, 255))
    draw.text((text_x, brand_y + 41), "QIAO LIAN PROPERTY", font=brand_sub_font, fill=(*accent, 224))

    # 左侧标题 / 户型。
    title = project or area or "精选房源"
    title, title_font = _fit_single_line_text(draw, title, max_width=620, start_size=58, min_size=38)
    title_y = 220
    draw.text((48, title_y), title, font=title_font, fill=text_color, stroke_width=1, stroke_fill=(0, 0, 0, 100))
    if layout:
        layout_text, layout_font = _fit_single_line_text(draw, layout, max_width=310, start_size=30, min_size=22)
        lb = draw.textbbox((0, 0), layout_text, font=layout_font)
        lw, lh = lb[2]-lb[0], lb[3]-lb[1]
        chip = (48, title_y + 78, 48 + lw + 28, title_y + 78 + lh + 16)
        draw.rounded_rectangle(chip, radius=18, fill=(10, 12, 16, 176), outline=(*accent, 230), width=2)
        draw.text((62-lb[0], title_y + 86-lb[1]), layout_text, font=layout_font, fill=text_color)

    # 右上租金锚点：深黑 87% + 金色描边。
    if price_text:
        price_font = _font(49, True)
        label_font = _font(15, False)
        pb = draw.textbbox((0, 0), price_text, font=price_font)
        pw, ph = pb[2]-pb[0], pb[3]-pb[1]
        x2, y1 = W - 48, 46
        box_w = max(255, pw + 54)
        box_h = ph + 62
        box = (x2 - box_w, y1, x2, y1 + box_h)
        draw.rounded_rectangle(box, radius=18, fill=(*price_rgb, COVER_PRICE_PANEL_ALPHA), outline=(*accent, 235), width=2)
        label = "Monthly Rent"
        lb = draw.textbbox((0,0), label, font=label_font)
        draw.text((box[0] + (box_w-(lb[2]-lb[0]))//2, y1+10), label, font=label_font, fill=(*text_color[:3], 230))
        draw.text((box[0] + (box_w-pw)//2 - pb[0], y1+31-pb[1]), price_text, font=price_font, fill=(*accent, 255))

    # 底栏：只保留位置、面积、楼层；最多三个短卖点。
    pad_x = 36
    py = panel_box[1] + 25
    cols = [
        ("位置", area or project or "金边"),
        ("面积", size or "待确认"),
        ("楼层", floor or "待确认"),
    ]
    col_w = (panel_box[2]-panel_box[0]-pad_x*2)//3
    for i, (label, value) in enumerate(cols):
        x = panel_box[0] + pad_x + i*col_w
        if i:
            draw.line((x-18, panel_box[1]+22, x-18, panel_box[3]-22), fill=(*accent, 105), width=1)
        draw.text((x, py), label, font=_font(18, False), fill=(*accent, 250))
        value, vf = _fit_single_line_text(draw, value, max_width=col_w-28, start_size=31, min_size=22)
        draw.text((x, py+31), value, font=vf, fill=text_color)

    chips = [str(v).strip() for v in (highlights or []) if str(v).strip()][:3]
    if chips:
        cx = panel_box[0] + pad_x
        cy = panel_box[3] - 42
        for raw in chips:
            raw, cf = _fit_single_line_text(draw, raw, max_width=135, start_size=15, min_size=12)
            cb = draw.textbbox((0,0), raw, font=cf)
            cw, ch = cb[2]-cb[0]+22, cb[3]-cb[1]+12
            draw.rounded_rectangle((cx, cy-ch, cx+cw, cy), radius=ch//2, fill=(20,20,20,125), outline=(*accent, 90))
            draw.text((cx+11-cb[0], cy-ch+6-cb[1]), raw, font=cf, fill=(*accent, 245))
            cx += cw+10

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    img.convert("RGB").save(output_path, "JPEG", quality=94, optimize=True)
'''

text = text[:start] + new_block + text[end:]
p.write_text(text, encoding='utf-8')

Path('tests/test_cover_theme_regression.py').write_text(r'''from PIL import Image
from cover_generator import (
    COVER_BOTTOM_PANEL_ALPHA,
    COVER_BOTTOM_PANEL_BLUR,
    COVER_LEFT_SHADE_ALPHA,
    COVER_LEFT_SHADE_WIDTH_RATIO,
    COVER_LOGO_WIDTH_RATIO,
    COVER_PRICE_PANEL_ALPHA,
    COVER_W,
    COVER_H,
    choose_cover_theme,
)


def test_cover_geometry_and_opacity_contract():
    assert (COVER_W, COVER_H) == (1280, 960)
    assert 0.14 <= COVER_LOGO_WIDTH_RATIO <= 0.16
    assert 0.38 <= COVER_LEFT_SHADE_WIDTH_RATIO <= 0.42
    assert 128 <= COVER_LEFT_SHADE_ALPHA <= 141
    assert 148 <= COVER_BOTTOM_PANEL_ALPHA <= 166
    assert 6 <= COVER_BOTTOM_PANEL_BLUR <= 10
    assert 209 <= COVER_PRICE_PANEL_ALPHA <= 225


def test_theme_selection_is_deterministic_for_reference_profiles():
    assert choose_cover_theme(Image.new('RGB', (100,100), (65,75,90))) == 'black_gold'
    assert choose_cover_theme(Image.new('RGB', (100,100), (210,215,220))) == 'navy_gold'
    assert choose_cover_theme(Image.new('RGB', (100,100), (190,150,105))) == 'warm_champagne'
''', encoding='utf-8')
