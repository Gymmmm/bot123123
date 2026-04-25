"""
v2 封面生成 — 高端设计模板（不依赖原图质量）。
风格：Old Money / 意式极简 / 深色奢华。
"""
from __future__ import annotations

import os
import re
import math
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps

_FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]


def _font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in _FONT_CANDIDATES:
        if os.path.isfile(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def _norm_price(price: str) -> tuple[str, str]:
    s = str(price or "").strip()
    if not s:
        return "价格待确认", ""
    if s.endswith("/月"):
        main = s[:-2].strip()
        digits = re.sub(r"[^\d]", "", main)
        if not digits or int(digits) == 0:
            return "价格待确认", ""
        return main, "/月"
    digits = re.sub(r"[^\d]", "", s)
    if digits and int(digits) > 0:
        return f"${int(digits)}", "/月"
    return "价格待确认", ""


def _build_hashtags(area: str, property_type: str, project: str) -> str:
    tags = ["#金边租房", "#柬埔寨房产"]
    area_l = (area or "").lower()
    if "bkk" in area_l:
        tags.append("#BKK区")
    elif "桑园" in area or "tk" in area_l:
        tags.append("#桑园区")
    elif "森速" in area or "sensok" in area_l:
        tags.append("#森速区")
    elif "钻石岛" in area:
        tags.append("#钻石岛")
    elif "永旺" in area or "aeon" in area_l:
        tags.append("#永旺商圈")

    pt = (property_type or "").strip()
    if "1房" in pt or "一房" in pt:
        tags.append("#一居室")
    elif "2房" in pt or "两房" in pt:
        tags.append("#两居室")
    elif "3房" in pt or "三房" in pt:
        tags.append("#三居室")
    elif "studio" in pt.lower():
        tags.append("#开间")

    proj = (project or "").strip().replace(" ", "").replace("·", "")
    if proj and proj not in ("侨联地产", "未识别", "—"):
        proj_clean = re.sub(r'(\d房\d卫|Studio)', '', proj, flags=re.I).strip()
        if proj_clean and len(proj_clean) > 1:
            tags.append(f"#{proj_clean}")

    tags.append("#实拍房源")
    return "  ".join(tags)


# ═══════════════════════════════════════════════════════════
# 通用背景生成器
# ═══════════════════════════════════════════════════════════
def _create_luxury_bg(width: int, height: int, style: str = "dark") -> Image.Image:
    """创建奢华背景（不使用外部图片）。"""
    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    if style == "dark":
        # 深蓝黑渐变背景
        for y in range(height):
            t = y / height
            r = int(8 + 18 * t)
            g = int(16 + 32 * t)
            b = int(40 + 60 * t)
            draw.line([(0, y), (width, y)], fill=(r, g, b, 255))

        # 几何装饰线
        draw.line([(60, 80), (60, height - 80)], fill=(246, 201, 72, 60), width=2)
        draw.line([(width - 60, 80), (width - 60, height - 80)], fill=(246, 201, 72, 60), width=2)

    elif style == "gold":
        # 深金渐变
        for y in range(height):
            t = y / height
            r = int(30 + 40 * t)
            g = int(25 + 35 * t)
            b = int(15 + 25 * t)
            draw.line([(0, y), (width, y)], fill=(r, g, b, 255))

        # 对角装饰线
        draw.line([(0, 0), (200, 200)], fill=(246, 201, 72, 80), width=3)
        draw.line([(width, 0), (width - 200, 200)], fill=(246, 201, 72, 80), width=3)

    return img


def _draw_rounded_rect(draw, xy, radius, fill):
    """绘制圆角矩形。"""
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=fill)


def _photo_bg(base_image_path: str, size: tuple[int, int], fallback: tuple[int, int, int]) -> Image.Image:
    """优先使用房源图；没有图时只给干净底色，不做花哨背景。"""
    if base_image_path and os.path.isfile(base_image_path):
        try:
            return ImageOps.fit(
                Image.open(base_image_path).convert("RGBA"),
                size,
                method=Image.Resampling.LANCZOS,
            )
        except Exception:
            pass
    return Image.new("RGBA", size, (*fallback, 255))


def _apply_cover_gradient(img: Image.Image) -> Image.Image:
    """顶部 10% + 底部 15% 轻微暗化，保证小 logo 和底卡边缘可读。"""
    base = img.convert("RGBA")
    w, h = base.size
    shade = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(shade)
    top_h = max(1, int(h * 0.10))
    bottom_h = max(1, int(h * 0.15))
    for y in range(top_h):
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, int(72 * (1 - y / top_h))))
    for y in range(h - bottom_h, h):
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, int(72 * ((y - (h - bottom_h)) / bottom_h))))
    return Image.alpha_composite(base, shade)


def _cover_bg(base_image_path: str, size: tuple[int, int], fallback: tuple[int, int, int]) -> Image.Image:
    return _apply_cover_gradient(_photo_bg(base_image_path, size, fallback))


def _draw_brand_chip(draw: ImageDraw.ImageDraw, x: int, y: int, scale: float = 1.0) -> None:
    """Compact brand mark based on channel references."""
    f_cn = _font(int(24 * scale))
    f_sub = _font(int(11 * scale))
    shadow = (0, 0, 0, 128)
    white = (255, 255, 255, 250)
    gold = (246, 201, 72, 235)

    icon_x = x
    icon_y = y + int(3 * scale)
    icon_w = int(20 * scale)
    icon_h = int(16 * scale)
    line_w = max(1, int(2 * scale))
    roof = [
        (icon_x, icon_y + int(8 * scale)),
        (icon_x + icon_w // 2, icon_y),
        (icon_x + icon_w, icon_y + int(8 * scale)),
    ]
    body = [
        (icon_x + int(3 * scale), icon_y + int(8 * scale)),
        (icon_x + int(3 * scale), icon_y + icon_h),
        (icon_x + icon_w - int(3 * scale), icon_y + icon_h),
        (icon_x + icon_w - int(3 * scale), icon_y + int(8 * scale)),
    ]
    for dx, dy in ((1, 1),):
        draw.line([(px + dx, py + dy) for px, py in roof], fill=shadow, width=line_w, joint="curve")
        draw.line([(px + dx, py + dy) for px, py in body], fill=shadow, width=line_w)
    draw.line(roof, fill=gold, width=line_w, joint="curve")
    draw.line(body, fill=gold, width=line_w)

    text_x = x + int(28 * scale)
    title_y = y - int(1 * scale)
    sub_y = y + int(24 * scale)
    for dx, dy in ((1, 1),):
        draw.text((text_x + dx, title_y + dy), "侨联地产", font=f_cn, fill=shadow)
        draw.text((text_x + dx, sub_y + dy), "金边租房", font=f_sub, fill=shadow)
    draw.text((text_x, title_y), "侨联地产", font=f_cn, fill=white)
    draw.text((text_x, sub_y), "金边租房", font=f_sub, fill=gold)


# ═══════════════════════════════════════════════════════════
# 模板 1：经典蓝卡 (1600×1200) - 使用纯色背景
# ═══════════════════════════════════════════════════════════
def generate_style_classic(
    base_image_path: str,
    output_path: str,
    project: str = "富力城",
    property_type: str = "1房1卫",
    area: str = "BKK1",
    size: str = "45㎡",
    floor: str = "8楼",
    price: str = "$680/月",
    highlights: list | None = None,
) -> None:
    """经典蓝卡：深蓝渐变背景 + 金色点缀 + 几何线条。"""
    W, H = 1600, 1200
    highlights = highlights or ["家具基本全新", "小区泳池", "健身房"]
    hl = [str(h).strip() for h in highlights if str(h).strip()][:3]
    while len(hl) < 3:
        hl.append("")

    # 创建背景
    img = _create_luxury_bg(W, H, "dark")
    draw = ImageDraw.Draw(img)

    # 字体
    f_brand_cn = _font(56)
    f_brand_en = _font(18)
    f_project = _font(84)
    f_type = _font(48)
    f_info = _font(32)
    f_price = _font(96)
    f_price_unit = _font(40)
    f_tag = _font(26)
    f_hash = _font(20)

    # ── 顶部品牌区 ──
    _draw_brand_chip(draw, 80, 52, scale=1.15)

    # 顶部装饰线
    draw.line([(80, 160), (520, 160)], fill=(246, 201, 72, 200), width=3)

    # ── 中部主视觉区 ──
    content_y = 280

    # 项目名称（超大）
    draw.text((80, content_y), project, font=f_project, fill=(255, 255, 255, 255))

    # 户型标签（金色pill）
    type_text = property_type
    type_w = 180
    type_h = 56
    type_x = 80
    type_y = content_y + 120
    draw.rounded_rectangle([type_x, type_y, type_x + type_w, type_y + type_h], radius=999, fill=(246, 201, 72, 230))
    draw.text((type_x + 22, type_y + 8), type_text, font=f_type, fill=(8, 16, 40, 255))

    # ── 信息区 ──
    info_x = 80
    info_y = type_y + 100
    info_line = f"📍 {area}    📐 {size}    🏢 {floor}"
    draw.text((info_x, info_y), info_line, font=f_info, fill=(255, 255, 255, 200))

    # ── 右侧价格（竖排设计）──
    price_x = W - 400
    price_y = content_y + 20

    # 价格背景框
    p_main, p_unit = _norm_price(price)
    draw.rounded_rectangle([price_x - 30, price_y - 20, W - 60, price_y + 140], radius=20, fill=(246, 201, 72, 240))

    # "租金"标签
    draw.text((price_x, price_y), "租金", font=f_info, fill=(8, 16, 40, 200))
    # 价格数字
    draw.text((price_x, price_y + 45), p_main, font=f_price, fill=(8, 16, 40, 255))
    if p_unit:
        draw.text((price_x, price_y + 105), p_unit, font=f_price_unit, fill=(8, 16, 40, 180))

    # ── 底部亮点区 ──
    hl_y = H - 200
    icons = ("🛋️", "🏊", "🏋️")
    for i in range(3):
        if hl[i]:
            xi = 80 + i * 280
            # 小圆点装饰
            draw.ellipse([xi, hl_y + 8, xi + 12, hl_y + 20], fill=(246, 201, 72, 200))
            draw.text((xi + 20, hl_y), f"{hl[i]}", font=f_tag, fill=(255, 255, 255, 230))

    # ── 底部hashtag──
    hash_line = _build_hashtags(area, property_type, project)
    draw.text((80, H - 50), hash_line, font=f_hash, fill=(255, 255, 255, 160))

    # 保存
    out = img.convert("RGB")
    out.save(output_path, "JPEG", quality=96, optimize=True)


# ═══════════════════════════════════════════════════════════
# 模板 2：极简白条 (1600×1200)
# ═══════════════════════════════════════════════════════════
def generate_style_minimal(
    base_image_path: str,
    output_path: str,
    project: str = "钻石岛",
    property_type: str = "2房2卫",
    area: str = "BKK1",
    size: str = "78㎡",
    floor: str = "18楼",
    price: str = "$1200/月",
    highlights: list | None = None,
) -> None:
    """极简白条：参考频道主图，照片为主 + 底部白色信息卡。"""
    W, H = 1280, 960
    img = _cover_bg(base_image_path, (W, H), (239, 243, 248))
    draw = ImageDraw.Draw(img)

    f_price = _font(64)
    f_info = _font(28)

    _draw_brand_chip(draw, 22, 22, 0.92)

    bar_h = int(H * 0.115)
    bar_y = H - bar_h
    draw.rectangle([0, bar_y, W, H], fill=(255, 255, 255, 242))

    p_main, p_unit = _norm_price(price)
    price_text = p_main + (p_unit or "")
    info_line = " · ".join([str(x).strip() for x in (area, property_type, floor) if str(x).strip()])
    f_price_lg = _font(80)
    draw.text((52, bar_y + 8), price_text, font=f_price_lg, fill=(15, 23, 42, 255))
    draw.text((54, bar_y + 80), info_line, font=f_info, fill=(71, 85, 105, 255))

    out = img.convert("RGB")
    out.save(output_path, "JPEG", quality=96, optimize=True)


# ═══════════════════════════════════════════════════════════
# 模板 3：右侧价签 (1600×1200)
# ═══════════════════════════════════════════════════════════
def generate_style_price_tag(
    base_image_path: str,
    output_path: str,
    project: str = "太子幸福",
    property_type: str = "单间",
    area: str = "桑园",
    size: str = "38㎡",
    floor: str = "26楼",
    price: str = "$480/月",
    highlights: list | None = None,
) -> None:
    """右侧价签：照片为主，右下轻价签，不压住主体。"""
    W, H = 1280, 960
    img = _cover_bg(base_image_path, (W, H), (235, 239, 245))
    draw = ImageDraw.Draw(img)

    f_info = _font(28)
    f_price = _font(64)
    f_type = _font(28)

    _draw_brand_chip(draw, 22, 22, 0.92)

    p_main, p_unit = _norm_price(price)
    price_text = p_main + (p_unit or "")
    bx1, by1, bx2, by2 = W - 400, H - 204, W - 18, H - 18
    draw.rounded_rectangle([bx1, by1, bx2, by2], radius=22, fill=(26, 58, 143, 235))
    f_price_lg = _font(72)
    draw.text((bx1 + 26, by1 + 16), price_text, font=f_price_lg, fill=(246, 201, 72, 255))
    draw.text((bx1 + 26, by1 + 112), str(property_type or "").strip(), font=f_type, fill=(255, 255, 255, 245))

    bottom_line = " · ".join([str(x).strip() for x in (area, property_type, floor) if str(x).strip()])
    draw.text((56, H - 58), bottom_line, font=f_info, fill=(255, 255, 255, 230))

    out = img.convert("RGB")
    out.save(output_path, "JPEG", quality=96, optimize=True)


# ═══════════════════════════════════════════════════════════
# 模板 4：竖版视频封面 (1080×1920)
# ═══════════════════════════════════════════════════════════
def generate_style_vertical(
    base_image_path: str,
    output_path: str,
    project: str = "富力城",
    property_type: str = "1房1卫",
    area: str = "BKK1",
    size: str = "55㎡",
    floor: str = "12楼",
    price: str = "$900/月",
    highlights: list | None = None,
) -> None:
    """竖版封面：1080×1920，手机全屏比例。"""
    W, H = 1080, 1920
    img = _photo_bg(base_image_path, (W, H), (16, 33, 72))

    top_mask = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    top_draw = ImageDraw.Draw(top_mask)
    top_h = int(H * 0.45)
    for y in range(top_h):
        alpha = int(220 * (1 - y / max(1, top_h)))
        top_draw.line([(0, y), (W, y)], fill=(8, 18, 45, alpha))
    img = Image.alpha_composite(img, top_mask)

    draw = ImageDraw.Draw(img)
    f_brand = _font(28)
    f_project = _font(80)
    f_info = _font(32)
    f_price = _font(72)

    _draw_brand_chip(draw, 46, 72, scale=1.05)

    mid_y = 600
    draw.rounded_rectangle([32, mid_y - 48, W - 32, mid_y + 188], radius=30, fill=(8, 18, 45, 116))
    draw.text((50, mid_y), project, font=f_project, fill=(255, 255, 255, 255))
    draw.text((50, mid_y + 110), f"{property_type} · {area}", font=f_info, fill=(255, 255, 255, 220))

    p_main, p_unit = _norm_price(price)
    price_text = p_main + (p_unit or "")
    bar_h = int(H * 0.12)
    bar_y = H - bar_h
    draw.rectangle([0, bar_y, W, H], fill=(26, 58, 143, 240))
    draw.text((60, bar_y + 56), price_text, font=f_price, fill=(246, 201, 72, 255))
    info_right = f"{area} · {property_type}"
    ib = draw.textbbox((0, 0), info_right, font=f_info)
    info_x = max(420, W - (ib[2] - ib[0]) - 60)
    draw.text((info_x, bar_y + 82), info_right, font=f_info, fill=(255, 255, 255, 245))

    out = img.convert("RGB")
    out.save(output_path, "JPEG", quality=96, optimize=True)


# ═══════════════════════════════════════════════════════════
# 统一入口（不再依赖外部图片）
# ═══════════════════════════════════════════════════════════
def generate_house_cover(
    base_image_path: str,
    output_path: str,
    project: str = "富力城",
    property_type: str = "1房1卫",
    area: str = "BKK1",
    size: str = "45㎡",
    floor: str = "8楼",
    price: str = "$680/月",
    highlights: list | None = None,
    style: str = "minimal",
) -> None:
    """
    统一封面生成入口。
    注：base_image_path 会用于照片底图；为空时回退到干净底色。
    """
    kwargs = {
        "base_image_path": base_image_path or "",
        "output_path": output_path,
        "project": project,
        "property_type": property_type,
        "area": area,
        "size": size,
        "floor": floor,
        "price": price,
        "highlights": highlights or ["实拍真房源", "中文顾问", "可预约看房"],
    }
    s = (style or "minimal").lower().strip()
    if s in ("classic", "1", "蓝卡"):
        generate_style_classic(**kwargs)
    elif s in ("minimal", "2", "极简", "白条"):
        generate_style_minimal(**kwargs)
    elif s in ("price_tag", "3", "价签", "右侧"):
        generate_style_price_tag(**kwargs)
    elif s in ("vertical", "4", "竖版", "视频"):
        generate_style_vertical(**kwargs)
    else:
        generate_style_minimal(**kwargs)


if __name__ == "__main__":
    # 测试生成
    generate_house_cover("", "output_classic.jpg", style="classic")
    generate_house_cover("", "output_minimal.jpg", style="minimal")
    generate_house_cover("", "output_price_tag.jpg", style="price_tag")
    generate_house_cover("", "output_vertical.jpg", style="vertical")
