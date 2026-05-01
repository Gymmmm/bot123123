#!/usr/bin/env python3
"""
cover_generator.py  —  侨联地产封面图生成器 v3
规格：1280×1280px（1:1）
模板：LOGO左上角品牌栏 + 房源实拍/渐变背景（占60-70%）+ 深蓝信息栏底部
颜色：品牌深蓝 #1A3A8F | 价格金黄 #FFD700 | 正文白 #FFFFFF | 副文浅灰 #E0E0E0

同组不拆原则：
  一个 source_post / 一个 media_group = 一套房，图片严格归属该组，不跨组取图。

新增方法：
  choose_best_cover_image(images) -> (path, index, reason)
  在当前房源组内选最适合做封面的图。
"""
import os
import re
import uuid
import hashlib
import sqlite3
import json
import random
import colorsys
import logging
import shutil
import subprocess
from typing import Optional, Tuple, List
from PIL import Image, ImageDraw, ImageFont, ImageEnhance, ImageFilter, ImageOps

# ── 日志 ─────────────────────────────────────────────────
log = logging.getLogger("cover_generator")

# ── 颜色规范 ─────────────────────────────────────────────
COLOR_BRAND_BLUE   = (26, 58, 143)      # #1A3A8F
COLOR_GOLD         = (255, 215, 0)      # #FFD700
COLOR_WHITE        = (255, 255, 255)
COLOR_LIGHT_GRAY   = (224, 224, 224)    # #E0E0E0

# ── 图片规格 ─────────────────────────────────────────────
CANVAS_W, CANVAS_H = 1280, 1280        # 1:1
LOGO_BAR_H         = 80                # 顶部品牌栏
INFO_BAR_H         = 240               # 底部信息栏（占25%）

# ── 路径配置 ─────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
COVER_DIR       = os.path.join(BASE_DIR, "media", "covers")
BG_DIRS         = [os.path.join(BASE_DIR, "assets", "backgrounds")]
DB_PATH_DEFAULT = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
os.makedirs(COVER_DIR, exist_ok=True)

# Server-side deploy root; imported DB paths use this prefix.
_SERVER_MEDIA_ROOT = "/opt/qiaolian_dual_bots"


def _remap_server_path(path: str) -> str:
    """Translate a server-absolute path to the local project equivalent."""
    if path and path.startswith(_SERVER_MEDIA_ROOT + "/"):
        return os.path.join(BASE_DIR, path[len(_SERVER_MEDIA_ROOT) + 1:])
    return path


# ── 渐变预设 ─────────────────────────────────────────────
GRADIENT_PRESETS = [
    [(15, 32, 90), (30, 60, 160)],
    [(10, 40, 80), (20, 80, 140)],
    [(25, 25, 60), (50, 50, 120)],
    [(20, 50, 80), (40, 90, 140)],
]

# ── 字体加载 ─────────────────────────────────────────────
_FONT_BOLD_CANDIDATES = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/System/Library/Fonts/STHeiti Medium.ttc",
    "/Library/Fonts/Arial Unicode.ttf",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]
_FONT_REG_CANDIDATES = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/System/Library/Fonts/STHeiti Light.ttc",
    "/Library/Fonts/Arial Unicode.ttf",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]

def _font(size: int, bold: bool = True) -> ImageFont.FreeTypeFont:
    for p in (_FONT_BOLD_CANDIDATES if bold else _FONT_REG_CANDIDATES):
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


# ══════════════════════════════════════════════════════════
# 核心方法：组内智能选最佳封面图
# ══════════════════════════════════════════════════════════

def _score_image(img_path: str) -> Tuple[float, str]:
    """
    对单张图片评分，返回 (score, reason_str)。
    分数越高越适合做封面。
    """
    try:
        img = Image.open(img_path).convert("RGB")
    except Exception as e:
        return -999.0, f"无法打开: {e}"

    w, h = img.size
    score = 0.0
    reasons = []

    # 1. 横图优先
    aspect = w / h if h > 0 else 1.0
    if aspect >= 1.3:
        score += 30
        reasons.append("横图")
    elif aspect >= 1.0:
        score += 12
        reasons.append("方图")
    else:
        score -= 15
        reasons.append("竖图-扣")

    # 2. 分辨率
    pixels = w * h
    if pixels >= 1920 * 1080:
        score += 25
        reasons.append("高清")
    elif pixels >= 1280 * 720:
        score += 15
        reasons.append("中清")
    elif pixels >= 640 * 480:
        score += 5
        reasons.append("低清")
    else:
        score -= 10
        reasons.append("过小-扣")

    # 3. 亮度（缩小后计算）
    thumb = img.resize((64, 64))
    gray = thumb.convert("L")
    px = list(gray.getdata())
    avg_brightness = sum(px) / len(px)
    if 55 <= avg_brightness <= 195:
        score += 20
        reasons.append(f"亮度正常({avg_brightness:.0f})")
    elif avg_brightness < 35:
        score -= 25
        reasons.append(f"过暗({avg_brightness:.0f})-扣")
    elif avg_brightness > 215:
        score -= 12
        reasons.append(f"过亮({avg_brightness:.0f})-扣")
    else:
        score += 5
        reasons.append(f"亮度可({avg_brightness:.0f})")

    # 4. 色彩饱和度（低饱和度可能是截图/文字图）
    try:
        rgb_px = list(thumb.getdata())
        sats = [colorsys.rgb_to_hsv(r/255, g/255, b/255)[1] for r, g, b in rgb_px]
        avg_sat = sum(sats) / len(sats)
        if avg_sat > 0.22:
            score += 15
            reasons.append(f"色彩丰富(sat={avg_sat:.2f})")
        elif avg_sat < 0.07:
            score -= 18
            reasons.append(f"疑似截图(sat={avg_sat:.2f})-扣")
        else:
            score += 4
            reasons.append(f"色彩一般(sat={avg_sat:.2f})")
    except Exception:
        pass

    # 5. 边缘复杂度（避免纯色/空白图）
    try:
        edges = thumb.filter(ImageFilter.FIND_EDGES).convert("L")
        edge_px = list(edges.getdata())
        edge_density = sum(1 for p in edge_px if p > 25) / len(edge_px)
        if edge_density > 0.12:
            score += 10
            reasons.append(f"内容丰富(edge={edge_density:.2f})")
        elif edge_density < 0.04:
            score -= 18
            reasons.append(f"内容过少(edge={edge_density:.2f})-扣")
    except Exception:
        pass

    # 6. 比例接近目标封面 4:3
    target_ratio = CANVAS_W / CANVAS_H  # 1.333
    ratio_diff = abs(aspect - target_ratio)
    if ratio_diff < 0.12:
        score += 10
        reasons.append("比例接近4:3")
    elif ratio_diff < 0.35:
        score += 4

    return score, " | ".join(reasons)


def choose_best_cover_image(images: List[str]) -> Tuple[Optional[str], int, str]:
    """
    从当前房源组的图片列表中选出最适合做封面的一张。

    严格原则：只在传入的 images 列表中选，不跨组取图。

    参数：
        images: 当前 source_post 的所有本地图片路径列表

    返回：
        (best_path, index, reason)
        - best_path: 最佳图片路径（None 表示无可用图片，退回默认背景）
        - index: 在原列表中的位置（0-based），-1 表示无
        - reason: 选择原因说明（含"第X张/共Y张 | 得分 | 原因"）
    """
    if not images:
        return None, -1, "无图片，使用默认背景"

    # 过滤：只保留本地存在的真实文件
    valid = []
    for i, path in enumerate(images):
        if not isinstance(path, str):
            continue
        if "dummy" in path or "cdn" in path or path.startswith("http"):
            continue  # 跳过假数据和远程 URL
        if path.startswith("/") and os.path.exists(path):
            valid.append((i, path))
        elif path.startswith("/"):
            log.debug(f"  图片文件不存在: {path}")

    if not valid:
        return None, -1, "所有图片路径无效或文件不存在，使用默认背景"

    # 发布封面优先第一张横图；没有横图时，用最大图居中裁切。
    largest: tuple[int, int, str, str] | None = None
    for orig_idx, path in valid:
        try:
            with Image.open(path) as img:
                w, h = img.size
        except Exception:
            continue
        pixels = w * h
        if w / max(h, 1) >= 1.25:
            return path, orig_idx, f"第{orig_idx + 1}张（共{len(images)}张）| 第一张横图"
        if largest is None or pixels > largest[0]:
            largest = (pixels, orig_idx, path, f"{w}x{h}")
    if largest:
        _, orig_idx, path, size_desc = largest
        return path, orig_idx, f"第{orig_idx + 1}张（共{len(images)}张）| 无横图，使用最大图居中裁切({size_desc})"

    # 对每张图打分
    scored = []
    for orig_idx, path in valid:
        score, reason = _score_image(path)
        scored.append((score, orig_idx, path, reason))

    # 按分数降序
    scored.sort(key=lambda x: x[0], reverse=True)

    best_score, best_orig_idx, best_path, best_reason = scored[0]

    # 如果最高分极低（所有图都不理想），退回默认背景
    if best_score < -10:
        return None, -1, f"所有图片质量不佳(最高分={best_score:.1f})，使用默认背景"

    reason_full = (
        f"第{best_orig_idx + 1}张（共{len(images)}张）| "
        f"得分={best_score:.1f} | {best_reason}"
    )
    return best_path, best_orig_idx, reason_full


# ── 背景生成 ─────────────────────────────────────────────
def _gradient_bg(w: int, h: int) -> Image.Image:
    c1, c2 = random.choice(GRADIENT_PRESETS)
    img = Image.new("RGB", (w, h))
    draw = ImageDraw.Draw(img)
    for y in range(h):
        t = y / h
        r = int(c1[0] + (c2[0] - c1[0]) * t)
        g = int(c1[1] + (c2[1] - c1[1]) * t)
        b = int(c1[2] + (c2[2] - c1[2]) * t)
        draw.line([(0, y), (w, y)], fill=(r, g, b))
    return img


def _load_bg(w: int, h: int, base_image_path: str = None) -> Image.Image:
    """优先用房源实拍图，其次素材池，最后渐变"""
    # 1. 房源实拍图
    if base_image_path and os.path.exists(base_image_path):
        try:
            img = Image.open(base_image_path).convert("RGB")
            iw, ih = img.size
            ratio = w / h
            if iw / ih > ratio:
                nw = int(ih * ratio)
                img = img.crop(((iw - nw) // 2, 0, (iw - nw) // 2 + nw, ih))
            else:
                nh = int(iw / ratio)
                img = img.crop((0, (ih - nh) // 2, iw, (ih - nh) // 2 + nh))
            img = img.resize((w, h), Image.LANCZOS)
            img = ImageEnhance.Brightness(img).enhance(0.58)
            return img
        except Exception as e:
            log.warning(f"加载底图失败: {e}")

    # 2. 素材池随机背景
    all_imgs = []
    for d in BG_DIRS:
        if os.path.isdir(d):
            for root, _, files in os.walk(d):
                for f in files:
                    if f.lower().endswith((".jpg", ".jpeg", ".png")):
                        all_imgs.append(os.path.join(root, f))
    if all_imgs:
        try:
            chosen = random.choice(all_imgs)
            img = Image.open(chosen).convert("RGB")
            iw, ih = img.size
            ratio = w / h
            if iw / ih > ratio:
                nw = int(ih * ratio)
                img = img.crop(((iw - nw) // 2, 0, (iw - nw) // 2 + nw, ih))
            else:
                nh = int(iw / ratio)
                img = img.crop((0, (ih - nh) // 2, iw, (ih - nh) // 2 + nh))
            img = img.resize((w, h), Image.LANCZOS)
            img = ImageEnhance.Brightness(img).enhance(0.58)
            return img
        except Exception:
            pass

    # 3. 渐变背景
    return _gradient_bg(w, h)


# ── 绘制 LOGO 栏 ──────────────────────────────────────────
def _draw_logo_bar(img: Image.Image, draw: ImageDraw.Draw):
    """顶部深蓝品牌栏（纯 PIL，不依赖 numpy）"""
    draw.rectangle([(0, 0), (CANVAS_W, LOGO_BAR_H)], fill=COLOR_BRAND_BLUE)
    draw.text((28, 12), "侨联地产", font=_font(38, bold=True), fill=COLOR_WHITE)
    slogan = "您在金边的自己人"
    f_sl = _font(20, bold=False)
    bbox = draw.textbbox((0, 0), slogan, font=f_sl)
    sw = bbox[2] - bbox[0]
    draw.text((CANVAS_W - sw - 28, 28), slogan, font=f_sl, fill=COLOR_LIGHT_GRAY)
    draw.line([(0, LOGO_BAR_H - 2), (CANVAS_W, LOGO_BAR_H - 2)], fill=COLOR_GOLD, width=3)


# ── 绘制信息栏 ────────────────────────────────────────────
def _draw_info_bar(img: Image.Image, draw: ImageDraw.Draw,
                   project: str, layout: str, area: str,
                   price, size: str, floor: str,
                   furniture: str, amenities: str):
    """底部深蓝信息栏"""
    bar_y = CANVAS_H - INFO_BAR_H
    draw.rectangle([(0, bar_y), (CANVAS_W, CANVAS_H)], fill=COLOR_BRAND_BLUE)
    draw.line([(0, bar_y), (CANVAS_W, bar_y)], fill=COLOR_GOLD, width=3)

    pad = 32
    y = bar_y + 20

    # 第一行：楼盘名 · 户型
    title_str = "  ·  ".join(filter(None, [project or "精品房源", layout]))
    draw.text((pad, y), f"🏠 {title_str}", font=_font(40, bold=True), fill=COLOR_WHITE)
    y += 56

    # 第二行：面积 | 楼层 | 区域
    parts = []
    if size:
        parts.append(f"📐 {size}")
    if floor:
        parts.append(f"🏢 {floor}")
    if area:
        parts.append(f"📍 {area}")
    left_text = "  |  ".join(parts)
    if left_text:
        draw.text((pad, y), left_text, font=_font(28, bold=False), fill=COLOR_LIGHT_GRAY)

    # 价格（右对齐金黄）
    if price:
        try:
            price_str = f"💰 ${int(price):,}/月"
        except (ValueError, TypeError):
            price_str = f"💰 {price}/月"
        f_price = _font(34, bold=True)
        bbox = draw.textbbox((0, 0), price_str, font=f_price)
        pw = bbox[2] - bbox[0]
        draw.text((CANVAS_W - pw - pad, y - 4), price_str, font=f_price, fill=COLOR_GOLD)
    y += 46

    # 第三行：家具 + 配套
    detail_parts = []
    if furniture:
        detail_parts.append(f"🛋 {furniture}")
    if amenities:
        detail_parts.append(f"🏊 {amenities}")
    if detail_parts:
        draw.text((pad, y), "   ".join(detail_parts), font=_font(26, bold=False), fill=COLOR_LIGHT_GRAY)

    # 右下角小字
    draw.text((CANVAS_W - 260, CANVAS_H - 30),
              "侨联地产 · 实拍房源",
              font=_font(20, bold=False), fill=COLOR_LIGHT_GRAY)


def _apply_cover_gradient(base: Image.Image) -> Image.Image:
    """顶部 10% + 底部 15% 轻微暗化，保证品牌和信息卡边缘可读。"""
    img = base.convert("RGBA")
    w, h = img.size
    shade = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(shade)
    top_h = max(1, int(h * 0.10))
    bottom_h = max(1, int(h * 0.15))
    for y in range(top_h):
        alpha = int(72 * (1 - y / top_h))
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, alpha))
    for y in range(h - bottom_h, h):
        alpha = int(72 * ((y - (h - bottom_h)) / bottom_h))
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, alpha))
    return Image.alpha_composite(img, shade)


def _apply_frosted_panel(
    base: Image.Image,
    box: tuple[int, int, int, int],
    *,
    radius: int,
    blur_radius: int,
    tint_rgb: tuple[int, int, int],
    tint_alpha: int,
    outline: Optional[Tuple[int, int, int, int]] = None,
) -> Image.Image:
    x1, y1, x2, y2 = [int(v) for v in box]
    region = base.crop((x1, y1, x2, y2)).convert("RGBA")
    region = region.filter(ImageFilter.GaussianBlur(max(1, blur_radius)))
    tint_layer = Image.new("RGBA", region.size, (*tint_rgb, max(0, min(255, tint_alpha))))
    region = Image.alpha_composite(region, tint_layer)

    mask = Image.new("L", region.size, 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, region.size[0] - 1, region.size[1] - 1), radius=radius, fill=255)
    base.paste(region, (x1, y1), mask)

    sheen = Image.new("RGBA", base.size, (0, 0, 0, 0))
    sheen_draw = ImageDraw.Draw(sheen)
    sheen_draw.rounded_rectangle(
        box,
        radius=radius,
        fill=(255, 255, 255, 14),
        outline=outline,
        width=max(1, int(radius * 0.10)) if outline else 0,
    )
    return Image.alpha_composite(base, sheen)


def _draw_house_outline_mark(
    draw: ImageDraw.ImageDraw,
    *,
    x: int,
    y: int,
    size: int,
    fill: tuple[int, int, int, int],
    shadow: Optional[Tuple[int, int, int, int]] = None,
) -> tuple[int, int]:
    w = max(14, int(size))
    h = max(10, int(w * 0.72))
    line_w = max(2, int(w * 0.11))
    roof = [(x, y + int(h * 0.42)), (x + w // 2, y), (x + w, y + int(h * 0.42))]
    left = (x + int(w * 0.16), y + int(h * 0.42))
    right = (x + int(w * 0.84), y + int(h * 0.42))
    bottom_y = y + h
    door_left = x + int(w * 0.42)
    door_right = x + int(w * 0.58)
    door_top = y + int(h * 0.56)

    def _paint(offset_x: int, offset_y: int, color: tuple[int, int, int, int]) -> None:
        pts = [(px + offset_x, py + offset_y) for px, py in roof]
        draw.line(pts, fill=color, width=line_w, joint="curve")
        draw.line(
            [
                (left[0] + offset_x, left[1] + offset_y),
                (left[0] + offset_x, bottom_y + offset_y),
                (right[0] + offset_x, bottom_y + offset_y),
                (right[0] + offset_x, right[1] + offset_y),
            ],
            fill=color,
            width=line_w,
        )
        draw.line(
            [
                (door_left + offset_x, bottom_y + offset_y),
                (door_left + offset_x, door_top + offset_y),
                (door_right + offset_x, door_top + offset_y),
                (door_right + offset_x, bottom_y + offset_y),
            ],
            fill=color,
            width=max(1, line_w - 1),
        )

    if shadow is not None:
        _paint(1, 1, shadow)
    _paint(0, 0, fill)
    return w, h


def _draw_compact_brand_chip(base: Image.Image, x: int, y: int, *, scale: float = 1.0) -> Image.Image:
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    # Ref 风格：固定蓝底品牌牌匾（避免玻璃态随底图漂色）
    font_cn = _font(max(17, int(22 * scale)), bold=True)
    font_sub = _font(max(8, int(10 * scale)), bold=False)
    stroke_w = 0
    pad_x = max(10, int(14 * scale))
    pad_y = max(7, int(10 * scale))
    logo_w = max(16, int(21 * scale))
    logo_h = max(12, int(logo_w * 0.72))
    logo_gap = max(5, int(6 * scale))
    line_gap = max(1, int(2 * scale))
    brand_text = "侨联地产"
    sub_text = "QIAO LIAN PROPERTY"
    b_brand = draw.textbbox((0, 0), brand_text, font=font_cn, stroke_width=stroke_w)
    b_sub = draw.textbbox((0, 0), sub_text, font=font_sub)
    title_w = b_brand[2] - b_brand[0]
    title_h = b_brand[3] - b_brand[1]
    sub_w = b_sub[2] - b_sub[0]
    sub_h = b_sub[3] - b_sub[1]
    text_w = max(title_w, sub_w)
    text_h = title_h + sub_h + line_gap
    box_w = pad_x * 2 + logo_w + logo_gap + text_w
    box_h = pad_y * 2 + max(logo_h, text_h)
    box = (x, y, x + box_w, y + box_h)
    draw.rounded_rectangle(
        box,
        radius=max(11, int(14 * scale)),
        fill=(27, 86, 196, 236),
        outline=(176, 205, 255, 120),
        width=max(1, int(1.2 * scale)),
    )
    center_y = y + box_h // 2
    icon_x = x + pad_x
    _draw_house_outline_mark(
        draw,
        x=icon_x,
        y=center_y - logo_h // 2,
        size=logo_w,
        fill=(235, 243, 255, 255),
        shadow=(15, 45, 103, 110),
    )
    text_x = icon_x + logo_w + logo_gap - b_brand[0]
    top_y = center_y - text_h // 2
    title_y = top_y - b_brand[1]
    sub_y = top_y + title_h + line_gap - b_sub[1]
    draw.text(
        (text_x, title_y),
        brand_text,
        font=font_cn,
        fill=(247, 251, 255, 255),
        stroke_width=stroke_w,
    )
    draw.text((text_x, sub_y), sub_text, font=font_sub, fill=(223, 236, 255, 242))
    return Image.alpha_composite(base, overlay)


# ── 新封面：实拍底图 + 暗色半透明遮罩（无图则 #1A1A1A），居中排版、无 emoji ──
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
) -> None:
    """横向封面：ref 系品牌角标 + 底部信息卡 + 右侧价格牌。"""
    W, H = 1280, 960

    layout = (layout or "").strip() or ""
    area = (area or "").strip() or ""

    price_text = "价格待确认"
    if price is not None and str(price).strip():
        p_str = str(price).strip()
        if p_str.endswith("/月"):
            price_text = p_str
        elif p_str.replace("$", "").replace(",", "").replace(".", "", 1).isdigit():
            pv = float(p_str.replace("$", "").replace(",", ""))
            price_text = f"${int(pv)}/月" if pv == int(pv) else f"${pv:.0f}/月"
        else:
            price_text = p_str if "月" in p_str else f"{p_str}/月"

    # 底图：实拍 fit + 轻微暗化；否则纯色 #1A1A1A
    img: Image.Image
    if base_image_path and os.path.isfile(base_image_path):
        try:
            bg = Image.open(base_image_path).convert("RGB")
            bg = ImageOps.fit(bg, (W, H), method=Image.Resampling.LANCZOS)
            base = bg.convert("RGBA")
            img = _apply_cover_gradient(base)
        except Exception as e:
            log.warning("封面实拍底图失败，改用纯色底: %s", e)
            img = Image.new("RGBA", (W, H), (26, 26, 26, 255))
    else:
        img = Image.new("RGBA", (W, H), (26, 26, 26, 255))

    draw = ImageDraw.Draw(img)

    img = _draw_compact_brand_chip(img, 18, 18, scale=0.92)
    draw = ImageDraw.Draw(img)

    panel_h = 176
    px1, py1 = 22, H - panel_h - 20
    px2, py2 = W - 22, H - 20
    img = _apply_frosted_panel(
        img,
        (px1, py1, px2, py2),
        radius=24,
        blur_radius=14,
        tint_rgb=(248, 251, 255),
        tint_alpha=228,
        outline=(198, 212, 236, 238),
    )
    draw = ImageDraw.Draw(img)

    title_main = project.strip() if project else area
    title_tail = layout.strip() if layout else "精选房源"
    title = f"{title_main}｜{title_tail}" if title_main else title_tail
    meta = "｜".join([x for x in [area, size, floor] if str(x).strip()]) or "实拍房源"
    hs = [str(h).strip() for h in (highlights or []) if str(h).strip()]
    hline = hs[0] if hs else "中文顾问｜可预约看房"

    f_title = _font(54, bold=True)
    f_meta = _font(28, bold=False)
    f_hint = _font(25, bold=False)
    draw.text((px1 + 24, py1 + 22), title[:20], font=f_title, fill=(18, 46, 95, 255))
    draw.text((px1 + 24, py1 + 88), meta[:28], font=f_meta, fill=(68, 96, 145, 245))
    draw.text((px1 + 24, py1 + 126), hline[:34], font=f_hint, fill=(52, 82, 132, 236))

    # 右侧价格牌（贴近 ref 视觉锚点）
    if price_text and price_text != "价格待确认":
        f_price = _font(50, bold=True)
        bbox = draw.textbbox((0, 0), price_text, font=f_price)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        pad_x, pad_y = 26, 16
        x2, y2 = W - 34, H - 40
        x1, y1 = x2 - tw - pad_x * 2, y2 - th - pad_y * 2
        img = _apply_frosted_panel(
            img,
            (x1, y1, x2, y2),
            radius=18,
            blur_radius=10,
            tint_rgb=(10, 31, 68),
            tint_alpha=244,
            outline=(146, 176, 228, 200),
        )
        draw = ImageDraw.Draw(img)
        f_label = _font(19, bold=False)
        draw.text((x1 + 18, y1 + 12), "租金", font=f_label, fill=(207, 222, 248, 235))
        draw.text(
            (x1 + pad_x - bbox[0], y1 + pad_y + 8 - bbox[1]),
            price_text,
            font=f_price,
            fill=(243, 248, 255, 255),
        )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    img.convert("RGB").save(output_path, "JPEG", quality=94, optimize=True)


# ── 主生成函数 ────────────────────────────────────────────
def generate_house_cover(
    output_path: str,
    project: str = "",
    property_type: str = "",
    area: str = "",
    size: str = "",
    floor: str = "",
    price=None,
    layout: str = "",
    highlights: list = None,
    base_image_path: str = None,
) -> str:
    """生成标准1280×960封面图，返回 output_path。"""
    if highlights is None:
        highlights = []

    _draw_new_cover(
        output_path=output_path,
        project=project,
        layout=layout or property_type,
        area=area,
        price=price,
        size=size,
        floor=floor,
        highlights=highlights,
        base_image_path=base_image_path,
    )
    return output_path


# ══════════════════════════════════════════════════════════
# CoverGenerator 类（同组不拆版 v3）
# ══════════════════════════════════════════════════════════
class CoverGenerator:
    """
    封面图生成器（同组不拆版）。

    选图原则：
      - 通过 draft_id → source_post_id → raw_images_json 获取该组图片
      - 调用 choose_best_cover_image() 在组内选最佳封面图
      - 严格不跨组取图
    """

    def __init__(self, db_path: str = DB_PATH_DEFAULT):
        self.db_path = db_path

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _calc_hash(self, path: str) -> str:
        try:
            with open(path, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception:
            return ""

    def _get_source_post_images(self, source_post_id) -> List[str]:
        """
        获取该 source_post（数据库行 id）下的本地图片路径列表。
        优先 media_assets（采集器已下载到磁盘），再回退 raw_images_json 中的本地路径。
        忽略 http(s) 链接：choose_best_cover_image 只接受本地文件。
        服务器绝对路径（/opt/qiaolian_dual_bots/...）自动重映射到本地项目目录。
        """
        if not source_post_id:
            return []
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """SELECT local_path FROM media_assets
                   WHERE owner_type='source_post' AND owner_ref_id=?
                     AND asset_type='photo' AND local_path IS NOT NULL AND local_path != ''
                   ORDER BY sort_order ASC, id ASC""",
                (str(source_post_id),),
            ).fetchall()
            paths = []
            for r in rows:
                lp = _remap_server_path(r["local_path"])
                if lp and os.path.isfile(lp):
                    paths.append(lp)
            if paths:
                return paths

            row = conn.execute(
                "SELECT raw_images_json FROM source_posts WHERE id = ?",
                (source_post_id,),
            ).fetchone()
        finally:
            conn.close()

        if not row or not row["raw_images_json"]:
            return []
        try:
            imgs = json.loads(row["raw_images_json"])
            out = []
            for x in imgs:
                path = None
                if isinstance(x, str):
                    path = x.strip()
                elif isinstance(x, dict):
                    path = (x.get("local_path") or x.get("path") or "").strip()
                if not path or path.startswith("http"):
                    continue
                path = _remap_server_path(path)
                if os.path.isfile(path):
                    out.append(path)
            return out
        except Exception:
            return []

    def _normalize_home_project(self, project: str, area: str) -> str:
        """中文频道首页标题清洗：避免把英文长句/原始抓取标题直接放进封面。"""
        p = str(project or "").strip()
        area_s = str(area or "").strip()
        if self._is_missing_text(area_s):
            area_s = ""
        if self._is_missing_text(p):
            p = ""
        if p:
            for token in ("🇨🇳", "🌵", "啊雷莎", "阿雷莎"):
                p = p.replace(token, " ")
            p = p.replace("【", "").replace("】", " ").replace("[", "").replace("]", " ")
            p = re.split(r"[|｜/]+", p)[0].strip()
            p = re.sub(r"^\s*\d{3,4}(?!米)", "", p)
            p = re.sub(r"\s+", " ", p).strip(" ·-")

        if not p:
            return f"{area_s}优选房源" if area_s else "精选房源"

        p_low = p.lower()
        looks_raw_english = bool(
            re.search(r"(for rent|apartment|bedroom|studio|condo|r\d{3,})", p_low)
        )
        if looks_raw_english or len(p) > 22:
            return f"{area_s}优选房源" if area_s else "精选房源"
        return p

    def _is_missing_text(self, value: str) -> bool:
        v = str(value or "").strip().lower()
        if not v:
            return True
        if v in {"-", "--", "---", "—", "——", "n/a", "na", "none", "null"}:
            return True
        if any(tok in v for tok in ("待确认", "未知", "未填", "无数据")):
            return True
        return False

    def _normalize_home_layout(self, layout: str) -> str:
        l = str(layout or "").strip()
        if self._is_missing_text(l):
            return "户型可咨询"
        low = l.lower()
        if "studio" in low:
            return "Studio"
        m = re.search(r"(\d+)\s*bed(room)?", low)
        if m:
            return f"{m.group(1)}房"
        return l

    def _normalize_home_size(self, size: str) -> str:
        s = str(size or "").strip()
        if self._is_missing_text(s):
            return "面积可咨询"
        s = s.replace("m²", "㎡").replace("M²", "㎡").replace("m2", "㎡").replace("M2", "㎡")
        if re.fullmatch(r"\d+(\.\d+)?", s):
            v = float(s)
            return f"{int(v)}㎡" if v.is_integer() else f"{v:.1f}㎡"
        if "㎡" in s:
            return s
        return s

    def _normalize_home_floor(self, floor: str) -> str:
        f = str(floor or "").strip()
        if self._is_missing_text(f):
            return "楼层可咨询"
        # 纯数字（如"45"）自动补"楼"字
        if re.fullmatch(r"\d+", f):
            return f"{f}楼"
        return f

    def _is_villa_cover(self, *, property_type: str, layout: str, project: str) -> bool:
        text = " ".join(
            [
                str(property_type or "").lower(),
                str(layout or "").lower(),
                str(project or "").lower(),
            ]
        )
        return ("别墅" in text) or ("villa" in text)

    def _pick_home_template_kind(
        self,
        *,
        draft_id: str = "",
        source_post_id=None,
        source_type: str = "",
        source_name: str = "",
        layout: str,
        price,
        property_type: str = "",
        project: str = "",
    ) -> str:
        """
        选择首页封面模板。
        默认全部走 hero_collage（纯 Pillow，无需 Chromium）。
        旧模板仍可通过环境变量强制开启，但不再自动分流。
        """
        _VALID_KINDS = {"hero_collage", "right_price_fixed", "villa_premium", "dark_glass"}

        # 强制指定（环境变量）
        force_kind = os.getenv("HOME_COVER_FORCE_KIND", "").strip().lower()
        if force_kind in _VALID_KINDS:
            return force_kind

        # 样式槽（旧兼容）
        style_slots = {
            "s1": "right_price_fixed",
            "s2": "villa_premium",
            "s3": "dark_glass",
            "s4": "right_price_fixed",
        }
        slot = os.getenv("HOME_STYLE_SLOT", "").strip().lower()
        if slot in style_slots:
            return style_slots[slot]

        # 明确指定的自动封面种类
        preferred = os.getenv("AUTO_HOME_COVER_KIND", "").strip().lower()
        if preferred in _VALID_KINDS:
            return preferred

        # 微信来源
        normalized_source_type = (source_type or "").strip().lower()
        normalized_source_name = (source_name or "").strip().lower()
        if normalized_source_type in {"wechat_note", "wechat_manual", "wechat_import"} \
                or "wechat" in normalized_source_name:
            wechat_kind = os.getenv("WECHAT_HOME_COVER_KIND", "").strip().lower()
            if wechat_kind in _VALID_KINDS:
                return wechat_kind
            # 微信来源也默认 hero_collage
            return "hero_collage"

        # 默认：hero_collage（不再在旧模板间随机分流）
        return "hero_collage"

    def _render_home_cover(
        self,
        *,
        output_path: str,
        project: str,
        property_type: str,
        layout: str,
        area: str,
        size: str,
        floor: str,
        price,
        highlights: list,
        bg_local_path: Optional[str] = None,
        source_images: Optional[List[str]] = None,
        draft_id: str = "",
        source_post_id=None,
        source_type: str = "",
        source_name: str = "",
    ) -> Tuple[bool, str]:
        """
        使用 tools/render_blue_card_template.py 生成首页封面。
        kind=hero_collage 时传入 --hero-img / --thumb1~3。
        返回 (ok, report)。
        """
        if os.getenv("AUTO_HOME_COVER_ENABLED", "1").strip().lower() not in {"1", "true", "yes"}:
            return False, "home_cover_disabled"

        render_script = os.path.join(BASE_DIR, "tools", "render_blue_card_template.py")
        if not os.path.isfile(render_script):
            return False, "render_script_missing"

        py_exec = os.path.join(BASE_DIR, ".venv", "bin", "python")
        if not os.path.isfile(py_exec):
            py_exec = shutil.which("python3") or "python3"

        clean_hl = [
            str(x).strip()
            for x in (highlights or [])
            if str(x).strip() and not self._is_missing_text(str(x))
        ]
        while len(clean_hl) < 3:
            clean_hl.append(["实拍房源", "中文顾问", "可预约看房"][len(clean_hl)])
        project_display = self._normalize_home_project(project, area)
        layout_display  = self._normalize_home_layout(layout)
        size_display    = self._normalize_home_size(size)
        floor_display   = self._normalize_home_floor(floor)
        kind = self._pick_home_template_kind(
            draft_id=draft_id,
            source_post_id=source_post_id,
            source_type=source_type or "",
            source_name=source_name or "",
            layout=layout_display,
            price=price,
            property_type=property_type,
            project=project_display,
        )

        # 锁死 hero_collage 默认尺寸 1280×960
        try:
            default_w = 1280
            default_h = 960
            viewport_w = int(os.getenv("HOME_COVER_W", str(default_w)))
            viewport_h = int(os.getenv("HOME_COVER_H", str(default_h)))
        except Exception:
            viewport_w, viewport_h = 1280, 960

        price_arg = "面议"
        try:
            raw_price = str(price or "").replace("$", "").replace(",", "").replace("/月", "").strip()
            if raw_price:
                price_num = float(raw_price)
                if price_num > 0:
                    price_arg = str(int(price_num) if price_num.is_integer() else price_num)
        except Exception:
            if str(price or "").strip():
                price_arg = str(price).strip()

        cmd = [
            py_exec,
            render_script,
            "--kind",    kind,
            "--w",       str(viewport_w),
            "--h",       str(viewport_h),
            "--project", (project_display or "精选房源"),
            "--layout",  (layout_display  or "户型可咨询"),
            "--area",    (area            or "金边"),
            "--size",    (size_display    or "面积可咨询"),
            "--floor",   (floor_display   or "条件可沟通"),
            "--price",   price_arg,
            "--h1",      clean_hl[0],
            "--h2",      clean_hl[1],
            "--h3",      clean_hl[2],
            "--out",     output_path,
        ]

        if kind == "hero_collage":
            # 主图：bg_local_path（组内最佳图）
            if bg_local_path and os.path.isfile(bg_local_path):
                cmd.extend(["--hero-img", bg_local_path])

            # thumb1/2/3：从 source_images 中取不同于主图的其他图
            imgs = [p for p in (source_images or []) if p and os.path.isfile(p)]
            # 排除主图，保留其他；不足时重复使用主图兜底
            other_imgs = [p for p in imgs if p != bg_local_path] or imgs
            fallback   = bg_local_path or (imgs[0] if imgs else None)
            for flag, idx in [("--thumb1", 0), ("--thumb2", 1), ("--thumb3", 2)]:
                src = other_imgs[idx] if idx < len(other_imgs) else fallback
                if src and os.path.isfile(src):
                    cmd.extend([flag, src])
        elif bg_local_path and os.path.isfile(bg_local_path):
            cmd.extend(["--bg-local", bg_local_path])

        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60,
                check=False,
            )
        except Exception as exc:
            return False, f"render_exec_error:{exc}"

        if proc.returncode != 0 or not os.path.isfile(output_path):
            tail = (proc.stderr or proc.stdout or "").strip().replace("\n", " ")
            tail = tail[:180] if tail else "unknown"
            return False, f"render_failed:{kind}:{tail}"
        return True, f"render_ok:{kind}"


    def generate_for_draft(self, draft_id: str, base_image_path: str = None) -> tuple:
        """
        为指定 draft_id 生成封面图。

        选图逻辑（同组不拆）：
          1. 若调用方传入 base_image_path，直接使用（最高优先级）
          2. 否则从 draft → source_post 的图片组中，用 choose_best_cover_image() 选最佳图
          3. 若该组无可用图，退回默认背景

        返回 (media_asset_db_id, local_path)，失败返回 (None, None)。
        """
        conn = self._get_conn()
        try:
            row = conn.execute(
                """SELECT d.id, d.source_post_id, d.price, d.layout, d.size, d.floor,
                          d.project, d.area, d.property_type, d.highlights,
                          sp.source_type AS source_type,
                          sp.source_name AS source_name
                   FROM drafts d
                   LEFT JOIN source_posts sp ON sp.id = d.source_post_id
                   WHERE d.draft_id = ?""",
                (draft_id,),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            log.warning(f"[CoverGenerator] Draft {draft_id} not found.")
            return None, None

        draft_db_id    = row["id"]
        source_post_id = row["source_post_id"]
        price          = row["price"]
        layout         = row["layout"]
        size           = row["size"]
        floor          = row["floor"]
        project        = row["project"]
        area           = row["area"]
        property_type  = row["property_type"]
        source_type    = row["source_type"]
        source_name    = row["source_name"]

        try:
            highlights = json.loads(row["highlights"]) if row["highlights"] else []
        except Exception:
            highlights = []

        # ── 同组不拆选图 ─────────────────────────────────
        chosen_image     = base_image_path
        selection_report = ""

        if not chosen_image:
            group_images = self._get_source_post_images(source_post_id)
            if group_images:
                # 别墅封面固定使用组内第一张（通常是大门/外立面），保持“宏伟”第一印象。
                if self._is_villa_cover(
                    property_type=property_type or "",
                    layout=layout or "",
                    project=project or "",
                ):
                    first = group_images[0]
                    if first and os.path.isfile(first):
                        chosen_image = first
                        selection_report = (
                            f"source_post_id={source_post_id} | "
                            f"组内共{len(group_images)}张 | 第1张（别墅固定首图）"
                        )
                    else:
                        chosen_image, chosen_idx, selection_reason = choose_best_cover_image(group_images)
                        selection_report = (
                            f"source_post_id={source_post_id} | "
                            f"组内共{len(group_images)}张 | 首图缺失，回退：{selection_reason}"
                        )
                else:
                    chosen_image, chosen_idx, selection_reason = choose_best_cover_image(group_images)
                    selection_report = (
                        f"source_post_id={source_post_id} | "
                        f"组内共{len(group_images)}张 | {selection_reason}"
                    )
            else:
                selection_report = (
                    f"source_post_id={source_post_id} | 无图片，使用默认背景"
                )

        log.info(f"[CoverGenerator] {draft_id} 选图: {selection_report}")
        print(f"  📸 {selection_report}")

        # ── 生成封面图 ───────────────────────────────────
        file_name   = f"cover_{draft_id}.jpg"
        output_path = os.path.join(COVER_DIR, file_name)

        try:
            ok, render_report = self._render_home_cover(
                output_path=output_path,
                project=project or "",
                property_type=property_type or "",
                layout=layout or property_type or "",
                area=area or "",
                size=size or "",
                floor=floor or "",
                price=price,
                highlights=highlights,
                bg_local_path=chosen_image,
                source_images=group_images if group_images else [],
                draft_id=draft_id,
                source_post_id=source_post_id,
                source_type=source_type or "",
                source_name=source_name or "",
            )
            if not ok:
                generate_house_cover(
                    output_path=output_path,
                    project=project or "",
                    property_type=property_type or "",
                    area=area or "",
                    size=size or "",
                    floor=floor or "",
                    price=price,
                    layout=layout or "",
                    highlights=highlights,
                    base_image_path=chosen_image,
                )
            selection_report = f"{selection_report} | {render_report}".strip(" |")
        except Exception as e:
            log.error(f"[CoverGenerator] Failed for {draft_id}: {e}")
            return None, None

        # ── 写入 media_assets（先删旧封面记录，避免重复）────
        asset_id  = f"AST_{uuid.uuid4()}"
        file_hash = self._calc_hash(output_path)
        file_size = os.path.getsize(output_path)

        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM media_assets WHERE owner_type='draft' AND owner_ref_id=? AND is_cover=1",
                (draft_db_id,),
            )
            cursor.execute(
                """INSERT INTO media_assets (
                    asset_id, owner_type, owner_ref_id, owner_ref_key,
                    asset_type, source_type, local_path, file_url, file_hash,
                    media_type, is_watermarked, is_cover, sort_order,
                    width, height, file_size, mime_type, meta_json, status,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (
                    asset_id, "draft", draft_db_id, draft_id,
                    "image", "generated", output_path,
                    f"/media/covers/{file_name}", file_hash,
                    "photo", 1, 1, 0,
                    CANVAS_W, CANVAS_H, file_size, "image/jpeg",
                    json.dumps({
                        "generated_from_draft_id": draft_id,
                        "source_post_id": source_post_id,
                        "selection_report": selection_report,
                        "base_image": chosen_image or "default_bg",
                    }),
                    "active",
                ),
            )
            conn.commit()
            media_asset_db_id = cursor.lastrowid
        except sqlite3.Error as e:
            log.error(f"[CoverGenerator] DB error: {e}")
            conn.rollback()
            return None, None
        finally:
            conn.close()

        # ── 更新 drafts.cover_asset_id ───────────────────
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE drafts SET cover_asset_id=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (media_asset_db_id, draft_id),
            )
            conn.commit()
        finally:
            conn.close()

        return media_asset_db_id, output_path


# ── 本地测试 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    print("="*60)
    print("测试 choose_best_cover_image()")
    print("="*60)

    test_images = []
    photos_dir = os.path.join(BASE_DIR, "media", "photos", "jinbianfangchanzushou")
    if os.path.isdir(photos_dir):
        for f in sorted(os.listdir(photos_dir))[:9]:
            if f.endswith(".jpg"):
                test_images.append(os.path.join(photos_dir, f))

    if test_images:
        print(f"\n测试图片组（{len(test_images)}张）：")
        for i, p in enumerate(test_images):
            score, reason = _score_image(p)
            print(f"  [{i}] {os.path.basename(p)}  得分={score:.1f}  {reason}")
        print()
        best_path, best_idx, reason = choose_best_cover_image(test_images)
        print(f"最终选图：")
        print(f"  路径：{best_path}")
        print(f"  说明：{reason}")

        out = generate_house_cover(
            output_path="/tmp/cover_test_v3.jpg",
            project="炳发城",
            layout="5房6卫",
            area="一号路",
            price=1800,
            size="6m×15m",
            floor="独栋",
            highlights=["家具家电齐全", "独立车库"],
            base_image_path=best_path,
        )
        print(f"\n封面图已生成：{out}")
    else:
        print("未找到测试图片，使用渐变背景")
        out = generate_house_cover(
            output_path="/tmp/cover_test_v3.jpg",
            project="香格里拉",
            layout="2+1房",
            area="钻石岛",
            price=900,
        )
        print(f"封面图已生成：{out}")
