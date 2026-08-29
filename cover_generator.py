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

def _score_image(img_path: str, *, property_type: str = "") -> Tuple[float, str]:
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

    # 6. 可解释的空间构图：客厅/客餐厅需要中央开阔、自然采光；降低餐桌局部、柜墙、床和卫生间常见的高密度局部构图。
    try:
        scene = img.resize((256, 256), Image.Resampling.LANCZOS).convert("L")
        top = scene.crop((0, 0, scene.width, scene.height // 2))
        lower = scene.crop((0, scene.height // 2, scene.width, scene.height))
        center = scene.crop((scene.width // 4, scene.height // 4, scene.width * 3 // 4, scene.height * 3 // 4))
        top_right = scene.crop((scene.width // 2, 0, scene.width * 98 // 100, scene.height * 55 // 100))
        top_mean = sum(top.getdata()) / max(1, len(list(top.getdata())))
        lower_mean = sum(lower.getdata()) / max(1, len(list(lower.getdata())))
        center_edges = center.filter(ImageFilter.FIND_EDGES)
        center_density = sum(1 for p in center_edges.getdata() if p > 25) / max(1, len(list(center_edges.getdata())))
        lower_edges = lower.filter(ImageFilter.FIND_EDGES)
        lower_density = sum(1 for p in lower_edges.getdata() if p > 25) / max(1, len(list(lower_edges.getdata())))
        top_right_edges = top_right.filter(ImageFilter.FIND_EDGES)
        top_right_density = sum(1 for p in top_right_edges.getdata() if p > 25) / max(1, len(list(top_right_edges.getdata())))
        top_right_mean = sum(top_right.getdata()) / max(1, len(list(top_right.getdata())))
        if top_mean >= 160:
            score += 8
            reasons.append("自然采光/窗景")
        elif top_mean < 115:
            score -= 5
            reasons.append("上部偏暗-扣")
        if center_density > 0.16:
            score -= 10
            reasons.append("中央局部/杂乱-扣")
        elif center_density < 0.12:
            score += 7
            reasons.append("中央开阔")
        if lower_mean < 112:
            score -= 5
            reasons.append("下部偏暗-扣")
        if lower_density > 0.22:
            score -= 6
            reasons.append("下部细碎-扣")
        kind = str(property_type or "").lower()
        is_villa = "别墅" in kind or "villa" in kind
        if not is_villa and aspect >= 1.20:
            # 横向普通公寓：避免低边缘密度、偏暗的柜体/玄关击败带窗客餐厅。
            if center_density < 0.14:
                score -= 28
                reasons.append("柜体/玄关低信息-扣")
            if 0.15 <= center_density <= 0.22 and top_right_mean >= 135 and top_right_density < 0.18:
                score += 20
                reasons.append("客餐厅/窗景空间")
            elif top_mean >= 135 and center_density >= 0.15:
                score += 8
                reasons.append("完整室内空间")
            if center_density > 0.25:
                score -= 8
                reasons.append("阳台/局部过碎-扣")
        if not is_villa and aspect < 1.20:
            # 竖向公寓：降低餐桌/局部家具，优先明亮且下部不碎的客厅全景。
            if center_density > 0.20:
                score -= 12
                reasons.append("餐桌/局部家具-扣")
            if (top_mean + lower_mean) / 2 >= 150 and lower_density <= 0.17:
                score += 18
                reasons.append("明亮客厅全景")
        if is_villa:
            # 外立面通常有更高的天空/绿植色彩与横向完整建筑轮廓；室内图不因价格或分辨率误选。
            if avg_sat >= 0.20 and top_mean >= 145:
                score += 16
                reasons.append("别墅外立面/庭院倾向")
    except Exception:
        pass

    # 7. 比例接近目标封面 4:3
    target_ratio = CANVAS_W / CANVAS_H  # 1.333
    ratio_diff = abs(aspect - target_ratio)
    if ratio_diff < 0.12:
        score += 10
        reasons.append("比例接近4:3")
    elif ratio_diff < 0.35:
        score += 4

    return score, " | ".join(reasons)


def choose_best_cover_image(images: List[str], *, property_type: str = "") -> Tuple[Optional[str], int, str]:
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

    # 对所有候选图打分；禁止“第一张横图”或“最大图”早退，避免餐厅/柜墙偶然胜出。
    scored = []
    for orig_idx, path in valid:
        score, reason = _score_image(path, property_type=property_type)
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
    # 目标用户为中文租房用户，封面不再放无必要的英文副标。
    sub_text = "金边租房"
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


def _fit_single_line_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    *,
    max_width: int,
    start_size: int,
    min_size: int,
):
    """中文封面标题单行自适应：先缩字号，仍放不下时才加省略号。"""
    value = str(text or "").strip()
    size = start_size
    font = _font(size, bold=True)
    while size > min_size:
        bbox = draw.textbbox((0, 0), value, font=font)
        if bbox[2] - bbox[0] <= max_width:
            return value, font
        size -= 2
        font = _font(size, bold=True)

    value_bbox = draw.textbbox((0, 0), value, font=font)
    if value_bbox[2] - value_bbox[0] <= max_width:
        return value, font
    clipped = value
    while clipped:
        candidate = clipped + "…"
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if bbox[2] - bbox[0] <= max_width:
            return candidate, font
        clipped = clipped[:-1]
    return "…", font


# ── 新封面：实拍底图 + 暗色半透明遮罩（无图则 #1A1A1A），居中排版、无 emoji ──
# ── 首页封面视觉规范：固定布局，按底图自动配色 ─────────────────
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
    source_type: str = "",
    source_name: str = "",
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
        source_type=source_type,
        source_name=source_name,
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
        生成手机验收版首页封面：单张实拍为主体，只叠项目、户型和月租。
        返回 (ok, report)。
        """
        if os.getenv("AUTO_HOME_COVER_ENABLED", "1").strip().lower() not in {"1", "true", "yes"}:
            return False, "home_cover_disabled"
        project_display = self._normalize_home_project(project, area)
        layout_display  = self._normalize_home_layout(layout)
        try:
            _draw_new_cover(
                output_path=output_path,
                project=project_display,
                layout=layout_display,
                area=area,
                price=price,
                size=size,
                floor=floor,
                highlights=highlights,
                base_image_path=bg_local_path,
                source_type=source_type,
                source_name=source_name,
            )
        except Exception as exc:
            return False, f"render_exec_error:{exc}"
        if not os.path.isfile(output_path):
            return False, "render_failed:acceptance_cover"
        return True, "render_ok:acceptance_cover"


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
                # 人工导入可能同时包含一张旧“封面”和若干“原图”。生成新模板时
                # 优先从原图选，避免旧价格、旧底栏或旧 Logo 被二次叠加。
                original_images = [
                    path for path in group_images
                    if "原图" in os.path.basename(str(path or ""))
                ]
                cover_candidates = original_images or group_images
                # 别墅封面固定使用组内第一张（通常是大门/外立面），保持“宏伟”第一印象。
                if self._is_villa_cover(
                    property_type=property_type or "",
                    layout=layout or "",
                    project=project or "",
                ):
                    first = cover_candidates[0]
                    if first and os.path.isfile(first):
                        chosen_image = first
                        selection_report = (
                            f"source_post_id={source_post_id} | "
                            f"组内共{len(group_images)}张 | 第1张（别墅固定首图）"
                        )
                    else:
                        chosen_image, chosen_idx, selection_reason = choose_best_cover_image(cover_candidates)
                        selection_report = (
                            f"source_post_id={source_post_id} | "
                            f"组内共{len(group_images)}张 | 首图缺失，回退：{selection_reason}"
                        )
                else:
                    chosen_image, chosen_idx, selection_reason = choose_best_cover_image(cover_candidates)
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
                    source_type=source_type or "",
                    source_name=source_name or "",
                )
            selection_report = f"{selection_report} | {render_report}".strip(" |")
        except Exception as e:
            log.error(f"[CoverGenerator] Failed for {draft_id}: {e}")
            return None, None

        # ── 写入 media_assets（先删旧封面记录，避免重复）────
        asset_id  = f"AST_{uuid.uuid4()}"
        file_hash = self._calc_hash(output_path)
        file_size = os.path.getsize(output_path)
        with Image.open(output_path) as generated_image:
            generated_width, generated_height = generated_image.size

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
                    generated_width, generated_height, file_size, "image/jpeg",
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
