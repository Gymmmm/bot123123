#!/usr/bin/env python3
"""
Render listing poster to image.

Modes:
    hero_collage  — Pure-Pillow composite: 1 hero + 3 thumbs + info panel + price card.
                    No Chromium needed. Default for auto-publishing.

HTML-template modes (Chromium required):
    right_price_fixed: `templates/03_右侧价格牌_fixed_v1_template_render.html`
    dark_glass:        `templates/06_暗夜玻璃_template_render.html`
    villa_premium:     `templates/12_别墅高级风_template_render.html`

Legacy kinds are still accepted as aliases and will fall back to `right_price_fixed`.
"""

from __future__ import annotations

import argparse
import base64
import html
import mimetypes
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps


ROOT = Path(__file__).resolve().parents[1]
RIGHT_PRICE_FIXED_TEMPLATE = ROOT / "templates" / "03_右侧价格牌_fixed_v1_template_render.html"
DARK_GLASS_TEMPLATE = ROOT / "templates" / "06_暗夜玻璃_template_render.html"
VILLA_PREMIUM_TEMPLATE = ROOT / "templates" / "12_别墅高级风_template_render.html"

KIND_ALIASES = {
    "blue_card": "right_price_fixed",
    "white_bar": "right_price_fixed",
    "right_price": "right_price_fixed",
    "magazine_white": "right_price_fixed",
    "metro_panel": "right_price_fixed",
    "lite_strip": "right_price_fixed",
    "portrait_luxe": "right_price_fixed",
}


# ─────────────────────────────────────────────────────────────────────────────
# hero_collage 参数表（Canvas 1280×960）
# ─────────────────────────────────────────────────────────────────────────────
HERO_COLLAGE_DEFAULTS: dict = {
    # Canvas
    "canvas_w": 1280,
    "canvas_h": 960,
    "bg_color": "#0F2538",

    # Hero image area (top)
    "hero_x": 0,
    "hero_y": 0,
    "hero_w": 1280,
    "hero_h": 650,

    # Bottom thumbnails
    "thumb_area_x": 0,
    "thumb_area_y": 650,
    "thumb_area_w": 1280,
    "thumb_area_h": 310,
    "thumb_gap": 6,
    "thumb_count": 3,

    # Left info panel (semi-transparent, sits on hero zone)
    "info_panel_x": 36,
    "info_panel_y": 36,
    "info_panel_w": 520,
    "info_panel_h": 480,
    "info_panel_radius": 22,
    "info_panel_opacity": 0.58,
    "info_panel_color": "#071827",

    # Code badge (inside panel, top)
    "code_badge_x": 48,
    "code_badge_y": 48,
    "code_badge_bg": "#F6D89A",
    "code_badge_text": "#08233A",
    "code_badge_radius": 14,
    "code_badge_font_size": 28,

    # Title (project name)
    "title_x": 48,
    "title_y": 120,
    "title_font_size": 50,
    "title_color": "#FFFFFF",
    "title_shadow": True,

    # Subtitle (layout)
    "subtitle_font_size": 32,
    "subtitle_color": "#E8F4FF",

    # Info rows (area / size / floor)
    "info_font_size": 27,
    "info_color": "#D8EAF8",
    "icon_color": "#F6D89A",
    "line_gap": 44,

    # Highlight tags (h1/h2/h3)
    "tag_font_size": 22,
    "tag_color": "#F6D89A",
    "tag_bg": "#0B2A44",
    "tag_radius": 10,

    # Price card (right side of hero zone)
    "price_card_w": 256,
    "price_card_h": 112,
    "price_card_x": 988,
    "price_card_y": 502,
    "price_card_bg": "#0B2A44",
    "price_card_border": "#F6D89A",
    "price_card_radius": 18,
    "price_font_size": 52,
    "price_color": "#F6D89A",

    # Brand chip (right-top of hero zone)
    "brand_x": 968,
    "brand_y": 52,

    # Thumb labels
    "thumb_label_enabled": True,
    "thumb_label_bg": "#163A5A",
    "thumb_label_opacity": 0.78,
    "thumb_label_color": "#F6D89A",
    "thumb_label_font_size": 18,
    "thumb_label_h": 30,

    # Output
    "jpg_quality": 90,
}

# ─────────────────────────────────────────────────────────────────────────────
# hero_collage 字体候选路径（与 cover_generator.py 保持一致）
# ─────────────────────────────────────────────────────────────────────────────
_HC_FONT_BOLD = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]
_HC_FONT_REG = [
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]


def _hc_font(size: int, bold: bool = True) -> ImageFont.FreeTypeFont:
    """加载最优中文字体；全部失败时回退到 PIL 默认字体。"""
    for p in (_HC_FONT_BOLD if bold else _HC_FONT_REG):
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _hex_rgb(hex_color: str) -> tuple[int, int, int]:
    """#RRGGBB → (R, G, B)"""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = h[0]*2 + h[1]*2 + h[2]*2
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _clean(value: object, fallback: str = "") -> str:
    """清除 None / null / nan / 空值，避免出现在封面上。"""
    v = str(value or "").strip()
    if not v or v.lower() in {"none", "null", "nan", "-", "--", "—", "——", "n/a"}:
        return fallback
    return v


def _crop_center(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """居中等比裁切到目标尺寸，不变形。"""
    iw, ih = img.size
    scale = max(target_w / iw, target_h / ih)
    nw, nh = int(iw * scale + 0.5), int(ih * scale + 0.5)
    img = img.resize((nw, nh), Image.Resampling.LANCZOS)
    left = (nw - target_w) // 2
    top  = (nh - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))


def _load_img_safe(path: str | None, w: int, h: int, fallback_color: str = "#0F2538") -> Image.Image:
    """
    安全加载图片并裁切到 (w, h)。
    找不到文件或加载失败时返回纯色兜底图，不崩。
    """
    if path:
        try:
            img = Image.open(path).convert("RGB")
            return _crop_center(img, w, h)
        except Exception:
            pass
    # 纯色兜底
    return Image.new("RGB", (w, h), _hex_rgb(fallback_color))


def _draw_rounded_rect_alpha(
    base: Image.Image,
    box: tuple[int, int, int, int],
    radius: int,
    fill_rgb: tuple[int, int, int],
    opacity: float,
) -> Image.Image:
    """在 base 上叠加一个半透明圆角矩形，返回合成后的 RGBA 图。"""
    base_rgba = base.convert("RGBA")
    layer = Image.new("RGBA", base_rgba.size, (0, 0, 0, 0))
    draw  = ImageDraw.Draw(layer)
    alpha = max(0, min(255, int(opacity * 255)))
    draw.rounded_rectangle(box, radius=radius, fill=(*fill_rgb, alpha))
    return Image.alpha_composite(base_rgba, layer)


def _draw_text_shadow(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int, int],
    shadow: tuple[int, int, int, int] = (0, 0, 0, 160),
    offset: int = 2,
) -> None:
    """带 1px 阴影的文字绘制。"""
    draw.text((xy[0] + offset, xy[1] + offset), text, font=font, fill=shadow)
    draw.text(xy, text, font=font, fill=fill)


def render_hero_collage(
    out_path: str | Path,
    *,
    hero_img: str | None = None,
    thumb1: str | None = None,
    thumb2: str | None = None,
    thumb3: str | None = None,
    code: str = "",
    project: str = "",
    layout: str = "",
    area: str = "",
    size: str = "",
    floor: str = "",
    price: str = "",
    h1: str = "",
    h2: str = "",
    h3: str = "",
    canvas_w: int = 0,
    canvas_h: int = 0,
    quality: int = 0,
) -> Path:
    """
    生成 hero_collage 封面图（纯 Pillow，无需 Chromium）。

    视觉结构：
        1280×960 单张封面
        ├── 上方主图区：1280×650
        │   ├── 大背景主图（居中等比裁切）
        │   ├── 左侧半透明信息面板（圆角，含代号 badge / 标题 / 信息行 / 标签）
        │   ├── 右侧价格卡（圆角深蓝底，金色字）
        │   └── 右上角品牌文字（侨联地产）
        └── 底部三图区：1280×310
            ├── 小图 1
            ├── 小图 2
            └── 小图 3

    缺图兜底：
        - 缺主图 → 纯色深蓝背景
        - 缺小图 → 用 hero_img 或已有小图兜底，不报错

    字段兜底：
        - 空字段不显示（不出现 None / null）
    """
    d = HERO_COLLAGE_DEFAULTS
    cw   = canvas_w  if canvas_w  > 0 else d["canvas_w"]
    ch   = canvas_h  if canvas_h  > 0 else d["canvas_h"]
    qual = quality   if quality   > 0 else d["jpg_quality"]

    hero_h: int = d["hero_h"]
    hero_w: int = cw

    # ── 字段清洗 ──────────────────────────────────────────────────────────────
    code_str    = _clean(code,    "房源")
    project_str = _clean(project, "精选房源")
    layout_str  = _clean(layout,  "")
    area_str    = _clean(area,    "金边")
    size_str    = _clean(size,    "")
    floor_str   = _clean(floor,   "")
    price_str   = _clean(price,   "")
    h1_str      = _clean(h1, "")
    h2_str      = _clean(h2, "")
    h3_str      = _clean(h3, "")
    tags        = [t for t in [h1_str, h2_str, h3_str] if t]

    # ── 价格格式化 ────────────────────────────────────────────────────────────
    if price_str:
        price_display = _price_line(price_str)
    else:
        price_display = ""

    # ── 缺图兜底列表 ──────────────────────────────────────────────────────────
    # 收集传入的小图；不足 3 张时用 hero_img 或前面已有的补齐
    raw_thumbs = [thumb1, thumb2, thumb3]
    # 过滤掉无效路径
    valid_thumbs = [t for t in raw_thumbs if t and os.path.isfile(str(t))]
    # 兜底图：优先 hero_img，其次第一张有效小图
    fallback_thumb = hero_img if (hero_img and os.path.isfile(str(hero_img))) else (valid_thumbs[0] if valid_thumbs else None)
    # 补齐到 3 张
    filled_thumbs: list[str | None] = []
    for t in raw_thumbs:
        if t and os.path.isfile(str(t)):
            filled_thumbs.append(t)
        else:
            filled_thumbs.append(fallback_thumb)

    # ── 创建画布 ──────────────────────────────────────────────────────────────
    canvas = Image.new("RGB", (cw, ch), _hex_rgb(d["bg_color"]))

    # ── 主图区（hero zone） ───────────────────────────────────────────────────
    hero = _load_img_safe(hero_img, hero_w, hero_h, d["bg_color"])
    canvas.paste(hero, (d["hero_x"], d["hero_y"]))

    # ── 底部三图区 ────────────────────────────────────────────────────────────
    ta_x: int = d["thumb_area_x"]
    ta_y: int = d["thumb_area_y"]
    ta_w: int = d["thumb_area_w"]
    ta_h: int = d["thumb_area_h"]
    gap:  int = d["thumb_gap"]
    n:    int = d["thumb_count"]
    tw = (ta_w - gap * (n - 1)) // n   # 每张小图宽

    for i, tpath in enumerate(filled_thumbs[:n]):
        # 最后一张小图撑满剩余宽度（避免1-2px空隙）
        this_w = ta_w - (tw + gap) * i if i == n - 1 else tw
        this_w = max(this_w, 1)
        tx = ta_x + i * (tw + gap)
        thumb_img = _load_img_safe(tpath, this_w, ta_h, d["bg_color"])
        canvas.paste(thumb_img, (tx, ta_y))

    # ── 小图区分隔线（深色间隔已由 gap 实现，再加顶部线条提升精致感）────────
    sep_draw = ImageDraw.Draw(canvas)
    sep_draw.line([(0, ta_y), (cw, ta_y)], fill=_hex_rgb(d["bg_color"]), width=gap)

    # ── 信息面板（半透明圆角矩形） ────────────────────────────────────────────
    px: int = d["info_panel_x"]
    py: int = d["info_panel_y"]
    pw: int = d["info_panel_w"]
    ph: int = d["info_panel_h"]
    pr: int = d["info_panel_radius"]
    panel_rgba = _draw_rounded_rect_alpha(
        canvas,
        (px, py, px + pw, py + ph),
        radius=pr,
        fill_rgb=_hex_rgb(d["info_panel_color"]),
        opacity=d["info_panel_opacity"],
    )
    canvas = panel_rgba.convert("RGB")

    draw = ImageDraw.Draw(canvas)

    # ── 代号 Badge ────────────────────────────────────────────────────────────
    bx: int = d["code_badge_x"]
    by: int = d["code_badge_y"]
    br: int = d["code_badge_radius"]
    badge_font = _hc_font(d["code_badge_font_size"], bold=True)
    bbox = draw.textbbox((0, 0), code_str, font=badge_font)
    badge_tw = bbox[2] - bbox[0]
    badge_th = bbox[3] - bbox[1]
    badge_pad_x, badge_pad_y = 16, 9
    badge_rect = (bx, by, bx + badge_tw + badge_pad_x * 2, by + badge_th + badge_pad_y * 2)
    canvas = _draw_rounded_rect_alpha(
        canvas, badge_rect, radius=br,
        fill_rgb=_hex_rgb(d["code_badge_bg"]), opacity=1.0,
    ).convert("RGB")
    draw = ImageDraw.Draw(canvas)
    draw.text(
        (bx + badge_pad_x - bbox[0], by + badge_pad_y - bbox[1]),
        code_str, font=badge_font, fill=_hex_rgb(d["code_badge_text"]),
    )
    badge_bottom = by + badge_th + badge_pad_y * 2 + 10

    # ── 标题（楼盘名） ────────────────────────────────────────────────────────
    title_font = _hc_font(d["title_font_size"], bold=True)
    title_y    = max(d["title_y"], badge_bottom + 8)
    title_x: int = d["title_x"]
    # 超长标题截断（不超过面板宽度）
    max_title_w = pw - (title_x - px) - 20
    proj_display = project_str
    while proj_display:
        tb = draw.textbbox((0, 0), proj_display, font=title_font)
        if tb[2] - tb[0] <= max_title_w:
            break
        proj_display = proj_display[:-1]
    _draw_text_shadow(
        draw, (title_x, title_y), proj_display,
        font=title_font, fill=(*_hex_rgb(d["title_color"]), 255),
    )
    cur_y = title_y + draw.textbbox((0, 0), proj_display, font=title_font)[3] + 6

    # ── 副标题（户型） ────────────────────────────────────────────────────────
    if layout_str:
        sub_font = _hc_font(d["subtitle_font_size"], bold=False)
        _draw_text_shadow(
            draw, (title_x, cur_y), layout_str,
            font=sub_font, fill=(*_hex_rgb(d["subtitle_color"]), 255),
        )
        cur_y += draw.textbbox((0, 0), layout_str, font=sub_font)[3] + 10

    # ── 分隔线 ────────────────────────────────────────────────────────────────
    sep_y = cur_y + 6
    draw.line(
        [(title_x, sep_y), (px + pw - 30, sep_y)],
        fill=(*_hex_rgb(d["icon_color"]), 100), width=1,
    )
    cur_y = sep_y + 14

    # ── 信息行（区域 / 面积 / 条件） ─────────────────────────────────────────
    info_font = _hc_font(d["info_font_size"], bold=False)
    icon_fill = (*_hex_rgb(d["icon_color"]), 255)
    text_fill = (*_hex_rgb(d["info_color"]), 255)
    info_rows = [
        ("📍", f" {area_str}") if area_str else None,
        ("📐", f" {size_str}") if size_str else None,
        ("🔑", f" {floor_str}") if floor_str else None,
    ]
    for row in info_rows:
        if row is None:
            continue
        icon_char, rest = row
        # 图标用金色，文字用浅色
        draw.text((title_x, cur_y), icon_char, font=info_font, fill=icon_fill)
        icon_w = draw.textbbox((0, 0), icon_char, font=info_font)[2]
        draw.text((title_x + icon_w, cur_y), rest, font=info_font, fill=text_fill)
        cur_y += d["line_gap"]

    # ── 高亮标签（h1 / h2 / h3） ─────────────────────────────────────────────
    if tags:
        cur_y += 4
        tag_font   = _hc_font(d["tag_font_size"], bold=False)
        tag_pad_x  = 12
        tag_pad_y  = 6
        tag_x      = title_x
        for tag_text in tags:
            tb = draw.textbbox((0, 0), f"✅ {tag_text}", font=tag_font)
            tag_w = tb[2] - tb[0] + tag_pad_x * 2
            tag_h = tb[3] - tb[1] + tag_pad_y * 2
            # 检查面板边界
            if cur_y + tag_h > py + ph - 10:
                break
            canvas = _draw_rounded_rect_alpha(
                canvas,
                (tag_x, cur_y, tag_x + tag_w, cur_y + tag_h),
                radius=d["tag_radius"],
                fill_rgb=_hex_rgb(d["tag_bg"]),
                opacity=0.88,
            ).convert("RGB")
            draw = ImageDraw.Draw(canvas)
            draw.text(
                (tag_x + tag_pad_x - tb[0], cur_y + tag_pad_y - tb[1]),
                f"✅ {tag_text}", font=tag_font,
                fill=(*_hex_rgb(d["tag_color"]), 255),
            )
            cur_y += tag_h + 6

    # ── 价格卡（右侧） ────────────────────────────────────────────────────────
    if price_display:
        price_font   = _hc_font(d["price_font_size"], bold=True)
        label_font   = _hc_font(18, bold=False)
        pb = draw.textbbox((0, 0), price_display, font=price_font)
        price_tw = pb[2] - pb[0]
        price_th = pb[3] - pb[1]
        pc_w: int  = max(d["price_card_w"], price_tw + 52)
        pc_h: int  = d["price_card_h"]
        pc_x: int  = cw - pc_w - 36
        pc_y: int  = d["price_card_y"]

        canvas = _draw_rounded_rect_alpha(
            canvas,
            (pc_x, pc_y, pc_x + pc_w, pc_y + pc_h),
            radius=d["price_card_radius"],
            fill_rgb=_hex_rgb(d["price_card_bg"]),
            opacity=0.96,
        ).convert("RGB")
        draw = ImageDraw.Draw(canvas)
        # 边框
        draw.rounded_rectangle(
            (pc_x, pc_y, pc_x + pc_w, pc_y + pc_h),
            radius=d["price_card_radius"],
            outline=(*_hex_rgb(d["price_card_border"]), 200),
            width=2,
        )
        # 小标签 "租金"
        draw.text(
            (pc_x + 18, pc_y + 10), "租金",
            font=label_font, fill=(*_hex_rgb("#A0C8E8"), 220),
        )
        # 价格数字（居中）
        tx = pc_x + (pc_w - price_tw) // 2 - pb[0]
        ty = pc_y + (pc_h - price_th) // 2 - pb[1] + 6
        draw.text(
            (tx, ty), price_display,
            font=price_font, fill=(*_hex_rgb(d["price_color"]), 255),
        )

    # ── 品牌元素（右上角） ────────────────────────────────────────────────────
    brand_x: int = d["brand_x"]
    brand_y: int = d["brand_y"]
    _draw_hero_brand_chip(canvas, brand_x, brand_y)
    draw = ImageDraw.Draw(canvas)

    # ── 输出 ──────────────────────────────────────────────────────────────────
    out = Path(out_path).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(str(out), "JPEG", quality=qual, optimize=True)
    return out


def _draw_hero_brand_chip(base: Image.Image, x: int, y: int) -> None:
    """
    在 base 图上就地绘制侨联地产品牌角标。
    如果找不到 logo 文件，降级为蓝底品牌文字，不报错。
    """
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw    = ImageDraw.Draw(overlay)

    font_cn  = _hc_font(22, bold=True)
    font_sub = _hc_font(11, bold=False)
    brand_cn  = "侨联地产"
    brand_sub = "QIAOLIAN REAL ESTATE"

    b_cn  = draw.textbbox((0, 0), brand_cn,  font=font_cn)
    b_sub = draw.textbbox((0, 0), brand_sub, font=font_sub)
    tw = max(b_cn[2] - b_cn[0], b_sub[2] - b_sub[0])
    th = (b_cn[3]  - b_cn[1])  + 3 + (b_sub[3] - b_sub[1])

    pad_x, pad_y = 14, 10
    box_w = tw + pad_x * 2
    box_h = th + pad_y * 2

    draw.rounded_rectangle(
        (x, y, x + box_w, y + box_h),
        radius=12,
        fill=(27, 86, 196, 230),
        outline=(176, 205, 255, 100),
        width=1,
    )
    text_x = x + pad_x - b_cn[0]
    text_y = y + pad_y - b_cn[1]
    draw.text((text_x, text_y), brand_cn,  font=font_cn,  fill=(247, 251, 255, 255))
    sub_y = text_y + (b_cn[3] - b_cn[1]) + 3 - b_sub[1]
    draw.text((text_x, sub_y),  brand_sub, font=font_sub, fill=(200, 225, 255, 220))

    result = Image.alpha_composite(base.convert("RGBA"), overlay)
    base.paste(result.convert("RGB"))


def _canonical_kind(kind: str) -> str:
    picked = str(kind or "").strip().lower() or "right_price_fixed"
    return KIND_ALIASES.get(picked, picked)


def _find_chromium() -> str | None:
    for name in ("chromium-browser", "chromium", "google-chrome", "google-chrome-stable"):
        p = shutil.which(name)
        if p:
            return p
    return None


def _bg_src(bg_url: str | None, bg_local: str | None, *, default_url: str) -> str:
    if bg_local:
        p = Path(bg_local).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"bg local file not found: {p}")
        mime = mimetypes.guess_type(str(p))[0] or "image/jpeg"
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    if bg_url:
        return html.escape(bg_url, quote=True)
    # Default: remote placeholder (requires network at render time)
    return html.escape(default_url, quote=True)


def _parse_kind(argv: list[str]) -> str:
    kind = "right_price_fixed"
    for i, a in enumerate(argv):
        if a == "--kind" and i + 1 < len(argv):
            kind = str(argv[i + 1]).strip().lower() or kind
            break
        if a.startswith("--kind="):
            kind = str(a.split("=", 1)[1]).strip().lower() or kind
            break
    return _canonical_kind(kind)


def _pick_writable_dir(candidates: list[Path]) -> Path:
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            probe = p / ".write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return p
        except Exception:
            continue
    raise PermissionError("No writable temp directory for renderer.")


def _price_line(price: str) -> str:
    p = str(price or "").strip()
    if not p:
        return ""
    if p.endswith("/月"):
        return p
    # If looks like a numeric monthly rent, normalize to "$1234/月"
    m = re.match(r"^\s*\$?\s*([0-9][0-9,]*)\s*$", p.replace(",", ""))
    if m:
        n = m.group(1).replace(",", "")
        return f"${n}/月"
    if p.startswith("$") and re.search(r"[0-9]", p) and "月" not in p:
        return f"{p}/月" if not p.endswith("/") else f"{p}月"
    return p


def main() -> int:
    argv = sys.argv[1:]
    kind = _parse_kind(argv)

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output image path (.jpg/.jpeg)")
    ap.add_argument(
        "--kind",
        choices=(
            "hero_collage",
            "right_price_fixed",
            "dark_glass",
            "villa_premium",
            "right_price",
            "blue_card",
            "white_bar",
            "magazine_white",
            "metro_panel",
            "lite_strip",
            "portrait_luxe",
        ),
        default=kind,
        help="Template kind. hero_collage = pure-Pillow (recommended). Others require Chromium.",
    )
    ap.add_argument("--template", default="", help="Override HTML template path (advanced).")
    ap.add_argument("--w", type=int, default=0, help="Canvas width. 0 = auto.")
    ap.add_argument("--h", type=int, default=0, help="Canvas height. 0 = auto.")
    ap.add_argument("--dpr", type=float, default=1.0, help="Device scale factor (HTML templates).")
    ap.add_argument("--jpeg-quality", type=int, default=90, help="JPEG quality (1-95).")

    # ── hero_collage 专属图片参数 ─────────────────────────────────────────────
    ap.add_argument("--hero-img", default=None, help="hero_collage: main background image path.")
    ap.add_argument("--thumb1",   default=None, help="hero_collage: bottom thumbnail 1 path.")
    ap.add_argument("--thumb2",   default=None, help="hero_collage: bottom thumbnail 2 path.")
    ap.add_argument("--thumb3",   default=None, help="hero_collage: bottom thumbnail 3 path.")
    ap.add_argument("--code",     default="",   help="hero_collage: listing code badge (e.g. L1023).")

    canonical_kind = _canonical_kind(kind)

    # ── 各模板默认展示字段 ────────────────────────────────────────────────────
    if canonical_kind in ("hero_collage", "right_price_fixed"):
        default_project = "富力城"
        default_layout  = "2房1厅1卫"
        default_area    = "BKK1"
        default_size    = "86㎡"
        default_floor   = "押一付一"
        default_price   = "$700/月"
        default_h1      = "支持视频实拍代看"
        default_h2      = "可预约线下看房"
        default_h3      = "真实房源"
    elif canonical_kind == "dark_glass":
        default_project = "天际ONE"
        default_layout  = "Studio"
        default_area    = "钻石岛"
        default_size    = "41㎡"
        default_floor   = "29楼"
        default_price   = "$680/月"
        default_h1      = "夜景开阔"
        default_h2      = "高层静音"
        default_h3      = "智能门锁"
    elif canonical_kind == "villa_premium":
        default_project = "集茂独栋别墅"
        default_layout  = "5房别墅"
        default_area    = "洪森大道"
        default_size    = "320㎡"
        default_floor   = "3楼"
        default_price   = "$2500/月"
        default_h1      = "家具家电齐全"
        default_h2      = "拎包入住"
        default_h3      = "实拍可核验"
    else:
        default_project = "太子幸福"
        default_layout  = "Studio"
        default_area    = "桑园"
        default_size    = "38㎡"
        default_floor   = "26楼"
        default_price   = "$480/月"
        default_h1      = "采光好"
        default_h2      = "近商场"
        default_h3      = "安静"

    ap.add_argument("--project",  default=default_project)
    ap.add_argument("--ref",      default="QC0315")
    ap.add_argument("--layout",   default=default_layout)
    ap.add_argument("--area",     default=default_area)
    ap.add_argument("--size",     default=default_size)
    ap.add_argument("--floor",    default=default_floor)
    ap.add_argument("--price",    default=default_price)
    ap.add_argument("--payment",  default="押1付1")
    ap.add_argument("--h1",       default=default_h1)
    ap.add_argument("--h2",       default=default_h2)
    ap.add_argument("--h3",       default=default_h3)

    g = ap.add_mutually_exclusive_group()
    g.add_argument("--bg-url",   default=None, help="Background image URL (HTML templates only).")
    g.add_argument("--bg-local", default=None, help="Background image local path (HTML templates only).")

    args = ap.parse_args(argv)
    args.kind = _canonical_kind(args.kind)

    # ══════════════════════════════════════════════════════════════════════════
    # hero_collage 分支：纯 Pillow，直接生成并返回
    # ══════════════════════════════════════════════════════════════════════════
    if args.kind == "hero_collage":
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            result = render_hero_collage(
                out_path,
                hero_img  = args.hero_img,
                thumb1    = args.thumb1,
                thumb2    = args.thumb2,
                thumb3    = args.thumb3,
                code      = args.code or args.ref or "",
                project   = args.project,
                layout    = args.layout,
                area      = args.area,
                size      = args.size,
                floor     = args.floor,
                price     = args.price,
                h1        = args.h1,
                h2        = args.h2,
                h3        = args.h3,
                canvas_w  = int(args.w) if args.w > 0 else 0,
                canvas_h  = int(args.h) if args.h > 0 else 0,
                quality   = int(args.jpeg_quality),
            )
        except Exception as exc:
            print(f"hero_collage render error: {exc}", file=sys.stderr)
            return 1
        print(str(result))
        return 0

    # ══════════════════════════════════════════════════════════════════════════
    # HTML + Chromium 分支（旧模板兼容）
    # ══════════════════════════════════════════════════════════════════════════
    explicit_template = str(args.template or "").strip()
    if explicit_template:
        tpl = Path(explicit_template).expanduser().resolve()
    else:
        tpl = {
            "right_price_fixed": RIGHT_PRICE_FIXED_TEMPLATE,
            "dark_glass":        DARK_GLASS_TEMPLATE,
            "villa_premium":     VILLA_PREMIUM_TEMPLATE,
        }.get(args.kind)
        if tpl is None:
            print(f"Unknown kind for HTML branch: {args.kind}", file=sys.stderr)
            return 2
    if not tpl.is_file():
        print(f"template not found: {tpl}", file=sys.stderr)
        return 2

    if args.kind == "right_price_fixed":
        default_bg = "https://images.unsplash.com/photo-1484154218962-a197022b5858?q=80&w=1800&auto=format&fit=crop"
    elif args.kind == "dark_glass":
        default_bg = "https://images.unsplash.com/photo-1494526585095-c41746248156?q=80&w=1800&auto=format&fit=crop"
    elif args.kind == "villa_premium":
        default_bg = "https://images.unsplash.com/photo-1600607687644-c7f34b5f7ef5?q=80&w=1800&auto=format&fit=crop"
    else:
        default_bg = "https://images.unsplash.com/photo-1505693416388-ac5ce068fe85?q=80&w=1800&auto=format&fit=crop"
    render_w, render_h = 1600, 1200

    if args.w > 0 and args.h > 0:
        out_w, out_h = int(args.w), int(args.h)
    else:
        out_w, out_h = render_w, render_h
    bg_src = _bg_src(args.bg_url, args.bg_local, default_url=default_bg)

    s = tpl.read_text(encoding="utf-8")
    repl: dict[str, str] = {
        "{{BG_SRC}}":   bg_src,
        "{{PROJECT}}":  html.escape(str(args.project), quote=False),
        "{{REF}}":      html.escape(str(args.ref), quote=False),
        "{{LAYOUT}}":   html.escape(str(args.layout), quote=False),
        "{{SIZE}}":     html.escape(str(args.size), quote=False),
        "{{FLOOR}}":    html.escape(str(args.floor), quote=False),
        "{{PAYMENT}}":  html.escape(str(args.payment), quote=False),
        "{{H1}}":       html.escape(str(args.h1), quote=False),
        "{{H2}}":       html.escape(str(args.h2), quote=False),
        "{{H3}}":       html.escape(str(args.h3), quote=False),
    }
    if "{{PRICE}}" in s:
        repl["{{PRICE}}"]      = html.escape(str(args.price), quote=False)
    if "{{AREA}}" in s:
        repl["{{AREA}}"]       = html.escape(str(args.area), quote=False)
    if "{{PRICE_LINE}}" in s:
        repl["{{PRICE_LINE}}"] = html.escape(_price_line(str(args.price)), quote=False)
    if "{{REF}}" not in s:
        repl.pop("{{REF}}", None)
    if "{{PAYMENT}}" not in s:
        repl.pop("{{PAYMENT}}", None)

    for k, v in repl.items():
        if k in s:
            s = s.replace(k, v)

    if "{{" in s and "}}" in s:
        tail = s[s.find("{{") : s.find("{{") + 80].replace("\n", " ")
        print(f"template still contains placeholders near: {tail}", file=sys.stderr)
        return 3

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_env = str(os.getenv("QIAOLIAN_RENDER_TMP", "")).strip()
    home = Path.home()
    candidates: list[Path] = []
    if tmp_env:
        candidates.append(Path(tmp_env).expanduser())
    candidates.extend(
        [
            home / "snap/chromium/common/qiaolian_raster_tmp",
            home / "qiaolian_raster_tmp",
            Path("/tmp/qiaolian/raster_tmp"),
            Path("/tmp/qiaolian_raster_tmp"),
            Path("/opt/qiaolian_dual_bots/media/renders/runtime/raster_tmp"),
            out_path.parent / "raster_tmp",
        ]
    )
    raster_root = _pick_writable_dir(candidates)
    raster_path = raster_root / (out_path.name + ".raster.png")
    raster_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_html = raster_path.with_suffix(".render.html")
    tmp_html.write_text(s, encoding="utf-8")

    chromium = _find_chromium()
    if not chromium:
        print("No chromium binary found (chromium-browser/chromium/chrome).", file=sys.stderr)
        return 4

    url = tmp_html.as_uri()
    viewport_pad = int(str(os.getenv("QIAOLIAN_RENDER_VIEWPORT_PAD", "120")).strip() or "120")
    capture_w, capture_h = int(render_w), int(render_h + max(0, viewport_pad))

    cmd = [
        chromium,
        "--headless=new",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--hide-scrollbars",
        f"--window-size={capture_w},{capture_h}",
        f"--force-device-scale-factor={float(args.dpr)}",
        "--virtual-time-budget=8000",
        f"--screenshot={str(raster_path)}",
        url,
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        print("chromium screenshot failed", file=sys.stderr)
        return 5

    if not raster_path.is_file():
        print(f"expected raster missing: {raster_path}", file=sys.stderr)
        return 6

    rendered = Image.open(raster_path)
    render_size = (int(render_w), int(render_h))
    if rendered.size != render_size:
        if rendered.width >= render_size[0] and rendered.height >= render_size[1]:
            rendered = rendered.crop((0, 0, render_size[0], render_size[1]))
        else:
            rendered = ImageOps.fit(
                rendered, render_size,
                method=Image.Resampling.LANCZOS, centering=(0.5, 0.5),
            )

    out_size = (int(out_w), int(out_h))
    if rendered.size != out_size:
        rendered = rendered.resize(out_size, Image.Resampling.LANCZOS)

    suf = out_path.suffix.lower()
    if suf in (".jpg", ".jpeg"):
        out_img = rendered.convert("RGB")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_img.save(out_path, "JPEG", quality=int(args.jpeg_quality), optimize=True)
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if rendered.mode not in ("RGB", "RGBA"):
            rendered = rendered.convert("RGBA")
        rendered.save(out_path)

    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
