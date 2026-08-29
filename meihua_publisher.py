"""
meihua_publisher.py
发布模块：从 drafts 读取已审核房源，生成封面图，
发布到 TG 频道，同步到 Notion，全程记录 publish_logs。

链路：drafts → cover_generator → media_assets
           → TG 频道发布 → posts
           → Notion 同步 → posts.notion_page_id
           → publish_logs
"""
from __future__ import annotations

import os
import json
import uuid
from publication_delivery import DeliveryBlocked, PublicationDeliveryRepository
import sqlite3
import asyncio
import logging
import time
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import io
from html import escape as he
from typing import Callable
from urllib.parse import quote

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter, ImageEnhance, ImageStat

from telegram import Bot, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.request import HTTPXRequest

from notion_client import Client as NotionClient

from cover_generator import CoverGenerator
from media_consistency import (
    assess_draft_media,
    mark_draft_media_broken,
    media_blocks_publish,
)
from qiaolian_dual.canonical_fact_projection import validate_facts
from qiaolian_dual.publishability_contract import evaluate_publishability
from qiaolian_dual.listing_taxonomy import PHYSICAL_AREAS
from qiaolian_dual.utils_formatting import _display_layout

logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────
def _normalize_bot_username(raw: str) -> str:
    return str(raw or "").strip().lstrip("@")


DB_PATH           = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
PUBLISHER_TOKEN   = os.getenv("PUBLISHER_BOT_TOKEN", "")
CHANNEL_ID        = os.getenv("CHANNEL_ID", "")
CHANNEL_USERNAME  = os.getenv("CHANNEL_USERNAME", "").strip().lstrip("@")
DISCUSSION_CHAT_ID = os.getenv("DISCUSSION_CHAT_ID", "")
DISCUSSION_MAP_FILE = Path(
    os.getenv("DISCUSSION_MAP_FILE", "/opt/qiaolian_dual_bots/data/discussion_map.json")
)
# 频道帖 id 与讨论区相册 media_group_id 不一致，用「发布时间队列 + 讨论组 mgid」桥接
DISCUSSION_BRIDGE_FILE = Path(
    os.getenv("DISCUSSION_BRIDGE_FILE", "/opt/qiaolian_dual_bots/data/discussion_bridge.json")
)
PUBLISHER_BOT_USER = _normalize_bot_username(os.getenv("PUBLISHER_BOT_USERNAME", ""))
BOT_USERNAME      = (
    _normalize_bot_username(os.getenv("DEEPLINK_BOT_USERNAME", ""))
    or _normalize_bot_username(os.getenv("USER_BOT_USERNAME", ""))
    or PUBLISHER_BOT_USER
)
BRAND_NAME        = os.getenv("BRAND_NAME", "侨联地产")
BRAND_NAME_EN     = os.getenv("BRAND_NAME_EN", "QIAO LIAN PROPERTY")
CHANNEL_BRAND_LINE = os.getenv("CHANNEL_BRAND_LINE", "金边华人租房 / 买房 / 视频看房")
CHANNEL_BRAND_CTA = os.getenv("CHANNEL_BRAND_CTA", "🏠 租房   🏡 买房   ▶ 视频看房")
ADVISOR_TG        = os.getenv("ADVISOR_TG", "@pengqingw")
NOTION_TOKEN      = os.getenv("NOTION_TOKEN", "")          # 可选
NOTION_DB_ID      = os.getenv("NOTION_DATABASE_ID", "")    # 可选
# 相册后紧跟的按钮引导语（纯文本即可）
CHANNEL_BTN_PROMPT = os.getenv(
    "CHANNEL_BTN_PROMPT",
    "请选择下方操作：",
)
# 讨论区首条引导（挂在自动转发帖的评论线程下）
DISCUSSION_MORE_PROMPT = os.getenv("DISCUSSION_MORE_PROMPT", "点击查看更多图片 详情")
# 讨论区三段式：第一段 - 预约承接
DISCUSSION_APPT_TEXT = os.getenv(
    "DISCUSSION_APPT_TEXT",
    "📅 这套房现在可以预约看房\n\n"
    "点击下方按钮，我会把你的预约请求同步给顾问后台：\n"
    "• 实地看房\n"
    "• 视频看房\n\n"
    "通常 15 分钟内会有顾问联系你",
)
# 讨论区三段式：第二段 - 补充实拍组图首图说明
DISCUSSION_EXTRA_INTRO = os.getenv(
    "DISCUSSION_EXTRA_INTRO",
    "📸 <b>补充实拍｜同一套房源</b>\n"
    "下面是这套房的更多现场照片，点击图片可查看原图。",
)
# 讨论区三段式：第三段 - 继续看房入口
DISCUSSION_CONTINUE_TEXT = os.getenv(
    "DISCUSSION_CONTINUE_TEXT",
    "还想继续看同区域房源？\n\n"
    "点下方侨联小助手，可以马上：\n"
    "• 推荐同区域在租房\n"
    "• 按预算继续筛选\n"
    "• 预约看房\n"
    "• 一键转顾问跟进\n\n"
    "👇 点击下方按钮进入",
)
# 讨论区分批发送时，第 2 批及以后首张图说明
DISCUSSION_EXTRA_INTRO_CONT = os.getenv(
    "DISCUSSION_EXTRA_INTRO_CONT",
    "📸 <b>更多现场实拍（续）</b>",
)
# 角标距边约 40px（随图幅按比例缩放）；品牌块背景透明度约 90%（230/255）
LISTING_OVERLAY_EDGE = float(os.getenv("LISTING_OVERLAY_EDGE", "40"))
LISTING_PANEL_ALPHA = int(os.getenv("LISTING_PANEL_ALPHA", "230"))
DETAIL_LOGO_PANEL_ALPHA = int(os.getenv("DETAIL_LOGO_PANEL_ALPHA", "190"))
DETAIL_LOGO_SCALE = float(os.getenv("DETAIL_LOGO_SCALE", "1.15"))
DETAIL_PHOTO_STYLE = os.getenv("DETAIL_PHOTO_STYLE", "mini_card").strip().lower()
DETAIL_MAIN_TAG_TEXT = os.getenv("DETAIL_MAIN_TAG_TEXT", "实拍房源")
DETAIL_FALLBACK_SUBTAG = os.getenv("DETAIL_FALLBACK_SUBTAG", "金边 · 精选房源")
# 单帖可采集的实拍上限（需大于频道主帖张数，才有「溢出图」进讨论区）
ALBUM_SOURCE_MAX = int(os.getenv("ALBUM_SOURCE_MAX", "30"))
# 6 张相册比例：landscape=横向 3:2（不少客户端更接近「3 列×2 行」观感）；square=1:1 方图（常为 2 列×3 行）
CHANNEL_ALBUM_SIX_ASPECT = os.getenv("CHANNEL_ALBUM_SIX_ASPECT", "landscape").strip().lower()
# 组图排版：one_three=首张横图+后三张方图循环（Telegram 常见「上一横、下三格」）；classic=按张数统一方图/原逻辑
CHANNEL_ALBUM_LAYOUT = os.getenv("CHANNEL_ALBUM_LAYOUT", "one_three").strip().lower()
# 1+3 主图比例 16:9；方图边长
ONE_THREE_HERO_BOX = (1280, 720)
ONE_THREE_TILE = int(os.getenv("ONE_THREE_TILE", "1080"))
# 评论区版主帖固定为封面 + 3 张实拍；其余图片进入关联评论区。
CHANNEL_MAIN_ALBUM_MAX = max(1, min(4, int(os.getenv("CHANNEL_MAIN_ALBUM_MAX", "4"))))
CHANNEL_FORCE_FOUR_IMAGES = os.getenv("CHANNEL_FORCE_FOUR_IMAGES", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)
PREMIUM_PUBLISH_MIN_SCORE = int(os.getenv("PREMIUM_PUBLISH_MIN_SCORE", "80"))
# 基础发布门槛默认与 AUTO_READY 的常见阈值保持一致，避免低质量稿件误发。
BASIC_PUBLISH_MIN_SCORE = int(os.getenv("BASIC_PUBLISH_MIN_SCORE", "75"))
PREMIUM_REAL_MEDIA_MIN = int(os.getenv("PREMIUM_REAL_MEDIA_MIN", "3"))
CORNER_LOGO_PATH = os.getenv(
    "CORNER_LOGO_PATH",
    str((Path(__file__).resolve().parent / "assets" / "brand" / "qiaolian_corner_mark_120x40.png").resolve()),
)

_CJK_FONT_PATHS = (
    "/System/Library/Fonts/PingFang.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/System/Library/Fonts/STHeiti Medium.ttc",
    "/Library/Fonts/Arial Unicode.ttf",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
)


def _load_corner_logo() -> Image.Image | None:
    p = Path(str(CORNER_LOGO_PATH or "")).expanduser()
    if not p.is_file():
        return None
    try:
        return Image.open(p).convert("RGBA")
    except Exception:
        return None


def _font_for_watermark(size: int):
    # 中文必须优先 CJK，否则 DejaVu 回退到 bitmap 字体，角标会像「蚂蚁字」
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    ]
    for p in candidates:
        if os.path.isfile(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _watermark_brand_lines() -> tuple[str, str]:
    """Avoid tofu boxes when a deployment is missing a Chinese font."""
    if any(os.path.isfile(path) for path in _CJK_FONT_PATHS):
        return BRAND_NAME, BRAND_NAME_EN
    return "QIAO LIAN", "PROPERTY · PHNOM PENH"


def _apply_frosted_panel(
    base: Image.Image,
    box: tuple[int, int, int, int],
    *,
    radius: int,
    blur_radius: int,
    tint_rgb: tuple[int, int, int],
    tint_alpha: int,
    outline: tuple[int, int, int, int] | None = None,
) -> Image.Image:
    x1, y1, x2, y2 = [int(v) for v in box]
    region = base.crop((x1, y1, x2, y2)).convert("RGBA")
    region = region.filter(ImageFilter.GaussianBlur(max(1, blur_radius)))
    tint_layer = Image.new("RGBA", region.size, (*tint_rgb, max(0, min(255, tint_alpha))))
    region = Image.alpha_composite(region, tint_layer)

    mask = Image.new("L", region.size, 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle(
        (0, 0, region.size[0] - 1, region.size[1] - 1),
        radius=radius,
        fill=255,
    )
    base.paste(region, (x1, y1), mask)

    sheen = Image.new("RGBA", base.size, (0, 0, 0, 0))
    sheen_draw = ImageDraw.Draw(sheen)
    sheen_draw.rounded_rectangle(
        box,
        radius=radius,
        fill=(255, 255, 255, 18),
        outline=outline,
        width=max(1, int(radius * 0.12)) if outline else 0,
    )
    return Image.alpha_composite(base, sheen)


def _draw_house_outline_mark(
    draw: ImageDraw.ImageDraw,
    *,
    x: int,
    y: int,
    size: int,
    fill: tuple[int, int, int, int],
    shadow: tuple[int, int, int, int] | None = None,
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


def _draw_detail_logo_badge(
    overlay: Image.Image,
    *,
    edge: int,
    scale: float,
    ref: float,
) -> None:
    """细节图仅绘制左上角品牌 badge，避免复用封面信息层。"""
    draw = ImageDraw.Draw(overlay)
    blue = (42, 92, 210, 244)
    white = (255, 255, 255, 255)
    sub_white = (239, 243, 251, 240)
    shadow = (8, 16, 36, 110)

    title = BRAND_NAME
    subtitle = BRAND_NAME_EN
    logo_w = max(230, min(440, int(ref * 0.36)))
    logo_h = max(76, min(132, int(logo_w * 0.30)))
    radius = max(16, int(22 * scale))
    draw.rounded_rectangle(
        [edge, edge, edge + logo_w, edge + logo_h],
        radius=radius,
        fill=blue,
    )

    icon_size = max(26, int(logo_h * 0.46))
    icon_x = edge + max(18, int(24 * scale))
    icon_y = edge + (logo_h - max(10, int(icon_size * 0.72))) // 2 - max(4, int(logo_h * 0.06))
    _draw_house_outline_mark(
        draw,
        x=icon_x,
        y=icon_y,
        size=icon_size,
        fill=white,
        shadow=shadow,
    )

    title_font = _font_for_listing(max(22, min(46, int(logo_h * 0.40))), bold=True)
    sub_font = _font_for_listing(max(10, min(18, int(logo_h * 0.16))), bold=False)

    text_x = icon_x + icon_size + max(14, int(18 * scale))
    title_box = draw.textbbox((0, 0), title, font=title_font)
    sub_box = draw.textbbox((0, 0), subtitle, font=sub_font)
    title_h = title_box[3] - title_box[1]
    sub_h = sub_box[3] - sub_box[1]
    gap = max(1, int(logo_h * 0.03))
    content_h = title_h + sub_h + gap
    title_y = edge + (logo_h - content_h) // 2 - title_box[1] - max(2, int(logo_h * 0.03))
    sub_y = title_y + title_h + gap - sub_box[1]

    draw.text((text_x, title_y), title, font=title_font, fill=white)
    draw.text((text_x, sub_y), subtitle, font=sub_font, fill=sub_white)


def _compact_layout_for_detail_tag(layout: str) -> str:
    raw = str(layout or "").strip()
    if not raw:
        return ""
    lower = raw.lower()
    if "studio" in lower or "单间" in raw:
        return "单间"
    m = re.search(r"([一二三四五六七八九\d]+)\s*房", raw)
    if m:
        return f"{m.group(1)}房"
    short = normalize_room_type(raw)
    if len(short) > 8:
        return short[:8]
    return short


def _detail_subtag_from_listing(listing: dict | None) -> str:
    if not listing:
        return DETAIL_FALLBACK_SUBTAG
    area = _listing_value(listing, "area", "project", "community", default="").strip()
    layout_raw = _listing_value(listing, "room_type", "layout", default="").strip()
    layout = _compact_layout_for_detail_tag(layout_raw)
    if area and layout:
        return f"{area} · {layout}"
    if area:
        return area
    if layout:
        return layout
    return DETAIL_FALLBACK_SUBTAG


def _source_visual_profile(listing: dict | None) -> tuple[str, str]:
    """人工/微信来源可标注为侨联实拍；外部自动采集只标注侨联地产。

    这里不把第三方来源图伪装成侨联自拍，但两类图都统一加侨联 Logo。
    """
    payload = listing or {}
    key = " ".join(
        str(payload.get(field) or "").strip().lower()
        for field in ("source_type", "source_name")
    )
    manual_markers = ("wechat", "manual", "admin_upload", "csv_intake", "excel_intake")
    if any(marker in key for marker in manual_markers):
        return "manual", "侨联实拍"
    return "collector", "侨联地产"


def _apply_source_color_style(image: Image.Image, listing: dict | None) -> Image.Image:
    """按素材来源统一调色，但不篡改房屋真实颜色。

    微信/人工：暖金自然，适合侨联自有实拍。
    自动采集：冷蓝清透，便于在频道中一眼区分来源。
    """
    profile, _ = _source_visual_profile(listing)
    rgb = image.convert("RGB")
    if profile == "manual":
        rgb = ImageEnhance.Brightness(rgb).enhance(1.035)
        rgb = ImageEnhance.Color(rgb).enhance(1.055)
        tint = Image.new("RGB", rgb.size, (255, 190, 105))
        rgb = Image.blend(rgb, tint, 0.035)
    else:
        rgb = ImageEnhance.Contrast(rgb).enhance(1.06)
        rgb = ImageEnhance.Color(rgb).enhance(0.97)
        tint = Image.new("RGB", rgb.size, (92, 158, 226))
        rgb = Image.blend(rgb, tint, 0.045)
    return rgb.convert("RGBA")


def _apply_detail_photo_shade(overlay: Image.Image) -> None:
    w, h = overlay.size
    d = ImageDraw.Draw(overlay)
    d.rectangle((0, 0, w, h), fill=(5, 18, 36, 28))
    top_h = max(1, int(h * 0.18))
    bottom_h = max(1, int(h * 0.42))
    for y in range(top_h):
        a = int(22 * (1 - y / max(1, top_h)))
        d.line((0, y, w, y), fill=(5, 18, 36, a))
    start = h - bottom_h
    for y in range(start, h):
        a = int(26 + (y - start) / max(1, bottom_h) * 86)
        d.line((0, y, w, y), fill=(5, 18, 36, min(124, a)))


def _draw_detail_mini_logo_badge(
    im: Image.Image,
    overlay: Image.Image,
    *,
    edge: int,
    scale: float,
    ref: float,
    listing: dict | None = None,
) -> Image.Image:
    """Draw an adaptive glass wordmark in the quietest image corner."""
    logo = _load_corner_logo()
    panel_w = max(156, min(286, int(ref * 0.205)))
    panel_h = max(52, min(88, int(panel_w * 0.295)))
    w, h = im.size
    candidates = (
        (edge, edge),
        (w - edge - panel_w, edge),
        (w - edge - panel_w, h - edge - panel_h),
        (edge, h - edge - panel_h),
    )

    def activity(position: tuple[int, int]) -> float:
        x, y = position
        crop = im.convert("L").crop((x, y, x + panel_w, y + panel_h)).resize((64, 24))
        edges = crop.filter(ImageFilter.FIND_EDGES)
        edge_mean = float(ImageStat.Stat(edges).mean[0])
        contrast = float(ImageStat.Stat(crop).stddev[0])
        return edge_mean + contrast * 0.18

    badge_x, badge_y = min(candidates, key=activity)
    badge_box = (badge_x, badge_y, badge_x + panel_w, badge_y + panel_h)
    im = _apply_frosted_panel(
        im,
        badge_box,
        radius=max(12, int(panel_h * 0.24)),
        blur_radius=max(7, int(10 * scale)),
        tint_rgb=(10, 24, 48),
        tint_alpha=min(148, max(88, DETAIL_LOGO_PANEL_ALPHA)),
        outline=(255, 255, 255, 54),
    )

    draw = ImageDraw.Draw(overlay)
    pad_x = max(12, int(panel_h * 0.22))
    if logo is not None:
        max_w = panel_w - pad_x * 2
        max_h = panel_h - max(10, int(panel_h * 0.20))
        ratio = logo.width / max(1, logo.height)
        logo_w = min(max_w, int(max_h * ratio))
        logo_h = max(1, int(logo_w / max(ratio, 0.01)))
        mark = logo.resize((logo_w, logo_h), Image.Resampling.LANCZOS)
        overlay.paste(
            mark,
            (badge_x + (panel_w - logo_w) // 2, badge_y + (panel_h - logo_h) // 2),
            mark,
        )
        return im

    icon_size = max(24, int(panel_h * 0.43))
    icon_x = badge_x + pad_x
    icon_y = badge_y + (panel_h - int(icon_size * 0.72)) // 2
    _draw_house_outline_mark(
        draw,
        x=icon_x,
        y=icon_y,
        size=icon_size,
        fill=(244, 207, 119, 245),
        shadow=(0, 0, 0, 84),
    )
    text_x = icon_x + icon_size + max(10, int(panel_h * 0.16))
    title_text, sub_text = _watermark_brand_lines()
    title_font = _font_for_listing(max(17, int(panel_h * 0.32)), bold=True)
    sub_font = _font_for_listing(max(8, int(panel_h * 0.135)), bold=False)
    title_box = draw.textbbox((0, 0), title_text, font=title_font)
    sub_box = draw.textbbox((0, 0), sub_text, font=sub_font)
    title_h = title_box[3] - title_box[1]
    sub_h = sub_box[3] - sub_box[1]
    gap = max(2, int(panel_h * 0.045))
    content_h = title_h + sub_h + gap
    title_y = badge_y + (panel_h - content_h) // 2 - title_box[1]
    sub_y = title_y + title_h + gap - sub_box[1]
    draw.text((text_x, title_y), title_text, font=title_font, fill=(255, 255, 255, 246))
    draw.text((text_x, sub_y), sub_text, font=sub_font, fill=(224, 232, 245, 205))
    return im

def _draw_detail_corner_tags(
    im: Image.Image,
    overlay: Image.Image,
    *,
    edge: int,
    scale: float,
    ref: float,
    listing: dict | None,
) -> Image.Image:
    draw = ImageDraw.Draw(overlay)
    main_text = str(DETAIL_MAIN_TAG_TEXT or "实拍房源").strip() or "实拍房源"
    sub_text = _detail_subtag_from_listing(listing)

    font_main = _font_for_listing(max(18, min(34, int(ref * 0.031))), bold=True)
    font_sub = _font_for_listing(max(16, min(30, int(ref * 0.026))), bold=True)

    m_box = draw.textbbox((0, 0), main_text, font=font_main)
    s_box = draw.textbbox((0, 0), sub_text, font=font_sub)
    m_w, m_h = m_box[2] - m_box[0], m_box[3] - m_box[1]
    s_w, s_h = s_box[2] - s_box[0], s_box[3] - s_box[1]

    gap = max(8, int(12 * scale))
    pad_x_m = max(12, int(18 * scale))
    pad_y_m = max(8, int(11 * scale))
    pad_x_s = max(11, int(16 * scale))
    pad_y_s = max(7, int(10 * scale))

    m_w2 = m_w + pad_x_m * 2
    m_h2 = m_h + pad_y_m * 2
    s_w2 = s_w + pad_x_s * 2
    s_h2 = s_h + pad_y_s * 2

    total_w = m_w2 + gap + s_w2
    x1 = edge
    y2 = overlay.size[1] - edge
    y1 = y2 - max(m_h2, s_h2)

    m_box_px = (x1, y1, x1 + m_w2, y1 + m_h2)
    s_box_px = (x1 + m_w2 + gap, y1 + (m_h2 - s_h2) // 2, x1 + total_w, y1 + (m_h2 - s_h2) // 2 + s_h2)

    im = _apply_frosted_panel(
        im,
        m_box_px,
        radius=max(12, int(16 * scale)),
        blur_radius=max(7, int(10 * scale)),
        tint_rgb=(7, 18, 36),
        tint_alpha=188,
        outline=(246, 210, 122, 154),
    )
    im = _apply_frosted_panel(
        im,
        s_box_px,
        radius=max(12, int(16 * scale)),
        blur_radius=max(7, int(10 * scale)),
        tint_rgb=(255, 255, 255),
        tint_alpha=44,
        outline=(255, 255, 255, 86),
    )

    draw.text(
        (m_box_px[0] + pad_x_m - m_box[0], m_box_px[1] + pad_y_m - m_box[1]),
        main_text,
        font=font_main,
        fill=(246, 210, 122, 255),
    )
    draw.text(
        (s_box_px[0] + pad_x_s - s_box[0], s_box_px[1] + pad_y_s - s_box[1]),
        sub_text,
        font=font_sub,
        fill=(255, 255, 255, 242),
    )
    return im


def _font_for_listing(size: int, *, bold: bool = False):
    """信息卡副文 / pill 用常规体，避免整段粗黑。"""
    bold_paths = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Medium.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    ]
    reg_paths = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    for p in (bold_paths if bold else reg_paths):
        if os.path.isfile(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return _font_for_watermark(size)


def _draft_price_str(d: dict | None) -> str:
    if not d:
        return "面议"
    price = d.get("price")
    if price is not None and str(price).replace(".", "", 1).isdigit():
        p = float(price) if "." in str(price) else int(price)
        if p <= 0:
            return "面议"
        if p == int(p):
            return f"${int(p):,} / 月"
        return f"${p} / 月"
    if price:
        return f"${price} / 月"
    return "面议"


def _display_floor(floor: str) -> str:
    flo = str(floor).strip()
    if not flo:
        return ""
    return flo if flo.endswith(("楼", "楼层")) or flo.upper().endswith("F") else f"{flo}楼"


def _overlay_price_compact(d: dict | None) -> str:
    """参考图：$1200/月（无空格），不用千分位逗号。"""
    if not d:
        return "面议"
    price = d.get("price")
    if price is not None and str(price).replace(".", "", 1).isdigit():
        p = float(price) if "." in str(price) else int(price)
        if p <= 0:
            return "面议"
        if p == int(p):
            return f"${int(p)}/月"
        return f"${p}/月"
    if price:
        s = str(price).strip()
        return s if "月" in s else f"{s}/月"
    return "面议"


def _listing_highlight_pills(listing: dict, max_n: int = 3) -> list[str]:
    raw = listing.get("highlights") or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = [raw] if raw.strip() else []
    if not isinstance(raw, list):
        return []
    out = [str(x).strip() for x in raw if str(x).strip()]
    return out[:max_n]


def _wrap_cover_title(
    draw: ImageDraw.ImageDraw,
    text: str,
    font,
    max_width: int,
    max_lines: int = 2,
) -> list[str]:
    """按实际字宽切分封面主标题，最多两行。

    封面是手机首页扫读入口，不允许过长项目名挤压价格或越出安全区。
    """
    remaining = str(text or "").strip()
    if not remaining:
        return []
    lines: list[str] = []
    for line_index in range(max_lines):
        if not remaining:
            break
        current = ""
        for char in remaining:
            candidate = current + char
            bbox = draw.textbbox((0, 0), candidate, font=font)
            if current and bbox[2] - bbox[0] > max_width:
                break
            current = candidate
        if not current:
            current = remaining[0]
        remaining = remaining[len(current):]
        if line_index == max_lines - 1 and remaining:
            ellipsis = "…"
            while current:
                bbox = draw.textbbox((0, 0), current + ellipsis, font=font)
                if bbox[2] - bbox[0] <= max_width:
                    break
                current = current[:-1]
            current += ellipsis
            remaining = ""
        lines.append(current)
    return lines


def build_channel_platform_header_html() -> str:
    """侨联频道统一版头。用户可见内容只保留中文。"""
    return f"<b>{BRAND_NAME}</b>\n━━━━━━━━━━"


def add_channel_listing_overlay(
    image_bytes: bytes,
    listing: dict | None = None,
    *,
    with_listing_footer: bool = False,
    detail_mode: bool = False,
    detail_listing: dict | None = None,
) -> io.BytesIO:
    """频道图片加角标：封面轻角标，细节图用更清晰 logo。"""
    # 频道首图也必须叠品牌层，不能直通原图；否则会出现"没封面/没 logo"的观感。

    im = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    if detail_mode:
        # 详情图只加单一轻 Logo；保持原始实拍颜色，不再按来源调色。
        im = im
    w, h = im.size
    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    scale = min(w, h) / 1280.0
    ref = float(min(w, h))
    logo_scale = min(1.0, DETAIL_LOGO_SCALE * 0.72) if detail_mode else 0.90
    edge = max(10, int(LISTING_OVERLAY_EDGE * scale * (0.44 if detail_mode else 0.54)))
    stroke_w = max(1, int(1.05 * scale))

    white = (255, 255, 255, 255)
    brand_text = BRAND_NAME
    sub_text = BRAND_NAME_EN
    fs_cn = max(16, min(30, int(ref * 0.021 * logo_scale)))
    fs_sub = max(8, min(11, int(ref * 0.0085 * logo_scale)))
    font_cn = _font_for_listing(fs_cn, bold=True)
    font_sub = _font_for_listing(fs_sub, bold=False)
    pad_x = max(10, int(15 * scale * logo_scale))
    pad_y = max(7, int(9 * scale * logo_scale))

    detail_payload = detail_listing or listing
    cover_mode = bool(with_listing_footer and listing and not detail_mode)
    safe_x = max(edge, int(w * 0.045)) if cover_mode else edge
    safe_top = max(edge, int(h * 0.05)) if cover_mode else edge

    # 封面只压暗左侧文字区，保留右侧房源主体的真实明暗和色彩。
    if cover_mode:
        gradient_w = max(1, int(w * 0.60))
        shade = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        shade_draw = ImageDraw.Draw(shade)
        for gx in range(gradient_w):
            progress = gx / gradient_w
            alpha = int(142 * ((1.0 - progress) ** 1.7))
            shade_draw.line([(gx, 0), (gx, h)], fill=(7, 17, 38, alpha))
        im = Image.alpha_composite(im, shade)

    # 频道首图在首页信息流里会脱离帖子文字被单独扫到，
    # 因此即使有房源信息栏也必须保留品牌标识。
    show_brand = True
    if show_brand:
        if detail_mode:
            style = DETAIL_PHOTO_STYLE
            if style in ("mini_card", "mini", "v2", "new"):
                im = _draw_detail_mini_logo_badge(
                    im, overlay, edge=edge, scale=scale, ref=ref, listing=detail_payload
                )
            else:
                _draw_detail_logo_badge(overlay, edge=edge, scale=scale, ref=ref)
        else:
            corner_logo = _load_corner_logo()
            logo_w = 0
            logo_h = 0
            logo_gap = max(5, int(6 * scale * logo_scale))
            logo_img = None
            if corner_logo is not None:
                base_w = max(24, int(ref * 0.045 * logo_scale))
                ratio = corner_logo.height / max(1, corner_logo.width)
                logo_w = base_w
                logo_h = max(24, int(base_w * ratio))
                logo_img = corner_logo.resize((logo_w, logo_h), Image.Resampling.LANCZOS)

            b_brand = draw.textbbox((0, 0), brand_text, font=font_cn, stroke_width=stroke_w)
            b_sub = draw.textbbox((0, 0), sub_text, font=font_sub)
            title_w = b_brand[2] - b_brand[0]
            title_h = b_brand[3] - b_brand[1]
            sub_w = b_sub[2] - b_sub[0]
            sub_h = b_sub[3] - b_sub[1]
            text_w = max(title_w, sub_w)
            line_gap = max(1, int(2 * scale))
            text_h = title_h + sub_h + line_gap

            if logo_img is None:
                logo_w = max(14, int(ref * 0.015 * logo_scale))
                logo_h = max(14, int(logo_w * 0.72))

            content_w = text_w + logo_w + logo_gap
            content_h = max(text_h, logo_h)

            brand_w = content_w + pad_x * 2
            brand_h = content_h + pad_y * 2
            brand_x = w - safe_x - brand_w if cover_mode else edge
            brand_y = safe_top if cover_mode else edge
            brand_box = (brand_x, brand_y, brand_x + brand_w, brand_y + brand_h)
            radius = max(9, int(13 * scale * logo_scale))
            panel_alpha = min(DETAIL_LOGO_PANEL_ALPHA, 110) if detail_mode else 92
            panel_tint = (28, 41, 68) if detail_mode else (20, 28, 46)
            im = _apply_frosted_panel(
                im,
                brand_box,
                radius=radius,
                blur_radius=max(6, int(10 * scale)),
                tint_rgb=panel_tint,
                tint_alpha=panel_alpha,
                outline=(255, 255, 255, 56),
            )

            cursor_x = brand_x + pad_x
            center_y = brand_y + brand_h // 2
            if logo_img is not None:
                ly = center_y - logo_h // 2
                overlay.paste(logo_img, (cursor_x, ly), logo_img)
                cursor_x += logo_w + logo_gap
            else:
                _draw_house_outline_mark(
                    draw,
                    x=cursor_x,
                    y=center_y - logo_h // 2,
                    size=logo_w,
                    fill=(248, 230, 179, 255),
                    shadow=(8, 14, 28, 118),
                )
                cursor_x += logo_w + logo_gap

            tx = cursor_x - b_brand[0]
            top_y = center_y - text_h // 2
            title_y = top_y - b_brand[1]
            sub_y = top_y + title_h + line_gap - b_sub[1]
            draw.text(
                (tx, title_y),
                brand_text,
                font=font_cn,
                fill=white,
                stroke_width=stroke_w,
                stroke_fill=(6, 11, 24, 124),
            )
            draw.text((tx, sub_y), sub_text, font=font_sub, fill=(230, 236, 247, 222))

    if with_listing_footer and listing:
        price_text = _overlay_price_compact(listing)
        if price_text:
            project_text = str(listing.get("project") or "").strip()
            area_text = str(listing.get("area") or "").strip()
            layout_text = str(listing.get("layout") or "").strip()
            size_text = str(listing.get("size") or "").strip()
            floor_text = _display_floor(str(listing.get("floor") or "").strip())
            meta_text = " · ".join(x for x in (area_text, size_text, floor_text) if x)

            fs_project = max(44, min(112, int(ref * 0.083)))
            fs_layout = max(28, min(60, int(ref * 0.046)))
            fs_meta = max(24, min(50, int(ref * 0.037)))
            fs_price = max(48, min(104, int(ref * 0.078)))
            fs_price_label = max(16, min(32, int(ref * 0.024)))
            font_project = _font_for_listing(fs_project, bold=True)
            font_layout = _font_for_listing(fs_layout, bold=True)
            font_meta = _font_for_listing(fs_meta, bold=True)
            font_price = _font_for_watermark(fs_price)
            font_price_label = _font_for_listing(fs_price_label, bold=False)

            title_x = safe_x
            title_y = max(safe_top + int(ref * 0.14), int(h * 0.19))
            title_max_w = int(w * 0.56)
            title_lines = _wrap_cover_title(
                draw, project_text, font_project, title_max_w, max_lines=2
            )
            title_line_h = int(fs_project * 1.10)
            for idx, line in enumerate(title_lines):
                draw.text(
                    (title_x, title_y + idx * title_line_h),
                    line,
                    font=font_project,
                    fill=(255, 255, 255, 255),
                    stroke_width=max(1, int(ref * 0.0022)),
                    stroke_fill=(4, 10, 24, 170),
                )

            cursor_y = title_y + len(title_lines) * title_line_h + int(ref * 0.025)
            if layout_text:
                layout_bbox = draw.textbbox((0, 0), layout_text, font=font_layout)
                layout_w = layout_bbox[2] - layout_bbox[0]
                layout_h = layout_bbox[3] - layout_bbox[1]
                layout_pad_x = int(ref * 0.020)
                layout_pad_y = int(ref * 0.010)
                pill = (
                    title_x,
                    cursor_y,
                    title_x + layout_w + layout_pad_x * 2,
                    cursor_y + layout_h + layout_pad_y * 2,
                )
                draw.rounded_rectangle(
                    pill,
                    radius=max(14, int(ref * 0.022)),
                    fill=(246, 201, 72, 242),
                )
                draw.text(
                    (title_x + layout_pad_x - layout_bbox[0], cursor_y + layout_pad_y - layout_bbox[1]),
                    layout_text,
                    font=font_layout,
                    fill=(12, 28, 58, 255),
                )
                cursor_y = pill[3] + int(ref * 0.024)

            if meta_text:
                draw.text(
                    (title_x, cursor_y),
                    meta_text,
                    font=font_meta,
                    fill=(255, 255, 255, 245),
                    stroke_width=max(1, int(ref * 0.0018)),
                    stroke_fill=(4, 10, 24, 175),
                )

            price_bbox = draw.textbbox((0, 0), price_text, font=font_price)
            price_w = price_bbox[2] - price_bbox[0]
            label_bbox = draw.textbbox((0, 0), "月租参考", font=font_price_label)
            label_h = label_bbox[3] - label_bbox[1]
            price_pad_x = max(24, int(ref * 0.028))
            price_pad_top = max(16, int(ref * 0.018))
            price_pad_bottom = max(20, int(ref * 0.024))
            panel_w = max(int(w * 0.28), price_w + price_pad_x * 2)
            panel_h = label_h + fs_price + price_pad_top + price_pad_bottom + int(ref * 0.010)
            x2 = w - safe_x
            x1 = x2 - panel_w
            y2 = h - max(int(h * 0.115), int(ref * 0.115))
            y1 = y2 - panel_h
            chip_radius = max(18, int(ref * 0.024))
            im = _apply_frosted_panel(
                im,
                (x1, y1, x2, y2),
                radius=chip_radius,
                blur_radius=max(8, int(12 * scale)),
                tint_rgb=(13, 28, 58),
                tint_alpha=220,
                outline=(246, 201, 72, 108),
            )
            label_x = x1 + (panel_w - (label_bbox[2] - label_bbox[0])) // 2
            draw.text(
                (label_x - label_bbox[0], y1 + price_pad_top - label_bbox[1]),
                "月租参考",
                font=font_price_label,
                fill=(236, 240, 248, 238),
            )
            price_x = x1 + (panel_w - price_w) // 2
            draw.text(
                (price_x - price_bbox[0], y2 - price_pad_bottom - fs_price - price_bbox[1]),
                price_text,
                font=font_price,
                fill=(246, 201, 72, 255),
            )

    out = Image.alpha_composite(im, overlay).convert("RGB")
    buf = io.BytesIO()
    buf.name = "wm.jpg"
    out.save(buf, "JPEG", quality=92)
    buf.seek(0)
    return buf

def add_brand_watermark(
    image_bytes: bytes,
    listing: dict | None = None,
    *,
    with_listing_footer: bool = False,
) -> io.BytesIO:
    """兼容旧调用名：等同 add_channel_listing_overlay。"""
    return add_channel_listing_overlay(
        image_bytes, listing, with_listing_footer=with_listing_footer
    )


def add_detail_logo_watermark(image_bytes: bytes, listing: dict | None = None) -> io.BytesIO:
    """详情图加自适应轻量磨砂角标，不改变原图色彩与比例。"""
    return add_channel_listing_overlay(
        image_bytes,
        listing,
        with_listing_footer=False,
        detail_mode=True,
        detail_listing=listing,
    )


def prepare_channel_photo_for_publish(
    image_bytes: bytes,
    listing: dict | None,
    *,
    is_generated_cover: bool,
) -> io.BytesIO:
    """发布阶段只处理一次视觉信息层。

    CoverGenerator 生成的首图已包含品牌、标题、户型和价格；这里必须原样透传，
    否则会出现双品牌、双价格和旧底栏残留。普通细节图仍只加轻量 logo。
    """
    if is_generated_cover:
        buf = io.BytesIO(image_bytes)
        buf.name = "cover.jpg"
        buf.seek(0)
        return buf
    return add_detail_logo_watermark(image_bytes, listing)


def normalize_album_image(
    image_bytes: bytes,
    *,
    target_size: int = 1280,
    force_square: bool = False,
    fit_box: tuple[int, int] | None = None,
) -> bytes:
    """
    统一相册图片尺寸。
    fit_box=(w,h) 时按框居中裁切（如 6 张用 3:2 横图，部分客户端宫格更接近 3×2）。
    force_square 时 1:1；否则仅等比缩放到最长边 ≤ target_size。
    """
    im = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    if fit_box:
        im = ImageOps.fit(im, fit_box, method=Image.Resampling.LANCZOS)
    elif force_square:
        im = ImageOps.fit(im, (target_size, target_size), method=Image.Resampling.LANCZOS)
    else:
        # 非强制方图时仅限制最长边，避免超大图传输抖动
        im.thumbnail((target_size, target_size), Image.Resampling.LANCZOS)
    out = io.BytesIO()
    im.save(out, "JPEG", quality=90)
    return out.getvalue()


def _album_layout_is_one_three() -> bool:
    return CHANNEL_ALBUM_LAYOUT in ("one_three", "1+3", "13", "true", "1", "yes")


def _normalize_for_album_slot(image_bytes: bytes, *, index: int, total: int) -> bytes:
    """
    按槽位输出尺寸：one_three 模式下每 4 张为一组——组内第 1 张 16:9 主图，第 2～4 张方图；
    2～3 张时首张主图、其余方图。classic 模式走原先按总张数的方图/6 张横图规则。
    """
    if _album_layout_is_one_three():
        if total >= 4:
            if index % 4 == 0:
                return normalize_album_image(
                    image_bytes, fit_box=ONE_THREE_HERO_BOX
                )
            return normalize_album_image(
                image_bytes, target_size=ONE_THREE_TILE, force_square=False
            )
        if total == 3:
            if index == 0:
                return normalize_album_image(image_bytes, fit_box=ONE_THREE_HERO_BOX)
            return normalize_album_image(
                image_bytes, target_size=ONE_THREE_TILE, force_square=False
            )
        if total == 2:
            if index == 0:
                return normalize_album_image(image_bytes, fit_box=ONE_THREE_HERO_BOX)
            return normalize_album_image(
                image_bytes, target_size=ONE_THREE_TILE, force_square=False
            )
        return normalize_album_image(image_bytes, target_size=1280, force_square=False)

    if total == 6 and CHANNEL_ALBUM_SIX_ASPECT in ("landscape", "3x2", "32"):
        h32 = max(720, int(round(1280 * 2 / 3)))
        return normalize_album_image(image_bytes, target_size=1280, fit_box=(1280, h32))

    # 详情实拍不强制裁切；仅封面使用设计模板，避免切掉房屋主体。
    return normalize_album_image(image_bytes, target_size=1280, force_square=False)


# ── 文案构造 ──────────────────────────────────────────────
def _parsed_normalized(d: dict) -> dict:
    """解析 normalized_data / extracted_data 为 dict，失败返回 {}。"""
    raw = d.get("normalized_data") or d.get("extracted_data") or ""
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else dict(raw)
    except Exception:
        return {}


def _as_list(value) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = [value] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [str(x).strip() for x in value if str(x).strip()]


def normalize_room_type(value: str) -> str:
    raw = str(value or "").strip()
    mapping = {
        "studio": "单间",
        "Studio": "单间",
        "STUDIO": "单间",
        "1br": "1房1卫",
        "1BR": "1房1卫",
        "2br": "2房1卫",
        "2BR": "2房1卫",
        "3br": "3房",
        "3BR": "3房",
    }
    return mapping.get(raw, raw)


def _listing_value(d: dict, *keys: str, default: str = "") -> str:
    nd = _parsed_normalized(d)
    for key in keys:
        value = d.get(key)
        if value not in (None, ""):
            return str(value).strip()
        value = nd.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return default


def _price_value(d: dict) -> int:
    raw = d.get("price") or _parsed_normalized(d).get("price") or 0
    try:
        return int(float(str(raw).replace("$", "").replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def _price_is_consultable(raw: str) -> bool:
    txt = str(raw or "").strip()
    if not txt:
        return False
    signals = ("面议", "咨询", "详聊", "私聊", "待确认", "可确认", "联系顾问", "沟通")
    return any(s in txt for s in signals)


def _price_compact_for_post(d: dict) -> str:
    price = _price_value(d)
    if price > 0:
        return f"${price:,}/月"
    raw = str(d.get("price") or _parsed_normalized(d).get("price") or "").strip()
    if _price_is_consultable(raw):
        return "面议"
    if raw:
        return raw if ("月" in raw or "面议" in raw) else f"{raw}/月"
    return "面议"


def _is_manual_intake_listing(d: dict) -> bool:
    st = str(d.get("source_type") or "").strip().lower()
    return st in {"csv_intake", "wechat_note", "excel_intake"}


def property_type_for_tags(d: dict) -> str:
    raw = _resolved_property_type(d)
    lowered = raw.lower()
    if any(x in lowered for x in ("villa", "house")) or "别墅" in raw:
        return "别墅"
    if "排屋" in raw:
        return "排屋"
    return "公寓"


def price_range_tag(d: dict) -> str:
    price = _price_value(d)
    if price <= 0:
        return "价格待确认"
    if price < 500:
        return "500以下"
    if price < 1000:
        return "500_1000"
    if price < 1500:
        return "1000_1500"
    return "1500以上"


def _tag_safe(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_]", "", str(value or "").replace(" ", ""))


def _area_tags(area: str) -> list[str]:
    raw = str(area or "").strip()
    compact = _tag_safe(raw)
    mapping = {
        "富力城": ["#富力城租房", "#RFCity"],
        "RFCity": ["#富力城租房", "#RFCity"],
        "RFCITY": ["#富力城租房", "#RFCity"],
        "BKK1": ["#BKK1租房", "#BKK1"],
        "BKK2": ["#BKK2租房", "#BKK2"],
        "BKK3": ["#BKK3租房", "#BKK3"],
        "钻石岛": ["#钻石岛租房", "#钻石岛"],
        "DiamondIsland": ["#钻石岛租房", "#DiamondIsland"],
        "KohPich": ["#钻石岛租房", "#DiamondIsland"],
        "俄罗斯市场": ["#俄罗斯市场租房", "#TTP"],
        "RussianMarket": ["#俄罗斯市场租房", "#TTP"],
        "TTP": ["#俄罗斯市场租房", "#TTP"],
    }
    if compact in mapping:
        return mapping[compact]
    if compact and compact not in {"金边", "未知"}:
        return [f"#{compact}租房"]
    return []


def _room_type_tags(room_type: str) -> list[str]:
    raw = str(room_type or "").strip().lower()
    if "studio" in raw or "单间" in raw or "单身" in raw:
        return ["#单间"]
    if "1房" in raw or "一房" in raw:
        return ["#一房一厅"]
    if "2房" in raw or "两房" in raw or "二房" in raw:
        return ["#两房一厅"]
    if any(x in raw for x in ("3房", "三房", "4房", "四房", "5房", "五房")):
        return ["#三房"]
    return []


def _price_range_tags(d: dict) -> list[str]:
    price = _price_value(d)
    if price <= 0:
        return []
    if price < 400:
        return ["#400美金以下"]
    if price < 800:
        return ["#400到800美金"]
    if price < 1500:
        return ["#800到1500美金"]
    return ["#1500美金以上"]


def _property_tags(d: dict) -> list[str]:
    raw = _resolved_property_type(d)
    lowered = raw.lower()
    if "服务" in raw or "serviced" in lowered or "service" in lowered:
        return ["#服务式公寓"]
    if "penthouse" in lowered or "顶层" in raw:
        return ["#Penthouse"]
    if "villa" in lowered or "别墅" in raw:
        return ["#别墅租赁", "#Villa"]
    if "排屋" in raw or "townhouse" in lowered:
        return ["#排屋出租"]
    if "office" in lowered or "办公室" in raw:
        return ["#办公室租赁"]
    return ["#金边公寓"]


def _feature_tags(d: dict) -> list[str]:
    highlights = " ".join(_as_list(d.get("highlights")) + _as_list(_parsed_normalized(d).get("highlights")))
    furniture = furniture_text(d)
    text = " ".join([highlights, furniture, _listing_value(d, "cost_notes", default="")]).lower()
    tags: list[str] = []
    for needles, tag in (
        (("宠", "pet"), "#可养宠物"),
        (("阳台", "balcony"), "#带阳台"),
        (("泳池", "pool"), "#游泳池"),
        (("健身", "gym"), "#健身房"),
        (("中文",), "#中文客服"),
        (("拎包", "家具齐全", "全家具", "fully furnished"), "#拎包入住"),
        (("实拍", "视频", "video"), "#实拍视频"),
        (("物业费", "包物业", "management fee"), "#包物业费"),
        (("超市", "supermarket"), "#近超市"),
        (("学校", "school"), "#近学校"),
        (("高层", "景观", "view"), "#高层视野"),
        (("安保", "security", "24/7"), "#24小时安保"),
    ):
        if any(needle in text for needle in needles):
            tags.append(tag)
    return tags


def build_listing_tags(d: dict) -> list[str]:
    area = _listing_value(d, "area", "project", "community", default="金边")
    room_type = normalize_room_type(_listing_value(d, "room_type", "layout", default=""))
    tags = [
        "#金边租房",
        "#金边华人租房",
        "#侨联实拍",
        *_area_tags(area),
        *_room_type_tags(room_type),
        *_price_range_tags(d),
        *_property_tags(d),
        *_feature_tags(d),
    ]
    out: list[str] = []
    for tag in tags:
        if tag and tag not in out:
            out.append(tag)
        if len(out) >= 8:
            break
    fallback_pool = ["#实地看房", "#视频看房", "#金边生活"]
    for tag in fallback_pool:
        if len(out) >= 6:
            break
        if tag not in out:
            out.append(tag)
    return out


def furniture_text(d: dict) -> str:
    raw = _listing_value(d, "furniture", "furnishing", default="")
    if raw:
        return raw
    highlights = " ".join(_as_list(d.get("highlights")) + _as_list(_parsed_normalized(d).get("highlights")))
    if any(x in highlights for x in ("全新", "齐全", "家具", "拎包")):
        return "家具齐全"
    return "可咨询确认"


def generate_advantages_and_notes(d: dict) -> tuple[list[str], list[str]]:
    """从结构化字段生成 2 条优点 + 2 条注意，避免空占位。"""
    area = _listing_value(d, "area", default="")
    floor = _listing_value(d, "floor", default="")
    size = _listing_value(d, "size", default="")
    furniture = furniture_text(d)
    highlights = _as_list(d.get("highlights")) + _as_list(_parsed_normalized(d).get("highlights"))
    raw_text = " ".join([area, floor, size, furniture, " ".join(highlights)])

    advantages: list[str] = []
    for h in highlights:
        if h not in advantages:
            advantages.append(h)
        if len(advantages) >= 2:
            break
    if any(x in raw_text for x in ("高层", "楼", "采光", "景观")) and "采光好" not in advantages:
        advantages.append("采光好")
    if any(x in raw_text for x in ("BKK", "市中心", "核心", "商场", "超市", "金边")) and "生活便利" not in advantages:
        advantages.append("生活便利")
    if any(x in raw_text for x in ("家具齐全", "拎包", "全新")) and "拎包入住" not in advantages:
        advantages.append("拎包入住")
    advantages = advantages[:2]

    notes: list[str] = []
    cost_notes = _listing_value(d, "cost_notes", default="")
    payment_contract = _payment_contract_summary(d)
    if cost_notes:
        notes.append(cost_notes)
    if payment_contract:
        notes.append(payment_contract)
    if any(x in raw_text for x in ("停车位有限", "停车少", "小停车")):
        notes.append("停车位有限")
    notes.append("价格和空房状态以实时确认为准")
    notes.append("看房时间需提前预约")
    dedup_notes: list[str] = []
    for note in notes:
        if note and note not in dedup_notes:
            dedup_notes.append(note)
        if len(dedup_notes) >= 2:
            break
    return advantages[:2], dedup_notes[:2]


def _compact_copy(value: str, max_len: int = 22) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= max_len:
        return text
    return text[: max(1, max_len - 1)].rstrip("，。；;,. ") + "…"


DISPLAY_NOISE_TOKENS = ("啊雷莎", "阿雷莎", "🇨🇳", "🌵")
GENERIC_PROJECT_VALUES = {
    "",
    "公寓",
    "别墅",
    "排屋",
    "住宅",
    "社区",
    "小区",
    "金边",
}


def _clean_display_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for token in DISPLAY_NOISE_TOKENS:
        text = text.replace(token, " ")
    text = re.sub(r"^\s*\d{3,4}(?!米)", "", text)
    text = re.sub(r"[#⭐️✨🏠🏡🏢🔥📍💰✅📝☎️]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -｜|·•,，")
    return text


def _clean_project_label(raw: str) -> str:
    text = _public_clean_text(_clean_display_text(raw), keep_project=True)
    if text in GENERIC_PROJECT_VALUES:
        return ""
    return text


def _resolved_property_type(d: dict) -> str:
    raw = _listing_value(d, "property_type", "category", default="公寓")
    blob = " ".join(
        [
            raw,
            _listing_value(d, "title", default=""),
            _listing_value(d, "project", default=""),
            _listing_value(d, "community", default=""),
        ]
    )
    lowered = blob.lower()
    if any(token in blob for token in ("独栋", "双拼", "泳池独栋")) or "villa" in lowered or "别墅" in blob:
        return "别墅"
    if "排屋" in blob or "townhouse" in lowered:
        return "排屋"
    if "服务式" in blob or "serviced apartment" in lowered:
        return "服务式公寓"
    return _clean_project_label(raw) or "公寓"


def _project_label_for_post(d: dict) -> str:
    """Return only canonical project_name; never infer from title or raw text."""
    normalized = _parsed_normalized(d)
    # drafts.project is the canonical projection written by ai_parser; accept
    # it when callers already decoded a draft without normalized_data.
    primary = normalized.get("project_name") or d.get("project_name") or d.get("project") or ""
    primary = str(primary or "").strip()
    return _compact_copy(primary, 24) if primary else ""


def _listing_snapshot_for_post(d: dict) -> str:
    items: list[str] = []
    property_type = _resolved_property_type(d)
    size = _listing_value(d, "size", default="")
    floor = _listing_value(d, "floor", default="")
    available = _listing_value(d, "available_date", default="")

    if property_type:
        items.append(_compact_copy(property_type, 8))
    if size:
        size_raw = str(size).strip()
        if size_raw and size_raw.replace(".", "", 1).isdigit():
            size_raw = f"{size_raw}平"
        items.append(_compact_copy(size_raw, 12))
    if floor:
        items.append(_compact_copy(_display_floor(floor), 10))
    if available:
        items.append(_compact_copy(f"可{available}入住", 14))
    if not items:
        return "实拍房源｜支持实地看房/实时视频代看"
    return "｜".join(items[:4])


def _normalize_deposit_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    low = text.lower()
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:month|months|个月|月)", low)
    if m:
        n = m.group(1)
        if n.endswith(".0"):
            n = n[:-2]
        return f"押{n}月"
    m_cn = re.search(r"(押[^，。；;\s]{1,8})", text)
    if m_cn:
        return m_cn.group(1)
    if "deposit" in low:
        digits = re.findall(r"\d+(?:\.\d+)?", low)
        if digits:
            n = digits[0]
            if n.endswith(".0"):
                n = n[:-2]
            return f"押{n}月"
    return _compact_copy(text.replace("Deposit", "").replace("deposit", "").strip(" :："), 12)


def _normalize_contract_term(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    m_direct = re.search(r"^([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月)$", text)
    if m_direct:
        return f"{m_direct.group(1)}{m_direct.group(2)}"
    m = re.search(
        r"(?:合同|租期|lease|contract|term|min(?:imum)?\s*lease)\s*(?:期限|期|:|：)?\s*([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月|month|months|year|years|yr|yrs)",
        text,
        flags=re.I,
    )
    if not m:
        m = re.search(
            r"([0-9]{1,2})\s*(year|years|yr|yrs|month|months|mo)\s*(?:lease|contract|term)?",
            text,
            flags=re.I,
        )
    if not m:
        m = re.search(r"([一二三四五六七八九十两]{1,3})\s*年\s*(?:起租|合同|租期)?", text)
        if m:
            return f"{m.group(1)}年"
        return ""
    num = str(m.group(1)).strip()
    unit = str(m.group(2)).strip().lower()
    if unit in {"year", "years", "yr", "yrs"}:
        unit = "年"
    elif unit in {"month", "months", "mo"}:
        unit = "个月"
    return f"{num}{unit}" if num and unit else ""


def _payment_contract_summary(d: dict) -> str:
    payment_terms = _normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default="")) or "待确认"
    contract_term = _normalize_contract_term(_listing_value(d, "contract_term", default="")) or "待确认"
    return f"付款/合同：{payment_terms}｜{contract_term}"


def _marketing_points(d: dict, fallback_points: list[str], max_n: int = 2) -> list[str]:
    raw_highlights = _as_list(d.get("highlights")) + _as_list(_parsed_normalized(d).get("highlights"))
    pool = [*raw_highlights, *fallback_points, "实拍房源", "中文顾问可约看房"]
    out: list[str] = []
    for item in pool:
        cleaned = _compact_copy(item, 18)
        if cleaned and cleaned not in out:
            out.append(cleaned)
        if len(out) >= max_n:
            break
    return out


def _is_noisy_highlight(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return True
    if len(s) < 2 or len(s) > 28:
        return True
    if s.isdigit():
        return True
    bad_needles = (
        "http",
        "t.me",
        "微信",
        "vx",
        "联系",
        "私聊",
        "咨询",
        "频道",
        "广告",
        "推广",
        "@",
    )
    return any(x in s for x in bad_needles)


def _normalize_fact_fragment(text: str, max_len: int = 18) -> str:
    cleaned = re.sub(r"[|｜]+", " ", str(text or "").strip())
    cleaned = _compact_copy(cleaned, max_len).strip("，。；;、 ")
    return cleaned


_PUBLIC_INTERNAL_ID_RE = re.compile(
    # QCxxxx is the approved public identifier; only legacy/internal identifiers
    # remain forbidden in customer-facing output.
    r"(?i)(?<![A-Za-z0-9])(?:B\d{3,}|L_?\d{2,}|SP_?\d{2,})(?![A-Za-z0-9])"
)
_PUBLIC_TOKEN_RE = re.compile(r"(?:\{\{[^{}]+\}\}|\$\{[^{}]+\})")

def _public_clean_text(value: object, *, keep_project: bool = False) -> str:
    """最终公开输出清洗：移除来源/内部编号，不改真正项目名正文。"""
    text = str(value or "").strip()
    text = _PUBLIC_INTERNAL_ID_RE.sub("", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"^[\s|｜·•:：、,，-]+|[\s|｜·•:：、,，-]+$", "", text)
    return text.strip()

def _semantic_area_values(d: dict) -> tuple[str, str, str]:
    nd = _parsed_normalized(d)
    land = _public_clean_text(d.get("land_size") or nd.get("land_size") or "")
    building = _public_clean_text(d.get("building_size") or nd.get("building_size") or "")
    size = _public_clean_text(d.get("size") or d.get("size_sqm") or nd.get("size") or nd.get("size_sqm") or "")
    # 带乘号的表达只能保留为尺寸，绝不能截成单一 sqm。
    if "×" in size or "x" in size.lower() or "*" in size:
        size = ""
    return size, land, building

def _public_area_lines(d: dict) -> list[str]:
    size, land, building = _semantic_area_values(d)
    out = []
    if size:
        value = size if re.search(r"㎡|m²|sqm", size, re.I) else f"{size}㎡"
        out.append(f"<b>面积：</b>{he(value)}")
    if land:
        out.append(f"<b>土地尺寸：</b>{he(land)}")
    if building:
        out.append(f"<b>建筑尺寸：</b>{he(building)}")
    return out

def assert_public_output_safe(*values: object, context: str = "public_output") -> None:
    visible = []
    for value in values:
        text = re.sub(r"<[^>]+>", "", str(value or ""))
        visible.append(text)
    blob = "\n".join(visible)
    token = _PUBLIC_TOKEN_RE.search(blob)
    if token:
        raise ValueError(f"public_output_unresolved_token:{context}:{token.group(0)}")
    internal = _PUBLIC_INTERNAL_ID_RE.search(blob)
    if internal:
        raise ValueError(f"public_output_internal_id:{context}:{internal.group(0)}")
    contact = re.search(
        r"(?:https?://|www\.|t\.me/|(?<![\w@])@[A-Za-z][A-Za-z0-9_]{3,}|"
        r"(?<!\d)(?:\+?\d[\d\s().-]{6,}\d)(?!\d))",
        blob, flags=re.I,
    )
    if contact:
        raise ValueError(f"public_output_source_contact:{context}:{contact.group(0)}")
    attribution = re.search(
        r"(?:来源频道|原频道|来源联系人|原联系人|联系(?:方式|人)?\s*[:：]|"
        r"微信\s*[:：]|wechat\s*[:：]|whatsapp\s*[:：]|telegram\s*[:：])",
        blob, flags=re.I,
    )
    if attribution:
        raise ValueError(f"public_output_source_attribution:{context}:{attribution.group(0)}")


def _canonical_highlight_phrase(text: str) -> str:
    s = str(text or "").strip()
    low = s.lower()
    if not s:
        return ""
    if ("家具" in s or "furnish" in low) and ("家电" in s or "齐全" in s or "拎包" in s):
        return "家具家电齐全"
    if "采光" in s:
        return "采光好"
    if "景观" in s or "view" in low:
        return "视野好"
    return s


def _collect_fee_fragments(raw_text: str, max_n: int = 2) -> list[str]:
    if not raw_text:
        return []
    keywords = (
        "押",
        "付",
        "物业",
        "管理",
        "停车",
        "水",
        "电",
        "网",
        "清洁",
        "垃圾",
        "费",
        "包含",
        "不含",
        "包",
        "include",
        "exclude",
    )
    out: list[str] = []
    for part in re.split(r"[；;，,\n/]+", str(raw_text)):
        cleaned = _normalize_fact_fragment(part, 16)
        if not cleaned or _is_noisy_highlight(cleaned):
            continue
        low = cleaned.lower()
        if not any((k in cleaned) or (k in low) for k in keywords):
            continue
        if cleaned not in out:
            out.append(cleaned)
        if len(out) >= max_n:
            break
    return out


def _factual_highlight_text(d: dict) -> str:
    out: list[str] = []

    size = _listing_value(d, "size", "size_sqm", default="")
    if size:
        raw = str(size).strip()
        if raw and raw.replace(".", "", 1).isdigit():
            raw = f"{raw}平"
        fact = _normalize_fact_fragment(raw, 10)
        if fact:
            out.append(fact)

    floor = _display_floor(_listing_value(d, "floor", default=""))
    if floor:
        fact = _normalize_fact_fragment(floor, 10)
        if fact and fact not in out:
            out.append(fact)

    furniture = furniture_text(d)
    if furniture and furniture != "可咨询确认":
        fact = _canonical_highlight_phrase(_normalize_fact_fragment(furniture, 12))
        if fact and fact not in out and all(fact not in x and x not in fact for x in out):
            out.append(fact)

    available = _listing_value(d, "available_date", default="")
    if available:
        fact = _normalize_fact_fragment(f"可{available}入住", 14)
        if fact and fact not in out:
            out.append(fact)

    raw_highlights = _as_list(d.get("highlights")) + _as_list(_parsed_normalized(d).get("highlights"))
    feature_needles = (
        "采光",
        "安静",
        "景观",
        "高层",
        "低楼层",
        "电梯",
        "泳池",
        "健身",
        "宠",
        "停车",
        "阳台",
        "新装修",
        "近",
        "通勤",
        "拎包",
        "家具",
        "通透",
        "视野",
        "全新",
        "南北",
        "朝南",
        "网络",
        "打扫",
        "实拍",
        "可看房",
        "view",
        "quiet",
        "balcony",
        "pool",
        "gym",
        "furnished",
    )
    for item in raw_highlights:
        cleaned = _canonical_highlight_phrase(_normalize_fact_fragment(item, 16))
        if _is_noisy_highlight(cleaned):
            continue
        low = cleaned.lower()
        if not any((needle in cleaned) or (needle in low) for needle in feature_needles):
            continue
        if cleaned not in out and all(cleaned not in x and x not in cleaned for x in out):
            out.append(cleaned)
        if len(out) >= 2:
            break
    if not out:
        return "以实拍与现场为准"
    return "；".join(out[:2])


def _factual_fee_text(d: dict) -> str:
    notes = _collect_fee_fragments(
        "；".join(
            [
                _listing_value(d, "cost_notes", default=""),
                _listing_value(d, "hidden_costs", default=""),
                _listing_value(d, "drawbacks", default=""),
            ]
        ),
        max_n=2,
    )

    payment_contract = _payment_contract_summary(d)
    if payment_contract and payment_contract not in notes:
        notes.insert(0, payment_contract)
    if not notes:
        return "付款方式和合同年限待确认，可先约看房"
    return "；".join(notes[:2])


def _audience_hint(room_type: str, d: dict) -> str:
    raw = str(room_type or "").lower()
    if "studio" in raw or "单间" in raw:
        base = "单人/情侣优先，通勤灵活"
    elif "1房" in raw or "一房" in raw:
        base = "单人或情侣，入住门槛低"
    elif "2房" in raw or "两房" in raw or "二房" in raw:
        base = "情侣或小家庭，功能更完整"
    elif any(x in raw for x in ("3房", "三房", "4房", "四房", "5房", "五房")):
        base = "家庭或多人同住，空间更充足"
    else:
        base = "可按预算和通勤再精筛同区房源"

    price = _price_value(d)
    if price > 0 and price <= 700:
        return f"{base}，预算友好"
    if price >= 1500:
        return f"{base}，偏中高配居住"
    return base


def _speed_hint(d: dict) -> str:
    price = _price_value(d)
    if 0 < price <= 700:
        return "该价位流转快，建议先锁看房时段"
    if price >= 1500:
        return "中高预算段可谈细节，先看房更有优势"
    return "同区域可快速对比，建议当天预约"


def _decision_hint(d: dict, note_text: str) -> str:
    note = str(note_text or "")
    if "押" in note or "付" in note:
        return "押付和费用细节可逐项确认后再定"
    if "物业" in note or "包" in note:
        return "费用边界先确认，再谈议价空间"
    if _price_value(d) <= 0:
        return "先确认租金区间，再决定是否线下看房"
    return "价格和空房以实时确认为准，建议先看再定"


def _normalize_caption_variant(caption_variant: str | None) -> str:
    v = str(caption_variant or "a").strip().lower()
    return v if v in {"a", "b", "c", "d"} else "a"


CAPTION_VARIANT_LABELS = {
    "a": "标准信息版",
    "b": "亮点价格版",
    "c": "专业参数版",
    "d": "简洁推广版",
}


def default_caption_variant_for_property(property_type: str | None) -> str:
    """Return the deterministic copy layout for one canonical property type.

    公寓/住宅强调手机首屏扫读；别墅/排屋强调空间与亮点；商办类强调
    参数和租赁条件。未知类型保守回退 A，不从标题或项目名重新猜类型。
    """
    raw = str(property_type or "").strip().lower()
    if any(token in raw for token in (
        "商铺", "店面", "办公室", "写字楼", "商业", "仓库", "厂房", "土地",
        "shop", "office", "commercial", "warehouse", "land",
    )):
        return "c"
    if any(token in raw for token in (
        "别墅", "排屋", "联排", "双拼", "整栋", "villa", "townhouse", "house", "building",
    )):
        return "b"
    return "a"


def resolve_caption_variant(d: dict, caption_variant: str | None = None) -> str:
    explicit = str(caption_variant or "").strip().lower()
    if explicit in {"a", "b", "c", "d"}:
        return explicit
    normalized = _parsed_normalized(d)
    property_type = (
        normalized.get("property_type_display")
        or normalized.get("property_type")
        or d.get("property_type_display")
        or d.get("property_type")
        or ""
    )
    return default_caption_variant_for_property(str(property_type))


def _attach_caption_variant_to_target(target: str, caption_variant: str | None = None) -> str:
    safe_target = str(target or "").strip()
    if not safe_target:
        return safe_target
    if re.search(r"(?:^|\|)cv=(a|b|c|d)(?:$|\|)", safe_target, flags=re.IGNORECASE):
        return safe_target
    raw_variant = str(caption_variant or "").strip().lower()
    if raw_variant not in {"a", "b", "c", "d"}:
        return safe_target
    return f"{safe_target}|cv={raw_variant}"


def _listing_ref_code(d: dict) -> str:
    existing = str(d.get("listing_id") or "").strip()
    if existing:
        return existing

    raw_id = str(d.get("id") or "").strip()
    if raw_id.isdigit():
        return f"l_{raw_id}"

    source_post_id = str(d.get("source_post_id") or "").strip()
    if source_post_id.isdigit():
        return f"sp_{source_post_id}"

    seed = "|".join(
        [
            str(d.get("draft_id") or "").strip(),
            str(d.get("title") or "").strip(),
            str(d.get("area") or "").strip(),
            str(d.get("layout") or "").strip(),
            str(d.get("price") or "").strip(),
        ]
    )
    digest = hashlib.md5(seed.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"ref_{digest}"


def _qc_code_from_draft(d: dict) -> str:
    """统一外显编号：QCxxxx（优先 listing_id / source_post_id 数字）。"""
    listing_id = str(d.get("listing_id") or "").strip()
    m = re.search(r"(\d{1,8})", listing_id)
    if m:
        return f"QC{m.group(1).zfill(4)}"

    source_post_id = str(d.get("source_post_id") or "").strip()
    digits = re.sub(r"\D", "", source_post_id)
    if digits:
        return f"QC{digits.zfill(4)}"

    raw_id = str(d.get("id") or "").strip()
    digits = re.sub(r"\D", "", raw_id)
    if digits:
        return f"QC{digits.zfill(4)}"

    ref_code = _listing_ref_code(d)
    digits = re.sub(r"\D", "", ref_code)
    if digits:
        return f"QC{digits.zfill(4)}"
    return "QC0000"


def _compact_listing_title(d: dict, area: str, room_type: str, price: str) -> str:
    """生成频道帖子标题：优先 项目名｜户型｜租金，始终保持短标题格式。
    只读 project/community，不读 title（title 字段往往是长句）。
    """
    raw = _listing_value(d, "project", "community", default="")
    project_label = _compact_copy(_clean_project_label(raw), 24) if raw else ""
    prefix = project_label or area
    return _compact_copy(f"{prefix}｜{room_type}｜{price}", 40)


_NOISE_KEYWORDS = (
    "噪",
    "马路",
    "高架",
    "临街",
    "车声",
    "highway",
    "loud",
    "noise",
    "吵",
    "嘈",
)
_MIN_LEASE_KEYWORDS = ("短租", "minimum", "min lease", "至少", "最少", "3个月", "半年")
_PARKING_KEYWORDS = ("停车", "parking", "车位少", "无车位")
_NO_PET_KEYWORDS = ("不允许宠物", "no pet", "禁止养宠")
_COMMERCIAL_ELEC_KEYWORDS = ("商业电", "商电", "commercial", "高电费", "电费贵")


def _contextual_viewing_hint(d: dict) -> str:
    """生成"提前说清"段：真实、靠谱，不写广告腔。最多 28 字。"""
    raw_notes = " ".join(
        [
            _listing_value(d, "cost_notes", default=""),
            _listing_value(d, "drawbacks", default=""),
            _listing_value(d, "hidden_costs", default=""),
        ]
    ).lower()

    if any(x in raw_notes for x in _NOISE_KEYWORDS):
        return "比较在意安静的话，看房时建议重点确认楼层和窗外环境"
    if any(x in raw_notes for x in _MIN_LEASE_KEYWORDS):
        return "有最短租期要求，短租需求请看房前先确认"
    if any(x in raw_notes for x in _PARKING_KEYWORDS):
        return "停车位有限，有用车需求的建议提前确认"
    if any(x in raw_notes for x in _NO_PET_KEYWORDS):
        return "业主不允许养宠，有宠物需求请提前说明"
    if any(x in raw_notes for x in _COMMERCIAL_ELEC_KEYWORDS):
        return "用的是商业电，电费会比民电高，建议看房时问清月均用电"

    # 无特定风险 → 通用付款提醒
    deposit = _confirmed_public_detail(_normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default="")))
    contract = _confirmed_public_detail(_normalize_contract_term(_listing_value(d, "contract_term", default="")))
    if deposit and contract:
        return _compact_copy(f"押付 {deposit}，合同 {contract}，细节看房前可逐项确认", 28)
    if deposit:
        return _compact_copy(f"押付 {deposit}，具体费用细节建议看房前问清", 28)
    return "价格和空房以实时确认为准，建议看房前先问清押付"


def _advisor_decision_hint(d: dict) -> str:
    """只基于已解析事实给建议，不虚构房源优缺点。"""
    raw_notes = " ".join(
        [
            _listing_value(d, "cost_notes", default=""),
            _listing_value(d, "drawbacks", default=""),
            _listing_value(d, "hidden_costs", default=""),
        ]
    ).lower()
    if any(x in raw_notes for x in _NOISE_KEYWORDS):
        return "位置方便；重视安静建议优先看高楼层"
    if any(x in raw_notes for x in _COMMERCIAL_ELEC_KEYWORDS):
        return "入住成本可能偏高，先核对月均电费"
    if any(x in raw_notes for x in _NO_PET_KEYWORDS):
        return "不适合养宠家庭，可让顾问另找同区房源"
    if any(x in raw_notes for x in _PARKING_KEYWORDS):
        return "有车用户先确认车位，再安排看房"
    return _factual_highlight_text(d)


def _verification_status_text(d: dict) -> str:
    """频道正式标准只展示状态，不让日期变化破坏固定版式。"""
    status = _listing_value(d, "verification_status", default="").strip().lower()
    if status in {"pending", "unverified", "待核验", "待确认"}:
        return "🟡 待核验"
    if status in {"expired", "已过期", "过期"}:
        return "🟠 需重新核验"
    if status in {"disputed", "有争议", "信息冲突"}:
        return "🔴 信息待复核"
    return "🟢 发布前已核实"


def _monthly_cost_summary(d: dict) -> str:
    """生成用户真正关心的入住后费用，不确定的信息明确标注待确认。"""
    fields = (
        ("管理", ("management_fee", "property_fee", "management")),
        ("水", ("water_rate", "water_fee")),
        ("电", ("electric_rate", "electricity_rate", "electric_fee")),
        ("网络", ("internet_fee", "wifi_fee", "internet")),
        ("停车", ("parking_fee", "parking")),
    )
    parts: list[str] = []
    for label, keys in fields:
        value = _listing_value(d, *keys, default="")
        cleaned = _normalize_fact_fragment(value, 14)
        if cleaned:
            parts.append(f"{label}{cleaned}")
    if parts:
        return "｜".join(parts[:5])

    notes = _collect_fee_fragments(
        "；".join(
            [
                _listing_value(d, "cost_notes", default=""),
                _listing_value(d, "hidden_costs", default=""),
            ]
        ),
        max_n=3,
    )
    if notes:
        return "｜".join(notes)
    return "管理费、水电、网络及停车待确认"


def _listing_detail_summary(d: dict) -> str:
    parts: list[str] = []
    property_type = _resolved_property_type(d)
    size = _listing_value(d, "size", "size_sqm", default="")
    floor = _display_floor(_listing_value(d, "floor", default=""))
    furniture = furniture_text(d)
    for value in (property_type, size, floor, furniture):
        cleaned = _normalize_fact_fragment(value, 12)
        if cleaned and cleaned != "可咨询确认" and cleaned not in parts:
            parts.append(cleaned)
    return "｜".join(parts[:4]) or "实拍房源"


def _caption_action_links(
    listing_id: str,
    listing: dict | None = None,
    post_token: str = "",
    caption_variant: str | None = "a",
) -> str:
    if not BOT_USERNAME:
        return ""
    user = BOT_USERNAME.lstrip("@")
    if post_token:
        appoint_payload = (
            f"a__{post_token}" if str(post_token).startswith("ql")
            else build_start_payload("a", listing_id, post_token)
        )
    else:
        appoint_payload = build_start_payload("a", listing_id)
    appoint = f"https://t.me/{user}?start={appoint_payload}"
    facts = listing or {}
    project = _compact_copy(_project_label_for_post(facts), 24)
    if not project:
        project = _compact_copy(
            _listing_value(facts, "public_location_display", "area", default=""),
            24,
        ) or "这套房"
    layout = _compact_copy(
        _display_layout(
            normalize_room_type(_listing_value(facts, "room_type", "layout", default="")),
            _resolved_property_type(facts),
        ),
        18,
    )
    price = _price_compact_for_post(facts)
    qc_code = _qc_code_from_draft({**facts, "listing_id": listing_id})
    summary = "｜".join(part for part in (project, layout, price) if part)
    consult_text = f"你好，我想咨询房源 {qc_code}"
    if summary:
        consult_text += f"\n（{summary}）"
    advisor = str(ADVISOR_TG or "@pengqingw").strip().lstrip("@") or "pengqingw"
    consult = f"https://t.me/{advisor}?text={quote(consult_text, safe='')}"
    return (
        f'<a href="{he(appoint, quote=True)}">📅 预约看房</a>'
        f'　｜　<a href="{he(consult, quote=True)}">💬 问这套</a>'
    )


def _discussion_fact_blob(d: dict) -> str:
    """只汇总可追溯的原始/结构化事实，供评论区费用与配套识别；不补造内容。"""
    normalized = _parsed_normalized(d)
    values = [
        _listing_value(d, "cost_notes", "hidden_costs", default=""),
        _listing_value(d, "raw_text", "source_text", default=""),
        "；".join(_as_list(d.get("highlights"))),
        "；".join(_as_list(normalized.get("highlights"))),
    ]
    return "\n".join(str(value) for value in values if str(value or "").strip())


def _discussion_fee_value(d: dict, keys: tuple[str, ...], aliases: tuple[str, ...]) -> str:
    direct = _normalize_fact_fragment(_listing_value(d, *keys, default=""), 20)
    if direct:
        return direct
    for segment in re.split(r"[\n；;]+", _discussion_fact_blob(d)):
        item = _normalize_fact_fragment(segment, 28)
        low = item.lower()
        if not item or not any(alias.lower() in low or alias in item for alias in aliases):
            continue
        if "免费" in item:
            return "免费"
        if "已含" in item or "包含" in item or "包" in item:
            return "已含"
        match = re.search(r"(?:[:：｜|]|为|是)\s*([^，,；;]+)", item)
        if match:
            value = _normalize_fact_fragment(match.group(1), 20)
            if value:
                return value
    return ""


def _discussion_amenities(d: dict) -> list[str]:
    blob = _discussion_fact_blob(d).lower()
    groups = (
        ("家具家电齐全", ("家具家电齐全", "家具齐全", "全家具", "fully furnished")),
        ("泳池", ("泳池", "pool")),
        ("健身房", ("健身", "gym")),
        ("停车场", ("停车场", "parking lot")),
        ("24H安保", ("24h安保", "24小时安保", "24h security", "security")),
    )
    return [label for label, needles in groups if any(needle in blob for needle in needles)]


def _confirmed_public_detail(value: object, max_len: int = 24) -> str:
    cleaned = _normalize_fact_fragment(str(value or ""), max_len)
    if not cleaned:
        return ""
    compact = cleaned.replace(" ", "")
    if any(token in compact for token in ("待确认", "待定", "未知", "___", "【", "】")):
        return ""
    return cleaned


def build_discussion_detail_text(d: dict) -> str:
    """评论区详情卡：仅输出已确认值，不生成横线、括号或“待确认”占位。"""
    raw_price = _listing_value(d, "price", default="")
    price = _price_compact_for_post(d) if (_price_value(d) > 0 or _price_is_consultable(raw_price)) else ""
    contract = _confirmed_public_detail(_normalize_contract_term(_listing_value(d, "contract_term", default="")))
    deposit = _confirmed_public_detail(_normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default="")))
    available = _confirmed_public_detail(_listing_value(d, "available_date", default=""), 16)
    management = _confirmed_public_detail(_discussion_fee_value(d, ("management_fee", "property_fee", "management"), ("管理费", "物业费", "property fee", "management fee")))
    internet = _confirmed_public_detail(_discussion_fee_value(d, ("internet_fee", "wifi_fee", "internet"), ("网络", "网费", "wifi", "internet")))
    water = _confirmed_public_detail(_discussion_fee_value(d, ("water_rate", "water_fee"), ("水费", "水", "water")))
    electric = _confirmed_public_detail(_discussion_fee_value(d, ("electric_rate", "electricity_rate", "electric_fee"), ("电费", "电", "electric")))
    parking = _confirmed_public_detail(_discussion_fee_value(d, ("parking_fee", "parking"), ("停车费", "停车", "车位", "parking")))
    amenities = _discussion_amenities(d)

    normalized = _parsed_normalized(d)
    area = _confirmed_public_detail(
        normalized.get("public_location_display")
        or normalized.get("normalized_area")
        or _listing_value(d, "public_location_display", "area", default=""),
        24,
    )
    property_type = _confirmed_public_detail(_resolved_property_type(d), 18)
    layout = _confirmed_public_detail(
        normalize_room_type(_listing_value(d, "room_type", "layout", default="")),
        24,
    )
    size = _confirmed_public_detail(_listing_value(d, "size", "size_sqm", default=""), 18)
    floor = _confirmed_public_detail(_display_floor(_listing_value(d, "floor", default="")), 18)
    qc_code = _qc_code_from_draft(d)
    lines = [f"🏠 <b>房源信息</b>｜<code>{he(qc_code)}</code>"]
    for line in (
        f"📍 区域｜{he(area)}" if area else "",
        f"🏡 类型｜{he(property_type)}" if property_type else "",
        f"🛏 户型｜{he(layout)}" if layout else "",
        f"📐 面积｜{he(size)}" if size else "",
        f"🏢 楼层｜{he(floor)}" if floor else "",
    ):
        if line:
            lines.append(line)
    rental_lines: list[str] = []
    if price:
        rental_lines.append(f"月租｜<b>{he(price)}</b>")
    if contract:
        rental_lines.append(f"租期｜{he(contract)}")
    if deposit:
        rental_lines.append(f"押付｜{he(deposit)}")
    if available:
        rental_lines.append(f"入住｜{he(available)}")
    if rental_lines:
        lines.extend(["", *rental_lines])

    fee_lines: list[str] = []
    if management or internet:
        fee_lines.append("　·　".join(
            part for part in (
                f"管理费｜{he(management)}" if management else "",
                f"网络｜{he(internet)}" if internet else "",
            ) if part
        ))
    if water or electric:
        fee_lines.append("　·　".join(
            part for part in (
                f"水费｜{he(water)}" if water else "",
                f"电费｜{he(electric)}" if electric else "",
            ) if part
        ))
    if parking:
        fee_lines.append(f"停车｜{he(parking)}")
    cost_notes = _confirmed_public_detail(_listing_value(d, "cost_notes", default=""), 80)
    if cost_notes and cost_notes not in " ".join(fee_lines):
        fee_lines.append(f"费用说明｜{he(cost_notes)}")
    if fee_lines:
        lines.extend(["", "🧾 <b>费用</b>", *fee_lines])
    if amenities:
        lines.extend(["", "🏡 <b>配套</b>", " · ".join(he(item) for item in amenities)])
    viewing_time = _confirmed_public_detail(_listing_value(d, "viewing_time", "viewing_hours", default=""), 28)
    video_viewing = _confirmed_public_detail(_listing_value(d, "video_viewing", "video_tour", default=""), 20)
    viewing: list[str] = []
    if viewing_time:
        viewing.append(f"看房时间｜{he(viewing_time)}")
    if video_viewing:
        viewing.append(f"视频看房｜{he(video_viewing)}")
    if viewing:
        lines.extend(["", "📅 <b>看房</b>", *viewing])
    return "\n".join(lines)[:4096]


def _short_room_label(value: str) -> str:
    """首页标题只显示房数，完整户型仍放在面积行，避免首屏过长。"""
    text = str(value or "").strip()
    match = re.search(r"(\d{1,2})\s*(?:\+\s*\d{1,2}\s*)?房", text)
    if not match:
        return text
    numerals = {"0": "零", "1": "一", "2": "两", "3": "三", "4": "四", "5": "五", "6": "六", "7": "七", "8": "八", "9": "九", "10": "十"}
    number = match.group(1)
    return f"{numerals.get(number, number)}房"


def build_chinese_listing_post(
    d: dict,
    caption_variant: str | None = None,
    post_token: str = "",
    has_extra_photos: bool = False,
) -> str:
    """Build one of three factual Telegram HTML layouts from one fact object."""
    normalized = _parsed_normalized(d)
    variant = resolve_caption_variant(d, caption_variant)
    area = str(normalized.get("normalized_area") or d.get("normalized_area") or d.get("area") or "").strip()
    area_display = f"{area}附近" if area and bool(normalized.get("nearby")) else area
    project = _compact_copy(_project_label_for_post(d), 20) or _compact_copy(area_display, 20) or "金边房源"
    room_type = normalize_room_type(_listing_value(d, "room_type", "layout", default="")) or _resolved_property_type(d)
    room_type = _compact_copy(room_type, 16) or "整租"
    listing_id = system_listing_id_from_draft(d)
    price = _price_compact_for_post(d)
    size, _land_size, _building_size = _semantic_area_values(d)
    size = _normalize_fact_fragment(size, 12)
    if size and re.fullmatch(r"\d+(?:\.\d+)?", size):
        size = f"{size}㎡"
    deposit = _confirmed_public_detail(
        _normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default=""))
    )
    contract = _confirmed_public_detail(
        _normalize_contract_term(_listing_value(d, "contract_term", default=""))
    )
    action_links = _caption_action_links(
        listing_id, listing=d, post_token=post_token, caption_variant=variant
    )

    normalized_type = str(normalized.get("property_type_display") or "").strip()
    heading_type = normalized_type or str(normalized.get("property_type") or d.get("property_type_display") or d.get("property_type") or "").strip()
    heading_type = _compact_copy(heading_type or _resolved_property_type(d), 16)
    room_type = _display_layout(room_type, heading_type)
    # 所有公开外显统一使用 QC 编号；内部 listing_id 不直接展示。
    qc_code = _qc_code_from_draft(d)
    title_room_match = re.search(r"\d{1,2}(?:\s*\+\s*\d{1,2})?房", room_type)
    title_room = re.sub(r"\s+", "", title_room_match.group(0)) if title_room_match else room_type
    floor = _display_floor(_listing_value(d, "floor", default=""))
    raw_original = _listing_value(d, "original_price", "original_monthly_rent_usd", default="")
    try:
        original_value = int(float(raw_original.replace("$", "").replace(",", "").strip()))
    except (TypeError, ValueError):
        original_value = 0
    current_value = _price_value(d)
    original_price = f"${original_value:,}" if original_value > 0 and original_value != current_value else ""
    price_markup = (
        f"<s>{he(original_price)}</s>　<b>{he(price)}</b>"
        if original_price else f"<b>{he(price)}</b>"
    )

    raw_highlights = _as_list(normalized.get("highlights")) + _as_list(d.get("highlights"))
    caption_highlights: list[str] = []
    for item in raw_highlights:
        cleaned = _canonical_highlight_phrase(_normalize_fact_fragment(item, 18))
        if _is_noisy_highlight(cleaned) or cleaned in {"实拍房源", "可预约看房", "中文顾问"}:
            continue
        if cleaned not in caption_highlights:
            caption_highlights.append(cleaned)
        if len(caption_highlights) >= 3:
            break
    verification = str(_listing_value(d, "verification_status", default="")).strip()
    verified_line = ""
    if "待核验" in verification or "待确认" in verification:
        verified_line = "🟡 待核验"
    else:
        approved_at = str(d.get("approved_at") or "").strip()
        verified_date = re.search(r"\d{4}-(\d{1,2})-(\d{1,2})", approved_at)
        if verified_date:
            verified_line = f"🟢 {int(verified_date.group(1))}月{int(verified_date.group(2))}日已核实"
    comment_line = (
        "📸 更多实拍与费用说明在评论区👇"
        if has_extra_photos
        else "📋 费用、押付与配套见评论区👇"
    )
    tag_values = ["金边租房"]
    for raw in (area_display, heading_type, title_room):
        tag = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(raw or "").replace("+", "加"))
        if tag and tag not in tag_values:
            tag_values.append(tag)
    tag_line = " ".join(f"#{tag}" for tag in tag_values[:4])
    # 生产只保留一个简洁版本。按钮已经承担咨询和预约动作，正文不再堆链接、
    # 内部状态或多套 A/B/C 口径。
    simple_heading = "｜".join(
        dict.fromkeys(part for part in (area_display, project, title_room) if part)
    )
    fact_parts = [part for part in (room_type, _confirmed_public_detail(size), _confirmed_public_detail(floor)) if part]
    public_tag = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", area_display or "金边租房")
    price_line = f"💰 <b>{he(price)}</b>/月" if price and "/月" not in price else f"💰 <b>{he(price)}</b>"
    public_floor = _confirmed_public_detail(floor)
    public_size = _confirmed_public_detail(size)
    if deposit:
        optional_fact_line = f"🔑 押付：{he(deposit)}"
    elif public_size:
        optional_fact_line = f"📐 面积：{he(public_size)}"
    elif public_floor:
        optional_fact_line = f"🏢 楼层：{he(public_floor)}"
    elif contract:
        optional_fact_line = f"📄 租期：{he(contract)}"
    elif caption_highlights:
        optional_fact_line = f"✨ 亮点：{he(caption_highlights[0])}"
    else:
        optional_fact_line = ""
    if variant == "b":
        lines = [
            price_line,
            f"🏠 <b>{he(simple_heading or '金边实拍房源')}</b>",
            f"<code>{he(qc_code)}</code>",
            f"✨ {' · '.join(he(item) for item in caption_highlights[:3])}" if caption_highlights else "",
            " · ".join(he(part) for part in fact_parts),
            f"🔑 {he(deposit)}" if deposit else "",
            f"📄 {he(contract)}" if contract else "",
            "",
            "📸 更多实拍与预约入口在评论区👇",
            f"#{public_tag} #金边租房",
        ]
    elif variant == "c":
        lines = [
            f"🏢 <b>{he(project or area_display or '金边实拍房源')}</b>",
            f"<code>{he(qc_code)}</code>",
            f"📍 位置｜{he(area_display)}" if area_display else "",
            f"🏷 类型｜{he(heading_type)}" if heading_type else "",
            f"🛏 户型｜{he(room_type)}" if room_type else "",
            f"📐 面积｜{he(public_size)}" if public_size else "",
            f"🏢 楼层｜{he(public_floor)}" if public_floor else "",
            f"💰 租金｜<b>{he(price)}</b>",
            f"🔑 押付｜{he(deposit)}" if deposit else "",
            f"📄 租期｜{he(contract)}" if contract else "",
            "",
            "📸 更多实拍与预约入口在评论区👇",
            f"#{public_tag} #金边租房",
        ]
    elif variant == "d":
        lines = [
            f"🏠 <b>{he(simple_heading or '金边实拍房源')}</b>",
            f"{price_line}　<code>{he(qc_code)}</code>",
            " · ".join(he(part) for part in fact_parts),
            f"✨ {' · '.join(he(item) for item in caption_highlights[:2])}" if caption_highlights else "",
            "",
            "📸 更多实拍与预约入口在评论区👇",
            f"#{public_tag} #金边租房",
        ]
    else:
        lines = [
            f"#金边租房 #{public_tag} #精装{re.sub(r'[^0-9A-Za-z\u4e00-\u9fff]+', '', heading_type or '房源')}",
            "",
            f"🏠 <b>侨联地产｜{he(title_room)}</b>",
            "",
            f"💰 月租：<b>{he(price)}</b>" if price else "",
            f"📍 区域：{he(area_display)}" if area_display else "",
            f"🛏 户型：{he(room_type)}" if room_type else "",
            optional_fact_line,
            "",
            "──────────────",
            action_links,
            "──────────────",
            "",
            "📸 更多实拍与预约入口在评论区👇",
        ]
    # 统一手机端正文：封面可以保留不同视觉风格，但正文只保留一套字段顺序，避免A/B/C/D变体造成用户理解成本。
    # 移动端首屏：只保留用户决定是否咨询所需的核心字段，避免项目名/区域重复。
    # 项目名和区域是两个不同事实。标题优先展示项目名，区域单独一行；
    # 只有没有项目名时才用区域兜底，避免 SKYTREE 被改写成“钻石岛”。
    compact_heading = "｜".join(
        dict.fromkeys(part for part in (project or area_display, title_room) if part)
    )
    compact_type = "｜".join(part for part in (heading_type, public_size) if part)
    compact_rental = "｜".join(part for part in (deposit, contract) if part)
    unified_lines = [
        f"#{public_tag} #金边租房",
        "",
        f"🏠 <b>{he(compact_heading or '金边实拍房源')}</b>",
        f"💰 <b>{he(price)}</b>/月" if price and "/月" not in price else (f"💰 <b>{he(price)}</b>" if price else ""),
        f"📍 {he(area_display)}" if area_display and area_display != project else "",
        f"🛏 {he(room_type)}" if room_type else "",
        f"🏢 {he(compact_type)}" if compact_type else "",
        f"🔑 {he(compact_rental)}" if compact_rental else "",
        f"✨ {' · '.join(he(item) for item in caption_highlights[:2])}" if caption_highlights else "",
        verified_line,
        f"📸 <code>{he(qc_code)}</code>｜{he(comment_line.replace('👇', ''))}",
    ]
    lines = unified_lines
    if action_links and action_links not in lines:
        insert_at = len(lines) - 1 if lines and str(lines[-1]).startswith("#") else len(lines)
        lines[insert_at:insert_at] = ["──────────────", action_links, "──────────────"]
    compact: list[str] = []
    for line in lines:
        if not line:
            if compact and compact[-1]:
                compact.append("")
            continue
        compact.append(line)
    while compact and not compact[-1]:
        compact.pop()
    return "\n".join(compact).strip()[:1024]

    section_break = "__CAPTION_SECTION_BREAK__"

    if variant == "b":
        summary = "｜".join(part for part in (area_display, room_type, size) if part)
        lines = [
            f"📍 <b>{he(summary or project)}</b>",
            f"<code>侨联 #{he(qc_code)}</code>",
            section_break,
            f"💰 {price_markup}",
            f"🏠 {he(project)}｜{he(heading_type)}" if project and heading_type else "",
            f"🏢 {he(floor)}" if floor else "",
            f"<i>亮点｜{' · '.join(he(item) for item in caption_highlights)}</i>" if caption_highlights else "",
            verified_line,
            section_break,
            "<code>实拍房源｜中文顾问｜可预约看房</code>",
        ]
    elif variant == "c":
        heading = "｜".join(dict.fromkeys(part for part in (project, heading_type) if part))
        lines = [
            f"🏢 <b>{he(heading or area_display or '金边房源')}</b>｜{price_markup}",
            f"<code>QIAOLIAN PROPERTY · {he(qc_code)}</code>",
            section_break,
            "━━━━━━━━━━━━",
            f"📍 位置｜{he(area_display)}" if area_display else "",
            f"🏷 类型｜{he(heading_type)}" if heading_type else "",
            f"🛏 户型｜{he(room_type)}" if room_type else "",
            f"📐 面积｜<u>{he(size)}</u>" if size else "",
            f"🏢 楼层｜{he(floor)}" if floor else "",
            f"💰 租金｜{price_markup}",
            f"🔑 押付｜{he(deposit)}" if deposit else "",
            f"📄 租期｜{he(contract)}" if contract else "",
            f"✨ {' · '.join(he(item) for item in caption_highlights)}" if caption_highlights else "",
            verified_line,
        ]
    else:
        heading = "｜".join(dict.fromkeys(part for part in (project, title_room) if part))
        lines = [
            f"<b>🏠 {he(heading or area_display or '金边房源')}</b>",
            f"<code>侨联 #{he(qc_code)}</code>",
            section_break,
            f"💰 月租｜{price_markup}",
            f"📍 区域｜{he(area_display)}" if area_display else "",
            f"🛏 户型｜{he(room_type)}" if room_type else "",
            f"📐 面积｜<u>{he(size)}</u>" if size else "",
            f"🏢 楼层｜{he(floor)}" if floor else "",
            f"🏷 类型｜{he(heading_type)}" if heading_type else "",
            f"<i>亮点｜{' · '.join(he(item) for item in caption_highlights)}</i>" if caption_highlights else "",
            verified_line,
            section_break,
            "<code>实拍房源｜中文顾问｜可预约看房</code>",
        ]

    lines = [line for line in lines if line]
    while lines and lines[-1] == section_break:
        lines.pop()
    lines.extend([section_break, comment_line])
    if action_links:
        lines.extend(["──────────────", action_links, "──────────────"])
    if tag_line:
        lines.append(tag_line)
    compact_lines: list[str] = []
    for line in lines:
        if line == section_break:
            line = ""
        if not line and compact_lines and not compact_lines[-1]:
            continue
        compact_lines.append(line)
    return "\n".join(compact_lines).strip()[:1024]

def build_cover_listing_data(d: dict) -> dict:
    normalized = _parsed_normalized(d)
    return {
        "project": normalized.get("project_name") or d.get("project_name") or d.get("project") or "",
        "project_alias": normalized.get("project_alias") or d.get("project_alias") or "",
        "property_type": normalized.get("property_type_display") or d.get("property_type_display") or d.get("property_type") or "",
        "layout": normalized.get("layout") or d.get("layout") or d.get("room_type") or "",
        "price": normalized.get("monthly_rent_usd") or d.get("price"),
        "area": normalized.get("public_location_display") or d.get("public_location_display") or d.get("area") or "",
        "size": normalized.get("size_sqm") or d.get("size") or "",
        "floor": normalized.get("floor") or d.get("floor") or "",
        "highlights": _listing_highlight_pills(d),
    }


def _base36_encode(value: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value <= 0:
        return "0"
    out: list[str] = []
    n = value
    while n:
        n, rem = divmod(n, 36)
        out.append(digits[rem])
    return "".join(reversed(out))


def make_post_token(channel_message_id: int | str | None) -> str:
    try:
        return _base36_encode(int(channel_message_id or 0))
    except (TypeError, ValueError):
        return ""


def build_start_payload(
    action: str,
    target: str,
    post_token: str = "",
    caption_variant: str | None = None,
) -> str:
    safe_target = _attach_caption_variant_to_target(target, caption_variant=caption_variant)
    if post_token:
        return f"{action}__{post_token}__{safe_target}"
    return f"{action}_{safe_target}"


def build_caption_consult_lines(d: dict, caption_variant: str | None = "a") -> list[str]:
    if BOT_USERNAME:
        return ["点下方「咨询这套」即可对接中文顾问"]
    return [f"咨询：{ADVISOR_TG}"]

def system_listing_id_from_draft(d: dict) -> str:
    """统一新房源编号：展示/深链都使用 l_房源ID，例如 l_1024。"""
    existing = str(d.get("listing_id") or "").strip()
    # 兼容历史 l_1024 / L1024 / L_1024 / QJ-1024 / QC1024，
    # 新发布统一收敛到内部协议 l_1024。
    match = re.fullmatch(r"(?i)(?:l[_-]?|qj[-_]?|qc[-_]?)(\d+)", existing)
    if match:
        return f"l_{match.group(1)}"
    if existing and not existing.startswith("LST_"):
        return existing
    raw_id = d.get("id")
    if raw_id not in (None, ""):
        try:
            return f"l_{int(raw_id)}"
        except (TypeError, ValueError):
            pass
    return f"l_{int(time.time())}"


def display_listing_id(listing_id: str) -> str:
    """所有客户、频道和管理端外显统一为 QCxxxx；内部仍保留 l_xxxx。"""
    raw = str(listing_id or "").strip()
    match = re.fullmatch(r"(?i)l[_-]?(\d+)", raw)
    if match:
        return f"QC{int(match.group(1)):04d}"
    return raw.upper()


def build_caption(d: dict, caption_variant: str | None = None) -> str:
    """发布层统一生成中文租房帖，不透传 AI 模板文案。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)

def build_detail_text(d: dict, caption_variant: str | None = None) -> str:
    """文字消息正文：统一中文结构，避免模板名/开发调试词进入频道。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)


def build_rich_album_caption(d: dict, caption_variant: str | None = None) -> str:
    """频道主帖文案：固定中文租房结构，不展示内部编号或模板名。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)


def build_channel_teaser_caption(d: dict, caption_variant: str | None = None) -> str:
    """频道首图 caption 同样使用中文租房结构。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)


def _merge_photo_labels_into_caption(main: str, photo_labels: list[str]) -> str:
    """
    Telegram 相册只在首图下展示一条 caption；逐张说明合并进首图。
    photo_labels 顺序对应「封面后的第 1 张实拍」起。
    """
    if not photo_labels:
        return main[:1024]
    lines = ["", "--- PHOTO INDEX ---"]
    for i, lab in enumerate(photo_labels, start=2):
        t = str(lab).strip()
        if not t:
            continue
        lines.append(f"{i}｜{t}")
    extra = "\n".join(lines)
    if len(main) + len(extra) <= 1024:
        return (main + extra)[:1024]
    out = main
    for i, lab in enumerate(photo_labels, start=2):
        t = str(lab).strip()
        if not t:
            continue
        piece = f"\n{i}｜{t}"
        if len(out) + len(piece) <= 1024:
            out += piece
        else:
            break
    return out[:1024]


def _image_difference_hash(path: str) -> int | None:
    """低成本 dHash；坏图返回 None，由调用方按原顺序容错。"""
    try:
        with Image.open(path) as image:
            gray = image.convert("L").resize((9, 8), Image.Resampling.BILINEAR)
            pixels = list(gray.getdata())
        value = 0
        for row in range(8):
            offset = row * 9
            for col in range(8):
                value = (value << 1) | int(pixels[offset + col] > pixels[offset + col + 1])
        return value
    except Exception:
        logger.warning("差异选图无法读取，保留顺序容错: %s", path)
        return None


def _select_diverse_detail_paths(paths: list[str], limit: int) -> list[str]:
    """贪心选择与已选图片最不相似的实拍，避免连续相似角度。"""
    unique = list(dict.fromkeys(paths))
    if limit <= 0 or len(unique) <= limit:
        return unique[:limit]
    hashes = {path: _image_difference_hash(path) for path in unique}
    valid = [path for path in unique if hashes[path] is not None]
    selected: list[str] = valid[:1]
    remaining = valid[1:]
    while remaining and len(selected) < limit:
        candidate = max(
            remaining,
            key=lambda path: min((hashes[path] ^ hashes[chosen]).bit_count() for chosen in selected),
        )
        selected.append(candidate)
        remaining.remove(candidate)
    for path in unique:
        if len(selected) >= limit:
            break
        if path not in selected:
            selected.append(path)
    return selected


def split_album_for_channel(paths: list[str]) -> tuple[list[str], list[str]]:
    """返回频道首页与关联评论图；extra 按未选路径计算，绝不使用位置切片。"""
    unique = list(dict.fromkeys(paths or []))
    if not unique:
        return [], []
    cap = max(1, CHANNEL_MAIN_ALBUM_MAX)
    cover = unique[0]
    selected = [cover, *_select_diverse_detail_paths(unique[1:], cap - 1)]
    selected = selected[:cap]
    selected_set = set(selected)
    extra = [path for path in unique if path not in selected_set]
    return selected, extra


def normalize_album_grid(paths: list[str]) -> list[str]:
    """兼容旧调用：频道默认保持封面 + 三张差异较大的独立实拍。"""
    selected, _ = split_album_for_channel(paths)
    return selected


async def resolve_discussion_chat_id(bot: Bot) -> str | None:
    """
    讨论组 chat id（字符串，含负数群 id）。
    排查顺序建议：① 频道是否绑定讨论组 ② Bot 是否在讨论组且可发言
    ③ 是否用「回复自动转发帖」识别（is_automatic_forward）④ 尽量别手填错 DISCUSSION_CHAT_ID。
    优先读环境变量 DISCUSSION_CHAT_ID，否则 get_chat(CHANNEL_ID).linked_chat_id。
    """
    if DISCUSSION_CHAT_ID:
        return str(DISCUSSION_CHAT_ID)
    try:
        ch = await bot.get_chat(CHANNEL_ID)
        linked = getattr(ch, "linked_chat_id", None)
        if linked:
            return str(linked)
    except Exception:
        return None
    return None


async def resolve_discussion_id(bot: Bot) -> int | None:
    """与 resolve_discussion_chat_id 同源；返回 int 讨论组 id，未配置则 None。"""
    raw = await resolve_discussion_chat_id(bot)
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def load_discuss_map() -> dict:
    if DISCUSSION_MAP_FILE.exists():
        try:
            with open(DISCUSSION_MAP_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            logger.exception("读取 discussion_map 失败")
    return {}


def save_discuss_map(data: dict) -> None:
    DISCUSSION_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCUSSION_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _default_discussion_bridge() -> dict:
    return {"publish_queue": [], "discuss_mgid": {}, "pending_forwards": []}


def load_discussion_bridge() -> dict:
    if DISCUSSION_BRIDGE_FILE.exists():
        try:
            with open(DISCUSSION_BRIDGE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("publish_queue", [])
                    data.setdefault("discuss_mgid", {})
                    data.setdefault("pending_forwards", [])
                    if not isinstance(data["publish_queue"], list):
                        data["publish_queue"] = []
                    if not isinstance(data["discuss_mgid"], dict):
                        data["discuss_mgid"] = {}
                    return data
        except Exception:
            logger.exception("读取 discussion_bridge 失败")
    return _default_discussion_bridge()


def save_discussion_bridge(data: dict) -> None:
    DISCUSSION_BRIDGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCUSSION_BRIDGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def add_discuss_publish_queue(channel_post_id: int) -> None:
    """记录频道首条 message_id，并对账已先到达的讨论区自动转发回执。"""
    if not channel_post_id:
        return
    now = time.time()
    data = load_discussion_bridge()
    pending = data.setdefault("pending_forwards", [])
    fresh = [item for item in pending if now - float(item.get("t", 0) or 0) <= 120]
    if fresh:
        receipt = fresh.pop(0)
        sk = str(int(channel_post_id))
        mapping = load_discuss_map()
        mapping[sk] = int(receipt["discussion_msg_id"])
        save_discuss_map(mapping)
        mgid = receipt.get("discuss_mgid")
        if mgid:
            data.setdefault("discuss_mgid", {})[str(mgid)] = {
                "channel_post_id": int(channel_post_id),
                "t": now,
            }
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    """UPDATE posts SET discuss_chat_id=?, discuss_thread_id=?, discuss_message_id=?, updated_at=CURRENT_TIMESTAMP WHERE channel_message_id=?""",
                    (str(receipt.get("discussion_chat_id") or ""), str(receipt.get("discussion_thread_id") or ""), str(receipt.get("discussion_msg_id") or ""), str(channel_post_id)),
                )
                conn.commit()
        except Exception:
            logger.exception("pending discussion receipt 回写 posts 失败: channel_post_id=%s", channel_post_id)
        matched_mgid = str(receipt.get("discuss_mgid") or "")
        if matched_mgid:
            fresh = [item for item in fresh if str(item.get("discuss_mgid") or "") != matched_mgid]
        data["pending_forwards"] = fresh
        logger.info("已对账提前到达的评论映射: channel_post_id=%s -> discussion_msg_id=%s", channel_post_id, receipt.get("discussion_msg_id"))
    else:
        data["pending_forwards"] = fresh
        data["publish_queue"].append({"t": now, "channel_post_id": int(channel_post_id)})
        if len(data["publish_queue"]) > 50:
            data["publish_queue"] = data["publish_queue"][-50:]
    save_discussion_bridge(data)


async def send_comment_to_discussion(
    bot: Bot,
    channel_post_id: int,
    text: str,
    reply_markup=None,
    parse_mode=None,
) -> int | None:
    """
    根据 discussion_map（channel_post_id -> 讨论区自动转发消息 id）发首条评论。
    返回发送后的 discussion message_id；映射未就绪或失败返回 None。
    """
    discussion_id = await resolve_discussion_chat_id(bot)
    if not discussion_id:
        logger.warning(
            "频道未绑定讨论组或读不到 linked_chat_id：请检查频道-讨论组绑定、Bot 在讨论组权限（CHANNEL_ID=%s）",
            CHANNEL_ID,
        )
        return None

    mapping = load_discuss_map()
    discussion_msg_id = mapping.get(str(channel_post_id))
    if not discussion_msg_id:
        logger.debug(
            "discussion 映射未就绪 channel_post_id=%s（等 v2 capture 写入 %s）",
            channel_post_id,
            DISCUSSION_MAP_FILE,
        )
        return None
    try:
        sent = await bot.send_message(
            chat_id=discussion_id,
            text=text,
            reply_to_message_id=int(discussion_msg_id),
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            allow_sending_without_reply=True,
        )
        return sent.message_id
    except Exception:
        logger.exception("send_comment_to_discussion 失败")
        return None


async def poll_discussion_first_reply(
    bot: Bot,
    channel_post_id: int,
    text: str,
    *,
    reply_markup=None,
    parse_mode: ParseMode | str | None = None,
    attempts: int = 25,
    delay_seconds: float = 1.0,
) -> int | None:
    """
    发频道帖后等「自动转发」映射落盘，再发讨论区首评。
    每轮先 sleep 再发（与常见稳定用法一致），避免映射未写入就发导致失败。
    """
    for _ in range(max(1, attempts)):
        await asyncio.sleep(delay_seconds)
        mid = await send_comment_to_discussion(
            bot, channel_post_id, text, reply_markup=reply_markup, parse_mode=parse_mode
        )
        if mid:
            return mid
    logger.warning(
        "poll_discussion_first_reply 耗尽: channel_post_id=%s attempts=%s",
        channel_post_id,
        attempts,
    )
    return None


async def send_discussion_cta_with_retry(
    bot: Bot,
    channel_post_id: int,
    text: str,
    *,
    reply_markup=None,
    parse_mode: ParseMode | str | None = None,
    attempts: int = 12,
    delay_seconds: float = 1.0,
) -> bool:
    """单图/模板发帖后：轮询发讨论区 CTA（成功返回 True）。"""
    mid = await poll_discussion_first_reply(
        bot,
        channel_post_id,
        text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        attempts=attempts,
        delay_seconds=delay_seconds,
    )
    return mid is not None


def _build_discussion_action_keyboard(listing_id: str, post_token: str) -> InlineKeyboardMarkup:
    """讨论区末尾只保留两个高意向动作。"""
    if BOT_USERNAME:
        user = BOT_USERNAME.lstrip("@")
        base = f"https://t.me/{user}?start="
        consult_payload = (
            f"q__{post_token}" if str(post_token).startswith("ql")
            else build_start_payload("q", listing_id, post_token)
        )
        appoint_payload = (
            f"a__{post_token}" if str(post_token).startswith("ql")
            else build_start_payload("a", listing_id, post_token)
        )
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(
                    "💬 问清费用",
                    url=base + consult_payload,
                ),
                InlineKeyboardButton(
                    "📅 预约看房",
                    url=base + appoint_payload,
                ),
            ]]
        )
    return InlineKeyboardMarkup([])


def _build_discussion_continue_keyboard(listing_id: str, post_token: str) -> InlineKeyboardMarkup:
    """讨论区第三段：继续看房入口，深链到用户 Bot 的讨论区入口。"""
    if BOT_USERNAME:
        user = BOT_USERNAME.lstrip("@")
        entry_payload = f"discussion_entry__{post_token or ''}__{listing_id}|entry=discussion|step=seg3"
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("🤖 打开侨联小助手", url=f"https://t.me/{user}?start={entry_payload}")]]
        )
    return InlineKeyboardMarkup([])


async def send_discussion_three_segments(
    bot: Bot,
    channel_post_id: int,
    listing_id: str,
    post_token: str,
    *,
    listing: dict | None = None,
    extra_album: list | None = None,
    frozen_detail_text: str = "",
    attempts: int = 30,
    delay_seconds: float = 2.0,
) -> tuple[bool, bool]:
    """讨论区按手机阅读顺序发布：实拍 → 费用/判断 → 行动入口。"""
    discussion_id = await resolve_discussion_chat_id(bot)
    if not discussion_id:
        logger.warning("讨论区发帖：无法获取讨论组 chat_id，跳过。channel_post_id=%s", channel_post_id)
        return False, False

    thread_reply_id = None
    for _ in range(max(1, attempts)):
        await asyncio.sleep(delay_seconds)
        mapping = load_discuss_map()
        thread_reply_id = mapping.get(str(channel_post_id))
        if thread_reply_id:
            break
    if not thread_reply_id:
        logger.warning("讨论区映射等待超时。channel_post_id=%s", channel_post_id)
        return False, False

    sent_any = False
    sent_extra_photos = False

    # 第一段：原比例补充实拍。每张只加轻量 Logo，不叠价格、户型或大字。
    if extra_album:
        chunk = 10
        total_extra = len(extra_album)
        for batch_start in range(0, total_extra, chunk):
            batch_paths = extra_album[batch_start : batch_start + chunk]
            extra_media = []
            for j, path in enumerate(batch_paths):
                try:
                    with open(path, "rb") as raw:
                        data_bytes = raw.read()
                    # Frozen discussion files are already final package outputs; send unchanged.
                    buf = io.BytesIO(data_bytes)
                    buf.name = f"extra_{batch_start + j}.jpg"
                    if j == 0:
                        cap = (
                            DISCUSSION_EXTRA_INTRO
                            if batch_start == 0
                            else DISCUSSION_EXTRA_INTRO_CONT
                        )
                        extra_media.append(
                            InputMediaPhoto(
                                media=buf,
                                caption=cap[:1024],
                                parse_mode=ParseMode.HTML,
                            )
                        )
                    else:
                        extra_media.append(InputMediaPhoto(media=buf))
                except Exception:
                    logger.exception("讨论区实拍处理失败，已跳过: %s", path)
            if not extra_media:
                continue
            try:
                if len(extra_media) == 1:
                    await bot.send_photo(
                        chat_id=discussion_id,
                        photo=extra_media[0].media,
                        caption=extra_media[0].caption,
                        parse_mode=ParseMode.HTML,
                        reply_to_message_id=int(thread_reply_id),
                        allow_sending_without_reply=True,
                    )
                else:
                    await bot.send_media_group(
                        chat_id=discussion_id,
                        media=extra_media,
                        reply_to_message_id=int(thread_reply_id),
                        allow_sending_without_reply=True,
                    )
                sent_any = True
                sent_extra_photos = True
            except Exception:
                logger.exception("讨论区实拍发送失败 batch_start=%s", batch_start)
            if batch_start + chunk < total_extra:
                await asyncio.sleep(0.6)

    # 第二段：新发布只发送 approved package 中冻结的房源详情；
    # 对旧 package 保留兼容生成，以便历史包不因新字段缺失而无法发送。
    d = listing or {}
    detail_text = str(frozen_detail_text or "").strip() or build_discussion_detail_text(d)
    try:
        await bot.send_message(
            chat_id=discussion_id,
            text=detail_text[:4096],
            parse_mode=ParseMode.HTML,
            reply_to_message_id=int(thread_reply_id),
            allow_sending_without_reply=True,
        )
        sent_any = True
    except Exception:
        logger.exception("讨论区房源详情发送失败。channel_post_id=%s", channel_post_id)

    # 第三段：正文内可点击链接，不发送 Telegram inline keyboard。
    try:
        action_links = _caption_action_links(
            listing_id,
            listing=listing or {},
            post_token=post_token,
            caption_variant="a",
        )
        await bot.send_message(
            chat_id=discussion_id,
            text=(
                "📅 <b>看房与咨询</b>\n"
                "可预约现场看房，也可以安排视频代看。咨询时会自动带上这套房的信息。\n\n"
                f"{action_links}"
            ),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=int(thread_reply_id),
            allow_sending_without_reply=True,
        )
        sent_any = True
    except Exception:
        logger.exception("讨论区行动入口发送失败。channel_post_id=%s", channel_post_id)

    return sent_any, sent_extra_photos

def build_channel_caption(
    d: dict,
    album_paths: list[str],
    caption_variant: str | None = None,
    post_token: str = "",
    has_extra_photos: bool = False,
) -> str:
    """生成发频道用的首图 caption。只输出中文租房帖，不附加调试图注。"""
    return build_chinese_listing_post(
        d,
        caption_variant=caption_variant,
        post_token=post_token,
        has_extra_photos=has_extra_photos,
    )[:1024]


def build_keyboard(
    listing_id: str,
    area: str = "",
    post_token: str = "",
    caption_variant: str | None = "a",
) -> InlineKeyboardMarkup:
    """租客只需要两个明确动作：预约或咨询。"""
    if not BOT_USERNAME:
        return InlineKeyboardMarkup([])
    public_token = ""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                """SELECT public_token FROM publication_packages
                   WHERE property_id=? AND status IN ('approved','published')
                     AND length(trim(coalesce(public_token,'')))>0
                   ORDER BY package_version DESC LIMIT 1""",
                (listing_id,),
            ).fetchone()
        public_token = str(row[0] or "").strip() if row else ""
    except Exception:
        logger.exception("读取公开房源 token 失败: %s", listing_id)
    if public_token:
        user_url = f"https://t.me/{BOT_USERNAME.lstrip('@')}?start="
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("📅 预约看房", url=f"{user_url}book_{listing_id}"),
            InlineKeyboardButton("💬 咨询这套", url=f"{user_url}consult__{public_token}"),
        ]])
    consult_payload = build_start_payload(
        "q",
        listing_id,
        post_token=post_token,
        caption_variant=caption_variant,
    )
    appoint_payload = build_start_payload(
        "a",
        listing_id,
        post_token=post_token,
        caption_variant=caption_variant,
    )
    base_url = f"https://t.me/{BOT_USERNAME.lstrip('@')}?start="
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📅 预约看房", url=f"{base_url}{appoint_payload}"),
        InlineKeyboardButton("💬 咨询这套", url=f"{base_url}{consult_payload}"),
    ]])


# ── 数据库工具 ────────────────────────────────────────────
class DB:
    def __init__(self, path: str):
        self.path = path

    def _conn(self):
        return sqlite3.connect(self.path)

    def fetch_one(self, sql, params=()):
        with self._conn() as c:
            return c.execute(sql, params).fetchone()

    def fetch_all(self, sql, params=()):
        with self._conn() as c:
            return c.execute(sql, params).fetchall()

    def execute(self, sql, params=()):
        conn = self._conn()
        try:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.lastrowid
        except sqlite3.Error as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_caption_variant_weights(self) -> dict[str, float]:
        """
        读取 system_config.caption_variant_weights（JSON）。
        失败时回退默认权重，且保证 a/b/c 都有有效值。
        """
        default = {"a": 0.4, "b": 0.3, "c": 0.3}
        try:
            row = self.fetch_one(
                "SELECT value FROM system_config WHERE key='caption_variant_weights' LIMIT 1"
            )
            if not row or row[0] in (None, ""):
                return default
            raw = json.loads(str(row[0]))
            if not isinstance(raw, dict):
                return default
            merged: dict[str, float] = {}
            for key in ("a", "b", "c"):
                try:
                    val = float(raw.get(key, default[key]))
                except (TypeError, ValueError):
                    val = default[key]
                merged[key] = max(val, 0.0)
            if sum(merged.values()) <= 0:
                return default
            return merged
        except Exception:
            logger.exception("读取 caption_variant_weights 失败，使用默认权重")
            return default

    def write_publish_analytics(
        self,
        *,
        draft_id: str,
        post_id: str,
        message_id: int | None,
        listing_id: str,
        area: str,
        property_type: str,
        monthly_rent: float | int | None,
        caption_variant: str,
        published_at: str,
    ) -> None:
        try:
            dt = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now()
        try:
            rent = float(monthly_rent or 0)
        except (TypeError, ValueError):
            rent = 0.0
        try:
            self.execute(
                """
                INSERT INTO publish_analytics (
                    draft_id, post_id, message_id, listing_id,
                    area, property_type, monthly_rent,
                    caption_variant, publish_hour, publish_day_of_week, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(draft_id or ""),
                    str(post_id or ""),
                    int(message_id) if message_id else None,
                    str(listing_id or ""),
                    str(area or ""),
                    str(property_type or ""),
                    rent,
                    str(caption_variant or "a"),
                    int(dt.hour),
                    int(dt.weekday()),
                    str(published_at or datetime.now().isoformat(timespec="seconds")),
                ),
            )
        except Exception:
            logger.exception("写 publish_analytics 失败: draft_id=%s listing_id=%s", draft_id, listing_id)

    def write_log(self, log_id, post_id, draft_id, listing_id,
                  target_type, target_ref, action, status,
                  attempt_no=1, request_payload=None, response_payload=None,
                  error_message=None, log_message=None):
        self.execute(
            """INSERT OR IGNORE INTO publish_logs (
                log_id, post_id, draft_id, listing_id,
                target_type, target_ref, action, status, attempt_no,
                request_payload, response_payload, error_message, log_message,
                log_level, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'INFO',CURRENT_TIMESTAMP)""",
            (
                log_id, post_id, draft_id, listing_id,
                target_type, target_ref, action, status, attempt_no,
                json.dumps(request_payload) if request_payload else None,
                json.dumps(response_payload) if response_payload else None,
                error_message, log_message,
            ),
        )

    def claim_draft_for_publish(self, draft_id: str) -> bool:
        """原子抢占发布权，避免同一 draft 并发重复发帖。"""
        conn = self._conn()
        try:
            cur = conn.execute(
                """UPDATE drafts
                   SET review_status='publishing', updated_at=CURRENT_TIMESTAMP
                   WHERE draft_id=?
                     AND review_status IN ('ready', 'approved', 'pending')""",
                (draft_id,),
            )
            conn.commit()
            return (cur.rowcount or 0) > 0
        finally:
            conn.close()

    def successful_channel_post(self, listing_id: str, channel_chat_id: str) -> tuple | None:
        """返回同一房源在同一频道的成功发布记录，用作发布幂等键。"""
        try:
            return self.fetch_one(
                """SELECT post_id, channel_message_id
                   FROM posts
                   WHERE listing_id=?
                     AND platform='telegram'
                     AND CAST(channel_chat_id AS TEXT)=?
                     AND publish_status IN ('published', 'success', 'ok')
                   ORDER BY id DESC LIMIT 1""",
                (str(listing_id or ""), str(channel_chat_id or "")),
            )
        except sqlite3.OperationalError as exc:
            # 极简离线预检库可能尚未迁移 posts；正式库初始化后必有该表。
            if "no such table: posts" in str(exc).lower():
                return None
            raise

    def create_post(self, post_id, listing_id, draft_id, platform,
                    channel_chat_id=None, channel_message_id=None,
                    media_group_id=None, button_message_id=None,
                    notion_page_id=None, post_text=None, publish_status="published"):
        return self.execute(
            """INSERT INTO posts (
                post_id, listing_id, draft_id, platform,
                channel_chat_id, channel_message_id, media_group_id,
                button_message_id, notion_page_id, post_text,
                publish_status, published_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (
                post_id, listing_id, draft_id, platform,
                channel_chat_id, str(channel_message_id) if channel_message_id else None,
                str(media_group_id) if media_group_id else None,
                str(button_message_id) if button_message_id else None,
                notion_page_id, post_text, publish_status,
            ),
        )


# ── TG 发布 ───────────────────────────────────────────────
def _album_paths_for_draft(d: dict, cover_path: str, db_path: str) -> list:
    """侨联模板封面 + 同 source_post 组内其余实拍，最多 ALBUM_SOURCE_MAX 张（再经宫格规整与溢出分流）。"""
    out = [cover_path] if cover_path else []
    sp_id = d.get("source_post_id")
    if not sp_id:
        return out
    if cover_path and not os.path.isfile(cover_path):
        return out
    gen = CoverGenerator(db_path)
    raw_paths = gen._get_source_post_images(sp_id)
    original_paths = [
        path for path in raw_paths
        if "原图" in os.path.basename(str(path or ""))
    ]
    if original_paths:
        raw_paths = original_paths
    base = None
    try:
        cid = d.get("cover_asset_id")
        if cid:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT meta_json FROM media_assets WHERE id=?",
                (int(cid),),
            ).fetchone()
            conn.close()
            if row and row["meta_json"]:
                meta = json.loads(row["meta_json"])
                b = meta.get("base_image")
                if b and b not in ("default_bg", "") and os.path.isfile(b):
                    base = b
    except Exception:
        pass
    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
    for path in raw_paths:
        if len(out) >= ALBUM_SOURCE_MAX:
            break
        if not path or not os.path.isfile(path):
            continue
        ext = os.path.splitext(str(path).lower())[1]
        if ext not in image_exts:
            # 原始组可能混入视频，频道主帖首页只保留有效图片，避免四图被视频占位。
            continue
        if path == base:
            continue
        if path in out:
            continue
        out.append(path)
    return out


def _real_media_paths_for_draft(d: dict, db_path: str) -> list[str]:
    album_paths = _album_paths_for_draft(d, "", db_path)
    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
    return [
        path
        for path in album_paths
        if path and os.path.isfile(path) and os.path.splitext(str(path).lower())[1] in image_exts
    ]


def sync_published_listing_for_user_bot(
    db_path: str,
    draft: dict,
    listing_id: str,
    cover_path: str,
    channel_message_id: int | None,
) -> str:
    """将频道发布结果同步到用户 Bot 使用的 listings 表。"""
    source_cover = Path(cover_path)
    listing_media_dir = Path(db_path).resolve().parent.parent / "media" / "listings"
    listing_media_dir.mkdir(parents=True, exist_ok=True)
    target_cover = listing_media_dir / f"{listing_id}{source_cover.suffix.lower() or '.jpg'}"
    target_cover.write_bytes(source_cover.read_bytes())

    highlights = draft.get("highlights") or []
    if isinstance(highlights, str):
        try:
            highlights = json.loads(highlights)
        except (TypeError, ValueError, json.JSONDecodeError):
            highlights = [highlights]
    size = str(draft.get("size") or "").strip().replace("平方米", "").replace("㎡", "")
    now = datetime.now().isoformat(timespec="seconds")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO listings (
                   listing_id, title, property_type, area, community, price, currency,
                   layout, size_sqm, tags_json, highlights, hidden_costs, drawbacks,
                   deposit_rule, available_date, media_file_id, media_type,
                   channel_message_id, source_post_url, status, created_at, updated_at
               ) VALUES (?,?,?,?,?,?,'USD',?,?,?,?,?,?,?,?,?,'photo',?,?,'pending',?,?)
               ON CONFLICT(listing_id) DO UPDATE SET
                   title=excluded.title, property_type=excluded.property_type,
                   area=excluded.area, community=excluded.community, price=excluded.price,
                   layout=excluded.layout, size_sqm=excluded.size_sqm,
                   highlights=excluded.highlights, hidden_costs=excluded.hidden_costs,
                   drawbacks=excluded.drawbacks, deposit_rule=excluded.deposit_rule,
                   available_date=excluded.available_date, media_file_id=excluded.media_file_id,
                   media_type=excluded.media_type, channel_message_id=excluded.channel_message_id,
                   source_post_url=excluded.source_post_url,
                   -- Preserve administrator-controlled listing status (e.g. rented/offline).
                   updated_at=excluded.updated_at""",
            (
                listing_id,
                str(draft.get("title") or "房源"),
                str(draft.get("property_type") or "公寓"),
                str(draft.get("area") or "金边"),
                str(draft.get("community") or draft.get("project") or ""),
                int(float(draft.get("price") or 0)),
                str(draft.get("layout") or ""),
                size,
                "[]",
                "\n".join(str(item) for item in highlights if str(item).strip()),
                str(draft.get("cost_notes") or ""),
                "\n".join(str(item) for item in (draft.get("drawbacks") or []) if str(item).strip()),
                str(draft.get("deposit") or ""),
                str(draft.get("available_date") or ""),
                str(target_cover),
                int(channel_message_id) if channel_message_id else None,
                str(draft.get("source_url") or ""),
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return str(target_cover)


def _draft_quality_score(d: dict) -> int:
    nd = _parsed_normalized(d)
    raw = d.get("queue_score")
    if raw in (None, ""):
        raw = nd.get("quality_score")
    if raw in (None, ""):
        quality = nd.get("quality") if isinstance(nd.get("quality"), dict) else {}
        raw = quality.get("score", 0)
    try:
        return int(float(raw or 0))
    except (TypeError, ValueError):
        return 0


def _first_number_int(text: str) -> int:
    m = re.search(r"(\d{2,5})", str(text or ""))
    if not m:
        return 0
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return 0


def _layout_rooms_count(layout: str) -> int:
    raw = str(layout or "").strip().lower()
    if not raw:
        return 0
    # 例如 5+1房 -> 6
    m_plus = re.search(r"(\d{1,2})\s*\+\s*(\d{1,2})\s*房", raw)
    if m_plus:
        try:
            return int(m_plus.group(1)) + int(m_plus.group(2))
        except (TypeError, ValueError):
            return 0
    m_cn = re.search(r"(\d{1,2})\s*房", raw)
    if m_cn:
        try:
            return int(m_cn.group(1))
        except (TypeError, ValueError):
            return 0
    m_en = re.search(r"\b(\d{1,2})\s*(?:br|bed|room|rooms)\b", raw)
    if m_en:
        try:
            return int(m_en.group(1))
        except (TypeError, ValueError):
            return 0
    return 0


def _size_value(size: str) -> float:
    m = re.search(r"(\d+(?:\.\d+)?)", str(size or ""))
    if not m:
        return 0.0
    try:
        return float(m.group(1))
    except (TypeError, ValueError):
        return 0.0


def _source_raw_text(d: dict, db_path: str) -> str:
    source_post_id = d.get("source_post_id")
    if source_post_id in (None, "", 0, "0"):
        return ""
    try:
        sid = int(source_post_id)
    except (TypeError, ValueError):
        return ""
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT raw_text FROM source_posts WHERE id=? LIMIT 1",
            (sid,),
        ).fetchone()
        conn.close()
    except Exception:
        return ""
    if not row:
        return ""
    return str(row[0] or "")


def _max_rooms_from_source(raw_text: str) -> int:
    text = str(raw_text or "")
    best = 0
    for m in re.finditer(r"(\d{1,2})\s*\+\s*(\d{1,2})\s*房", text):
        try:
            n = int(m.group(1)) + int(m.group(2))
            best = max(best, n)
        except (TypeError, ValueError):
            continue
    for m in re.finditer(r"(\d{1,2})\s*房(?!间)", text):
        try:
            n = int(m.group(1))
            best = max(best, n)
        except (TypeError, ValueError):
            continue
    return best


def _review_quality_flags(draft: dict) -> set[str]:
    """Read machine quality only from canonical facts/quality_json.

    review_note is historical human-facing text and is deliberately not a
    source for publish decisions. Missing canonical quality fails closed in
    the surrounding gate instead of being reconstructed from prose.
    """
    quality = {}
    raw_quality = draft.get("quality_json")
    if raw_quality:
        try:
            parsed = json.loads(raw_quality) if isinstance(raw_quality, str) else raw_quality
            quality = parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            quality = {}
    if not quality:
        normalized = _parsed_normalized(draft)
        quality = normalized.get("quality") if isinstance(normalized.get("quality"), dict) else {}
    return {
        str(flag).strip().lower()
        for key in ("hard_flags", "review_flags", "blocking_flags", "warning_flags", "info_flags", "all_flags")
        for flag in (quality.get(key) or [])
        if str(flag).strip()
    }


def evaluate_publish_gate(d: dict, cover_path: str, db_path: str, frozen_media_paths: list[str] | None = None) -> dict:
    real_media = [str(p) for p in (frozen_media_paths if frozen_media_paths is not None else _real_media_paths_for_draft(d, db_path)) if str(p).strip() and os.path.isfile(str(p))]
    score = _draft_quality_score(d)
    project = (d.get("project") or "").strip()
    area = (d.get("area") or "").strip()
    layout = normalize_room_type((d.get("room_type") or d.get("layout") or "").strip())
    price = d.get("price") or 0
    raw_price_value = d.get("price")
    if raw_price_value in (None, "", 0, 0.0, "0", "0.0"):
        price_raw = str(_parsed_normalized(d).get("price") or "").strip()
    else:
        price_raw = str(raw_price_value).strip()

    try:
        price_value = int(float(price))
    except (TypeError, ValueError):
        price_value = 0
    if price_value <= 0:
        price_value = _first_number_int(price_raw)

    quality_flags = _review_quality_flags(d)
    source_text = _source_raw_text(d, db_path)
    source_room_max = _max_rooms_from_source(source_text)
    layout_room_count = _layout_rooms_count(layout)
    property_type = str(d.get("property_type") or "").strip().lower()
    size_raw = str(d.get("size") or "").strip()
    size_value = _size_value(size_raw)

    normalized = _parsed_normalized(d)
    unified_contract = evaluate_publishability(
        normalized,
        media_count=len(real_media),
        cover_exists=bool(cover_path and os.path.isfile(cover_path)),
    )
    hard_block_reasons: list[str] = list(unified_contract["blocking"])
    fallback_reasons: list[str] = []
    quality_warnings: list[str] = list(unified_contract["warnings"])
    normalized_area = str(normalized.get("normalized_area") or d.get("normalized_area") or "").strip()
    # Canonical facts distinguish precise physical area from safe public location.
    # For canonical v1, a confirmed market/unique-project location is publishable
    # without inventing a sangkat/khan.  City, brand-only and unknown locations
    # remain invalid.  Legacy drafts retain the historical canonical-area check.
    canonical_schema = str(normalized.get("schema_version") or "").strip()
    canonical_level = str(normalized.get("publication_location_level") or "").strip()
    canonical_public_location = str(normalized.get("public_location_display") or "").strip()
    canonical_blocking = set((normalized.get("quality") or {}).get("blocking_flags") or [])
    if canonical_schema == "canonical_facts.v1":
        # Canonical v1 may publish without a location; copy/cover must omit it
        # rather than inventing an area.  Known project/market locations remain
        # preferred, but geographic precision is not an administrator form.
        valid_area = True
    else:
        # Project/community is descriptive only; it cannot replace a canonical
        # searchable area. Legacy drafts without normalized_data may use only an
        # exact value from the physical-area catalog; city and free text fail closed.
        physical_area_values = {
            value
            for item in PHYSICAL_AREAS
            for value in (item.key, item.display)
        }
        valid_area = bool(
            (normalized_area and normalized_area in physical_area_values)
            or (not normalized_area and area in physical_area_values)
        )
    # 频道自动发布必须带明确价格，避免"价格待确认"误发。
    valid_price = 100 <= price_value <= 20000
    valid_room = bool(layout and layout not in {"整租", "住宅"})

    if score < BASIC_PUBLISH_MIN_SCORE:
        hard_block_reasons.append(f"low_score:{score}")
    if not valid_price:
        hard_block_reasons.append("invalid_price")
    if not valid_area:
        hard_block_reasons.append("invalid_area")
    if not valid_room:
        hard_block_reasons.append("invalid_layout")
    if not cover_path or not os.path.isfile(cover_path):
        hard_block_reasons.append("missing_cover")
    if not real_media:
        hard_block_reasons.append("missing_real_media")
    if "missing_price" in quality_flags:
        hard_block_reasons.append("quality_missing_price")
    if canonical_schema != "canonical_facts.v1" and "missing_area" in quality_flags:
        hard_block_reasons.append("quality_missing_area")
    if "missing_layout" in quality_flags:
        hard_block_reasons.append("quality_missing_layout")
    if source_room_max >= 2 and layout_room_count > 0 and layout_room_count < (source_room_max - 1):
        hard_block_reasons.append(f"layout_mismatch:{layout_room_count}_lt_src_{source_room_max}")
    if "别墅" in property_type and size_value > 0 and size_value < 25:
        quality_warnings.append(f"suspicious_villa_size:{size_value:g}")
    if "别墅" in property_type and size_raw and re.search(r"[xX×*]", source_text) and size_value > 0 and size_value < 40:
        quality_warnings.append("villa_size_may_be_dimension_not_area")
    if "sale" in property_type and 0 < price_value < 5000:
        hard_block_reasons.append("price_unit_ambiguous")
    if ("rent" in property_type or "rental" in property_type) and price_value > 50000:
        hard_block_reasons.append("suspicious_sale_price_in_rent")

    deposit = _normalize_deposit_text(
        _listing_value(d, "payment_terms", "deposit", default="")
    )
    recurring_costs = _monthly_cost_summary(d)
    if not deposit:
        quality_warnings.append("missing_deposit_details")
    contract = _normalize_contract_term(
        _listing_value(d, "contract_term", default="")
    )
    if not contract:
        quality_warnings.append("missing_contract_details")
    if recurring_costs == "管理费、水电、网络及停车待确认":
        quality_warnings.append("missing_recurring_cost_details")

    if score < PREMIUM_PUBLISH_MIN_SCORE:
        fallback_reasons.append(f"score_below_premium:{score}")
    if len(real_media) < PREMIUM_REAL_MEDIA_MIN:
        fallback_reasons.append(f"real_media_lt_{PREMIUM_REAL_MEDIA_MIN}")

    mode = "premium_4image"
    gate_cover_path = cover_path if cover_path and os.path.isfile(cover_path) else None
    # 频道相册顺序：封面在前，后接同组实拍；主帖截前 4 张，其余进讨论组。
    album_all = ([cover_path] + list(real_media)) if gate_cover_path else list(real_media)
    if hard_block_reasons:
        mode = "blocked"
        gate_cover_path = None
        album_all = []
    elif fallback_reasons:
        mode = "fallback_media"
        album_all = ([cover_path] + list(real_media)) if gate_cover_path else list(real_media)

    reasons = hard_block_reasons if hard_block_reasons else fallback_reasons
    return {
        "mode": mode,
        "score": score,
        "real_media_count": len(real_media),
        "reasons": reasons,
        "warnings": quality_warnings,
        "album_all": album_all,
        "cover_path": gate_cover_path,
        "cover": gate_cover_path,
        "is_premium": mode == "premium_4image",
        "is_publishable": mode != "blocked",
        "price_value": price_value,
        "source_room_max": source_room_max,
        "layout_room_count": layout_room_count,
    }


async def _tg_publish(
    d: dict,
    cover_path: str,
    gate: dict | None = None,
    caption_variant: str | None = "a",
    frozen_caption: str | None = None,
    frozen_discussion_text: str | None = None,
    publish_layout: str = "links",
    publish_cover: str = "cover",
) -> dict:
    """
    排版：频道主帖最多 CHANNEL_MAIN_ALBUM_MAX 张（默认10，封面在前/实拍在后）+ 首图短 caption；
    频道只发两条：媒体（单图/相册）+ 按钮消息；
    多出来的实拍进讨论组（补充组图首图配文为 DISCUSSION_EXTRA_INTRO，避免与频道长文重复）。
    """
    req = HTTPXRequest(
        connect_timeout=60.0,
        read_timeout=300.0,
        write_timeout=300.0,
        pool_timeout=120.0,
    )
    bot = Bot(token=PUBLISHER_TOKEN, request=req)
    listing_id = d["listing_id"]
    area = d.get("area") or ""
    gate = gate or evaluate_publish_gate(d, cover_path, DB_PATH)
    if not gate.get("is_publishable", True):
        raise ValueError("publish_blocked:" + ",".join(gate.get("reasons") or []))
    publish_layout = "links" if str(publish_layout).lower() == "links" else "buttons"
    publish_cover = "none" if str(publish_cover).lower() == "none" else "cover"
    # 管理员可独立选择行动按钮和封面。未使用封面时，以冻结实拍首图作为频道首图。
    album_all = gate.get("album_all") or _album_paths_for_draft(d, cover_path, DB_PATH)
    if publish_cover == "none":
        album_all = [path for path in album_all if os.path.abspath(path) != os.path.abspath(cover_path)]
    if not album_all:
        raise ValueError("publish_no_media_after_cover_choice")
    if publish_layout == "buttons":
        # 带行动按钮时频道只留一张首图，完整实拍由用户 Bot 承接。
        album = album_all[:1]
        overflow_album = []
    else:
        album, overflow_album = split_album_for_channel(album_all)
    extra_album = list(dict.fromkeys(overflow_album))

    # 文案（首张图 caption，限 1024）
    # 频道正文先使用保守文案；只有评论区补充图实际发送成功后，
    # 才在下方编辑 caption 承诺“更多实拍”。
    # Production freeze rule: an approved package supplies the complete caption.
    # Only the post-token injection after Telegram assigns a message id may alter it.
    caption = str(frozen_caption or gate.get("frozen_post_text") or "").strip()
    if not caption:
        raise ValueError("publish_missing_frozen_caption")
    post_token = ""

    button_message_id = None
    reply_markup = None

    def _button_layout_keyboard(message_id: int, token: str) -> InlineKeyboardMarkup:
        user = BOT_USERNAME.lstrip("@")
        appointment = f"https://t.me/{user}?start=book_{listing_id}"
        # Photos use a stable listing deep-link so rebuilding the package queue
        # cannot invalidate already-published channel buttons.
        photos = f"https://t.me/{user}?start=photos_{listing_id}"
        helper = f"https://t.me/{user}?start=view_{listing_id}"
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📅 预约看房", url=appointment),
                InlineKeyboardButton("📸 更多实拍", url=photos),
            ],
            [InlineKeyboardButton("🤖 侨联找房助手", url=helper)],
        ])

    def _prepare_channel_photo_buf(data: bytes, *, is_cover: bool, slot_index: int) -> io.BytesIO:
        """Return the frozen final bytes unchanged; package build owns every pixel transform."""
        buf = io.BytesIO(data)
        buf.name = f"p{slot_index}.jpg"
        buf.seek(0)
        return buf

    # 1) 发送图片（单图或相册），首图带 caption；相册不再额外发送独立按钮消息
    if len(album) == 1:
        with open(album[0], "rb") as raw:
            data = raw.read()
        is_cover = bool(cover_path and os.path.abspath(album[0]) == os.path.abspath(cover_path))
        buf = _prepare_channel_photo_buf(data, is_cover=is_cover, slot_index=0)
        sent = await bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=buf,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
        )
        add_discuss_publish_queue(int(sent.message_id))
        media_group_id = str(getattr(sent, "media_group_id", None) or sent.message_id)
        media_message_ids = [sent.message_id]
        file_ids = [sent.photo[-1].file_id]
        post_token = make_post_token(sent.message_id)
    else:
        prepared: list[io.BytesIO] = []
        na = len(album)
        for i, path in enumerate(album):
            try:
                with open(path, "rb") as raw:
                    data = raw.read()
                is_cover = bool(cover_path and os.path.abspath(path) == os.path.abspath(cover_path))
                prepared.append(_prepare_channel_photo_buf(data, is_cover=is_cover, slot_index=i))
            except Exception:
                logger.exception("频道主帖图片处理失败，已跳过: %s", path)
                continue

        if not prepared:
            raise ValueError("publish_no_valid_media")

        if len(prepared) == 1:
            sent = await bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=prepared[0],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
            add_discuss_publish_queue(int(sent.message_id))
            media_group_id = str(getattr(sent, "media_group_id", None) or sent.message_id)
            media_message_ids = [sent.message_id]
            file_ids = [sent.photo[-1].file_id]
            post_token = make_post_token(sent.message_id)
        else:
            media = []
            for i, buf in enumerate(prepared):
                if i == 0:
                    media.append(
                        InputMediaPhoto(
                            media=buf, caption=caption, parse_mode=ParseMode.HTML
                        )
                    )
                else:
                    media.append(InputMediaPhoto(media=buf))
            msgs = await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
            mgid = msgs[0].media_group_id if msgs else None
            media_group_id = str(mgid) if mgid else str(msgs[0].message_id)
            # 讨论组自动转发常缺 forward_from_message_id，用 media_group_id 与 pending 对齐映射
            if msgs:
                add_discuss_publish_queue(int(msgs[0].message_id))
            media_message_ids = [m.message_id for m in msgs]
            file_ids = []
            for m in msgs:
                if m.photo:
                    file_ids.append(m.photo[-1].file_id)

            # media_group 不能挂 inline keyboard；咨询入口已保留在 caption 中，避免频道多出一条 CTA 消息。
            first_message_id = msgs[0].message_id if msgs else None
            post_token = make_post_token(first_message_id)
            button_message_id = None

    # 单图 / 多图共用：发讨论区三段式（预约承接 + 补充实拍 + 继续看房入口）
    channel_mid = media_message_ids[0] if media_message_ids else None
    discussion_photos_published = False
    if channel_mid:
        discuss_id = await resolve_discussion_chat_id(bot)
        if discuss_id and str(discuss_id) != str(CHANNEL_ID):
            _, discussion_photos_published = await send_discussion_three_segments(
                bot,
                channel_mid,
                listing_id,
                post_token,
                listing=d,
                extra_album=extra_album if extra_album else None,
                frozen_detail_text=str(frozen_discussion_text or ""),
                attempts=60,
                delay_seconds=2.0,
            )
        elif extra_album:
            logger.warning(
                "有 %s 张溢出实拍但未配置讨论区或讨论区与频道相同，已跳过",
                len(extra_album),
            )

    # Telegram 分配消息 id 后补上带 post_token 的咨询/预约链接，保证渠道归因。
    channel_mid = media_message_ids[0] if media_message_ids else None
    if channel_mid and post_token:
        if frozen_caption:
            tracked_caption = _inject_frozen_post_token(frozen_caption, post_token)
        else:
            # Compatibility only: production publish_draft always passes frozen_caption.
            tracked_caption = build_channel_caption(
                d,
                album,
                caption_variant=caption_variant,
                post_token=post_token,
                has_extra_photos=discussion_photos_published,
            )
        try:
            final_markup = (
                _button_layout_keyboard(int(channel_mid), str(d.get("_frozen_public_token") or post_token))
                if publish_layout == "buttons" else None
            )
            await bot.edit_message_caption(
                chat_id=CHANNEL_ID,
                message_id=int(channel_mid),
                caption=tracked_caption,
                parse_mode=ParseMode.HTML,
                reply_markup=final_markup,
            )
            caption = tracked_caption
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                logger.warning(
                    "补充带追踪参数的 caption 失败，但主帖已发布 message_id=%s",
                    channel_mid,
                    exc_info=True,
                )
        except Exception:
            logger.warning(
                "补充带追踪参数的 caption 失败，但主帖已发布 message_id=%s",
                channel_mid,
                exc_info=True,
            )

    return {
        "media_group_id": media_group_id,
        "media_message_ids": media_message_ids,
        "button_message_id": button_message_id,
        "file_ids": file_ids,
        "caption": caption,
        "post_token": post_token,
        "publish_mode": gate.get("mode"),
        "publish_gate_reasons": gate.get("reasons") or [],
    }


def _inject_frozen_post_token(caption: str, post_token: str) -> str:
    """Only inject tracking tokens into existing book/consult Deep Links.

    No listing facts, prices, areas, layouts, or wording are regenerated here.
    """
    if not post_token:
        return caption
    pattern = re.compile(r"(start=(?:book|consult)_+)(l_\d+)")
    return pattern.sub(lambda m: f"{m.group(1).split('_')[0]}__{post_token}__{m.group(2)}", caption)


def tg_publish(
    d: dict,
    cover_path: str,
    gate: dict | None = None,
    caption_variant: str | None = "a",
    frozen_caption: str | None = None,
    frozen_discussion_text: str | None = None,
    publish_layout: str = "links",
    publish_cover: str = "cover",
) -> dict:
    return asyncio.run(
        _tg_publish(
            d,
            cover_path,
            gate=gate,
            caption_variant=caption_variant,
            frozen_caption=frozen_caption,
            frozen_discussion_text=frozen_discussion_text,
            publish_layout=publish_layout,
            publish_cover=publish_cover,
        )
    )


# ── Notion 同步 ───────────────────────────────────────────
def notion_sync(d: dict, listing_id: str) -> str | None:
    """
    将房源同步到 Notion 数据库，返回 page_id 或 None。
    若未配置 NOTION_TOKEN / NOTION_DB_ID，跳过并返回 None。
    """
    if not NOTION_TOKEN or not NOTION_DB_ID:
        print("[Notion] NOTION_TOKEN 或 NOTION_DATABASE_ID 未配置，跳过 Notion 同步。")
        return None

    notion = NotionClient(auth=NOTION_TOKEN)
    price  = d.get("price")
    price_str = f"${int(price):,}/月" if price and str(price).isdigit() else (f"${price}/月" if price else "")

    highlights = d.get("highlights") or []
    if isinstance(highlights, str):
        try:
            highlights = json.loads(highlights)
        except Exception:
            highlights = []
    hl_str = "、".join(highlights) if highlights else ""

    try:
        page = notion.pages.create(
            parent={"database_id": NOTION_DB_ID},
            properties={
                "标题":   {"title":  [{"text": {"content": d.get("title") or listing_id}}]},
                "项目":   {"rich_text": [{"text": {"content": d.get("project") or ""}}]},
                "区域":   {"rich_text": [{"text": {"content": d.get("area") or ""}}]},
                "户型":   {"rich_text": [{"text": {"content": d.get("layout") or ""}}]},
                "面积":   {"rich_text": [{"text": {"content": str(d.get("size") or "")}}]},
                "楼层":   {"rich_text": [{"text": {"content": str(d.get("floor") or "")}}]},
                "月租":   {"rich_text": [{"text": {"content": price_str}}]},
                "押付":   {"rich_text": [{"text": {"content": d.get("deposit") or ""}}]},
                "亮点":   {"rich_text": [{"text": {"content": hl_str}}]},
                "顾问点评": {"rich_text": [{"text": {"content": d.get("advisor_comment") or ""}}]},
                "房源编号": {"rich_text": [{"text": {"content": listing_id}}]},
                "状态":   {"select": {"name": "在租"}},
            },
        )
        page_id = page["id"]
        print(f"[Notion] Page created: {page_id}")
        return page_id
    except Exception as e:
        print(f"[Notion] Failed to create page: {e}")
        return None


# ── 主发布流程 ────────────────────────────────────────────
class MeihuaPublisher:
    def __init__(
        self,
        db_path: str = DB_PATH,
        *,
        telegram_sender: Callable[..., dict] | None = None,
    ):
        self.db = DB(db_path)
        self.cover_gen = CoverGenerator(db_path)
        # Injectable only for isolated regression; production defaults to the
        # sole real Telegram sender defined in this module.
        self.telegram_sender = telegram_sender or tg_publish

    def _draft_to_dict(self, row) -> dict:
        cols = [
            "id", "draft_id", "source_post_id", "listing_id", "title",
            "project", "community", "area", "property_type", "price",
            "layout", "size", "floor", "deposit", "available_date",
            "highlights", "drawbacks", "advisor_comment", "cost_notes",
            "extracted_data", "normalized_data", "review_status",
            "review_note", "operator_user_id", "cover_asset_id", "queue_score",
            "approved_at", "published_at", "created_at", "updated_at",
        ]
        d = dict(zip(cols, row))
        for f in ("highlights", "drawbacks"):
            if isinstance(d.get(f), str):
                try:
                    d[f] = json.loads(d[f])
                except Exception:
                    d[f] = []
        return d

    def publish_draft(
        self,
        draft_id: str,
        caption_variant: str | None = None,
        *,
        allow_republish: bool = False,
    ) -> bool:
        """
        发布单条 draft。完整链路：
        draft → cover_generator → media_assets
              → TG 发布 → posts
              → Notion 同步
              → publish_logs
        返回 True/False。
        """
        print(f"\n{'='*60}")
        print(f"[Publisher] 开始发布 draft: {draft_id}")

        # 1. 读取 draft。显式列顺序，避免迁移追加列后 SELECT * 错位。
        draft_cols = [
            "id", "draft_id", "source_post_id", "listing_id", "title",
            "project", "community", "area", "property_type", "price",
            "layout", "size", "floor", "deposit", "available_date",
            "highlights", "drawbacks", "advisor_comment", "cost_notes",
            "extracted_data", "normalized_data", "review_status",
            "review_note", "operator_user_id", "cover_asset_id", "queue_score",
            "approved_at", "published_at", "created_at", "updated_at",
        ]
        row = self.db.fetch_one(
            f"SELECT {', '.join(draft_cols)} FROM drafts WHERE draft_id=?",
            (draft_id,),
        )
        if not row:
            print(f"[Publisher] Draft {draft_id} 不存在。")
            return False
        d = self._draft_to_dict(row)
        source_post_id = d.get("source_post_id")
        if source_post_id not in (None, ""):
            # 兼容早期数据库：旧版 source_posts 尚无这两个来源字段。
            source_columns = {
                str(column[1])
                for column in self.db.fetch_all("PRAGMA table_info(source_posts)")
            }
            selected = [
                column
                for column in ("source_type", "source_name")
                if column in source_columns
            ]
            src_row = (
                self.db.fetch_one(
                    f"SELECT {', '.join(selected)} FROM source_posts WHERE id=? LIMIT 1",
                    (source_post_id,),
                )
                if selected
                else None
            )
            if src_row:
                for index, column in enumerate(selected):
                    d[column] = src_row[index]
        original_status = str(d.get("review_status") or "").strip().lower()
        # 发布前必须存在 approved package；Telegram 发送只读取冻结包资产。
        try:
            from publication_package import approved_package
            frozen_package = approved_package(self.db.path, draft_id)
        except Exception as exc:
            print(f"[Publisher] approved package 读取失败：{exc}")
            frozen_package = None
        if not frozen_package:
            print(f"[Publisher] 发布拦截：draft {draft_id} 没有 approved package")
            media_status = assess_draft_media(draft_id, self.db.path)
            if media_blocks_publish(media_status):
                mark_draft_media_broken(draft_id, media_status, self.db.path)
                reason = ",".join(media_status.issue_codes) or "media_precheck_failed"
                target_type = "media_consistency"
            else:
                current_note = str(d.get("review_note") or "").strip()
                note_parts = [part.strip() for part in current_note.split("|") if part.strip()]
                for marker in ("publish_gate_blocked", "approved_package_missing"):
                    if marker not in note_parts:
                        note_parts.append(marker)
                self.db.execute(
                    "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                    (" | ".join(note_parts)[-500:], draft_id),
                )
                reason = "approved_package_missing"
                target_type = "publication_package"
            self.db.execute(
                "UPDATE drafts SET review_status=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                ("pending", draft_id),
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}",
                post_id=None,
                draft_id=draft_id,
                listing_id=str(d.get("listing_id") or ""),
                target_type=target_type,
                target_ref="telegram",
                action="publish_precheck",
                status="failed",
                error_message=reason,
                log_message=f"发布前安全拦截：{reason}",
            )
            return False
        try:
            frozen_snapshot = json.loads(frozen_package.get("snapshot_json") or "{}")
            frozen_main_images = json.loads(frozen_package.get("main_images_json") or "[]")
            frozen_discussion_images = json.loads(frozen_package.get("discussion_images_json") or "[]")
            frozen_source_identity = json.loads(frozen_package.get("source_identity_json") or "{}")
        except Exception as exc:
            print(f"[Publisher] 发布拦截：approved package 快照无效：{exc}")
            return False
        # The approved package owns the caption variant. A stale review_note,
        # scheduler argument or old callback may not relabel or regenerate it
        # after approval.
        frozen_caption_variant = str(frozen_snapshot.get("caption_variant") or "").strip().lower()
        if frozen_caption_variant in {"a", "b", "c", "d"}:
            if caption_variant not in (None, "") and str(caption_variant).strip().lower() != frozen_caption_variant:
                logger.warning(
                    "ignored caption variant %s for frozen package %s; using %s",
                    caption_variant,
                    frozen_package.get("package_id"),
                    frozen_caption_variant,
                )
            caption_variant = frozen_caption_variant
        # Canonical-fact contract: packages built by the new intake pipeline must
        # still represent the current approved draft immediately before any Telegram call.
        frozen_canonical = frozen_snapshot.get("canonical_facts") or {}
        live_canonical = _parsed_normalized(d)
        if frozen_canonical:
            canonical_errors = validate_facts(live_canonical)
            if canonical_errors:
                print(f"[Publisher] 发布拦截：live canonical facts 无效：{','.join(canonical_errors)}")
                return False
            frozen_hash = str(frozen_canonical.get("canonical_facts_hash") or "")
            live_hash = str(live_canonical.get("canonical_facts_hash") or "")
            package_hash = str(frozen_package.get("canonical_facts_hash") or "")
            if not frozen_hash or frozen_hash != live_hash or package_hash != live_hash:
                print("[Publisher] 发布拦截：approved package canonical facts 与当前 draft 不一致")
                return False
            if str(frozen_canonical.get("publication_location_level") or "") not in {
                "level_2_physical_confirmed", "level_1_market_confirmed", "level_1_project_confirmed"
            }:
                print("[Publisher] 发布拦截：approved package 缺少可公开展示位置")
                return False
        # Freeze integrity: new packages bind caption, discussion detail and exact media bytes.
        # Existing approved packages predate discussion_text; keep them publishable without rewriting history.
        base_payload = {
            "package_id": frozen_package.get("package_id"),
            "draft_id": frozen_package.get("draft_id"),
            "property_id": frozen_package.get("property_id"),
            "public_token": frozen_package.get("public_token"),
            "package_version": frozen_package.get("package_version"),
            "source_type": frozen_package.get("source_type"),
            "listing_type": frozen_package.get("listing_type"),
            "media_type": frozen_package.get("media_type"),
            "cover_template": frozen_package.get("cover_template"),
            "cover_path": frozen_package.get("cover_path"),
            "main_images": frozen_main_images,
            "discussion_images": frozen_discussion_images,
            "post_text": frozen_package.get("post_text"),
            "snapshot": frozen_snapshot,
        }
        legacy_hash = hashlib.sha256(json.dumps(base_payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        hardened_legacy_payload = dict(base_payload)
        hardened_legacy_payload["source_identity_hash"] = frozen_package.get("source_identity_hash") or ""
        hardened_legacy_payload["media_asset_hashes"] = frozen_source_identity.get("media_asset_hashes") or []
        hardened_legacy_hash = hashlib.sha256(json.dumps(hardened_legacy_payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        frozen_payload = dict(base_payload)
        frozen_payload["discussion_text"] = frozen_package.get("discussion_text") or ""
        hardened_payload = dict(frozen_payload)
        hardened_payload["source_identity_hash"] = frozen_package.get("source_identity_hash") or ""
        hardened_payload["media_asset_hashes"] = frozen_source_identity.get("media_asset_hashes") or []
        hardened_hash = hashlib.sha256(json.dumps(hardened_payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        stored_hash = str(frozen_package.get("content_hash") or "")
        freeze_schema = str(frozen_snapshot.get("freeze_schema") or "").strip()
        has_frozen_discussion = bool(str(frozen_package.get("discussion_text") or "").strip())
        if freeze_schema == "FREEZE_V2":
            expected_hash = hardened_hash if has_frozen_discussion else hardened_legacy_hash
            if not stored_hash or stored_hash != expected_hash:
                print(f"[Publisher] 发布拦截：FREEZE_V2 content_hash 不匹配：{frozen_package.get('package_id')}")
                return False
            frozen_file_hashes = frozen_snapshot.get("frozen_file_hashes") or {}
            if not isinstance(frozen_file_hashes, dict) or not frozen_file_hashes:
                print(f"[Publisher] 发布拦截：FREEZE_V2 缺少 frozen_file_hashes：{frozen_package.get('package_id')}")
                return False
            for frozen_path, expected_hash in frozen_file_hashes.items():
                path = Path(str(frozen_path)).resolve()
                if not path.is_file():
                    print(f"[Publisher] 发布拦截：冻结文件不存在：{path}")
                    return False
                actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
                if actual_hash != str(expected_hash):
                    print(f"[Publisher] 发布拦截：冻结文件内容已变化：{path}")
                    return False
        elif not stored_hash or stored_hash not in {legacy_hash, hardened_legacy_hash, hardened_hash}:
            print(f"[Publisher] 发布拦截：legacy approved package content_hash 不匹配：{frozen_package.get('package_id')}")
            return False
        if not isinstance(frozen_source_identity, dict) or not frozen_source_identity.get("source_post_db_id"):
            print(f"[Publisher] 发布拦截：approved package 缺少 source identity：{frozen_package.get('package_id')}")
            return False
        if str(frozen_source_identity.get("source_post_db_id")) != str(d.get("source_post_id") or ""):
            print("[Publisher] 发布拦截：package/source_post 不一致")
            return False
        expected_assets = {str(x) for x in (frozen_source_identity.get("media_asset_ids") or []) if str(x)}
        if not expected_assets:
            print("[Publisher] 发布拦截：package 缺少 media_asset_ids")
            return False
        actual_asset_rows = self.db.fetch_all(
            "SELECT asset_id FROM media_assets WHERE owner_type='source_post' AND owner_ref_id=?",
            (str(d.get("source_post_id")),),
        )
        actual_assets = {str(row[0]) for row in actual_asset_rows if row[0]}
        if not expected_assets.issubset(actual_assets):
            print("[Publisher] 发布拦截：冻结媒体不属于同一 source_post")
            return False

        package_listing_id = str(frozen_package.get("property_id") or "").strip()
        if not package_listing_id or package_listing_id != str(d.get("listing_id") or "").strip():
            print(f"[Publisher] 发布拦截：package/listing 不一致：{package_listing_id} != {d.get('listing_id')}")
            return False
        for key in ("area", "property_type", "price", "layout"):
            if str(frozen_snapshot.get(key) or "").strip() != str(d.get(key) or "").strip():
                print(f"[Publisher] 发布拦截：冻结快照字段不一致：{key}")
                return False
        frozen_cover_path = str(frozen_package.get("cover_path") or "").strip()
        frozen_all_images = [str(x) for x in (frozen_main_images + frozen_discussion_images) if str(x).strip()]
        if not frozen_cover_path or not os.path.isfile(frozen_cover_path):
            print(f"[Publisher] 发布拦截：冻结封面不存在：{frozen_cover_path}")
            return False
        if not frozen_all_images or any(not os.path.isfile(p) for p in frozen_all_images):
            print("[Publisher] 发布拦截：冻结图片缺失")
            return False
        d["_frozen_package_id"] = frozen_package.get("package_id")
        d["_frozen_content_hash"] = frozen_package.get("content_hash")
        d["_frozen_post_text"] = frozen_package.get("post_text") or ""
        d["_frozen_discussion_text"] = frozen_package.get("discussion_text") or ""
        d["_frozen_public_token"] = frozen_package.get("public_token") or ""
        d["_frozen_publish_layout"] = str(frozen_snapshot.get("publish_layout") or "links")
        d["_frozen_publish_cover"] = str(frozen_snapshot.get("publish_cover") or "cover")
        expected_qc = _qc_code_from_draft(d)
        if expected_qc not in d["_frozen_post_text"]:
            print(f"[Publisher] 发布拦截：冻结正文缺少统一 QC 编号 {expected_qc}")
            return False
        d["_frozen_main_images"] = frozen_main_images
        d["_frozen_discussion_images"] = frozen_discussion_images
        d["_frozen_cover_path"] = frozen_cover_path
        assert_public_output_safe(
            d["_frozen_post_text"],
            d["_frozen_discussion_text"],
            frozen_package.get("advice_text"),
            context=f"approved_package:{frozen_package.get('package_id')}",
        )
        if original_status == "published" and not allow_republish:
            print(f"[Publisher] Draft {draft_id} 已是 published，跳过重复发布。")
            return False

        if allow_republish:
            # Re-publication is never a flag on an existing frozen package.  It
            # requires a new draft and a newly approved frozen package.
            print("[Publisher] 发布拦截：不支持复用已审核发布包重新公开发帖。")
            return False
        listing_id = system_listing_id_from_draft(d)
        existing_post = self.db.successful_channel_post(listing_id, str(CHANNEL_ID))
        if existing_post:
            print(
                f"[Publisher] 房源 {listing_id} 已发布到当前频道，"
                f"message_id={existing_post[1]}，已阻止重复发布。"
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=existing_post[0],
                draft_id=draft_id, listing_id=listing_id,
                target_type="telegram_channel", target_ref=str(CHANNEL_ID),
                action="prevent_duplicate_publish", status="blocked",
                response_payload={"existing_message_id": existing_post[1]},
                log_message="同一 listing_id 已有成功频道帖；重新公开发帖须新建草稿并重新审核冻结包",
            )
            return False
        delivery = PublicationDeliveryRepository(self.db.path)
        try:
            attempt = delivery.prepare(
                package_id=str(frozen_package["package_id"]),
                draft_id=draft_id,
                listing_id=listing_id,
                channel_chat_id=str(CHANNEL_ID),
            )
        except DeliveryBlocked as exc:
            print(f"[Publisher] 发布拦截：{exc}")
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=None, draft_id=draft_id,
                listing_id=listing_id, target_type="publication_delivery",
                target_ref=str(CHANNEL_ID), action="prepare_attempt", status="blocked",
                error_message=str(exc), log_message="发布 attempt 已存在或需要人工对账，拒绝再次公开发送",
            )
            return False
        if attempt.state == "committed":
            print(f"[Publisher] attempt {attempt.attempt_id} 已完成，拒绝重复发布。")
            return False
        if attempt.state == "sent":
            try:
                delivery.commit_saved_result(attempt=attempt, post_id=f"TG_{attempt.attempt_id}")
            except DeliveryBlocked as exc:
                print(f"[Publisher] 已保存回执的本地恢复失败：{exc}")
                return False
            print(f"[Publisher] 已从 durable Telegram 回执恢复 attempt {attempt.attempt_id}，未再次发送。")
            return True
        d["review_status"] = "publishing"

        # 2. 统一 listing_id：新发布使用 l_房源ID（例：l_1024）
        if d.get("listing_id") != listing_id:
            self.db.execute(
                "UPDATE drafts SET listing_id=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (listing_id, draft_id),
            )
            d["listing_id"] = listing_id
        print(f"[Publisher] listing_id: {listing_id}")

        # approved package 是发布事实源；此处只验证冻结文件存在，不重新评估当前 draft 媒体。
        if not frozen_all_images or any(not os.path.isfile(path) for path in frozen_all_images):
            print("[Publisher] 发布拦截：冻结媒体文件缺失")
            return False

        # 3. 读取 approved package 冻结的封面与四图；发布时不重新抓图、不重新选图。
        cover_path = d["_frozen_cover_path"]
        media_asset_id = d.get("cover_asset_id")
        print(f"[Publisher] 使用冻结发布包封面: {cover_path}")
        print(f"[Publisher] approved package: {d.get('_frozen_package_id')} hash={d.get('_frozen_content_hash')}")
        d["cover_asset_id"] = media_asset_id

        gate = evaluate_publish_gate(d, cover_path, self.db.path, frozen_media_paths=frozen_all_images)
        gate["album_all"] = list(d.get("_frozen_main_images") or []) + list(d.get("_frozen_discussion_images") or [])
        gate["cover_path"] = cover_path
        gate["cover"] = cover_path
        gate["frozen_post_text"] = d.get("_frozen_post_text") or ""
        if gate.get("price_value", 0) > 0:
            raw_price = d.get("price")
            try:
                raw_price_num = int(float(raw_price))
            except (TypeError, ValueError):
                raw_price_num = 0
            if raw_price_num <= 0:
                d["price"] = int(gate["price_value"])
        gate_note = f"publish_path:{gate['mode']};score={gate['score']};real_media={gate['real_media_count']}"
        if gate.get("reasons"):
            gate_note += ";reasons=" + ",".join(gate["reasons"])
        if gate.get("warnings"):
            gate_note += ";warnings=" + ",".join(gate["warnings"])
        merged_review_note = f"{(d.get('review_note') or '').strip()} | {gate_note}".strip(" |")
        self.db.execute(
            "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (merged_review_note[:500], draft_id),
        )
        if not gate.get("is_publishable", True):
            print(f"[Publisher] 发布拦截：{gate_note}")
            # No Telegram call has started.  Record a safe pre-send failure so
            # a later retry must re-check the draft approval boundary.
            delivery.mark_failed_before_send(attempt.attempt_id, f"publish_gate:{','.join(gate.get('reasons') or [])}")
            restore_status = "pending" if any(
                r in {"missing_cover", "missing_real_media"} for r in gate.get("reasons", [])
            ) else (original_status or "ready")
            self.db.execute(
                "UPDATE drafts SET review_status=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (restore_status, draft_id),
            )
            if restore_status == "pending":
                media_status = assess_draft_media(draft_id, self.db.path)
                mark_draft_media_broken(draft_id, media_status, self.db.path)
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}",
                post_id=None,
                draft_id=draft_id,
                listing_id=listing_id,
                target_type="publish_gate",
                target_ref="telegram",
                action="evaluate_publish_gate",
                status="failed",
                error_message=",".join(gate.get("reasons") or []),
                response_payload=gate,
                log_message=f"发布拦截：{gate_note}",
            )
            return False

        # 4. TG channel delivery.  After mark_sending, every failure is treated
        # as externally ambiguous until a durable Telegram receipt proves otherwise.
        print(f"[Publisher] 发布到 TG 频道 {CHANNEL_ID}...")
        tg_result = None
        tg_post_id = f"TG_{attempt.attempt_id}"
        try:
            if caption_variant is None:
                caption_variant = resolve_caption_variant(d)
            else:
                caption_variant = _normalize_caption_variant(caption_variant)
            delivery.mark_sending(attempt.attempt_id)
            print(f"[Publisher] caption_variant: {caption_variant}")
            tg_result = self.telegram_sender(
                d,
                cover_path,
                gate=gate,
                caption_variant=caption_variant,
                frozen_caption=d.get("_frozen_post_text"),
                frozen_discussion_text=d.get("_frozen_discussion_text"),
                publish_layout=d.get("_frozen_publish_layout") or "links",
                publish_cover=d.get("_frozen_publish_cover") or "cover",
            )
            channel_message_id = tg_result["media_message_ids"][0]
            delivery.mark_sent(attempt.attempt_id, tg_result)
            delivery.commit_success(
                attempt_id=attempt.attempt_id,
                post_id=tg_post_id,
                package_id=str(frozen_package["package_id"]),
                draft_id=draft_id,
                listing_id=listing_id,
                channel_chat_id=str(CHANNEL_ID),
                telegram_result=tg_result,
            )
            print(f"[Publisher] TG 发布并持久化成功：msg_id={channel_message_id}")
        except Exception as exc:
            print(f"[Publisher] TG 投递未完成：{exc}")
            delivery.mark_unknown(attempt.attempt_id, str(exc), tg_result)
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="publication_delivery",
                target_ref=str(CHANNEL_ID), action="telegram_delivery", status="unknown",
                error_message=str(exc),
                log_message="Telegram 调用后或本地提交期间发生异常；已熔断，禁止自动重发，需以回执恢复或人工对账",
            )
            return False

        # Non-core enrichments run only after the public send is durably committed.
        self.db.write_publish_analytics(
            draft_id=draft_id, post_id=tg_post_id,
            message_id=tg_result["media_message_ids"][0], listing_id=listing_id,
            area=str(d.get("area") or ""), property_type=str(d.get("property_type") or ""),
            monthly_rent=d.get("price"), caption_variant=caption_variant,
            published_at=datetime.now().isoformat(timespec="seconds"),
        )
        self.db.write_log(
            log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
            listing_id=listing_id, target_type="telegram_channel", target_ref=str(CHANNEL_ID),
            action="send_frozen_package", status="success",
            response_payload={
                "attempt_id": attempt.attempt_id,
                "package_id": frozen_package["package_id"],
                "message_id": tg_result["media_message_ids"][0],
                "publish_mode": tg_result.get("publish_mode"),
                "caption_variant": caption_variant,
            },
            log_message="approved frozen package 已发送并完成本地状态提交",
        )
        try:
            synced_cover = sync_published_listing_for_user_bot(
                self.db.path, d, listing_id, cover_path, tg_result["media_message_ids"][0]
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="user_bot_listings", target_ref=listing_id,
                action="sync_listing", status="success", response_payload={"media_file_id": synced_cover},
                log_message="已同步用户 Bot 房源投影",
            )
        except Exception as exc:
            # The public send remains committed.  Do not mutate its state or retry Telegram.
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="user_bot_listings", target_ref=listing_id,
                action="sync_listing", status="failed", error_message=str(exc),
                log_message="频道发布已提交；用户房源投影同步失败，需要本地修复，不会重发频道帖",
            )

        # 5. Notion 同步
        print(f"[Publisher] 同步到 Notion...")
        notion_page_id = notion_sync(d, listing_id)
        if notion_page_id:
            self.db.execute(
                "UPDATE posts SET notion_page_id=?, updated_at=CURRENT_TIMESTAMP WHERE post_id=?",
                (notion_page_id, tg_post_id),
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="notion",
                target_ref=NOTION_DB_ID, action="create_page", status="success",
                response_payload={"page_id": notion_page_id},
                log_message=f"Notion 同步成功，page_id={notion_page_id}",
            )
        else:
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="notion",
                target_ref=NOTION_DB_ID or "N/A", action="create_page", status="skipped",
                log_message="Notion 未配置或同步失败，已跳过",
            )

        # 6. The draft, listing, package and post were already committed atomically.
        # Deduplicate sibling drafts only after that fact exists.
        spid = d.get("source_post_id")
        if spid not in (None, ""):
            self.db.execute(
                """UPDATE drafts
                   SET review_status='deduped', updated_at=CURRENT_TIMESTAMP
                   WHERE source_post_id=?
                     AND draft_id<>?
                     AND review_status IN ('ready', 'pending', 'approved', 'publishing')""",
                (spid, draft_id),
            )
        print(f"[Publisher] Draft {draft_id} 已标记为 published。")
        print(f"[Publisher] 发布完成 ✓")
        return True

    def publish_all_approved(self) -> dict:
        """发布所有 review_status='approved' 的 drafts。"""
        rows = self.db.fetch_all(
            "SELECT draft_id FROM drafts WHERE review_status='approved' ORDER BY id"
        )
        if not rows:
            print("[Publisher] 没有 approved 状态的 drafts。")
            return {"success": 0, "failed": 0}
        results = {"success": 0, "failed": 0}
        for (did,) in rows:
            ok = self.publish_draft(did)
            if ok:
                results["success"] += 1
            else:
                results["failed"] += 1
        return results


if __name__ == "__main__":
    import sys

    db_path = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
    publisher = MeihuaPublisher(db_path)

    # 检查配置
    print("=" * 60)
    print("MeihuaPublisher 配置检查")
    print(f"  DB_PATH          : {db_path}")
    print(f"  PUBLISHER_TOKEN  : {'已设置' if PUBLISHER_TOKEN else '未设置！'}")
    print(f"  CHANNEL_ID       : {CHANNEL_ID or '未设置！'}")
    print(
        f"  发布Bot @        : @{PUBLISHER_BOT_USER} (PUBLISHER_BOT_USERNAME)"
        if PUBLISHER_BOT_USER
        else "  发布Bot @        : (未设 PUBLISHER_BOT_USERNAME)"
    )
    print(f"  按钮深链 Bot     : @{BOT_USERNAME or '(未设 USER_BOT_USERNAME)'}")
    print(f"  NOTION_TOKEN     : {'已设置' if NOTION_TOKEN else '未配置（跳过）'}")
    print(f"  NOTION_DATABASE_ID: {NOTION_DB_ID or '未配置（跳过）'}")
    print("=" * 60)

    print(
        "安全退出：禁止直接执行 meihua_publisher.py 批量发布。\n"
        "请通过 systemd 管理的审核发布 Bot 操作；它只接受人工审核并冻结的 publication package。"
    )
    sys.exit(2)
