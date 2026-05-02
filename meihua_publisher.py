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
import sqlite3
import asyncio
import logging
import time
import re
import hashlib
import random
from datetime import datetime, timezone
from pathlib import Path
import io
from html import escape as he

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter

from telegram import Bot, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

from notion_client import Client as NotionClient

from cover_generator import CoverGenerator

logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────
def _normalize_bot_username(raw: str) -> str:
    return str(raw or "").strip().lstrip("@")


DB_PATH           = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
PUBLISHER_TOKEN   = os.getenv("PUBLISHER_BOT_TOKEN", "")
CHANNEL_ID        = os.getenv("CHANNEL_ID", "")
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
    "📎 <b>补充实拍</b>\n\n"
    "真实房源现场拍摄\n"
    "户型 / 公区 / 采光情况\n\n"
    "侨联地产实拍\n"
    "金边租房更透明",
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
    "📎 <b>补充实拍（续）</b>",
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
# 频道主帖最多发几张；默认 4（固定 1+3），多出来的全部走讨论组。设为 9 可恢复「频道九宫格」旧行为。
CHANNEL_MAIN_ALBUM_MAX = max(1, int(os.getenv("CHANNEL_MAIN_ALBUM_MAX", "4")))
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
) -> Image.Image:
    draw = ImageDraw.Draw(overlay)
    logo_w = max(196, min(320, int(ref * 0.28)))
    logo_h = max(62, min(110, int(logo_w * 0.34)))
    x2 = overlay.size[0] - edge
    x1 = x2 - logo_w
    y1 = edge
    y2 = y1 + logo_h

    im = _apply_frosted_panel(
        im,
        (x1, y1, x2, y2),
        radius=max(10, int(14 * scale)),
        blur_radius=max(7, int(10 * scale)),
        tint_rgb=(7, 18, 36),
        tint_alpha=178,
        outline=(246, 210, 122, 138),
    )

    pad_x = max(12, int(16 * scale))
    pad_y = max(8, int(10 * scale))
    cn_font = _font_for_listing(max(18, min(34, int(logo_h * 0.40))), bold=True)
    en_font = _font_for_listing(max(8, min(13, int(logo_h * 0.16))), bold=False)
    cn = BRAND_NAME
    en = BRAND_NAME_EN

    cn_box = draw.textbbox((0, 0), cn, font=cn_font)
    en_box = draw.textbbox((0, 0), en, font=en_font)
    cn_h = cn_box[3] - cn_box[1]
    gap = max(1, int(logo_h * 0.04))
    cn_y = y1 + pad_y - cn_box[1]
    en_y = cn_y + cn_h + gap - en_box[1]
    tx = x1 + pad_x

    draw.text((tx, cn_y), cn, font=cn_font, fill=(246, 210, 122, 255))
    draw.text((tx, en_y), en, font=en_font, fill=(232, 238, 247, 225))
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
    return flo if flo.endswith("楼") or flo.upper().endswith("F") else f"{flo}楼"


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
    show_brand = detail_mode or not with_listing_footer
    if show_brand:
        if detail_mode:
            style = DETAIL_PHOTO_STYLE
            if style in ("mini_card", "mini", "v2", "new"):
                _apply_detail_photo_shade(overlay)
                im = _draw_detail_mini_logo_badge(im, overlay, edge=edge, scale=scale, ref=ref)
                im = _draw_detail_corner_tags(
                    im,
                    overlay,
                    edge=edge,
                    scale=scale,
                    ref=ref,
                    listing=detail_payload,
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
            brand_box = (edge, edge, edge + brand_w, edge + brand_h)
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

            cursor_x = edge + pad_x
            center_y = edge + brand_h // 2
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
            fs_price = max(20, min(40, int(ref * 0.034)))
            font_price = _font_for_watermark(fs_price)
            bbox = draw.textbbox((0, 0), price_text, font=font_price)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            chip_pad_x = max(14, int(20 * scale))
            chip_pad_y = max(9, int(12 * scale))
            chip_w = tw + chip_pad_x * 2
            chip_h = th + chip_pad_y * 2
            x2 = w - edge
            y2 = h - edge
            x1 = x2 - chip_w
            y1 = y2 - chip_h
            chip_radius = max(10, int(15 * scale))
            im = _apply_frosted_panel(
                im,
                (x1, y1, x2, y2),
                radius=chip_radius,
                blur_radius=max(8, int(12 * scale)),
                tint_rgb=(248, 250, 255),
                tint_alpha=182,
                outline=(255, 255, 255, 88),
            )
            draw.text(
                (x1 + chip_pad_x - bbox[0], y1 + chip_pad_y - bbox[1]),
                price_text,
                font=font_price,
                fill=(42, 84, 188, 255),
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
    """细节图加小图样式品牌层（右上 mini logo + 左下标签）。"""
    return add_channel_listing_overlay(
        image_bytes,
        listing,
        with_listing_footer=False,
        detail_mode=True,
        detail_listing=listing,
    )
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
                image_bytes, target_size=ONE_THREE_TILE, force_square=True
            )
        if total == 3:
            if index == 0:
                return normalize_album_image(image_bytes, fit_box=ONE_THREE_HERO_BOX)
            return normalize_album_image(
                image_bytes, target_size=ONE_THREE_TILE, force_square=True
            )
        if total == 2:
            if index == 0:
                return normalize_album_image(image_bytes, fit_box=ONE_THREE_HERO_BOX)
            return normalize_album_image(
                image_bytes, target_size=ONE_THREE_TILE, force_square=True
            )
        return normalize_album_image(image_bytes, target_size=1280, force_square=False)

    if total == 6 and CHANNEL_ALBUM_SIX_ASPECT in ("landscape", "3x2", "32"):
        h32 = max(720, int(round(1280 * 2 / 3)))
        return normalize_album_image(image_bytes, target_size=1280, fit_box=(1280, h32))

    force_square_grid = total in (4, 6, 9)
    return normalize_album_image(
        image_bytes, target_size=1280, force_square=force_square_grid
    )


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
        return f"${price}/月"
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
    advantages = advantages[:2] or ["实拍房源", "中文顾问可约看房"]
    while len(advantages) < 2:
        advantages.append("生活配套方便")

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
    text = _clean_display_text(raw)
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
    raw = _listing_value(d, "project", "community", "title", default="")
    cleaned = _clean_project_label(raw)
    return _compact_copy(cleaned, 24) if cleaned else ""


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
    return v if v in {"a", "b", "c"} else "a"


def _pick_weighted_caption_variant(db: "DB") -> str:
    fixed_variant = str(os.getenv("CAPTION_VARIANT_FIXED", "a")).strip().lower()
    if fixed_variant in {"a", "b", "c"}:
        return fixed_variant
    weights = db.get_caption_variant_weights()
    variants = ["a", "b", "c"]
    probs = [float(weights.get(v, 0.0)) for v in variants]
    if sum(probs) <= 0:
        return "a"
    try:
        return random.choices(variants, weights=probs, k=1)[0]
    except Exception:
        logger.exception("按权重选择 caption_variant 失败，回退 A")
        return "a"


def _attach_caption_variant_to_target(target: str, caption_variant: str | None = None) -> str:
    safe_target = str(target or "").strip()
    if not safe_target:
        return safe_target
    if re.search(r"(?:^|\|)cv=(a|b|c)(?:$|\|)", safe_target, flags=re.IGNORECASE):
        return safe_target
    raw_variant = str(caption_variant or "").strip().lower()
    if raw_variant not in {"a", "b", "c"}:
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


_NOISE_KEYWORDS = ("噪", "马路", "高架", "highway", "loud", "noise", "吵", "嘈")
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
    deposit = _normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default=""))
    contract = _normalize_contract_term(_listing_value(d, "contract_term", default=""))
    if deposit and contract:
        return _compact_copy(f"押付 {deposit}，合同 {contract}，细节看房前可逐项确认", 28)
    if deposit:
        return _compact_copy(f"押付 {deposit}，具体费用细节建议看房前问清", 28)
    return "价格和空房以实时确认为准，建议看房前先问清押付"


def build_chinese_listing_post(d: dict, caption_variant: str | None = "a") -> str:
    """生成频道标准发帖正文（固定顺序 + 系统标签 + SEO 标签）。"""
    area = _listing_value(d, "area", default="金边")
    room_type = normalize_room_type(_listing_value(d, "room_type", "layout", default="整租"))
    if not room_type:
        room_type = _resolved_property_type(d)
    listing_id = system_listing_id_from_draft(d)
    price = _price_compact_for_post(d)
    title = _compact_listing_title(d, area, room_type, price)
    location_seed = _listing_value(d, "area", "project", "community", default="金边")
    location = _compact_copy(location_seed, 22)
    deposit_raw = _normalize_deposit_text(_listing_value(d, "payment_terms", "deposit", default="")) or "待确认"
    contract_raw = _normalize_contract_term(_listing_value(d, "contract_term", default="")) or "待确认"
    payment_contract = _compact_copy(f"付款/合同：{deposit_raw}｜{contract_raw}", 28)
    viewing_hint = _compact_copy(_contextual_viewing_hint(d), 32)
    judge_hint = _compact_copy(_factual_highlight_text(d), 30)
    tags = " ".join(build_listing_tags(d)[:8]).strip()

    lines = [
        f"<b>{he(title)}</b>",
        "<code>QIAOLIAN VERIFIED LISTING</code>",
        "━━━━━━━━━━━━",
        f"房源编号：<code>{he(listing_id)}</code>",
        f"位置：{he(location)}",
        f"户型：{he(room_type)}",
        f"租金：{he(price)}",
        f"押付：{he(payment_contract)}",
        "━━━━━━━━━━━━",
        f"提前说清：{he(viewing_hint)}",
        f"侨联判断：{he(judge_hint)}",
        "━━━━━━━━━━━━",
        f"{he(BRAND_NAME)}｜您在金边的自己人",
        "",
        tags,
    ]
    return "\n".join(lines)[:1024]


def build_cover_listing_data(d: dict) -> dict:
    return {
        "project": "",
        "layout": "",
        "price": d.get("price"),
        "area": "",
        "size": "",
        "floor": "",
        "highlights": [],
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
    existing = (d.get("listing_id") or "").strip()
    if existing.startswith("l_"):
        return existing
    if existing.startswith("QJ-") and existing[3:].isdigit():
        return f"l_{existing[3:]}"
    if existing and not existing.startswith("LST_"):
        return existing
    raw_id = d.get("id")
    if raw_id not in (None, ""):
        try:
            return f"l_{int(raw_id)}"
        except (TypeError, ValueError):
            pass
    return f"l_{int(time.time())}"


def build_caption(d: dict, caption_variant: str | None = "a") -> str:
    """发布层统一生成中文租房帖，不透传 AI 模板文案。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)

def build_detail_text(d: dict, caption_variant: str | None = "a") -> str:
    """文字消息正文：统一中文结构，避免模板名/开发调试词进入频道。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)


def build_rich_album_caption(d: dict, caption_variant: str | None = "a") -> str:
    """频道主帖文案：固定中文租房结构，不展示内部编号或模板名。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)


def build_channel_teaser_caption(d: dict, caption_variant: str | None = "a") -> str:
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


def normalize_album_grid(paths: list[str]) -> list[str]:
    """
    频道主帖只取前 CHANNEL_MAIN_ALBUM_MAX 张（默认 4 = 一套 1+3），
    其余路径留在 album_all 里由调用方作为 extra 发讨论组。
    源图张数 ≤ 上限时频道全发、讨论组不加图。
    """
    if not paths:
        return paths
    n = len(paths)
    cap = CHANNEL_MAIN_ALBUM_MAX
    selected = paths[:cap] if n > cap else list(paths)
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
    return {"publish_queue": [], "discuss_mgid": {}}


def load_discussion_bridge() -> dict:
    if DISCUSSION_BRIDGE_FILE.exists():
        try:
            with open(DISCUSSION_BRIDGE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("publish_queue", [])
                    data.setdefault("discuss_mgid", {})
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
    """发帖成功后调用：记录频道首条 message_id，供讨论区自动转发对齐（与频道 mgid 无关）。"""
    if not channel_post_id:
        return
    data = load_discussion_bridge()
    data["publish_queue"].append(
        {"t": time.time(), "channel_post_id": int(channel_post_id)}
    )
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


def _build_discussion_appt_keyboard(listing_id: str, post_token: str) -> InlineKeyboardMarkup:
    """讨论区第一段：预约看房按钮，深链到用户 Bot 的预约流程。"""
    if BOT_USERNAME:
        user = BOT_USERNAME.lstrip("@")
        listing_target = f"{listing_id}|entry=discussion|step=seg1"
        appt_payload = build_start_payload("a", listing_target, post_token)
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("📅 预约看房", url=f"https://t.me/{user}?start={appt_payload}")]]
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
    extra_album: list | None = None,
    attempts: int = 30,
    delay_seconds: float = 2.0,
) -> bool:
    """
    频道发帖后，讨论区三段式：
    1) 预约承接（文案 + 预约按钮）
    2) 补充实拍组图（若有 extra_album）
    3) 继续看房入口（文案 + 小助手深链按钮）
    成功发出第一段返回 True，否则 False。
    """
    discussion_id = await resolve_discussion_chat_id(bot)
    if not discussion_id:
        logger.warning("三段式讨论区发帖：无法获取讨论组 chat_id，跳过。channel_post_id=%s", channel_post_id)
        return False

    # 等待讨论区自动转发就绪，然后发第一段（预约承接）
    appt_keyboard = _build_discussion_appt_keyboard(listing_id, post_token)
    seg1_mid = await poll_discussion_first_reply(
        bot,
        channel_post_id,
        DISCUSSION_APPT_TEXT,
        reply_markup=appt_keyboard if appt_keyboard.inline_keyboard else None,
        attempts=attempts,
        delay_seconds=delay_seconds,
    )
    if not seg1_mid:
        logger.warning("三段式讨论区：第一段发送失败。channel_post_id=%s", channel_post_id)
        return False

    # 第二段：补充实拍（若有）
    if extra_album:
        mapping = load_discuss_map()
        thread_reply_id = mapping.get(str(channel_post_id)) or seg1_mid
        chunk = 10
        total_extra = len(extra_album)
        for batch_start in range(0, total_extra, chunk):
            batch_paths = extra_album[batch_start : batch_start + chunk]
            extra_media = []
            for j, path in enumerate(batch_paths):
                with open(path, "rb") as raw:
                    data_bytes = raw.read()
                data_bytes = normalize_album_image(data_bytes, target_size=1280, force_square=False)
                buf = add_detail_logo_watermark(data_bytes)
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
            except Exception:
                logger.exception("三段式讨论区：第二段实拍发送失败 batch_start=%s", batch_start)
            if batch_start + chunk < total_extra:
                await asyncio.sleep(0.6)
        logger.info("三段式讨论区：第二段已发 %s 张实拍", total_extra)

    # 第三段：继续看房入口
    try:
        mapping = load_discuss_map()
        thread_reply_id = mapping.get(str(channel_post_id)) or seg1_mid
        continue_keyboard = _build_discussion_continue_keyboard(listing_id, post_token)
        continue_text = str(DISCUSSION_CONTINUE_TEXT or "").strip()
        if "机器人对话" not in continue_text:
            continue_text = (continue_text + "\n\n🤖 点击下方按钮进入侨联机器人对话").strip()
        await bot.send_message(
            chat_id=discussion_id,
            text=continue_text,
            reply_to_message_id=int(thread_reply_id),
            reply_markup=continue_keyboard if continue_keyboard.inline_keyboard else None,
            allow_sending_without_reply=True,
        )
        logger.info("三段式讨论区：第三段已发。channel_post_id=%s listing_id=%s", channel_post_id, listing_id)
    except Exception:
        logger.exception("三段式讨论区：第三段发送失败。channel_post_id=%s", channel_post_id)

    return True


def build_channel_caption(
    d: dict, album_paths: list[str], caption_variant: str | None = "a"
) -> str:
    """生成发频道用的首图 caption。只输出中文租房帖，不附加调试图注。"""
    return build_chinese_listing_post(d, caption_variant=caption_variant)[:1024]


def build_keyboard(
    listing_id: str,
    area: str = "",
    post_token: str = "",
    caption_variant: str | None = "a",
) -> InlineKeyboardMarkup:
    """房源帖固定 CTA：咨询、预约、收藏、同区域更多。"""
    if BOT_USERNAME:
        user = BOT_USERNAME.lstrip("@")
        base = f"https://t.me/{user}?start="
        rows = [
            [
                InlineKeyboardButton(
                    "💬 咨询这套",
                    url=f"{base}{build_start_payload('q', listing_id, post_token, caption_variant)}",
                ),
                InlineKeyboardButton(
                    "📅 预约看房",
                    url=f"{base}{build_start_payload('a', listing_id, post_token, caption_variant)}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❤️ 收藏这套",
                    url=f"{base}{build_start_payload('f', listing_id, post_token, caption_variant)}",
                ),
                InlineKeyboardButton(
                    "🏠 同区更多",
                    url=f"{base}{build_start_payload('m', area or '金边', post_token, caption_variant)}",
                ),
            ],
        ]
    else:
        ch_link = f"https://t.me/c/{str(CHANNEL_ID).lstrip('-100')}"
        rows = [[InlineKeyboardButton(f"📞 联系{BRAND_NAME}", url=ch_link)]]
    return InlineKeyboardMarkup(rows)


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


def _draft_quality_score(d: dict) -> int:
    nd = _parsed_normalized(d)
    raw = d.get("queue_score")
    if raw in (None, ""):
        raw = nd.get("quality_score", 0)
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
    for m in re.finditer(r"(\d{1,2})\s*房", text):
        try:
            n = int(m.group(1))
            best = max(best, n)
        except (TypeError, ValueError):
            continue
    return best


def _review_quality_flags(review_note: str) -> set[str]:
    note = str(review_note or "")
    m = re.search(r"quality:([^|]+)", note)
    if not m:
        return set()
    raw = m.group(1)
    out = set()
    for item in raw.split(","):
        tag = str(item or "").strip().lower()
        if tag:
            out.add(tag)
    return out


def evaluate_publish_gate(d: dict, cover_path: str, db_path: str) -> dict:
    real_media = _real_media_paths_for_draft(d, db_path)
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

    hard_block_reasons: list[str] = []
    fallback_reasons: list[str] = []

    quality_flags = _review_quality_flags(d.get("review_note") or "")
    source_text = _source_raw_text(d, db_path)
    source_room_max = _max_rooms_from_source(source_text)
    layout_room_count = _layout_rooms_count(layout)
    property_type = str(d.get("property_type") or "").strip().lower()
    size_raw = str(d.get("size") or "").strip()
    size_value = _size_value(size_raw)

    valid_area = bool(area and area not in {"金边", "未知"}) or bool(project and project not in {"金边", "未知", ""})
    # 频道自动发布必须带明确价格，避免"价格待确认"误发。
    valid_price = 100 <= price_value <= 20000
    valid_room = bool(layout and layout not in {"整租", "住宅"})

    if score < BASIC_PUBLISH_MIN_SCORE:
        hard_block_reasons.append(f"low_score:{score}")
    if not valid_price:
        hard_block_reasons.append("invalid_price")
    if not valid_area:
        fallback_reasons.append("vague_area")
    if not valid_room:
        hard_block_reasons.append("invalid_layout")
    if not cover_path or not os.path.isfile(cover_path):
        hard_block_reasons.append("missing_cover")
    if not real_media:
        hard_block_reasons.append("missing_real_media")
    if "missing_price" in quality_flags:
        hard_block_reasons.append("quality_missing_price")
    if "missing_area" in quality_flags:
        fallback_reasons.append("quality_missing_area")
    if "missing_layout" in quality_flags:
        hard_block_reasons.append("quality_missing_layout")
    if source_room_max >= 2 and layout_room_count > 0 and layout_room_count < (source_room_max - 1):
        hard_block_reasons.append(f"layout_mismatch:{layout_room_count}_lt_src_{source_room_max}")
    if "别墅" in property_type and size_value > 0 and size_value < 25:
        hard_block_reasons.append(f"suspicious_villa_size:{size_value:g}")
    if "别墅" in property_type and size_raw and re.search(r"[xX×*]", source_text) and size_value > 0 and size_value < 40:
        hard_block_reasons.append("villa_size_may_be_dimension_not_area")
    if "sale" in property_type and 0 < price_value < 5000:
        hard_block_reasons.append("price_unit_ambiguous")
    if ("rent" in property_type or "rental" in property_type) and price_value > 50000:
        hard_block_reasons.append("suspicious_sale_price_in_rent")

    if score < PREMIUM_PUBLISH_MIN_SCORE:
        fallback_reasons.append(f"score_below_premium:{score}")
    if len(real_media) < PREMIUM_REAL_MEDIA_MIN:
        fallback_reasons.append(f"real_media_lt_{PREMIUM_REAL_MEDIA_MIN}")

    mode = "premium_4image"
    gate_cover_path = cover_path if cover_path and os.path.isfile(cover_path) else None
    # 频道相册顺序：封面在前，后接同组实拍；主帖截前 4 张，其余进讨论组。
    album_all = _album_paths_for_draft(d, cover_path, db_path) if gate_cover_path else list(real_media)
    if hard_block_reasons:
        mode = "blocked"
        gate_cover_path = None
        album_all = []
    elif fallback_reasons:
        mode = "fallback_media"
        album_all = _album_paths_for_draft(d, cover_path, db_path) if gate_cover_path else list(real_media)

    reasons = hard_block_reasons if hard_block_reasons else fallback_reasons
    return {
        "mode": mode,
        "score": score,
        "real_media_count": len(real_media),
        "reasons": reasons,
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
) -> dict:
    """
    排版：频道主帖仅 CHANNEL_MAIN_ALBUM_MAX 张（默认 4，封面在前/实拍在后）+ 首图短 caption；
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
    cover_listing = build_cover_listing_data(d)

    # 收集图片（封面 + 实拍）→ 频道最多 CHANNEL_MAIN_ALBUM_MAX 张，其余 extra
    album_all = gate.get("album_all") or _album_paths_for_draft(d, cover_path, DB_PATH)
    album = normalize_album_grid(album_all)
    extra_album = album_all[len(album):]

    # 文案（首张图 caption，限 1024）
    caption = build_channel_caption(d, album, caption_variant=caption_variant)
    post_token = ""

    button_message_id = None
    keyboard = build_keyboard(listing_id, area, caption_variant=caption_variant)

    def _prepare_channel_photo_buf(data: bytes, *, is_cover: bool, slot_index: int) -> io.BytesIO:
        """频道图品牌层容错：坏图/异常时回退原图，避免整条发布失败。"""
        try:
            buf = (
                add_brand_watermark(data, cover_listing, with_listing_footer=True)
                if is_cover
                else add_detail_logo_watermark(data, d)
            )
        except Exception:
            logger.exception(
                "频道图片品牌层处理失败，回退原图 slot=%s cover=%s",
                slot_index,
                is_cover,
            )
            buf = io.BytesIO(data)
        buf.name = f"p{slot_index}.jpg"
        buf.seek(0)
        return buf

    # 1) 发送图片（单图或相册），首图带 caption；相册不再额外发送独立按钮消息
    if len(album) == 1:
        with open(album[0], "rb") as raw:
            data = raw.read()
        data = normalize_album_image(data, target_size=1280, force_square=False)
        is_cover = bool(cover_path and os.path.abspath(album[0]) == os.path.abspath(cover_path))
        buf = _prepare_channel_photo_buf(data, is_cover=is_cover, slot_index=0)
        sent = await bot.send_photo(
            chat_id=CHANNEL_ID,
            photo=buf,
            caption=caption,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
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
                data = _normalize_for_album_slot(data, index=i, total=na)
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
                reply_markup=keyboard,
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

    # 单图 / 多图共用：发讨论区三段式（预约承接 + 补充实拍 + 继续看房入口）
    channel_mid = media_message_ids[0] if media_message_ids else None
    if channel_mid:
        discuss_id = await resolve_discussion_chat_id(bot)
        if discuss_id and str(discuss_id) != str(CHANNEL_ID):
            await send_discussion_three_segments(
                bot,
                channel_mid,
                listing_id,
                post_token,
                extra_album=extra_album if extra_album else None,
                attempts=60,
                delay_seconds=2.0,
            )
        elif extra_album:
            logger.warning(
                "有 %s 张溢出实拍但未配置讨论区或讨论区与频道相同，已跳过",
                len(extra_album),
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


def tg_publish(
    d: dict,
    cover_path: str,
    gate: dict | None = None,
    caption_variant: str | None = "a",
) -> dict:
    return asyncio.run(_tg_publish(d, cover_path, gate=gate, caption_variant=caption_variant))


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
    def __init__(self, db_path: str = DB_PATH):
        self.db = DB(db_path)
        self.cover_gen = CoverGenerator(db_path)

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

    def publish_draft(self, draft_id: str, caption_variant: str | None = None) -> bool:
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
            src_row = self.db.fetch_one(
                "SELECT source_type, source_name FROM source_posts WHERE id=? LIMIT 1",
                (source_post_id,),
            )
            if src_row:
                d["source_type"] = src_row[0]
                d["source_name"] = src_row[1]
        original_status = str(d.get("review_status") or "").strip().lower()
        if original_status == "published":
            print(f"[Publisher] Draft {draft_id} 已是 published，跳过重复发布。")
            return False
        if not self.db.claim_draft_for_publish(draft_id):
            print(f"[Publisher] Draft {draft_id} 正在发布或状态不允许，跳过。")
            return False
        d["review_status"] = "publishing"

        # 2. 统一 listing_id：新发布使用 l_房源ID（例：l_1024）
        listing_id = system_listing_id_from_draft(d)
        if d.get("listing_id") != listing_id:
            self.db.execute(
                "UPDATE drafts SET listing_id=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (listing_id, draft_id),
            )
            d["listing_id"] = listing_id
        print(f"[Publisher] listing_id: {listing_id}")

        from media_consistency import (
            assess_draft_media,
            mark_draft_media_broken,
            media_blocks_publish,
        )

        media_status = assess_draft_media(draft_id, self.db.path)
        if media_blocks_publish(media_status):
            media_note = media_status.note()
            print(f"[Publisher] 媒体文件缺失，中止发布：{media_note}")
            mark_draft_media_broken(draft_id, media_status, self.db.path)
            self.db.execute(
                "UPDATE drafts SET review_status='pending', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (draft_id,),
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}",
                post_id=None,
                draft_id=draft_id,
                listing_id=listing_id,
                target_type="media_consistency",
                target_ref="filesystem",
                action="pre_publish_check",
                status="failed",
                error_message=",".join(media_status.issue_codes),
                response_payload={
                    "cover_path": media_status.cover_path,
                    "existing_real_media": len(media_status.existing_real_media),
                    "missing_real_media": media_status.missing_real_media[:20],
                },
                log_message=f"媒体文件缺失，已退回 pending：{media_note}",
            )
            return False

        # 3. 生成封面图 → media_assets
        print(f"[Publisher] 生成封面图...")
        media_asset_id, cover_path = self.cover_gen.generate_for_draft(draft_id)
        if not cover_path or not os.path.exists(cover_path):
            print(f"[Publisher] 封面图生成失败，中止发布。")
            self.db.execute(
                "UPDATE drafts SET review_status=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (original_status or "ready", draft_id),
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=None, draft_id=draft_id,
                listing_id=listing_id, target_type="cover_generator",
                target_ref="local", action="generate_cover", status="failed",
                error_message="封面图生成失败",
            )
            return False
        print(f"[Publisher] 封面图路径: {cover_path}")
        d["cover_asset_id"] = media_asset_id

        gate = evaluate_publish_gate(d, cover_path, self.db.path)
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
        merged_review_note = f"{(d.get('review_note') or '').strip()} | {gate_note}".strip(" |")
        self.db.execute(
            "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (merged_review_note[:500], draft_id),
        )
        if not gate.get("is_publishable", True):
            print(f"[Publisher] 发布拦截：{gate_note}")
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

        # 4. TG 频道发布
        print(f"[Publisher] 发布到 TG 频道 {CHANNEL_ID}...")
        tg_result = None
        tg_post_id = f"TG_{uuid.uuid4()}"
        try:
            if caption_variant is None:
                caption_variant = _pick_weighted_caption_variant(self.db)
            else:
                caption_variant = _normalize_caption_variant(caption_variant)
            print(f"[Publisher] caption_variant: {caption_variant}")
            tg_result = tg_publish(d, cover_path, gate=gate, caption_variant=caption_variant)
            channel_message_id = tg_result["media_message_ids"][0]
            button_message_id  = tg_result["button_message_id"]
            media_group_id     = tg_result["media_group_id"]
            print(f"[Publisher] TG 发布成功：msg_id={channel_message_id}, btn_id={button_message_id}")

            # 写入 posts
            post_db_id = self.db.create_post(
                post_id=tg_post_id,
                listing_id=listing_id,
                draft_id=draft_id,
                platform="telegram",
                channel_chat_id=str(CHANNEL_ID),
                channel_message_id=channel_message_id,
                media_group_id=media_group_id,
                button_message_id=button_message_id,
                post_text=tg_result["caption"],
                publish_status="published",
            )
            print(f"[Publisher] posts 记录写入：db_id={post_db_id}")
            self.db.write_publish_analytics(
                draft_id=draft_id,
                post_id=tg_post_id,
                message_id=channel_message_id,
                listing_id=listing_id,
                area=str(d.get("area") or ""),
                property_type=str(d.get("property_type") or ""),
                monthly_rent=d.get("price"),
                caption_variant=caption_variant,
                published_at=datetime.now().isoformat(timespec="seconds"),
            )

            # publish_log: TG 成功
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="telegram_channel",
                target_ref=str(CHANNEL_ID), action="send_photo", status="success",
                response_payload={
                    "message_id": channel_message_id,
                    "button_message_id": button_message_id,
                    "publish_mode": tg_result.get("publish_mode"),
                    "caption_variant": caption_variant,
                    "post_token": tg_result.get("post_token"),
                },
                log_message=f"TG 频道发布成功，msg_id={channel_message_id}; mode={tg_result.get('publish_mode')}",
            )
        except Exception as e:
            print(f"[Publisher] TG 发布失败: {e}")
            self.db.execute(
                "UPDATE drafts SET review_status=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (original_status or "ready", draft_id),
            )
            self.db.write_log(
                log_id=f"LOG_{uuid.uuid4()}", post_id=tg_post_id, draft_id=draft_id,
                listing_id=listing_id, target_type="telegram_channel",
                target_ref=str(CHANNEL_ID), action="send_photo", status="failed",
                error_message=str(e),
                log_message=f"TG 频道发布失败: {e}",
            )
            return False

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

        # 6. 更新 draft 状态为 published
        self.db.execute(
            "UPDATE drafts SET review_status='published', published_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (draft_id,),
        )
        # 同一 source_post 的其余草稿降级为去重，避免连续发布同一组房源。
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

    if not PUBLISHER_TOKEN or not CHANNEL_ID:
        print("错误：PUBLISHER_BOT_TOKEN 或 CHANNEL_ID 未设置，无法发布。")
        sys.exit(1)

    # 查找 approved 状态的 drafts，若无则将 pending 的第一条临时设为 approved 用于测试
    approved = publisher.db.fetch_all(
        "SELECT draft_id, title FROM drafts WHERE review_status='approved' LIMIT 5"
    )
    if not approved:
        print("\n没有 approved 状态的 drafts，将第一条 pending draft 临时设为 approved 进行测试...")
        first = publisher.db.fetch_one(
            "SELECT draft_id FROM drafts WHERE review_status='pending' LIMIT 1"
        )
        if not first:
            print("没有任何 drafts，请先运行 ai_parser.py")
            sys.exit(1)
        publisher.db.execute(
            "UPDATE drafts SET review_status='approved' WHERE draft_id=?", (first[0],)
        )
        approved = [(first[0], "（测试）")]

    print(f"\n共 {len(approved)} 条 approved drafts 待发布：")
    for did, title in approved:
        print(f"  - {did}: {title}")

    results = publisher.publish_all_approved()
    print(f"\n发布结果：成功 {results['success']} 条，失败 {results['failed']} 条")

    # 验证 publish_logs
    logs = publisher.db.fetch_all(
        "SELECT target_type, action, status, log_message FROM publish_logs ORDER BY id DESC LIMIT 10"
    )
    print("\n最新 publish_logs：")
    for log in logs:
        print(f"  [{log[2]}] {log[0]} / {log[1]}: {log[3]}")
