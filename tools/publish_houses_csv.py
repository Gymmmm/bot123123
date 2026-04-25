#!/usr/bin/env python3
"""
CSV -> cover render -> album caption -> Telegram channel publish.

Workflow:
1. Read houses CSV rows.
2. Ensure cover image (use image_cover if exists, otherwise auto render from template).
3. Send media group (cover + image2 + image3 + image4) to channel,
   or prepare pending-upload assets with --prepare-only.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from telegram import Bot, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

RENDER_SCRIPT = ROOT / "tools" / "render_blue_card_template.py"
DEFAULT_CSV = ROOT / "data" / "houses.csv"
DEFAULT_RENDERS = ROOT / "media" / "renders" / "csv_auto"
DEFAULT_PREPARED = ROOT / "media" / "renders" / "pending_upload"
DEFAULT_COVER_W = int(os.getenv("CSV_COVER_W", "800") or 800)
DEFAULT_COVER_H = int(os.getenv("CSV_COVER_H", "600") or 600)
COVER_KINDS = (
    "right_price_fixed",
    "dark_glass",
    "villa_premium",
    "right_price",
)

LEGACY_COVER_KIND_ALIASES = {
    "blue_card": "right_price_fixed",
    "white_bar": "right_price_fixed",
    "magazine_white": "right_price_fixed",
    "metro_panel": "right_price_fixed",
    "lite_strip": "right_price_fixed",
    "portrait_luxe": "right_price_fixed",
}

DETAIL_BRAND_LINES = (
    "侨联地产",
    "QIAO LIAN PROPERTY",
    "金边华人租房 / 买房 / 视频看房",
    "租房   买房   视频看房",
    "━━━━━━━━━━",
)

ROOM_ORDER = {
    "living": 0,
    "kitchen": 1,
    "dining": 2,
    "bedroom": 3,
    "balcony": 4,
    "bathroom": 5,
    "exterior": 6,
    "unknown": 99,
}

ROOM_KEYWORDS = {
    "living": ("客厅", "大厅", "会客", "沙发", "living", "livingroom", "living_room", "lounge", "sofa"),
    "kitchen": ("厨房", "厨", "kitchen", "cook", "island"),
    "dining": ("餐厅", "饭厅", "dining"),
    "bedroom": ("主卧", "次卧", "卧室", "房间", "bedroom", "bed", "master", "room"),
    "balcony": ("阳台", "露台", "balcony", "terrace"),
    "bathroom": ("卫生间", "浴室", "洗手间", "bathroom", "bath", "toilet", "wc"),
    "exterior": ("外景", "外立面", "楼体", "building", "exterior", "facade", "outside"),
}

DISPLAY_NOISE_TOKENS = ("啊雷莎", "阿雷莎", "🇨🇳", "🌵")
RANDOM3_STYLES = ("s1", "s2", "s3")
RANDOM3_COVER_KINDS = ("right_price_fixed", "villa_premium", "dark_glass")
FIXED_COVER_COPY_VARIANTS = (
    (
        "实拍房源 / 中文顾问",
        "可预约实地看房 · 可预约视频带看",
        "侨联实拍，细节真实可核",
    ),
    (
        "实拍房源 / 中文顾问",
        "没时间到现场，可约视频实拍代看",
        "支持同区多套对比，快速筛选",
    ),
    (
        "实拍房源 / 中文顾问",
        "可预约实地看房 / 视频看房",
        "想看这套，直接私信顾问",
    ),
)


def _first_non_empty(*values: str) -> str:
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _clean_caption_text(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for token in DISPLAY_NOISE_TOKENS:
        text = text.replace(token, " ")
    text = re.sub(r"^\s*\d{3,4}(?!米)", "", text)
    text = re.sub(r"[#⭐️✨🏠🏡🏢🔥📍💰✅📝☎️]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -｜|·•,，")
    return text


def _brand_font(size: int, *, bold: bool = False):
    bold_paths = [
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
    ]
    regular_paths = [
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
    ]
    for font_path in (bold_paths if bold else regular_paths):
        if os.path.isfile(font_path):
            try:
                return ImageFont.truetype(font_path, size)
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


def _add_detail_brand_logo(image_bytes: bytes, *, variant: int = 1) -> bytes:
    im = Image.open(BytesIO(image_bytes)).convert("RGBA")
    w, h = im.size
    ref = float(min(w, h))
    scale = ref / 1280.0

    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    edge = max(10, int(16 * scale))
    pad_x = max(10, int(15 * scale))
    pad_y = max(7, int(9 * scale))
    logo_w = max(16, int(ref * 0.018))
    logo_h = max(12, int(logo_w * 0.72))
    logo_gap = max(5, int(6 * scale))
    line_gap = max(1, int(2 * scale))
    stroke_w = max(1, int(1.0 * scale))
    font_cn = _brand_font(max(16, min(28, int(ref * 0.020))), bold=True)
    font_en = _brand_font(max(8, min(10, int(ref * 0.008))), bold=False)
    brand_text = "侨联地产"
    sub_text = "QIAO LIAN PROPERTY"
    b_brand = draw.textbbox((0, 0), brand_text, font=font_cn, stroke_width=stroke_w)
    b_sub = draw.textbbox((0, 0), sub_text, font=font_en)
    title_w = b_brand[2] - b_brand[0]
    title_h = b_brand[3] - b_brand[1]
    sub_w = b_sub[2] - b_sub[0]
    sub_h = b_sub[3] - b_sub[1]
    text_w = max(title_w, sub_w)
    text_h = title_h + sub_h + line_gap
    box_w = pad_x * 2 + logo_w + logo_gap + text_w
    box_h = pad_y * 2 + max(logo_h, text_h)
    box = (edge, edge, edge + box_w, edge + box_h)
    im = _apply_frosted_panel(
        im,
        box,
        radius=max(9, int(13 * scale)),
        blur_radius=max(6, int(10 * scale)),
        tint_rgb=(24, 34, 56),
        tint_alpha=96,
        outline=(255, 255, 255, 54),
    )

    center_y = edge + box_h // 2
    icon_x = edge + pad_x
    _draw_house_outline_mark(
        draw,
        x=icon_x,
        y=center_y - logo_h // 2,
        size=logo_w,
        fill=(248, 230, 179, 255),
        shadow=(8, 14, 28, 118),
    )
    text_x = icon_x + logo_w + logo_gap - b_brand[0]
    top_y = center_y - text_h // 2
    title_y = top_y - b_brand[1]
    sub_y = top_y + title_h + line_gap - b_sub[1]
    draw.text(
        (text_x, title_y),
        brand_text,
        font=font_cn,
        fill=(255, 255, 255, 252),
        stroke_width=stroke_w,
        stroke_fill=(6, 11, 24, 120),
    )
    draw.text((text_x, sub_y), sub_text, font=font_en, fill=(230, 236, 247, 220))

    out = Image.alpha_composite(im, overlay).convert("RGB")
    buf = BytesIO()
    buf.name = "detail_logo.jpg"
    out.save(buf, "JPEG", quality=92)
    return buf.getvalue()


def _normalize_price(raw: str) -> str:
    val = str(raw or "").strip()
    if not val:
        return ""
    if "/月" in val:
        return val
    digits = re.sub(r"[^\d]", "", val)
    if digits:
        return f"${digits}/月"
    return val


def _normalize_payment_terms(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    m = re.search(
        r"押\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?\s*[，,/、\s]*付\s*([一二三四五六七八九十两0-9]{1,3})(?:个?月)?",
        text,
    )
    if m:
        return f"押{m.group(1)}付{m.group(2)}"
    m = re.search(r"(?:deposit|押金)\s*[:：]?\s*([0-9]+(?:\.\d+)?)", text, flags=re.I)
    if m:
        n = m.group(1).rstrip("0").rstrip(".")
        return f"押{n}月"
    m = re.search(r"(押[一二三四五六七八九十两0-9][^，。；;\s]{0,8})", text)
    if m:
        return m.group(1)
    return ""


def _normalize_contract_term(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    m = re.search(r"^([一二三四五六七八九十两0-9]{1,3})\s*(年|个月|月)$", text)
    if m:
        return f"{m.group(1)}{m.group(2)}"
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


def _listing_ref_code(row: dict[str, str]) -> str:
    listing_id = str(row.get("listing_id", "") or "").strip()
    if listing_id:
        return listing_id

    raw_id = str(row.get("id", "") or "").strip()
    if raw_id.isdigit():
        return f"l_{raw_id}"

    source_post_id = str(row.get("source_post_id", "") or "").strip()
    if source_post_id.isdigit():
        return f"sp_{source_post_id}"

    seed = "|".join(
        [
            str(row.get("draft_id", "") or "").strip(),
            str(row.get("title", "") or "").strip(),
            str(row.get("area", "") or "").strip(),
            str(row.get("layout", "") or "").strip(),
            str(row.get("price", "") or "").strip(),
        ]
    )
    digest = hashlib.md5(seed.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"ref_{digest}"


def _normalize_line_block(raw: str, fallback_items: list[str] | None = None) -> str:
    text = str(raw or "").strip()
    if not text and fallback_items:
        text = "｜".join([x for x in fallback_items if str(x or "").strip()])
    if not text:
        return "-"
    # allow csv using | ; 、 as separators
    parts = [p.strip() for p in re.split(r"[|；;、]+", text) if p.strip()]
    if not parts:
        return "-"
    return "\n".join([f"• {p}" for p in parts])


def _caption_from_row(row: dict[str, str], text_style: str = "ch1") -> str:
    custom = str(row.get("caption") or "").strip()
    style = str(text_style or "s1").strip().lower()
    if custom and style != "qc":
        return custom[:1024]

    project = _clean_caption_text(_first_non_empty(row.get("project", ""), row.get("title", ""), "房源推荐"))
    layout = _first_non_empty(row.get("layout", ""), row.get("type", ""), "房源")
    area = _first_non_empty(row.get("area", ""), "待确认")
    price = _normalize_price(row.get("price", ""))
    if not price:
        price = "价格私聊"
    size = _first_non_empty(row.get("size", ""), "待确认")
    floor = _first_non_empty(row.get("floor", ""), "待确认")
    available_date = _first_non_empty(row.get("available_date", ""), "实时确认")
    payment_terms = _normalize_payment_terms(
        _first_non_empty(
            row.get("payment_terms", ""),
            row.get("deposit", ""),
            row.get("cost_notes", ""),
        )
    ) or "待确认"
    contract_term = _normalize_contract_term(
        _first_non_empty(
            row.get("contract_term", ""),
            row.get("lease_term", ""),
            row.get("cost_notes", ""),
        )
    ) or "待确认"
    pay_contract_line = f"💳 付款/合同：{payment_terms}｜{contract_term}"

    h_items = _extract_highlight_items(row, limit=3)
    highlights = "\n".join([f"• {x}" for x in h_items])

    d_raw = _first_non_empty(row.get("drawbacks", ""), row.get("cost_notes", ""))
    d_items = [p.strip() for p in re.split(r"[|｜；;、\n]+", d_raw) if p.strip()][:2]
    if not d_items:
        d_items = ["价格和空房状态以实时确认为准"]
    drawbacks = "\n".join([f"• {x}" for x in d_items])

    contact = _first_non_empty(row.get("contact", ""), "@pengqingw")
    furniture = _first_non_empty(row.get("furniture", ""), row.get("feature1", ""), "可咨询确认")
    hashtags = _first_non_empty(row.get("hashtags", ""), row.get("tags", ""), "#金边租房 #柬埔寨租房 #PhnomPenhRent")

    brand = _first_non_empty(row.get("brand", ""), "侨联地产")
    ref_line = f"🆔 房源编号：{_listing_ref_code(row)}"
    if style not in {"ch1", "qc", "s1", "s2", "s3", "s4", "s5"}:
        style = "ch1"

    if style == "ch1":
        price_digits = re.sub(r"[^\d]", "", price)
        price_line = f"💰 ${price_digits}/月" if price_digits else f"💰 {price}"
        caption = (
            f"🏠 {brand}实拍｜{area}｜{layout}\n\n"
            f"{price_line}\n"
            f"📌 房源：{project}\n"
            f"✨ 核心亮点：{h_items[0]}｜{h_items[1]}｜{h_items[2]}\n"
            f"{pay_contract_line}\n"
            f"{ref_line}\n\n"
            f"👀 支持实地看房 / 视频代看\n"
            f"💬 咨询顾问：{contact}\n\n"
            f"{brand}｜中文顾问带看"
        )
    elif style == "qc":
        def _money_only(raw: str) -> str:
            if not raw:
                return ""
            m = re.search(r"(\$?\s*[0-9][0-9,]*)", str(raw))
            if not m:
                return ""
            token = m.group(1).replace(" ", "")
            return token if token.startswith("$") else f"${token}"

        price_now = _money_only(price)
        price_old = _money_only(
            _first_non_empty(
                row.get("price_old", ""),
                row.get("original_price", ""),
                row.get("market_price", ""),
            )
        )
        if price_old and price_old != price_now:
            price_line = f"💰 {price_old} {price_now}"
        else:
            price_line = f"💰 {price_now or price}".replace("/月", "").strip()

        size_piece = str(size or "").strip()
        area_layout = f"{area}｜{layout}"
        if size_piece and size_piece not in {"-", "待确认"}:
            area_layout = f"{area_layout}｜{size_piece}"

        hi_line = "、".join([x for x in h_items if str(x or "").strip()][:3])
        if not hi_line:
            hi_line = "实拍房源、中文顾问、可预约看房"

        caption = (
            f"📍 {area_layout}\n"
            f"{price_line}\n"
            f"✅ 实拍房源｜中文顾问｜可预约看房\n"
            f"亮点：{hi_line}\n"
            f"👉 下方点「咨询这套」"
        )
    elif style == "s2":
        caption = (
            f"🏠 {price}｜{area}｜{layout}\n\n"
            f"📍 {project}\n"
            f"✨ 亮点：{h_items[0]}｜{h_items[1]}｜{h_items[2]}\n"
            f"{pay_contract_line}\n"
            f"📐 {size} ｜ 🏢 {floor}\n"
            f"{ref_line}\n\n"
            f"👉 点击咨询 / 预约看房\n"
            f"💬 人工顾问：{contact}\n\n"
            f"{brand}｜先咨询，再安排看房\n"
            f"{hashtags}"
        )
    elif style == "s3":
        caption = (
            f"📌 {brand}今日精选\n\n"
            f"🏠 {area}｜{layout}\n"
            f"📍 房源：{project}\n"
            f"💰 {price}\n"
            f"{pay_contract_line}\n"
            f"📐 {size} ｜ {floor}\n"
            f"{ref_line}\n\n"
            f"✅ {h_items[0]}\n"
            f"✅ {h_items[1]}\n"
            f"⚠️ {d_items[0]}\n\n"
            f"💬 咨询：{contact}\n"
            f"💎 {brand}｜中文顾问带看\n"
            f"{hashtags}"
        )
    elif style == "s4":
        caption = (
            f"🏠 {brand}实拍｜{area}｜{layout}\n\n"
            f"📍 房源：{project}\n"
            f"💰 {price}\n"
            f"{pay_contract_line}\n"
            f"📐 {size} ｜ {floor}\n"
            f"🪑 {furniture}\n\n"
            f"✅ {h_items[0]}\n"
            f"✅ {h_items[1]}\n"
            f"✅ {h_items[2]}\n\n"
            f"📅 可入住：{available_date}\n"
            f"{ref_line}\n"
            f"💬 咨询/预约：{contact}\n\n"
            f"💎 {brand}｜支持实地 / 视频看房\n"
            f"{hashtags}"
        )
    elif style == "s5":
        caption = (
            f"🏠 {brand}实拍｜{area}｜{layout}\n"
            f"📍 房源：{project}\n"
            f"{ref_line}\n"
            f"💰 {price}\n"
            f"{pay_contract_line}\n"
            f"📐 {size} ｜ {floor}\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"✅ 优势亮点\n"
            f"{highlights}\n\n"
            f"⚠️ 注意事项\n"
            f"{drawbacks}\n\n"
            f"📅 可入住：{available_date}\n"
            f"📹 支持视频看房\n"
            f"💬 人工顾问：{contact}\n\n"
            f"💎 {brand}｜中文顾问带看\n"
            f"{hashtags}"
        )
    else:
        caption = (
            f"🏠 {brand}实拍｜{area}｜{layout}\n\n"
            f"📍 房源：{project}\n"
            f"💰 {price}\n"
            f"{pay_contract_line}\n"
            f"📐 {size} ｜ {floor}\n\n"
            f"✅ 房源亮点\n"
            f"{highlights}\n\n"
            f"📅 可入住：{available_date}\n\n"
            f"{ref_line}\n"
            f"💬 咨询/预约：{contact}\n"
            f"💎 {brand}｜中文顾问带看\n"
            f"{hashtags}"
        )
    raw_ref = _listing_ref_code(row).upper()
    if raw_ref.startswith("QC"):
        qc_tag = raw_ref
    elif raw_ref.startswith("SP_"):
        digits = re.sub(r"\D", "", raw_ref)
        qc_tag = f"QC{digits.zfill(4)}" if digits else "QC0000"
    else:
        digits = re.sub(r"\D", "", str(row.get("source_post_id", "")))
        if digits:
            qc_tag = f"QC{digits.zfill(4)}"
        else:
            digits = re.sub(r"\D", "", raw_ref)
            qc_tag = f"QC{digits.zfill(4)}" if digits else "QC0000"
    default_prefix = f"侨联 #{qc_tag}"

    forced_prefix = _first_non_empty(
        row.get("post_prefix", ""),
        row.get("caption_prefix", ""),
        default_prefix,
    ).strip()
    caption = str(caption or "").strip()
    if forced_prefix:
        lines = caption.splitlines()
        if lines:
            first = lines[0].strip()
            if first.startswith("侨联 #") or re.fullmatch(r"(?:侨联\s*)?#?QC[0-9A-Za-z_-]+", first, flags=re.IGNORECASE):
                lines = lines[1:]
        body = "\n".join(lines).lstrip()
        caption = f"{forced_prefix}\n{body}" if body else forced_prefix

    return caption[:1024]


def _resolve_text_style(row: dict[str, str], requested: str) -> str:
    row_style = _first_non_empty(
        row.get("text_style", ""),
        row.get("caption_style", ""),
        row.get("copy_style", ""),
    ).strip().lower()
    if row_style:
        requested = row_style
    style = str(requested or "ch1").strip().lower()
    if style in {"random3", "rand3"}:
        return random.choice(RANDOM3_STYLES)
    if style in {"ch1", "qc", "s1", "s2", "s3", "s4", "s5"}:
        return style
    return "ch1"


def _resolve_path(base_dir: Path, raw: str) -> Path | None:
    s = str(raw or "").strip()
    if not s:
        return None
    p = Path(s).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def _split_image_values(raw: str) -> list[str]:
    parts = [p.strip() for p in re.split(r"[|｜；;、,\n]+", str(raw or "").strip()) if p.strip()]
    return parts


def _resolve_image_list(base_dir: Path, raw: str) -> list[Path]:
    out: list[Path] = []
    for item in _split_image_values(raw):
        p = _resolve_path(base_dir, item)
        if p:
            out.append(p)
    return out


def _infer_room_type_from_name(path: Path) -> str:
    stem_raw = path.stem.strip()
    stem_lower = stem_raw.lower()
    for room_type, keywords in ROOM_KEYWORDS.items():
        for keyword in keywords:
            if keyword in stem_raw or keyword in stem_lower:
                return room_type
    return "unknown"


def _sort_paths_by_room_type(paths: list[Path]) -> list[Path]:
    indexed = list(enumerate(paths))
    indexed.sort(
        key=lambda item: (
            ROOM_ORDER.get(_infer_room_type_from_name(item[1]), ROOM_ORDER["unknown"]),
            item[0],
        )
    )
    return [path for _, path in indexed]


def _collect_detail_images(row: dict[str, str], csv_dir: Path) -> list[Path]:
    # Keep a deterministic room-by-room order for Excel-driven operations.
    grouped_columns = [
        ("living", "image_living"),
        ("living", "image_living_room"),
        ("kitchen", "image_kitchen"),
        ("dining", "image_dining"),
        ("bedroom", "image_bedroom"),
        ("bedroom", "image_bedroom2"),
        ("bedroom", "image_bedroom3"),
        ("balcony", "image_balcony"),
        ("bathroom", "image_bathroom"),
        ("bathroom", "image_bathroom2"),
        ("exterior", "image_exterior"),
        ("exterior", "image_building"),
    ]
    images: list[Path] = []
    seen: set[str] = set()
    typed_paths: list[tuple[int, int, Path]] = []
    order_seq = 0

    for room_type, col in grouped_columns:
        for p in _resolve_image_list(csv_dir, row.get(col, "")):
            key = str(p)
            if key not in seen:
                seen.add(key)
                typed_paths.append((ROOM_ORDER.get(room_type, ROOM_ORDER["unknown"]), order_seq, p))
                order_seq += 1

    # Generic columns can rely on filename keywords like 客厅/厨房/卧室/阳台/bathroom.
    generic_paths: list[Path] = []
    generic_columns = (
        "images",
        "photo_paths",
        "detail_images",
        "detail_photos",
        "image1",
        "image2",
        "image3",
        "image4",
        "image5",
        "image6",
        "image7",
        "image8",
        "image9",
        "image10",
    )
    for col in generic_columns:
        generic_paths.extend(_resolve_image_list(csv_dir, row.get(col, "")))

    for p in _sort_paths_by_room_type(generic_paths):
        key = str(p)
        if key not in seen:
            seen.add(key)
            typed_paths.append((ROOM_ORDER.get(_infer_room_type_from_name(p), ROOM_ORDER["unknown"]), order_seq, p))
            order_seq += 1

    typed_paths.sort(key=lambda item: (item[0], item[1]))
    images = [path for _, _, path in typed_paths]

    return images


def _safe_slug(raw: str) -> str:
    text = re.sub(r"\s+", "_", str(raw or "").strip())
    text = re.sub(r"[^0-9A-Za-z_\-\u4e00-\u9fff]", "", text)
    return text[:60] or "listing"


def _category_folder_name(row: dict[str, str]) -> str:
    blob = " ".join(
        [
            str(row.get("type", "") or ""),
            str(row.get("property_type", "") or ""),
            str(row.get("layout", "") or ""),
            str(row.get("title", "") or ""),
        ]
    ).lower()
    if any(k in blob for k in ("villa", "别墅", "独栋", "双拼")):
        return "01_别墅"
    if any(k in blob for k in ("townhouse", "排屋")):
        return "02_排屋"
    if any(k in blob for k in ("shophouse", "商铺", "店屋")):
        return "03_商铺"
    if any(k in blob for k in ("studio", "公寓", "apartment", "服务式")):
        return "04_公寓"
    return "99_其他"


def _clean_cover_project_name(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"侨联\s*#?[A-Za-z0-9_-]+", s, flags=re.IGNORECASE):
        return ""
    if re.fullmatch(r"#?ref_[0-9a-f]{6,}", s, flags=re.IGNORECASE):
        return ""
    if re.fullmatch(r"#?QC[0-9A-Za-z_-]+", s, flags=re.IGNORECASE):
        return ""
    s = re.sub(r"^侨联\s*[|：:·-]*\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^[\s◆◇♦◈⬥⬦🔹🔸⭐✨🔥💎\-\|·•!！:：]+", "", s).strip()
    s = re.sub(r"[\s◆◇♦◈⬥⬦🔹🔸⭐✨🔥💎\-\|·•!！:：]+$", "", s).strip()
    # 标题里常见形态：`249永旺一 🇨🇳 Aeon1公寓｜1房｜金边`
    # 封面项目名只保留项目主段，避免把户型/区域整串打进项目名。
    if "｜" in s or "|" in s:
        s = re.split(r"[｜|]", s, maxsplit=1)[0].strip()
    # 去掉前缀编号（如 249/No.249/#249）
    s = re.sub(r"^\s*(?:no\.?\s*)?#?\s*\d{2,6}\s*", "", s, flags=re.IGNORECASE).strip()
    # 去掉国旗等区域指示符 emoji（保留项目文字）
    s = re.sub(r"[\U0001F1E6-\U0001F1FF]{2}", "", s).strip()
    s = re.sub(r"(?:for\s*rent|for\s*sale)\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"(?:出租|出售|招租)$", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip(" -|：:")
    return s


def _truncate_display_text(raw: str, *, max_units: float) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""

    def _units(ch: str) -> float:
        if re.match(r"[\u4e00-\u9fff]", ch):
            return 1.0
        if ch.isdigit() or ch.isalpha():
            return 0.62
        if ch.isspace():
            return 0.30
        return 0.55

    total = 0.0
    out_chars: list[str] = []
    for ch in s:
        u = _units(ch)
        if total + u > max_units:
            break
        out_chars.append(ch)
        total += u
    out = "".join(out_chars).strip()
    if out and out != s:
        out = out.rstrip(" -|：:,，。.") + "..."
    return out or s[: max(1, int(max_units))]


def _clean_cover_layout(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"[|｜/]\s*-\s*$", "", s).strip()
    s = re.sub(r"[-–—]\s*$", "", s).strip()
    return s


def _clean_cover_area(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -|：:")


def _cover_kind_from_row(row: dict[str, str], default_kind: str) -> str:
    # 支持 CSV 每行单独指定封面模板：cover_kind/template_kind/kind 任一字段。
    row_kind = _first_non_empty(
        row.get("cover_kind", ""),
        row.get("template_kind", ""),
        row.get("kind", ""),
    ).strip()
    picked = row_kind or str(default_kind or "").strip()
    picked = picked.lower()
    picked = LEGACY_COVER_KIND_ALIASES.get(picked, picked)
    if picked in {"random3", "rand3"}:
        return random.choice(RANDOM3_COVER_KINDS)
    if picked in COVER_KINDS:
        return picked
    return "right_price_fixed"


def _extract_highlight_items(row: dict[str, str], limit: int = 3) -> list[str]:
    highlights_raw = _first_non_empty(
        row.get("highlights", ""),
        row.get("feature1", ""),
        row.get("feature2", ""),
        row.get("feature3", ""),
    )
    if not highlights_raw:
        items = []
    else:
        items = [p.strip() for p in re.split(r"[|｜；;、\n]+", highlights_raw) if p.strip()]
    if not items:
        items = ["实拍房源", "中文顾问可约看房", "支持视频看房"]
    while len(items) < limit:
        items.append("可咨询确认")
    return items[:limit]


def _render_cover(
    row: dict[str, str],
    csv_dir: Path,
    kind: str,
    out_dir: Path,
    render_template: str,
    cover_w: int = DEFAULT_COVER_W,
    cover_h: int = DEFAULT_COVER_H,
) -> Path:
    if not RENDER_SCRIPT.is_file():
        raise FileNotFoundError(f"missing render script: {RENDER_SCRIPT}")

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_slug(_first_non_empty(row.get("title", ""), row.get("area", ""), row.get("type", "")))
    out_path = out_dir / f"{stem}.jpg"

    project = _first_non_empty(
        _clean_cover_project_name(row.get("project", "")),
        _clean_cover_project_name(row.get("community", "")),
        _clean_cover_project_name(row.get("estate", "")),
        _clean_cover_project_name(row.get("compound", "")),
        _clean_cover_project_name(row.get("title", "")),
        row.get("area", ""),
        "精选房源",
    )
    project = _truncate_display_text(project, max_units=14.5)
    layout = _first_non_empty(
        _clean_cover_layout(row.get("layout", "")),
        _clean_cover_layout(row.get("property_type", "")),
        _clean_cover_layout(row.get("type", "")),
        "公寓",
    )
    area = _first_non_empty(_clean_cover_area(row.get("area", "")), "金边")
    size = _first_non_empty(row.get("size", ""), "-")
    floor = _first_non_empty(row.get("floor", ""), "-")
    price = _normalize_price(row.get("price", ""))
    ref = _listing_ref_code(row)
    payment = _normalize_payment_terms(
        _first_non_empty(
            row.get("payment_terms", ""),
            row.get("deposit", ""),
            row.get("cost_notes", ""),
        )
    ) or "押1付1"
    h_items = _extract_highlight_items(row, limit=3)
    h1, h2, h3 = h_items[0], h_items[1], h_items[2]
    if kind == "right_price_fixed":
        copy_mode = _first_non_empty(row.get("cover_copy_mode", "")).strip().lower()
        if copy_mode != "row":
            v1, v2, v3 = random.choice(FIXED_COVER_COPY_VARIANTS)
            h1, h2, h3 = v1, v2, v3

    cmd = [
        sys.executable,
        str(RENDER_SCRIPT),
        "--kind",
        kind,
        "--project",
        project,
        "--ref",
        ref,
        "--layout",
        layout,
        "--area",
        area,
        "--size",
        size,
        "--floor",
        floor,
        "--price",
        price or "价格私聊",
        "--payment",
        payment,
        "--h1",
        h1,
        "--h2",
        h2,
        "--h3",
        h3,
        "--out",
        str(out_path),
    ]
    if int(cover_w or 0) > 0:
        cmd.extend(["--w", str(int(cover_w))])
    if int(cover_h or 0) > 0:
        cmd.extend(["--h", str(int(cover_h))])
    template_path = str(render_template or "").strip()
    if template_path:
        cmd.extend(["--template", template_path])

    detail_images = _collect_detail_images(row, csv_dir)
    bg = _resolve_path(csv_dir, row.get("bg_image", "")) or (detail_images[0] if detail_images else None)
    if bg and bg.is_file():
        cmd.extend(["--bg-local", str(bg)])

    subprocess.run(cmd, check=True)
    if not out_path.is_file():
        raise FileNotFoundError(f"cover render output missing: {out_path}")
    return out_path


@dataclass
class RowPayload:
    title: str
    caption: str
    images: list[Path]


def _dedupe_image_paths(paths: list[Path], *, check_files: bool) -> list[Path]:
    """Deduplicate by normalized path first, then by file-content hash."""
    out: list[Path] = []
    seen_paths: set[str] = set()
    seen_hashes: set[str] = set()

    for p in paths:
        if not p:
            continue
        try:
            norm = str(p.resolve())
        except Exception:
            norm = str(p)
        if norm in seen_paths:
            continue

        digest = ""
        if check_files and p.is_file():
            try:
                digest = hashlib.sha1(p.read_bytes()).hexdigest()
            except Exception:
                digest = ""
            if digest and digest in seen_hashes:
                continue

        seen_paths.add(norm)
        if digest:
            seen_hashes.add(digest)
        out.append(p)
    return out


def _build_payload(
    row: dict[str, str],
    csv_dir: Path,
    kind: str,
    auto_cover_dir: Path,
    text_style: str,
    render_template: str,
    cover_w: int = DEFAULT_COVER_W,
    cover_h: int = DEFAULT_COVER_H,
    force_render_cover: bool = False,
    check_files: bool = True,
) -> RowPayload:
    title = _first_non_empty(row.get("title", ""), row.get("area", ""), "untitled")
    chosen_text_style = _resolve_text_style(row, text_style)
    row["_resolved_cover_kind"] = kind

    cover = _resolve_path(csv_dir, row.get("image_cover", ""))
    # 当封面模板随机时，默认重渲染，避免沿用 CSV 里老的 image_cover。
    effective_force_render_cover = force_render_cover or str(kind).strip().lower() in RANDOM3_COVER_KINDS
    if check_files and (effective_force_render_cover or not (cover and cover.is_file())):
        cover = _render_cover(
            row,
            csv_dir,
            kind=kind,
            out_dir=auto_cover_dir,
            render_template=render_template,
            cover_w=cover_w,
            cover_h=cover_h,
        )

    detail_images: list[Path] = []
    seen_detail: set[str] = set()

    # Lock album order to cover + image2 + image3 + image4.
    for col in ("image2", "image3", "image4"):
        for p in _resolve_image_list(csv_dir, row.get(col, "")):
            key = str(p)
            if key in seen_detail:
                continue
            seen_detail.add(key)
            detail_images.append(p)
            break

    # Backfill from generic detail pool when some of image2/3/4 are missing.
    if len(detail_images) < 3:
        for p in _collect_detail_images(row, csv_dir):
            key = str(p)
            if key in seen_detail:
                continue
            if cover and key == str(cover):
                continue
            seen_detail.add(key)
            detail_images.append(p)
            if len(detail_images) >= 3:
                break

    image_candidates = [cover, *detail_images[:3]]
    if check_files:
        images = [p for p in image_candidates if p and p.is_file()]
    else:
        images = [p for p in image_candidates if p]
    images = _dedupe_image_paths(images, check_files=check_files)
    # Telegram media group supports 2-10; for single image fallback to one photo.
    if not images:
        if check_files:
            raise FileNotFoundError(f"{title}: no valid image files")
        images = [auto_cover_dir / f"{_safe_slug(title)}.jpg"]

    row["_resolved_text_style"] = chosen_text_style
    return RowPayload(title=title, caption=_caption_from_row(row, text_style=chosen_text_style), images=images[:4])


def _resolve_detail_logo_variant(detail_brand_style: str, idx: int) -> int:
    if detail_brand_style == "v1":
        return 1
    if detail_brand_style == "v2":
        return 2
    if detail_brand_style == "v3":
        return 3
    return ((idx - 1) % 3) + 1


def _prepare_payload_assets(
    payload: RowPayload,
    row: dict[str, str],
    *,
    out_root: Path,
    detail_brand_mark: bool,
    detail_brand_style: str,
) -> Path:
    ref = _listing_ref_code(row).upper()
    category = _category_folder_name(row)
    folder = out_root / category / f"{ref}_{_safe_slug(payload.title)}"
    folder.mkdir(parents=True, exist_ok=True)

    # Clear previous prepared files for this listing to avoid stale leftovers.
    for old in folder.glob("*"):
        if old.is_file():
            try:
                old.unlink()
            except Exception:
                pass

    outputs: list[str] = []
    for idx, path in enumerate(payload.images):
        raw = path.read_bytes()
        if detail_brand_mark and idx > 0:
            logo_variant = _resolve_detail_logo_variant(detail_brand_style, idx)
            raw = _add_detail_brand_logo(raw, variant=logo_variant)

        ext = path.suffix.lower()
        if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
            ext = ".jpg"

        if idx == 0:
            name = f"{idx + 1:02d}_cover{ext}"
        else:
            name = f"{idx + 1:02d}_detail{idx}{ext}"
        out_path = folder / name
        out_path.write_bytes(raw)
        outputs.append(name)

    caption_path = folder / "caption.txt"
    caption_path.write_text(payload.caption, encoding="utf-8")

    meta_path = folder / "meta.txt"
    meta_path.write_text(
        "\n".join(
            [
                f"title={payload.title}",
                f"ref={ref}",
                f"cover_kind={row.get('_resolved_cover_kind', '')}",
                f"text_style={row.get('_resolved_text_style', '')}",
                f"category={category}",
                f"type={row.get('type', '')}",
                f"images={','.join(outputs)}",
            ]
        ),
        encoding="utf-8",
    )
    return folder


async def _send_payload(
    bot: Bot,
    channel_id: str,
    payload: RowPayload,
    *,
    detail_brand_mark: bool = True,
    detail_brand_style: str = "auto",
) -> None:
    parse_mode = (
        ParseMode.HTML
        if re.search(r"</?[a-zA-Z][^>]*>", str(payload.caption or ""))
        else None
    )
    if len(payload.images) == 1:
        with payload.images[0].open("rb") as f:
            await bot.send_photo(
                chat_id=channel_id,
                photo=f,
                caption=payload.caption,
                parse_mode=parse_mode,
            )
        return

    media: list[InputMediaPhoto] = []
    opened: list[BytesIO] = []
    try:
        for idx, path in enumerate(payload.images):
            raw = path.read_bytes()
            if detail_brand_mark and idx > 0:
                logo_variant = _resolve_detail_logo_variant(detail_brand_style, idx)
                raw = _add_detail_brand_logo(raw, variant=logo_variant)
            buf = BytesIO(raw)
            buf.name = path.name
            opened.append(buf)
            if idx == 0:
                media.append(
                    InputMediaPhoto(
                        media=buf,
                        caption=payload.caption,
                        parse_mode=parse_mode,
                    )
                )
            else:
                media.append(InputMediaPhoto(media=buf))
        await bot.send_media_group(chat_id=channel_id, media=media)
    finally:
        for buf in opened:
            try:
                buf.close()
            except Exception:
                pass


def _read_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [{k: str(v or "").strip() for k, v in row.items()} for row in reader]
    return rows


async def _run(args: argparse.Namespace) -> int:
    token = _first_non_empty(args.bot_token, os.getenv("PUBLISHER_BOT_TOKEN", ""), os.getenv("BOT_TOKEN", ""))
    channel_id = _first_non_empty(args.channel_id, os.getenv("CHANNEL_ID", ""))
    if not args.dry_run and not args.prepare_only:
        if os.getenv("CONFIRM_CHANNEL_PUBLISH", "").strip().lower() != "yes":
            print("ERROR: Direct channel publish requires CONFIRM_CHANNEL_PUBLISH=yes env var.")
            print("This prevents accidental bulk publishing. Set it explicitly if you are sure.")
            sys.exit(1)
        if not token:
            raise RuntimeError("missing bot token: set --bot-token or PUBLISHER_BOT_TOKEN")
        if not channel_id:
            raise RuntimeError("missing channel id: set --channel-id or CHANNEL_ID")

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(f"csv not found: {csv_path}")

    rows = _read_rows(csv_path)
    if args.limit > 0:
        rows = rows[: args.limit]
    if not rows:
        print("no rows in csv")
        return 0

    csv_dir = csv_path.parent
    auto_cover_dir = Path(args.auto_cover_dir).expanduser().resolve()
    prepared_dir = Path(args.prepared_dir).expanduser().resolve()

    bot = None if (args.dry_run or args.prepare_only) else Bot(token=token)
    sent = 0
    skipped = 0
    prepared = 0
    for i, row in enumerate(rows, start=1):
        row_kind = _cover_kind_from_row(row, args.kind)
        try:
            payload = _build_payload(
                row,
                csv_dir,
                kind=row_kind,
                auto_cover_dir=auto_cover_dir,
                text_style=args.text_style,
                render_template=args.render_template,
                cover_w=args.cover_w,
                cover_h=args.cover_h,
                force_render_cover=args.force_render_cover,
                check_files=True,
            )
        except Exception as e:
            skipped += 1
            print(f"[skip {i}] {row.get('title','(untitled)')}: build failed: {e}")
            continue

        min_images = max(1, int(getattr(args, "min_images", 1) or 1))
        if len(payload.images) < min_images:
            skipped += 1
            print(
                f"[skip {i}] {payload.title}: images={len(payload.images)} < min_images={min_images}"
            )
            continue

        if args.dry_run:
            print(
                f"[dry-run {i}] {payload.title} | images={len(payload.images)}"
                f" | style={row.get('_resolved_text_style','ch1')}"
                f" | cover={row.get('_resolved_cover_kind','right_price')}"
            )
            continue

        if args.prepare_only:
            try:
                out_folder = _prepare_payload_assets(
                    payload,
                    row,
                    out_root=prepared_dir,
                    detail_brand_mark=args.detail_brand_mark == "on",
                    detail_brand_style=args.detail_brand_style,
                )
                prepared += 1
                print(
                    f"[prepared {i}] {payload.title}"
                    f" | images={len(payload.images)}"
                    f" | out={out_folder}"
                )
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as e:
                skipped += 1
                print(f"[skip {i}] {payload.title}: prepare failed: {e}")
            continue

        try:
            await _send_payload(
                bot,
                channel_id,
                payload,
                detail_brand_mark=args.detail_brand_mark == "on",
                detail_brand_style=args.detail_brand_style,
            )
            sent += 1
            print(
                f"[sent {i}] {payload.title}"
                f" | style={row.get('_resolved_text_style','ch1')}"
                f" | cover={row.get('_resolved_cover_kind','right_price')}"
            )
            if args.sleep > 0:
                time.sleep(args.sleep)
        except RetryAfter as e:
            wait_s = int(getattr(e, "retry_after", 5) or 5)
            print(f"[retry {i}] flood wait {wait_s}s")
            time.sleep(wait_s + 1)
            await _send_payload(
                bot,
                channel_id,
                payload,
                detail_brand_mark=args.detail_brand_mark == "on",
                detail_brand_style=args.detail_brand_style,
            )
            sent += 1
            print(
                f"[sent {i}] {payload.title} (after retry)"
                f" | style={row.get('_resolved_text_style','ch1')}"
                f" | cover={row.get('_resolved_cover_kind','right_price')}"
            )
        except TelegramError as e:
            skipped += 1
            print(f"[skip {i}] {payload.title}: telegram error: {e}")
        except Exception as e:
            skipped += 1
            print(f"[skip {i}] {payload.title}: unexpected: {e}")

    if args.prepare_only:
        print(f"done prepared={prepared} skipped={skipped} total={len(rows)}")
    else:
        print(f"done sent={sent} skipped={skipped} total={len(rows)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Publish house listings from CSV to Telegram channel.")
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--bot-token", default="")
    ap.add_argument("--channel-id", default="")
    ap.add_argument(
        "--kind",
        choices=(*COVER_KINDS, "random3", "rand3"),
        default="right_price_fixed",
    )
    ap.add_argument(
        "--text-style",
        choices=("ch1", "qc", "s1", "s2", "s3", "s4", "s5", "random3", "rand3"),
        default="qc",
        help="Caption style. Use random3/rand3 to randomly choose among s1/s2/s3 per row.",
    )
    ap.add_argument("--auto-cover-dir", default=str(DEFAULT_RENDERS))
    ap.add_argument(
        "--prepared-dir",
        default=str(DEFAULT_PREPARED),
        help="Output folder for --prepare-only mode (cover/detail images + caption).",
    )
    ap.add_argument(
        "--render-template",
        default="",
        help="Optional custom HTML render template path for cover generation (supports {{PROJECT}} etc placeholders).",
    )
    ap.add_argument(
        "--force-render-cover",
        action="store_true",
        help="Always regenerate cover from template, ignoring existing image_cover file.",
    )
    ap.add_argument(
        "--cover-w",
        type=int,
        default=DEFAULT_COVER_W,
        help="Rendered cover width in px (default: 800).",
    )
    ap.add_argument(
        "--cover-h",
        type=int,
        default=DEFAULT_COVER_H,
        help="Rendered cover height in px (default: 600).",
    )
    ap.add_argument(
        "--detail-brand-mark",
        choices=("on", "off"),
        default="on",
        help="Apply Qiao Lian brand panel on album detail photos (image2/image3/image4).",
    )
    ap.add_argument(
        "--detail-brand-style",
        choices=("auto", "v1", "v2", "v3"),
        default="auto",
        help="Logo style for detail photos: auto rotates 3 styles across image2/3/4.",
    )
    ap.add_argument("--limit", type=int, default=0, help="0 means all rows")
    ap.add_argument(
        "--min-images",
        type=int,
        default=1,
        help="Minimum number of images required to publish/prepare this row.",
    )
    ap.add_argument("--sleep", type=float, default=1.6, help="Seconds between posts")
    ap.add_argument(
        "--prepare-only",
        action="store_true",
        help="Build rendered assets and caption files only; do not send to Telegram.",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
