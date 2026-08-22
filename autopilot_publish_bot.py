#!/usr/bin/env python3
"""
侨联「自动发帖助手」Bot — 管理员专用。

注意：
  - 这是 v2 发布 Bot 内部复用的兼容 helper，不再作为独立 systemd 服务运行
  - 生产长轮询入口当前只保留 qiaolian-publisher-bot.service

职责：
  - 按配置时点从 ready 队列发房源（meihua_publisher）
  - /pending 预览 + 按钮
  - 管理员可改发送时段、每日固定广播、单次发帖、频道置顶菜单
  - 文案模版（每日广播用）

环境变量（整站只读一份项目根 .env）：
  PUBLISHER_BOT_TOKEN   发帖 HTTP（meihua）及默认轮询 Token（未设 AUTOPILOT 时）
  AUTOPILOT_BOT_TOKEN   可选；不设则等于发布 Bot。**同一 Token 在 Telegram 只能被一个进程 long poll**
  ADMIN_IDS / DB_PATH / CHANNEL_ID / USER_BOT_USERNAME
  PUBLISH_SLOTS / AUTOPILOT_TIMEZONE / BRAND_NAME

发布策略：生产只保留 v2（qiaolian-publisher-bot）。本文件仅用于兼容与本地调试；若单独运行，务必确保不会和生产服务抢同一个 Token。
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import io
import json
import logging
import os
import re
import sqlite3
import sys
from time import monotonic
from datetime import datetime, time
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from zoneinfo import ZoneInfo

from v2.qiaolian_publisher_v2.keyboards import admin_menu
from review_queue_view import display_title, pending_overview

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("autopilot_publish_bot")

BOT_TOKEN = (
    os.getenv("AUTOPILOT_BOT_TOKEN", "").strip()
    or os.getenv("PUBLISHER_BOT_TOKEN", os.getenv("BOT_TOKEN", "")).strip()
)
def _resolve_db_path() -> str:
    raw = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db").strip() or "data/qiaolian_dual_bot.db"
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    return str(p)


DB_PATH = _resolve_db_path()
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
TZ_NAME = os.getenv("AUTOPILOT_TIMEZONE", "Asia/Phnom_Penh")
TZ = ZoneInfo(TZ_NAME)
SLOTS_RAW = os.getenv("PUBLISH_SLOTS", "09:00,12:00,15:00,20:00")
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
USER_BOT_USERNAME = os.getenv("USER_BOT_USERNAME", "").strip().lstrip("@")
DEEPLINK_BOT_USERNAME = (
    os.getenv("DEEPLINK_BOT_USERNAME", "").strip().lstrip("@")
    or USER_BOT_USERNAME
    or os.getenv("PUBLISHER_BOT_USERNAME", "").strip().lstrip("@")
)
BRAND_NAME = os.getenv("BRAND_NAME", "侨联地产")
ADVISOR_TG = os.getenv("ADVISOR_TG", "@pengqingw").strip()
PREVIEW_MIN_SCORE = int(os.getenv("PREVIEW_MIN_SCORE", "60"))

# Avoid flooding the admin chat when a scheduler/callback retries the same failed
# draft several times in a short window. The failure remains in publish_logs.
_FAILURE_NOTICE_TTL_SECONDS = 120.0
_failure_notice_cache: dict[tuple[int, str], float] = {}


async def _notify_publish_failure_once(context: ContextTypes.DEFAULT_TYPE, chat_id: int, draft_id: str) -> None:
    key = (int(chat_id), str(draft_id))
    now = monotonic()
    previous = _failure_notice_cache.get(key, 0.0)
    if now - previous < _FAILURE_NOTICE_TTL_SECONDS:
        logger.info("suppressed duplicate publish failure notice: chat=%s draft=%s", chat_id, draft_id)
        return
    _failure_notice_cache[key] = now
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "❌ 这套房源暂时没有发布成功。\n"
            "系统没有再次发送，避免重复发帖。\n"
            "请点“检查问题”查看原因；确认无误后再从“发布房源”继续。"
        ),
    )

# bot_settings keys
KEY_SLOTS = "publish_slots"
KEY_DAILY_TIME = "daily_broadcast_time"
KEY_DAILY_TEXT = "daily_broadcast_html"
KEY_DAILY_ON = "daily_broadcast_enabled"
KEY_PIN_TEXT = "channel_pin_html"


def _direct_publish_enabled() -> bool:
    # 生产只允许 approved frozen package 正式路径；兼容 helper 默认禁止直接发布。
    return os.getenv("AUTOPILOT_DIRECT_PUBLISH_ENABLED", "no").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

# 每日广播预设模版（编号 -> (标题, HTML 片段)）
DAILY_TEMPLATES: dict[int, tuple[str, str]] = {
    1: (
        "早间房源提示",
        f"<b>{BRAND_NAME} 今日实拍房源已更新</b>\n"
        "📸 全部实拍直发，编号可追溯\n"
        "📍 金边华人租房 · 中文顾问 · 可约看房\n"
        "点下方按钮按区域或预算直接找。"
    ),
    2: (
        "品牌定位说明",
        f"<b>{BRAND_NAME} · 您在金边的自己人</b>\n"
        "看对房 · 签约稳 · 入住顺\n\n"
        "我们做三件事：\n"
        "• 实拍先行，帖内费用透明\n"
        "• 中文顾问带看，从咨询到入住不断档\n"
        "• 押付水电等隐性项签前说清楚"
    ),
    3: (
        "看房准备建议",
        "<b>看房前建议先想清楚 3 件事</b>\n"
        "💰 预算区间（含水电物业，不只租金）\n"
        "📍 意向区域（通勤 / 生活圈优先）\n"
        "📅 可入住时间（越具体越好锁房）\n\n"
        "发给顾问后，中文跟进，实拍匹配。"
    ),
    4: (
        "周末值班通知",
        "<b>周末正常值班，实地 / 视频代看均可约</b>\n"
        "发区域 + 预算 → 顾问帮你收窄 1–3 套\n"
        "当天预约当天看，决策更快。"
    ),
    5: (
        "服务亮点说明",
        f"<b>{BRAND_NAME} 服务亮点</b>\n"
        "📷 实拍房源，所见即实况\n"
        "📋 费用透明：水电押付物业提前标注\n"
        "🧑‍💼 中文顾问：预约、签约、入住、报修全程跟\n"
        "📹 视频代看：不到场也能清楚看房"
    ),
}



def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _resolve_admin_draft_id(raw_id: str) -> str | None:
    """管理员只输入 QC 编号；内部仍解析到 draft_id。旧 DRF 仅作兼容，不再展示。"""
    raw = str(raw_id or "").strip()
    if not raw:
        return None
    with _conn() as c:
        if re.fullmatch(r"(?i)qc[-_]?\d+", raw):
            n = int(re.search(r"\d+", raw).group(0))
            internal = f"l_{n}"
            row = c.execute("SELECT draft_id FROM drafts WHERE listing_id=? ORDER BY id DESC LIMIT 1", (internal,)).fetchone()
            if row:
                return str(row[0])
            row = c.execute("SELECT draft_id FROM drafts WHERE id=? LIMIT 1", (n,)).fetchone()
            return str(row[0]) if row else None
        if re.fullmatch(r"(?i)l[-_]?\d+", raw):
            n = int(re.search(r"\d+", raw).group(0))
            row = c.execute("SELECT draft_id FROM drafts WHERE listing_id=? ORDER BY id DESC LIMIT 1", (f"l_{n}",)).fetchone()
            return str(row[0]) if row else None
        if re.fullmatch(r"DRF_[A-Za-z0-9-]+", raw):
            row = c.execute("SELECT draft_id FROM drafts WHERE draft_id=? LIMIT 1", (raw,)).fetchone()
            return str(row[0]) if row else None
    return None

def _admin_qc_for_draft(draft_id: str) -> str:
    with _conn() as c:
        row = c.execute("SELECT listing_id,id FROM drafts WHERE draft_id=? LIMIT 1", (draft_id,)).fetchone()
    value = str(row[0] or "") if row else ""
    m = re.fullmatch(r"(?i)l[_-]?(\d+)", value)
    if not m and row:
        m = re.fullmatch(r"\d+", str(row[1] or ""))
    return f"QC{int(m.group(1)):04d}" if m else "QC-"

def _table_columns(table_name: str) -> set[str]:
    try:
        with _conn() as c:
            rows = c.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(r["name"]) for r in rows}
    except Exception:
        logger.exception("read table columns failed: %s", table_name)
        return set()


def _ensure_collect_sources_table() -> None:
    with _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS collect_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL UNIQUE,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT,
                fetch_mode TEXT DEFAULT 'manual',
                fetch_rule_json TEXT,
                is_enabled INTEGER DEFAULT 1,
                last_fetched_at TEXT,
                remark TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        c.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_collect_sources_source_key ON collect_sources (source_key)"
        )
        c.commit()


def _ensure_default_collect_source() -> None:
    _ensure_collect_sources_table()
    with _conn() as c:
        c.execute(
            """
            INSERT OR IGNORE INTO collect_sources (
                source_key, source_name, source_type, source_url, fetch_mode, is_enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            ("zufang555", "zufang555 频道", "telethon", "https://t.me/zufang555", "channel"),
        )
        c.commit()


def _get_setting(key: str, default: str = "") -> str:
    try:
        with _conn() as c:
            r = c.execute(
                "SELECT setting_value FROM bot_settings WHERE setting_key=?",
                (key,),
            ).fetchone()
        if r and r["setting_value"] is not None:
            return str(r["setting_value"])
    except Exception:
        logger.exception("read setting %s", key)
    return default


def _set_setting(key: str, value: str) -> None:
    with _conn() as c:
        c.execute(
            """INSERT INTO bot_settings (setting_key, setting_value, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(setting_key) DO UPDATE SET
                 setting_value=excluded.setting_value,
                 updated_at=CURRENT_TIMESTAMP""",
            (key, value),
        )
        c.commit()


def _slots_raw_effective() -> str:
    v = _get_setting(KEY_SLOTS, "").strip()
    return v if v else SLOTS_RAW


def _parse_slots_from_raw(raw: str) -> list[time]:
    out: list[time] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^(\d{1,2}):(\d{2})$", part)
        if not m:
            continue
        h, mi = int(m.group(1)), int(m.group(2))
        if h > 23 or mi > 59:
            continue
        out.append(time(h, mi, tzinfo=TZ))
    return out or [
        time(9, 0, tzinfo=TZ),
        time(12, 0, tzinfo=TZ),
        time(15, 0, tzinfo=TZ),
        time(20, 0, tzinfo=TZ),
    ]


def _parse_hhmm(s: str) -> tuple[int, int] | None:
    m = re.match(r"^(\d{1,2}):(\d{2})$", s.strip())
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 23 or mi > 59:
        return None
    return h, mi


def _is_admin(uid: int | None) -> bool:
    return uid is not None and uid in ADMIN_IDS


def _extract_wechat_note_fields(raw_text: str) -> dict:
    """Extract explicit intake facts for the admin summary without inventing data."""
    text = (raw_text or "").replace("\u00a0", " ").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    title = lines[0] if lines else "微信笔记房源"

    def _pick(patterns: list[str]) -> str:
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
            if match:
                return str(match.group(1) or "").strip()
        return ""

    project = _pick([r"(?:项目|小区|楼盘)\s*[:：]\s*([^\n]+)"])
    raw_area = _pick([
        r"(?:位置|地址|区域|地段)\s*[:：]\s*([^\n]+)",
        r"(钻石岛|俄罗斯市场|俄市|BKK\s*[123]|万景岗\s*[123]|堆谷|TK|森速|Sen\s*Sok|新金边|富力城|炳发城|一号路|洪森大道)",
    ])
    try:
        from qiaolian_dual.area_normalization import normalize_area
        normalized_area = normalize_area(raw_area, text)
    except Exception:
        normalized_area = ""

    layout_line = _pick([r"(?:房间数量|户型|房型)\s*[:：]\s*([^\n]+)"])
    layout = _pick([
        r"(?<!\d)(\d{1,2}\s*\+\s*\d{1,2}\s*房\s*\d{1,2}\s*(?:厅|卫))",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*厅\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*\+\s*\d{1,2}\s*房)",
        r"(?<!\d)(\d{1,2}\s*房\s*\d{1,2}\s*卫)",
        r"(?<!\d)(\d{1,2}\s*房)",
    ])
    if not layout and layout_line:
        layout = layout_line[:30]
    floor = _pick([r"(?:楼层|层楼|楼层情况)\s*[:：]\s*([0-9]{1,3}(?:\s*/\s*[0-9]{1,3})?)"])
    price = _pick([
        r"(?:房间价格|租金|月租|价格)\s*[:：]\s*\$?\s*([0-9][0-9,]{2,})",
        r"([0-9][0-9,]{2,})\s*(?:美金|美元|\$)\s*(?:/\s*月|每月)?",
        r"\$\s*([0-9][0-9,]{2,})\s*(?:/\s*月|每月)?",
    ]).replace(",", "")
    payment_terms = _pick([
        r"(?:押金情况|押金|押付|付款)\s*[:：]\s*(押\s*[一二三四五六七八九十两0-9]+\s*付\s*[一二三四五六七八九十两0-9]+)",
        r"(押\s*[一二三四五六七八九十两0-9]+\s*付\s*[一二三四五六七八九十两0-9]+)",
    ])
    contract_term = _pick([
        r"(?:合同情况|签约合同|合同|租期)\s*[:：]\s*([0-9一二三四五六七八九十两]+\s*(?:年|个月|月))",
        r"([0-9]+\s*(?:year|years|month|months))",
    ])
    contact = _pick([r"(?:飞机|telegram|tg)\s*[:：]\s*(@[A-Za-z0-9_]+)", r"(?:微信|wechat)\s*[:：]\s*([A-Za-z0-9_]+)", r"(?:电话|phone)\s*[:：]\s*([+0-9]{6,})"])

    lower = text.lower()
    if "独栋" in text or "独立别墅" in text:
        prop = "独栋别墅"
    elif "双拼" in text or "twin villa" in lower:
        prop = "双拼别墅"
    elif "联排" in text or "排屋" in text or "townhouse" in lower or "link villa" in lower:
        prop = "联排别墅"
    elif "别墅" in text or "villa" in lower:
        prop = "别墅"
    elif "商铺" in text or "shophouse" in lower:
        prop = "商铺"
    elif "写字楼" in text or "office" in lower:
        prop = "写字楼"
    else:
        prop = "公寓"

    return {
        "title": title,
        "project": project,
        "area": normalized_area or raw_area,
        "normalized_area": normalized_area or None,
        "layout": re.sub(r"\s+", "", layout),
        "floor": floor,
        "property_type": prop,
        "price": int(price) if price.isdigit() else None,
        "payment_terms": re.sub(r"\s+", "", payment_terms),
        "contract_term": re.sub(r"\s+", "", contract_term),
        "contact": contact,
    }


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _draft_row_by_pk(did: int) -> sqlite3.Row | None:
    with _conn() as c:
        return c.execute("SELECT * FROM drafts WHERE id=?", (did,)).fetchone()


def _draft_to_caption_dict(row: sqlite3.Row) -> dict:
    """Return the only safe display projection for admin previews.

    Legacy draft columns may contain polluted titles/project/area values.
    Preview must use normalized canonical facts exactly like package building.
    """
    import json

    d = dict(row)
    for f in ("highlights", "drawbacks"):
        if isinstance(d.get(f), str):
            try:
                d[f] = json.loads(d[f])
            except Exception:
                d[f] = []
    try:
        facts = json.loads(d.get("normalized_data") or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        facts = {}
    if isinstance(facts, dict) and facts:
        try:
            from qiaolian_dual.canonical_facts import draft_projection
            projection = draft_projection(facts)
            d.update({
                "title": projection.get("title", ""),
                "project": projection.get("project", ""),
                "community": projection.get("community", ""),
                "area": projection.get("public_location_display", projection.get("area", "")),
                "property_type": projection.get("property_type", ""),
                "price": projection.get("price", ""),
                "original_price": projection.get("original_price", ""),
                "layout": projection.get("layout", ""),
                "size": projection.get("size", ""),
                "size_sqm": projection.get("size_sqm", facts.get("size_sqm")),
                "floor": projection.get("floor", ""),
                "deposit": projection.get("deposit", ""),
                "deposit_rule": projection.get("deposit", ""),
                "payment_terms": projection.get("payment_terms", ""),
                "contract_term": projection.get("contract_term", ""),
                "available_date": projection.get("available_date", ""),
                "management_fee": projection.get("management_fee", ""),
                "internet_fee": projection.get("internet_fee", ""),
                "water_rate": projection.get("water_rate", ""),
                "electric_rate": projection.get("electric_rate", ""),
                "parking_fee": projection.get("parking_fee", ""),
                "viewing_time": projection.get("viewing_time", ""),
                "video_viewing": projection.get("video_viewing", ""),
                "cost_notes": projection.get("cost_notes", ""),
                "advisor_comment": "",
                "drawbacks": [],
                "highlights": projection.get("highlights", []),
                "project_name": facts.get("project_name", ""),
                "project_alias": facts.get("project_alias", ""),
            })
        except Exception:
            logger.exception("canonical preview projection failed for %s", d.get("draft_id"))
    return d


def _formal_preview_cover(row: sqlite3.Row) -> str | None:
    """Render the same canonical cover as the package, without writing a package."""
    try:
        from publication_package import render_cover_preview
        render_dir = BASE_DIR / "media" / "renders" / "runtime"
        render_dir.mkdir(parents=True, exist_ok=True)
        draft_id = str(row["draft_id"])
        output = render_dir / f"admin_preview_cover_{draft_id}.png"
        # Caller runs this helper in a worker thread.
        render_cover_preview(DB_PATH, draft_id, str(output))
        return str(output) if output.is_file() else None
    except Exception:
        logger.exception("formal admin preview cover failed for %s", row["draft_id"] if row else "-")
        return None


def _cover_path_for_draft(row: sqlite3.Row) -> str | None:
    cid = row["cover_asset_id"]
    if not cid:
        return None
    with _conn() as c:
        r = c.execute(
            "SELECT local_path FROM media_assets WHERE id=?", (cid,)
        ).fetchone()
    if not r or not r["local_path"]:
        return None
    p = Path(r["local_path"])
    return str(p) if p.is_file() else None


def _kb_preview(draft_pk: int, selected_variant: str = "a") -> InlineKeyboardMarkup:
    """Simple review keyboard; package/version details stay internal."""
    p = str(draft_pk)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ 确认这套房源", callback_data=f"ap:r:{p}")],
            [
                InlineKeyboardButton("✏️ 补充修改说明", callback_data=f"ap:e:{p}"),
                InlineKeyboardButton("🖼 重新选主图", callback_data=f"ap:c:{p}"),
            ],
        ]
    )


def build_channel_menu_keyboard() -> InlineKeyboardMarkup:
    """频道置顶帖：四个按钮 2×2（按区域｜按预算｜最新房源｜顾问咨询）。"""
    if DEEPLINK_BOT_USERNAME:
        base = f"https://t.me/{DEEPLINK_BOT_USERNAME}?start="
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📍 区域找房", url=f"{base}find_area"),
                    InlineKeyboardButton("💰 预算找房", url=f"{base}find_budget"),
                ],
                [
                    InlineKeyboardButton("🆕 最新房源", url=f"{base}latest"),
                    InlineKeyboardButton("💬 中文顾问", url=f"{base}advisor"),
                ],
            ]
        )
    ch = CHANNEL_ID.replace("-100", "").lstrip("-")
    if ch:
        url = f"https://t.me/c/{ch}"
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"进入 {BRAND_NAME} 频道", url=url)],
            ]
        )
    return InlineKeyboardMarkup([])


def default_pin_html() -> str:
    custom = _get_setting(KEY_PIN_TEXT, "").strip()
    if custom:
        return custom
    # Never expose environment labels such as “测试” in public channel copy.
    public_brand = (BRAND_NAME or "侨联地产").replace("测试", "").strip() or "侨联地产"
    b = html.escape(public_brand)
    return (
        f"<b>🏠 {b}｜金边华人租房</b>\n"
        "实拍房源 · 费用先说 · 中文带看\n\n"
        "看中房源，直接点帖内「咨询」或「预约」，\n"
        "系统会自动带上房源编号。\n\n"
        "👇 也可以按区域或预算开始找"
    )


def channel_index_html() -> str:
    """旧 /post_index 复用唯一置顶文案，避免产生第二套导航口径。"""
    return default_pin_html()


async def cmd_post_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """发布频道索引帖（第二条置顶）。"""
    if not _is_admin(update.effective_user.id):
        return
    if not CHANNEL_ID:
        await update.message.reply_text("未配置 CHANNEL_ID。")
        return
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        await update.effective_message.reply_text("⛔ 当前生产配置已关闭直接发布，未发送频道消息。")
        return
    text = channel_index_html()
    kb = build_channel_menu_keyboard()
    try:
        msg = await context.bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb if kb.inline_keyboard else None,
        )
        try:
            await context.bot.pin_chat_message(chat_id=CHANNEL_ID, message_id=msg.message_id, disable_notification=True)
            note = "已尝试置顶。"
        except Exception as e:
            note = f"发帖成功，置顶失败：{e}"
        await update.message.reply_text(f"频道索引帖已发送。\n{note}")
    except Exception as e:
        logger.exception("cmd_post_index")
        await update.message.reply_text(f"发送失败：{e}")


# ── 命令 ──────────────────────────────────────────────────


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "🏠 <b>侨联发布助手</b>\n\n"
        "发房源只要三步：\n"
        "1. 发文字和图片\n"
        "2. 看预览，缺什么就补什么\n"
        "3. 确认后发布\n\n"
        "直接点下面第一个按钮开始。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        "📘 <b>怎么操作</b>\n\n"
        "导入房源 → 三版预览 → 审核冻结 → 已审核队列发布。\n\n"
        "大多数时候只需要点击下面的按钮。\n"
        "如果发布失败，不要重复点击，先点“检查问题”。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu(),
    )


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    with _conn() as c:
        rows = c.execute(
            "SELECT review_status, COUNT(*) AS n FROM drafts GROUP BY review_status"
        ).fetchall()
    lines = [f"{r['review_status']}: {r['n']}" for r in rows]
    paused = _scheduler_paused()
    daily_on = _get_setting(KEY_DAILY_ON, "0").strip() in ("1", "true", "yes")
    await update.message.reply_text(
        "drafts 统计：\n"
        + "\n".join(lines)
        + f"\n\n房源定时器：{'暂停' if paused else '运行'}"
        + f"\n每日广播：{'开' if daily_on else '关'} {_get_setting(KEY_DAILY_TIME, '未设')}",
    )


# 管理面板与质量门禁共用 review_note 中已有的 quality:... 事实，
# 不在这里重新解析或修改任何草稿/房源数据。
_QUALITY_REASON_LABELS = {
    "missing_area": "位置还没确认",
    "geo_precision_unconfirmed": "请补准确区域",
    "missing_public_location": "缺少公开位置",
    "missing_contract_term": "合同期限未填写",
    "missing_deposit_terms": "押付方式未填写",
    "missing_rental_intent": "租赁意向未确认",
    "missing_price": "缺价格",
    "missing_layout": "缺户型",
    "unknown_property_type": "请确认房屋类型",
    "special_price": "请确认租金",
    # 旧草稿留下的 missing_* 与当前规则中的 warning_* 都不是单独阻止审核的理由；
    # 面板统一按“可补充”展示，避免把非区域问题误导成房源不可用。
    "missing_size": "可补充面积/尺寸",
    "warning_missing_size": "可补充面积/尺寸",
    "missing_highlights": "可补充房源亮点",
    "warning_missing_highlights": "可补充房源亮点",
    "missing_price_manual_consult": "价格需人工确认",
    "missing_rental_intent": "租赁意图待确认",
    "whitelist_core_area": "核心区域识别（非阻塞）",
    "whitelist_known_property": "已识别项目（非阻塞）",
    "package_source_post_has_no_media_assets": "缺已登记源图片",
    "publish_gate_blocked": "发布门禁拦截",
}

# 历史解析器写入这些增强识别标签时，表示已识别而非缺失；不能计入阻塞项。
_NON_BLOCKING_QUALITY_CODES = {"whitelist_core_area", "whitelist_known_property"}


def _quality_codes(review_note: str | None) -> list[str]:
    """只读地取出当前质量评估写入的阻塞码，兼容用 | 串接的历史备注。"""
    note = str(review_note or "")
    match = re.search(r"(?:^|\|)\s*quality:([^|]+)", note)
    if not match:
        return []
    return [item.strip() for item in match.group(1).split(",") if item.strip()]


def _quality_text(codes: list[str]) -> str:
    if not codes:
        return "待人工复核"
    actionable = [code for code in codes if code not in _NON_BLOCKING_QUALITY_CODES]
    if not actionable:
        return "无阻塞项（仅识别标签）"
    return "、".join(_QUALITY_REASON_LABELS.get(code, code) for code in actionable)


async def cmd_quality(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """面板“质量检查”：列出真实阻塞项和可继续处理的草稿，不创建/修改发布包。"""
    if not _is_admin(update.effective_user.id):
        return
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT draft_id, title, review_status, queue_score, review_note,
                          area, price, layout, cover_asset_id, updated_at
                   FROM drafts
                   WHERE review_status IN ('pending', 'ready')
                   ORDER BY CASE review_status WHEN 'ready' THEN 0 ELSE 1 END,
                            COALESCE(queue_score, 0) DESC, id DESC
                   LIMIT 60"""
            ).fetchall()
    except Exception:
        logger.exception("quality panel query failed")
        await update.message.reply_text("❌ 暂时无法读取草稿质量数据，请稍后重试。", reply_markup=admin_menu())
        return

    pending = [row for row in rows if row["review_status"] == "pending"]
    ready = [row for row in rows if row["review_status"] == "ready"]
    reason_counts: dict[str, int] = {}
    informational_counts: dict[str, int] = {}
    no_cover = 0
    for row in pending:
        if not row["cover_asset_id"]:
            no_cover += 1
        for code in _quality_codes(row["review_note"]):
            target = informational_counts if code in _NON_BLOCKING_QUALITY_CODES else reason_counts
            target[code] = target.get(code, 0) + 1

    lines = [
        "🔍 <b>检查问题</b>",
        f"需要处理：<b>{len(pending)}</b> 套 · 已准备审核：<b>{len(ready)}</b>",
    ]
    if not rows:
        lines.append("当前没有待处理房源。")
    elif reason_counts or no_cover:
        lines.append("\n<b>最常见阻塞</b>")
        ranked = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
        for code, count in ranked:
            lines.append(f"• {_QUALITY_REASON_LABELS.get(code, code)}：{count} 套")
        if no_cover:
            lines.append(f"• 尚无审核封面：{no_cover} 套")
    if informational_counts:
        tags = "、".join(
            f"{_QUALITY_REASON_LABELS.get(code, code)} {count} 套"
            for code, count in sorted(informational_counts.items(), key=lambda item: (-item[1], item[0]))
        )
        lines.append(f"\n已识别但不影响处理：{tags}")
    if pending:
        lines.append("\n<b>优先处理（最多 6 套）</b>")
        for row in pending[:6]:
            title = html.escape(str(row["title"] or "（无标题）")[:32])
            issues = html.escape(_quality_text(_quality_codes(row["review_note"])))
            lines.append(
                f"• {title}\n"
                f"  需要处理：{issues}"
            )
        lines.append("\n请点击对应房源卡片继续；不确定时先点“检查问题”。")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_menu())


async def cmd_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """面板“数据仪表盘”：给出运营所需的真实只读总览，不触发发布或采集。"""
    if not _is_admin(update.effective_user.id):
        return
    try:
        with _conn() as c:
            pending = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='pending'").fetchone()[0]
            ready = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='ready'").fetchone()[0]
            approved = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='approved'").fetchone()[0]
            published_today = c.execute(
                """SELECT COUNT(*) FROM posts
                   WHERE publish_status='published'
                     AND date(COALESCE(published_at, updated_at), 'localtime') = date('now', 'localtime')"""
            ).fetchone()[0]
            source_today = c.execute(
                "SELECT COUNT(*) FROM source_posts WHERE date(COALESCE(created_at, fetched_at), 'localtime') = date('now', 'localtime')"
            ).fetchone()[0]
            appointments_today = 0
            if _table_exists(c, "appointments"):
                appointments_today = c.execute(
                    "SELECT COUNT(*) FROM appointments WHERE date(created_at, 'localtime') = date('now', 'localtime')"
                ).fetchone()[0]
    except Exception:
        logger.exception("dashboard panel query failed")
        await update.message.reply_text("❌ 暂时无法读取运营数据，请稍后重试。", reply_markup=admin_menu())
        return

    paused = _scheduler_paused()
    lines = [
        "📊 <b>数据仪表盘（今日）</b>",
        f"新采集：<b>{source_today}</b> · 待审核：<b>{pending}</b>",
        f"审核包 ready：<b>{ready}</b> · 已批准待发：<b>{approved}</b>",
        f"频道已发：<b>{published_today}</b> · 新预约：<b>{appointments_today}</b>",
        f"队列：<b>{'暂停' if paused else '运行'}</b> · 槽位：<code>{html.escape(_slots_raw_effective())}</code>",
        "\n此页只读；发布前请对单套执行 <code>/check QC0001</code>。",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_menu())


def _parse_analytics_days(context: ContextTypes.DEFAULT_TYPE) -> int:
    days = 7
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    return max(1, min(days, 90))


async def cmd_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    days = _parse_analytics_days(context)
    try:
        from analytics.channel_analytics_integrated import QiaolianAnalytics

        analytics = QiaolianAnalytics(db_path=DB_PATH)
        report = analytics.generate_report(days)
        text = analytics.format_telegram_report(report)
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.exception("analytics report failed")
        await update.message.reply_text(f"生成报表失败：{e}")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    with _conn() as c:
        pending = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='pending'").fetchone()[0]
        ready = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='ready'").fetchone()[0]
        published_today = c.execute(
            """SELECT COUNT(*) FROM posts
               WHERE publish_status='published'
                 AND date(COALESCE(published_at, updated_at), 'localtime') = date('now', 'localtime')"""
        ).fetchone()[0]
    paused = _scheduler_paused()
    daily_on = _get_setting(KEY_DAILY_ON, "0").strip() in ("1", "true", "yes")
    daily_time = _get_setting(KEY_DAILY_TIME, "未设")
    await update.message.reply_text(
        "运行状态：\n"
        f"待审核：{pending}\n"
        f"ready：{ready}\n"
        f"今日已发：{published_today}\n"
        f"房源定时：{'暂停' if paused else '运行'}\n"
        f"每日广播：{'开' if daily_on else '关'} {daily_time}\n"
        f"槽位：{_slots_raw_effective()}",
        reply_markup=admin_menu(),
    )


async def cmd_ops(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """一屏运营看板：高频数据集中展示。"""
    if not _is_admin(update.effective_user.id):
        return
    with _conn() as c:
        pending = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='pending'").fetchone()[0]
        ready = c.execute("SELECT COUNT(*) FROM drafts WHERE review_status='ready'").fetchone()[0]
        published_today = c.execute(
            """SELECT COUNT(*) FROM posts
               WHERE publish_status='published'
                 AND date(COALESCE(published_at, updated_at), 'localtime') = date('now', 'localtime')"""
        ).fetchone()[0]
        top_pending = c.execute(
            """SELECT draft_id, title, queue_score
               FROM drafts
               WHERE review_status='pending'
               ORDER BY queue_score DESC, id DESC
               LIMIT 1"""
        ).fetchone()
        next_ready = c.execute(
            """SELECT draft_id, queue_score
               FROM drafts
               WHERE review_status='ready'
               ORDER BY queue_score DESC, id ASC
               LIMIT 1"""
        ).fetchone()

    paused = _scheduler_paused()
    daily_on = _get_setting(KEY_DAILY_ON, "0").strip() in ("1", "true", "yes")
    daily_time = _get_setting(KEY_DAILY_TIME, "未设")
    slots = _slots_raw_effective()
    pending_line = (
        f"<code>{html.escape(top_pending['draft_id'])}</code> "
        f"(score={int(float(top_pending['queue_score'] or 0))})"
    ) if top_pending else "无"
    ready_line = (
        f"<code>{html.escape(next_ready['draft_id'])}</code> "
        f"(score={int(float(next_ready['queue_score'] or 0))})"
    ) if next_ready else "无"
    await update.message.reply_text(
        "⚡ <b>运营一屏总览</b>\n\n"
        f"待审核：<b>{pending}</b>\n"
        f"审核包待批准：<b>{ready}</b>\n"
        f"今日已发：<b>{published_today}</b>\n\n"
        f"队列状态：<b>{'暂停' if paused else '运行'}</b>\n"
        f"每日广播：<b>{'开' if daily_on else '关'}</b>  {html.escape(daily_time)}\n"
        f"发帖时段：<code>{html.escape(slots)}</code>\n\n"
        f"待审核Top：{pending_line}\n"
        f"下一个审核包待批准：{ready_line}\n\n"
        "快捷：<code>/pending</code> <code>/send DRF_xxx</code> <code>/slots 10:30,17:00,21:30</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu(),
    )


async def cmd_sources(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """查看采集源状态，方便排查“怎么没新房源”。"""
    if not _is_admin(update.effective_user.id):
        return
    _ensure_collect_sources_table()
    _ensure_default_collect_source()
    lines = ["📚 <b>采集源状态</b>"]
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT id, source_key, source_name, source_type, fetch_mode, is_enabled, last_fetched_at
                   FROM collect_sources
                   ORDER BY is_enabled DESC, updated_at DESC, id DESC
                   LIMIT 12"""
            ).fetchall()
        if rows:
            for r in rows:
                state = "🟢" if int(r["is_enabled"] or 0) else "⚪️"
                fetched = (r["last_fetched_at"] or "未采集")[:16]
                lines.append(
                    f"{state} <b>{html.escape(r['source_name'] or '-')}</b>"
                    f" ({html.escape(r['source_type'] or '-')}/{html.escape(r['fetch_mode'] or '-')})"
                    f"\nkey: <code>{html.escape(r['source_key'] or '-')}</code>  id: {int(r['id'] or 0)}"
                    f"\n最后：{html.escape(fetched)}"
                )
        else:
            lines.append("暂无采集源记录。\n可用：<code>/source_add 频道名 telethon channel https://t.me/xxx</code>")
    except Exception as e:
        lines.append(f"读取采集源失败：{html.escape(str(e))}")
    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_menu())


def _slug_source_key(name: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "_", name.strip().lower()).strip("_")
    return key[:48] or f"source_{int(datetime.now().timestamp())}"


async def cmd_source_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _ensure_collect_sources_table()
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "用法：<code>/source_add <name> <type> [mode] [url]</code>\n"
            "示例：<code>/source_add zufang999 telethon channel https://t.me/zufang999</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )
        return
    source_name = args[0].strip()
    source_type = args[1].strip()
    fetch_mode = (args[2].strip() if len(args) >= 3 else "manual")
    source_url = (args[3].strip() if len(args) >= 4 else "")
    source_key = _slug_source_key(source_name)
    try:
        with _conn() as c:
            c.execute(
                """
                INSERT INTO collect_sources (
                    source_key, source_name, source_type, source_url, fetch_mode, is_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(source_key) DO UPDATE SET
                    source_name=excluded.source_name,
                    source_type=excluded.source_type,
                    source_url=excluded.source_url,
                    fetch_mode=excluded.fetch_mode,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (source_key, source_name, source_type, source_url, fetch_mode),
            )
            c.commit()
        await update.message.reply_text(
            "✅ 采集源已保存\n"
            f"name: <b>{html.escape(source_name)}</b>\n"
            f"key: <code>{html.escape(source_key)}</code>\n"
            f"type/mode: {html.escape(source_type)}/{html.escape(fetch_mode)}",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )
    except Exception as e:
        logger.exception("source_add failed")
        await update.message.reply_text(f"新增采集源失败：{html.escape(str(e))}", parse_mode=ParseMode.HTML)


async def _set_source_enabled(update: Update, context: ContextTypes.DEFAULT_TYPE, enabled: bool) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _ensure_collect_sources_table()
    args = context.args or []
    if not args:
        cmd = "/source_on <key>" if enabled else "/source_off <key>"
        await update.message.reply_text(f"用法：<code>{cmd}</code>", parse_mode=ParseMode.HTML)
        return
    key = args[0].strip()
    with _conn() as c:
        cur = c.execute(
            "UPDATE collect_sources SET is_enabled=?, updated_at=CURRENT_TIMESTAMP WHERE source_key=?",
            (1 if enabled else 0, key),
        )
        c.commit()
    if cur.rowcount <= 0:
        await update.message.reply_text(f"未找到采集源：<code>{html.escape(key)}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(
            f"{'🟢 已启用' if enabled else '⚪️ 已停用'}：<code>{html.escape(key)}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )


async def cmd_source_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_source_enabled(update, context, True)


async def cmd_source_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _set_source_enabled(update, context, False)


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """查看最近发帖记录与失败原因。"""
    if not _is_admin(update.effective_user.id):
        return
    out = ["🧾 <b>最近发布日志</b>"]
    try:
        post_cols = _table_columns("posts")
        time_expr = "COALESCE(published_at, updated_at)"
        if "created_at" in post_cols:
            time_expr = "COALESCE(published_at, updated_at, created_at)"
        with _conn() as c:
            posts = c.execute(
                f"""SELECT draft_id, listing_id, publish_status,
                          {time_expr} AS t
                   FROM posts
                   ORDER BY id DESC
                   LIMIT 6"""
            ).fetchall()
            log_cols = _table_columns("publish_logs")
            fail_time_expr = "created_at" if "created_at" in log_cols else "id"
            fails = c.execute(
                f"""SELECT draft_id, status, error_message, {fail_time_expr} AS t
                   FROM publish_logs
                   WHERE COALESCE(status, '') NOT IN ('success', 'ok', 'published')
                   ORDER BY id DESC
                   LIMIT 3"""
            ).fetchall()
        if posts:
            out.append("✅ <b>最近已处理</b>")
            for p in posts:
                t = (p["t"] or "")[:16]
                out.append(
                    f"• <code>{html.escape(p['draft_id'] or '-')}</code> "
                    f"{html.escape(str(p['listing_id'] or '-'))} "
                    f"[{html.escape(p['publish_status'] or '-')}] {html.escape(t)}"
                )
        else:
            out.append("✅ 最近无发布记录。")
        if fails:
            out.append("\n❌ <b>最近失败</b>")
            for f in fails:
                msg = (f["error_message"] or "未知错误").replace("\n", " ")
                if len(msg) > 90:
                    msg = msg[:90] + "…"
                out.append(
                    f"• <code>{html.escape(f['draft_id'] or '-')}</code> "
                    f"[{html.escape(f['status'] or '-')}] {html.escape(msg)} "
                    f"{html.escape(str(f['t'] or '')[:16])}"
                )
    except Exception as e:
        out.append(f"读取日志失败：{html.escape(str(e))}")
    await update.message.reply_text("\n".join(out), parse_mode=ParseMode.HTML, reply_markup=admin_menu())


async def cmd_publish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """兼容旧命令：/publish == /send"""
    await cmd_send(update, context)


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Build and approve one selected frozen package; never publish it."""
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.effective_message.reply_text(
            "用法：<code>/approve QC0001 A</code>\n"
            "A=标准信息，B=亮点价格，C=专业参数。省略版本时按物业类型自动选择。\n"
            "更推荐直接点 <code>/pending</code> 中的三版预览按钮。",
            parse_mode=ParseMode.HTML,
        )
        return
    input_id = context.args[0].strip()
    draft_id = _resolve_admin_draft_id(input_id)
    if not draft_id:
        await update.effective_message.reply_text(
            f"❌ 未找到房源：<code>{html.escape(input_id)}</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        from publication_package import approve_package, build_package
        with _conn() as c:
            draft = c.execute("SELECT * FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
            pkg = c.execute(
                "SELECT package_id, package_version, status, property_id, snapshot_json FROM publication_packages WHERE draft_id=? ORDER BY package_version DESC LIMIT 1",
                (draft_id,),
            ).fetchone()
        if not draft:
            await update.effective_message.reply_text(f"❌ 未找到房源：<code>{html.escape(input_id)}</code>", parse_mode=ParseMode.HTML)
            return
        draft_state = str(draft["review_status"] or "").lower()
        if draft_state == 'publishing':
            await update.effective_message.reply_text(
                "⛔ 此草稿正在投递或等待对账，不能重新审核或再次发送。请先处理投递记录。",
                parse_mode=ParseMode.HTML,
            )
            return
        if draft_state == 'published':
            await update.effective_message.reply_text(
                "⛔ 此草稿已经发布。需要重新公开发布时必须新建草稿并重新生成、审核冻结包。",
                parse_mode=ParseMode.HTML,
            )
            return
        if pkg and str(pkg[2] or '').lower() == 'approved':
            try:
                frozen_variant = str(json.loads(pkg[4] or "{}").get("caption_variant") or "a").lower()
            except (TypeError, ValueError, json.JSONDecodeError):
                frozen_variant = "a"
            await update.effective_message.reply_text(
                f"✅ {_admin_qc_for_draft(draft_id)} 已审核，冻结文案为 <b>{_caption_variant_label(frozen_variant)}</b>。\n"
                "点击下方按钮即可发布。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📤 发布到频道", callback_data=f"ap:n{frozen_variant}:{int(draft['id'])}"),
                ]]),
            )
            return

        variant = _selected_variant_for_draft(draft)
        if len(context.args) >= 2:
            raw_variant = str(context.args[1] or "").strip().lower()
            aliases = {
                "a": "a", "标准": "a", "标准信息": "a",
                "b": "b", "亮点": "b", "亮点价格": "b",
                "c": "c", "专业": "c", "专业参数": "c",
            }
            if raw_variant not in aliases:
                await update.effective_message.reply_text("文案版本只能是 A、B 或 C。")
                return
            variant = aliases[raw_variant]
        _save_caption_variant_for_draft(draft_id, variant)

        package_variant = ""
        if pkg and pkg[4]:
            try:
                package_variant = str(json.loads(pkg[4]).get("caption_variant") or "").lower()
            except (TypeError, ValueError, json.JSONDecodeError):
                package_variant = ""
        reusable = bool(
            pkg
            and str(pkg[2] or "").lower() == "package_ready"
            and re.fullmatch(r"(?i)l_\d+", str(pkg[3] or ""))
            and package_variant == variant
        )
        if not reusable:
            await asyncio.to_thread(
                build_package,
                DB_PATH,
                draft_id,
                caption_variant_override=variant,
            )
        approved = await asyncio.to_thread(approve_package, DB_PATH, draft_id, str(update.effective_user.id))
        with _conn() as c:
            c.execute(
                "UPDATE drafts SET review_status='approved', approved_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (draft_id,),
            )
            c.commit()
        _log_action(update.effective_user.id, 'approve_package', draft_id, str(approved.get('package_id') or ''))
        await update.effective_message.reply_text(
            f"✅ {_admin_qc_for_draft(draft_id)} 审核通过\n"
            f"文案：<b>{_caption_variant_label(variant)}</b>（已冻结）\n"
            "下一步：点击下方按钮发布到频道。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📤 发布到频道", callback_data=f"ap:n{variant}:{int(draft['id'])}"),
            ]]),
        )
    except Exception as exc:
        logger.exception('approve package failed')
        reason = str(exc)
        if reason in {"package_source_post_has_no_media_assets", "package_source_media_missing_asset_ids", "missing_usable_images"}:
            text = (
                "❌ 审核包未生成：这套房源没有可冻结的原始图片。\n"
                "请通过「微信导入」重新发送至少 1 张图片后再点完成；系统不会用其他房源图片补位。"
            )
        else:
            text = f"❌ 审核未完成：<code>{html.escape(reason)}</code>"
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """兼容旧命令：按 draft_id 丢弃草稿。"""
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("用法：<code>/reject QC0001</code>", parse_mode=ParseMode.HTML)
        return
    input_id = context.args[0].strip()
    draft_id = _resolve_admin_draft_id(input_id)
    if not draft_id:
        await update.message.reply_text(f"未找到房源：<code>{html.escape(input_id)}</code>", parse_mode=ParseMode.HTML)
        return
    display_id = _admin_qc_for_draft(draft_id)
    with _conn() as c:
        row = c.execute("SELECT id, review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if not row:
            await update.message.reply_text(f"未找到草稿：<code>{html.escape(display_id)}</code>", parse_mode=ParseMode.HTML)
            return
        note = (row["review_note"] or "").strip()
        extra = "rejected_by_command"
        next_note = f"{note} | {extra}" if note else extra
        c.execute(
            """UPDATE drafts
               SET review_status='rejected',
                   review_note=?,
                   updated_at=CURRENT_TIMESTAMP
               WHERE draft_id=?""",
            (next_note, draft_id),
        )
        c.commit()
    _log_action(update.effective_user.id, "reject", draft_id, "from /reject")
    await update.message.reply_text(f"🗑 已丢弃草稿：<code>{html.escape(display_id)}</code>", parse_mode=ParseMode.HTML)


def _scheduler_paused() -> bool:
    try:
        v = _get_setting("autopilot_publish_paused", "0")
        return str(v).strip() in ("1", "true", "yes")
    except Exception:
        return False


def _set_scheduler_paused(on: bool) -> None:
    _set_setting("autopilot_publish_paused", "1" if on else "0")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _set_scheduler_paused(True)
    await update.message.reply_text("已暂停<b>房源</b>定时发帖（/send 与按钮「立即发布」仍可用）。", parse_mode=ParseMode.HTML)


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _set_scheduler_paused(False)
    await update.message.reply_text("已恢复房源定时发帖。")


async def cmd_slots(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text(
            f"当前房源定时槽（{TZ_NAME}）：\n<code>{html.escape(_slots_raw_effective())}</code>\n\n"
            "修改例：<code>/slots 09:00,12:00,15:30,20:00</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    raw = " ".join(args).replace(" ", "")
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    bad = [p for p in parts if _parse_hhmm(p) is None]
    if bad:
        await update.message.reply_text(f"格式错误：{bad}，请用 HH:MM，逗号分隔。")
        return
    _set_setting(KEY_SLOTS, ",".join(parts))
    await update.message.reply_text(f"已保存房源定时槽（立即生效）：\n<code>{html.escape(','.join(parts))}</code>", parse_mode=ParseMode.HTML)


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Read-only operator preflight. It never generates, approves, or sends a package."""
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.effective_message.reply_text("用法：<code>/check QC0001</code>", parse_mode=ParseMode.HTML)
        return
    input_id = str(context.args[0]).strip()
    draft_id = _resolve_admin_draft_id(input_id)
    if not draft_id:
        await update.effective_message.reply_text(f"未找到房源：<code>{html.escape(input_id)}</code>", parse_mode=ParseMode.HTML)
        return
    display_id = _admin_qc_for_draft(draft_id)
    with _conn() as c:
        row = c.execute(
            """SELECT d.id,d.draft_id,d.source_post_id,d.review_status,d.listing_id,d.price,d.layout,d.deposit,
                      d.normalized_data,
                      (SELECT COUNT(*) FROM media_assets m WHERE m.owner_type='source_post'
                         AND CAST(m.owner_ref_id AS TEXT)=CAST(d.source_post_id AS TEXT)
                      ) AS source_media_count,
                      p.package_id,p.status AS package_status,p.cover_path,p.main_images_json,p.discussion_images_json
               FROM drafts d
               LEFT JOIN publication_packages p ON p.id=(
                   SELECT p2.id FROM publication_packages p2 WHERE p2.draft_id=d.draft_id
                   ORDER BY p2.package_version DESC LIMIT 1
               )
               WHERE d.draft_id=? LIMIT 1""",
            (draft_id,),
        ).fetchone()
    if not row:
        await update.effective_message.reply_text("未找到该草稿。")
        return
    try:
        normalized = json.loads(row["normalized_data"] or "{}")
    except Exception:
        normalized = {}
    area = str(normalized.get("normalized_area") or "").strip()
    contract_term = str(normalized.get("contract_term") or "").strip()
    nearby = bool(normalized.get("nearby"))
    area_display = f"{area}附近" if area and nearby else (area or "待人工确认")
    blockers: list[str] = []
    if not area:
        blockers.append("缺具体区域")
    try:
        if float(row["price"] or 0) <= 0:
            blockers.append("缺有效租金")
    except (TypeError, ValueError):
        blockers.append("缺有效租金")
    if not str(row["layout"] or "").strip():
        blockers.append("缺户型")
    if int(row["source_media_count"] or 0) < 1:
        blockers.append("缺同源图片")

    frozen_count = 0
    frozen_missing = 0
    package_status = str(row["package_status"] or "").strip()
    if row["package_id"]:
        try:
            frozen = json.loads(row["main_images_json"] or "[]") + json.loads(row["discussion_images_json"] or "[]")
        except Exception:
            frozen = []
            blockers.append("冻结图片清单异常")
        frozen_count = len(frozen)
        frozen_missing = sum(1 for path in frozen if not Path(str(path)).is_file())
        if not row["cover_path"] or not Path(str(row["cover_path"])).is_file():
            blockers.append("冻结封面缺失")
        if frozen_missing:
            blockers.append(f"冻结图片缺失 {frozen_missing} 张")

    if blockers:
        next_step = "请先补齐上面标出的信息，再回到“待审核房源”继续。系统不会替换图片，也不会发送频道。"
        verdict = "⛔ 还不能发布"
    elif package_status == "approved":
        next_step = "资料已审核，可以使用“发布房源”。"
        verdict = "✅ 可以发布"
    elif package_status == "package_ready":
        next_step = "请先在“待审核房源”确认封面和内容，再批准。"
        verdict = "🟡 等待确认"
    else:
        next_step = "请先进入审核流程，生成审核预览。"
        verdict = "🟡 等待审核"

    text = (
        f"<b>🔍 房源检查 #{int(row['id'])}</b>\n\n"
        f"状态：<b>{verdict}</b>\n"
        f"位置：{html.escape(area_display)}\n"
        f"租金：{html.escape(str(row['price'] or '-'))}\n"
        f"户型：{html.escape(str(row['layout'] or '-'))}\n"
        f"押付：{html.escape(str(row['deposit'] or '-'))}\n"
        f"合同：{html.escape(contract_term or '-')}\n"
        f"图片：{int(row['source_media_count'] or 0)} 张"
        + (f"\n\n需要处理：{'、'.join(blockers)}" if blockers else "")
        + f"\n\n下一步：{next_step}"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        try:
            with _conn() as c:
                rows = c.execute(
                    """SELECT d.*, pp.snapshot_json AS package_snapshot_json
                       FROM drafts d
                       JOIN publication_packages pp
                         ON pp.id = (
                           SELECT pp2.id FROM publication_packages pp2
                           WHERE pp2.draft_id=d.draft_id
                           ORDER BY pp2.package_version DESC, pp2.id DESC LIMIT 1
                         )
                       WHERE lower(COALESCE(d.review_status,''))='approved'
                         AND lower(COALESCE(pp.status,''))='approved'
                       ORDER BY COALESCE(pp.approved_at,d.approved_at,d.updated_at) DESC
                       LIMIT 8"""
                ).fetchall()
        except sqlite3.Error:
            logger.exception("approved publish queue query failed")
            rows = []
        if not rows:
            await update.effective_message.reply_text(
                "📤 当前没有已审核待发布房源。\n先点“📋 待审房源”，选择文案并审核。",
                reply_markup=admin_menu(),
            )
            return
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            d = _draft_to_caption_dict(row)
            try:
                frozen_variant = str(json.loads(row["package_snapshot_json"] or "{}").get("caption_variant") or "").lower()
            except (TypeError, ValueError, json.JSONDecodeError):
                frozen_variant = ""
            if frozen_variant not in {"a", "b", "c"}:
                frozen_variant = _selected_variant_for_draft(row)
            qc = _admin_qc_for_draft(str(row["draft_id"]))
            area = str(d.get("area") or "").strip()
            layout = str(d.get("layout") or d.get("property_type") or "").strip()
            try:
                price = f"${int(float(d.get('price'))):,}"
            except (TypeError, ValueError):
                price = str(d.get("price") or "").strip()
            summary = " · ".join(part for part in (qc, area, layout, price) if part)[:58]
            buttons.append([
                InlineKeyboardButton(
                    f"📤 {summary}",
                    callback_data=f"ap:n{frozen_variant}:{int(row['id'])}",
                )
            ])
        state_note = "" if _direct_publish_enabled() else "\n\n⛔ 当前生产配置已暂停直接发布。"
        await update.effective_message.reply_text(
            "📤 <b>已审核待发布</b>\n\n"
            "点击一套即发送其已冻结的封面、图片、文案和评论区详情；不会临时重写。"
            f"{state_note}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return
    input_id = context.args[0].strip()
    draft_id = _resolve_admin_draft_id(input_id)
    if not draft_id:
        await update.message.reply_text(f"未找到房源：<code>{html.escape(input_id)}</code>", parse_mode=ParseMode.HTML)
        return
    display_id = _admin_qc_for_draft(draft_id)
    if not _direct_publish_enabled():
        await update.effective_message.reply_text(
            "⛔ 当前暂时不能发布房源，请联系管理员。",
            reply_markup=admin_menu(),
        )
        return
    with _conn() as c:
        draft = c.execute("SELECT draft_id, review_status, review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        pkg = c.execute(
            "SELECT package_id, package_version, status, cover_path, main_images_json, discussion_images_json, post_text FROM publication_packages WHERE draft_id=? ORDER BY package_version DESC LIMIT 1",
            (draft_id,),
        ).fetchone()
    if not draft:
        await update.effective_message.reply_text(f"❌ preflight：draft 不存在：<code>{html.escape(display_id)}</code>", parse_mode=ParseMode.HTML)
        return
    if not pkg:
        await update.effective_message.reply_text(
            "❌ 这套房源还没有完成审核。请回到“待审房源”进入审核流程。",
            reply_markup=admin_menu(),
        )
        return
    status = str(pkg[2] or '').lower()
    if status != 'approved':
        next_step = f"/approve {display_id}" if status == 'package_ready' else f"/approve {display_id}（重新生成审核包）"
        await update.effective_message.reply_text(
            "🟡 这套房源还在等待审核确认。请先回到“待审房源”完成审核。",
            reply_markup=admin_menu(),
        )
        return
    import os as _os, json as _json
    missing = []
    if not pkg[3] or not _os.path.isfile(str(pkg[3])):
        missing.append('cover')
    try:
        frozen_paths = _json.loads(pkg[4] or '[]') + _json.loads(pkg[5] or '[]')
    except Exception:
        frozen_paths = []
        missing.append('media_json_invalid')
    if not frozen_paths or any(not _os.path.isfile(str(path)) for path in frozen_paths):
        missing.append('media')
    post_text = str(pkg[6] or '')
    if '{{' in post_text or '}}' in post_text:
        missing.append('unresolved_template_token')
    if missing:
        await update.effective_message.reply_text(
            "❌ 这套房源资料还不完整，暂时没有发送。请点“检查问题”查看原因。",
            reply_markup=admin_menu(),
        )
        return
    from meihua_publisher import MeihuaPublisher
    pub = MeihuaPublisher(DB_PATH)
    ok = await asyncio.to_thread(pub.publish_draft, draft_id)
    if ok:
        _log_action(update.effective_user.id, "send_one", draft_id)
        await update.effective_message.reply_text(
            "✅ 房源已发布到频道。",
            reply_markup=admin_menu(),
        )
    else:
        with _conn() as c:
            fail = c.execute(
                "SELECT error_message, log_message FROM publish_logs WHERE draft_id=? ORDER BY id DESC LIMIT 1",
                (draft_id,),
            ).fetchone()
        await update.effective_message.reply_text(
            "❌ 这套房源暂时没有发布成功。\n"
            "系统没有再次发送，避免重复发帖。\n"
            "请点“检查问题”查看原因；确认无误后再从“发布房源”继续。",
            reply_markup=admin_menu(),
        )


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    on = _get_setting(KEY_DAILY_ON, "0").strip() in ("1", "true", "yes")
    tm = _get_setting(KEY_DAILY_TIME, "")
    body = _get_setting(KEY_DAILY_TEXT, "")
    preview = (body[:500] + "…") if len(body) > 500 else body
    await update.message.reply_text(
        f"每日广播：{'<b>开</b>' if on else '关'}\n"
        f"时间：<code>{html.escape(tm or '未设置')}</code>\n"
        f"正文预览（HTML）：\n{html.escape(preview) if preview else '（空，请 /daily_text 或 /tpl_use）'}\n\n"
        "/daily_time /daily_on /daily_off /daily_text /tpl /tpl_use",
        parse_mode=ParseMode.HTML,
    )


async def cmd_daily_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _set_setting(KEY_DAILY_ON, "1")
    await update.message.reply_text("每日广播已开启（需已设时间与正文）。")


async def cmd_daily_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    _set_setting(KEY_DAILY_ON, "0")
    await update.message.reply_text("每日广播已关闭。")


async def cmd_daily_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if not context.args or _parse_hhmm(context.args[0]) is None:
        await update.message.reply_text("用法：<code>/daily_time 08:00</code>", parse_mode=ParseMode.HTML)
        return
    hm = _parse_hhmm(context.args[0])
    assert hm
    _set_setting(KEY_DAILY_TIME, f"{hm[0]:02d}:{hm[1]:02d}")
    await update.message.reply_text(f"每日广播时间已设为 <code>{hm[0]:02d}:{hm[1]:02d}</code>（{TZ_NAME}）", parse_mode=ParseMode.HTML)


async def cmd_daily_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    context.user_data["await"] = "daily_html"
    await update.message.reply_text("请下一条消息发送每日广播正文（支持 HTML）。发送 /cancel 取消。")


async def cmd_pin_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    context.user_data["await"] = "pin_html"
    await update.message.reply_text("请下一条消息发送频道置顶帖正文（HTML）。发送 /cancel 取消。")


async def cmd_intake(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    context.user_data["await"] = "intake_text"
    context.user_data["intake_text"] = ""
    context.user_data["intake_images"] = []
    await update.message.reply_text(
        "📥 <b>微信房源导入</b>\n\n"
        "把微信里的房源文字和图片直接发给我。\n"
        "文字、图片谁先发都可以，多张图可以连续发。\n\n"
        "收齐后点 <b>✅ 完成导入</b>，我会自动合并、解析并生成待审草稿。",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ 完成导入", callback_data="cmd:intake_done")],
            [InlineKeyboardButton("❌ 取消", callback_data="pub:cancel")],
        ]),
    )


async def cmd_batch_generate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Rebuild all previously imported sources into cleaned drafts and covers."""
    if not _is_admin(update.effective_user.id):
        return
    status = await update.message.reply_text(
        "⚡ 正在批量读取以前的微信房源并生成封面、文案……\n"
        "完成后只汇报成功数量和少数异常。"
    )
    try:
        from ai_parser import AIParserModule

        parser = AIParserModule(DB_PATH)
        parsed_stats = await asyncio.to_thread(parser.process_pending_source_posts)
        refreshed = await asyncio.to_thread(parser.refresh_pending_drafts, 500)

        with _conn() as c:
            rows = c.execute(
                """SELECT * FROM drafts
                   WHERE review_status IN ('pending','ready')
                   ORDER BY id"""
            ).fetchall()
        covers = 0
        cover_failed = 0
        for row in rows:
            try:
                path = await asyncio.to_thread(_formal_preview_cover, row)
                if path:
                    covers += 1
                else:
                    cover_failed += 1
            except Exception:
                cover_failed += 1
                logger.exception("batch cover failed for %s", row["draft_id"])

        with _conn() as c:
            total = c.execute(
                "SELECT COUNT(*) FROM drafts WHERE review_status IN ('pending','ready')"
            ).fetchone()[0]
        newly_parsed = sum(
            int(value or 0) for key, value in parsed_stats.items()
            if key in {"parsed", "recanonicalized"}
        )
        await status.edit_text(
            "✅ <b>以前的微信房源已批量生成</b>\n\n"
            f"读取并更新：<b>{refreshed + newly_parsed}</b> 套\n"
            f"封面和文案已生成：<b>{covers}</b> 套\n"
            f"需要补原图或关键资料：<b>{cover_failed}</b> 套\n"
            f"当前结果：<b>{int(total)}</b> 套\n\n"
            "点击下面按钮查看生成结果。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👀 查看生成结果", callback_data="cmd:pending")
            ]]),
        )
    except Exception as exc:
        logger.exception("batch generate previous wechat listings failed")
        await status.edit_text(
            "⚠️ 批量生成没有全部完成，原始微信数据和图片均已保留。\n"
            f"原因：{html.escape(str(exc))}",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu(),
        )


async def cmd_intake_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    context.user_data.pop("await", None)
    context.user_data.pop("intake_text", None)
    context.user_data.pop("intake_images", None)
    await update.message.reply_text("已取消微信笔记导入。")


async def cmd_intake_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    text = str(context.user_data.get("intake_text") or "").strip()
    images = list(context.user_data.get("intake_images") or [])
    if not text:
        await update.message.reply_text("还没收到文本。请先发送微信笔记文字内容。")
        return

    parsed = _extract_wechat_note_fields(text)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_post_id = f"wechat_{int(datetime.now().timestamp() * 1000)}"
    dedupe = hashlib.sha1(
        f"wechat_note_manual|{source_post_id}|{text}".encode("utf-8", errors="ignore")
    ).hexdigest()
    batch_id = f"BATCH_TG_{int(datetime.now().timestamp())}"
    row_id = f"ROW_TG_{source_post_id}"
    media_result = {"registered": 0, "existing": 0, "missing": 0}

    try:
        with _conn() as c:
            c.execute(
                """
                INSERT INTO source_posts (
                    source_type, source_name, source_post_id, source_url, source_author,
                    raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json,
                    dedupe_hash, parse_status, fetched_at, created_at, updated_at
                ) VALUES (?, ?, ?, '', ?, ?, ?, '[]', ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    "wechat_note",
                    "wechat_note_manual",
                    source_post_id,
                    f"admin:{update.effective_user.id}",
                    text,
                    json.dumps(images, ensure_ascii=False),
                    parsed.get("contact", ""),
                    json.dumps({"source": "autopilot_intake", **parsed}, ensure_ascii=False),
                    dedupe,
                    now,
                    now,
                    now,
                ),
            )
            source_row_id = int(c.execute("SELECT last_insert_rowid()").fetchone()[0])
            # 手工导入的图片此前只保存到 raw_images_json，导致审核包能取到图却缺 source identity。
            # 这里立即登记为 source_post 媒体资产，后续 package / Publisher 使用同一份身份。
            from publication_package import ensure_source_media_assets
            media_result = ensure_source_media_assets(c, source_row_id)

            if _table_exists(c, "excel_intake_batches"):
                c.execute(
                    """
                    INSERT OR IGNORE INTO excel_intake_batches (
                        batch_id, source_name, source_file, source_type, imported_rows, valid_rows, invalid_rows, status,
                        operator_user_id, notes, created_at, updated_at
                    ) VALUES (?, 'wechat_note_manual', 'telegram_private', 'excel_intake', 0, 0, 0, 'imported', ?, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (batch_id, str(update.effective_user.id)),
                )
            if _table_exists(c, "excel_listing_rows"):
                c.execute(
                    """
                    INSERT INTO excel_listing_rows (
                        row_id, batch_id, source_row_no, listing_id, title, area, property_type, layout,
                        monthly_rent, payment_terms, contract_term, contact, raw_row_json,
                        image_cover, image2, image3, image4,
                        desired_cover_w, desired_cover_h, desired_cover_kind,
                        ingestion_status, validation_errors, normalized_data, source_post_id, draft_id, publish_status,
                        created_at, updated_at
                    ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 800, 600, 'right_price_fixed', 'imported', '', ?, ?, '', 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        row_id,
                        batch_id,
                        source_post_id,
                        parsed.get("title", ""),
                        parsed.get("area", ""),
                        parsed.get("property_type", ""),
                        parsed.get("layout", ""),
                        parsed.get("price"),
                        parsed.get("payment_terms", ""),
                        parsed.get("contract_term", ""),
                        parsed.get("contact", ""),
                        json.dumps({"source": "autopilot_intake", **parsed}, ensure_ascii=False),
                        images[0] if len(images) > 0 else "",
                        images[1] if len(images) > 1 else "",
                        images[2] if len(images) > 2 else "",
                        images[3] if len(images) > 3 else "",
                        json.dumps(parsed, ensure_ascii=False),
                        source_row_id,
                    ),
                )
                c.execute(
                    """
                    UPDATE excel_intake_batches
                    SET imported_rows=imported_rows+1, valid_rows=valid_rows+1, updated_at=CURRENT_TIMESTAMP
                    WHERE batch_id=?
                    """,
                    (batch_id,),
                )
            c.commit()
    except Exception as e:
        logger.exception("intake_done failed")
        await update.message.reply_text(f"导入失败：{html.escape(str(e))}", parse_mode=ParseMode.HTML)
        return

    context.user_data.pop("await", None)
    context.user_data.pop("intake_text", None)
    context.user_data.pop("intake_images", None)

    # 立即触发 AI 解析，生成草稿
    draft_id_gen = ""
    parse_msg = "⏳ 正在解析..."
    try:
        from ai_parser import AIParserModule
        parser = AIParserModule(DB_PATH)
        result = parser.process_single_source_post(source_row_id)
        # 查询刚生成的草稿
        with _conn() as c:
            row = c.execute(
                "SELECT draft_id FROM drafts WHERE source_post_id=? ORDER BY id DESC LIMIT 1",
                (source_row_id,),
            ).fetchone()
        if row and row["draft_id"]:
            draft_id_gen = str(row["draft_id"])
            parse_msg = f"✅ 草稿已生成：<code>{html.escape(draft_id_gen)}</code>\n下一步请执行 <code>/approve {html.escape(draft_id_gen)}</code> 生成审核包；确认预览后再次执行 /approve，状态 approved 后才能 /send。"
        else:
            parse_msg = f"⚠️ 解析完成但未找到草稿，可用 <code>/intake_pending</code> 查看。"
    except Exception as e:
        logger.exception("auto parse after intake_done failed")
        parse_msg = f"⚠️ 自动解析失败：{html.escape(str(e))}\n可用 <code>/intake_pending</code> 查看。"

    await update.message.reply_text(
        "✅ 已导入微信笔记\n"
        f"source_post_id: <code>{html.escape(source_post_id)}</code>\n"
        f"title: {html.escape(parsed.get('title') or '-')}\n"
        f"images: {len(images)} · 已登记源图片: {media_result.get('registered', 0) + media_result.get('existing', 0)}"
        + (f" · 缺失: {media_result.get('missing', 0)}" if media_result.get('missing', 0) else "")
        + "\n\n"
        + parse_msg,
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu(),
    )


async def cmd_intake_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    out = ["📋 <b>微信导入待发布草稿</b>"]
    try:
        with _conn() as c:
            rows = c.execute(
                """
                SELECT d.draft_id, d.title, d.review_status, COALESCE(d.queue_score, 0) AS score,
                       COALESCE(d.updated_at, d.created_at) AS t
                FROM drafts d
                JOIN source_posts s ON s.id = d.source_post_id
                WHERE s.source_type='wechat_note'
                  AND d.review_status IN ('pending','ready')
                ORDER BY d.id DESC
                LIMIT 12
                """
            ).fetchall()
        if not rows:
            out.append("暂无微信导入的待发布草稿。")
        else:
            for r in rows:
                out.append(
                    f"• <code>{html.escape(str(r['draft_id'] or '-'))}</code>\n"
                    f"  [{html.escape(str(r['review_status'] or '-'))}] "
                    f"score={int(float(r['score'] or 0))} "
                    f"{html.escape(str(r['t'] or '')[:16])}\n"
                    f"  {html.escape(str(r['title'] or '（无标题）')[:40])}"
                )
            out.append("\n状态说明：pending/ready 需先执行 <code>/approve QC0001</code> 生成并审核 package；只有 approved 后才能 /send。")
    except Exception as e:
        out.append(f"读取失败：{html.escape(str(e))}")
    await update.message.reply_text("\n".join(out), parse_mode=ParseMode.HTML, reply_markup=admin_menu())


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    context.user_data.pop("await", None)
    context.user_data.pop("note_draft_pk", None)
    context.user_data.pop("intake_text", None)
    context.user_data.pop("intake_images", None)
    await update.message.reply_text("已取消当前输入。")


async def cmd_tpl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    lines = ["<b>每日广播模版</b>（<code>/tpl_use 编号</code>）\n"]
    for k, (title, _) in DAILY_TEMPLATES.items():
        lines.append(f"{k}. {html.escape(title)}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_tpl_use(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("用法：<code>/tpl_use 1</code>", parse_mode=ParseMode.HTML)
        return
    n = int(context.args[0])
    if n not in DAILY_TEMPLATES:
        await update.message.reply_text("编号不存在，先 /tpl 查看。")
        return
    _, body = DAILY_TEMPLATES[n]
    _set_setting(KEY_DAILY_TEXT, body)
    await update.message.reply_text(f"已套用模版 {n} 作为每日广播正文。\n可用 /daily 查看。", parse_mode=ParseMode.HTML)


async def cmd_tpl_test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """各模版各发一条，便于核对 HTML 与按钮（默认发频道）。"""
    if not _is_admin(update.effective_user.id):
        return
    args = [a.lower() for a in (context.args or [])]
    only_here = "here" in args or "private" in args or "dm" in args
    if not only_here and not CHANNEL_ID:
        await update.message.reply_text(
            "未配置 CHANNEL_ID。使用 <code>/tpl_test here</code> 仅在当前聊天预览各模版。",
            parse_mode=ParseMode.HTML,
        )
        return
    if (not only_here) and (not _direct_publish_enabled()):
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        await update.effective_message.reply_text("⛔ 当前生产配置已关闭频道发布，未发送模板。")
        return
    dest = update.effective_chat.id if only_here else CHANNEL_ID
    kb = build_channel_menu_keyboard()
    markup = kb if kb.inline_keyboard else None
    sent = 0
    errs: list[str] = []
    for n, (title, body) in sorted(DAILY_TEMPLATES.items()):
        header = f"<b>【测试·模版{n}·{html.escape(title)}】</b>\n\n"
        text = header + body
        if len(text) > 3900:
            text = text[:3900]
        try:
            await context.bot.send_message(
                chat_id=dest,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=markup,
            )
            sent += 1
            await asyncio.sleep(0.35)
        except Exception as e:
            errs.append(f"{n}: {e}")
    loc = "频道" if not only_here else "本聊天"
    msg = f"已发往{loc}：{sent}/{len(DAILY_TEMPLATES)} 条。"
    if errs:
        msg += "\n失败：" + "；".join(errs)
    await update.message.reply_text(msg)


async def cmd_post_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if not CHANNEL_ID:
        await update.message.reply_text("未配置 CHANNEL_ID。")
        return
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        await update.effective_message.reply_text("⛔ 当前生产配置已关闭频道发布，未发送置顶菜单。")
        return
    text = default_pin_html()
    kb = build_channel_menu_keyboard()
    try:
        msg = await context.bot.send_message(
            chat_id=CHANNEL_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb if kb.inline_keyboard else None,
        )
        try:
            await context.bot.pin_chat_message(chat_id=CHANNEL_ID, message_id=msg.message_id, disable_notification=True)
            pin_note = "已尝试置顶（需 Bot 为频道管理员且有置顶权限）。"
        except Exception as e:
            pin_note = f"发帖成功，置顶失败：{e}"
        await update.message.reply_text(f"频道已发送菜单帖。\n{pin_note}")
    except Exception as e:
        logger.exception("post_menu")
        await update.message.reply_text(f"发送失败：{e}")


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """面板“待审房源”：先说明全部待审的真实阻塞状态，再发送可安全预览的少量草稿。"""
    if not _is_admin(update.effective_user.id):
        return
    limit = 1
    with _conn() as c:
        overview = pending_overview(
            c,
            preview_min_score=PREVIEW_MIN_SCORE,
            preview_limit=limit,
            queue_limit=12,
        )
    pending_total = int(overview["pending_total"])
    all_pending = overview["queue_rows"]
    rows = [
        row for row in overview["preview_rows"]
        if str(row["area"] or "").strip()
        and str(row["price"] or "").strip()
        and str(row["layout"] or "").strip()
    ][:1]
    display_titles = overview["display_titles"]
    if pending_total == 0:
        await update.message.reply_text("📋 当前没有待审核草稿。", reply_markup=admin_menu())
        return
    missing_area = [
        row for row in all_pending
        if not str(row["area"] or "").strip()
        or any(code in {"missing_area", "missing_public_location", "geo_precision_unconfirmed"}
               for code in _quality_codes(row["review_note"]))
    ][:1]
    if missing_area:
        row = missing_area[0]
        title = display_titles.get(str(row["draft_id"]), display_title(row["title"]))[:32]
        await update.message.reply_text(
            f"🧹 <b>逐套处理 · 第 1/{pending_total} 套</b>\n\n"
            f"🏠 {html.escape(title)}\n"
            "缺少：<b>准确区域</b>\n\n"
            "点下面按钮，然后直接回复区域名称。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "📍 填写区域",
                callback_data=f"ap:f:{int(row['id'])}",
            )]]),
        )
        return
    if not rows:
        row = all_pending[0]
        title = display_titles.get(str(row["draft_id"]), display_title(row["title"]))[:32]
        await update.message.reply_text(
            f"🧹 <b>逐套处理 · 第 1/{pending_total} 套</b>\n\n"
            f"🏠 {html.escape(title)}\n"
            f"需要补充：{html.escape(_quality_text(_quality_codes(row['review_note'])))}\n\n"
            "点下面按钮，直接告诉我需要修改什么。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "✏️ 补充房源信息",
                callback_data=f"ap:e:{int(row['id'])}",
            )]]),
        )
        return
    from meihua_publisher import build_caption

    for row in rows:
        d = _draft_to_caption_dict(row)
        cap = build_caption(d, caption_variant="a")
        canonical_head = display_title(d.get("title") or d.get("project") or d.get("area") or row["title"])
        head = (
            "🏠 <b>请确认这套房源</b>\n"
            f"{html.escape(canonical_head)}\n\n"
        )
        text = head + (cap[:3200] if len(cap) > 3200 else cap)
        # Pending review must display the formal canonical cover, not a legacy
        # cover_asset that may have been generated from polluted draft columns.
        img = await asyncio.to_thread(_formal_preview_cover, row) or _cover_path_for_draft(row)
        kb = _kb_preview(row["id"])
        try:
            if img:
                with open(img, "rb") as f:
                    await update.message.reply_photo(
                        photo=f,
                        caption=text[:1024],
                        reply_markup=kb,
                        parse_mode=ParseMode.HTML,
                    )
                if len(text) > 1024:
                    await update.message.reply_text(
                        text[1024 : 1024 + 3500],
                        reply_markup=kb,
                        parse_mode=ParseMode.HTML,
                    )
            else:
                await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("发送预览失败")
            await update.message.reply_text(text[:3500], reply_markup=kb, parse_mode=ParseMode.HTML)


def _log_action(operator_id: int, action: str, target: str, payload: str = "") -> None:
    try:
        with _conn() as c:
            c.execute(
                """INSERT INTO admin_actions
                   (operator_id, action, target_type, target_id, payload, created_at)
                   VALUES (?, ?, 'draft', ?, ?, CURRENT_TIMESTAMP)""",
                (str(operator_id), action, target, payload[:2000]),
            )
            c.commit()
    except Exception:
        logger.exception("admin_actions 写入失败")


def _variant_from_action(act: str) -> str:
    if len(act) >= 2 and act[1] in {"a", "b", "c"}:
        return act[1]
    return "a"


def _caption_variant_label(variant: str) -> str:
    return {
        "a": "A·标准信息版",
        "b": "B·亮点价格版",
        "c": "C·专业参数版",
    }.get(str(variant or "").lower(), "A·标准信息版")


def _default_variant_for_draft(row: sqlite3.Row | dict) -> str:
    from meihua_publisher import default_caption_variant_for_property
    try:
        d = _draft_to_caption_dict(row)
    except Exception:
        d = dict(row)
    return default_caption_variant_for_property(d.get("property_type"))


def _variant_from_note(note: str | None, default: str = "a") -> str:
    if not note:
        return default if default in {"a", "b", "c"} else "a"
    m = re.search(r"caption_variant:(a|b|c)", str(note))
    return m.group(1) if m else (default if default in {"a", "b", "c"} else "a")


def _selected_variant_for_draft(row: sqlite3.Row | dict) -> str:
    return _variant_from_note(row["review_note"], _default_variant_for_draft(row))


def _note_with_caption_variant(note: str | None, variant: str) -> str:
    variant = variant if variant in {"a", "b", "c"} else "a"
    current = str(note or "").strip()
    if re.search(r"caption_variant:(a|b|c)", current):
        return re.sub(r"caption_variant:(a|b|c)", f"caption_variant:{variant}", current, count=1)
    if not current:
        return f"caption_variant:{variant}"
    return f"{current} | caption_variant:{variant}"


def _save_caption_variant_for_draft(draft_id: str, variant: str) -> None:
    with _conn() as c:
        row = c.execute("SELECT review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if not row:
            return
        c.execute(
            "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (_note_with_caption_variant(row["review_note"], variant), draft_id),
        )
        c.commit()


def _return_publish_blocked_to_pending(draft_id: str) -> None:
    with _conn() as c:
        row = c.execute("SELECT review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        current = (row["review_note"] or "").strip() if row else ""
        parts = [p.strip() for p in current.split("|") if p.strip()]
        if "publish_gate_blocked" not in parts:
            parts.append("publish_gate_blocked")
        c.execute(
            """UPDATE drafts
               SET review_status='pending',
                   review_note=?,
                   updated_at=CURRENT_TIMESTAMP
               WHERE draft_id=?""",
            (" | ".join(parts), draft_id),
        )
        c.commit()


async def _send_visual_preview(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    row: sqlite3.Row,
    caption_variant: str = "a",
) -> None:
    from meihua_publisher import (
        _album_paths_for_draft,
        build_channel_caption,
        build_discussion_detail_text,
        evaluate_publish_gate,
    )

    d = _draft_to_caption_dict(row)
    cover = await asyncio.to_thread(_formal_preview_cover, row) or _cover_path_for_draft(row) or ""
    gate = evaluate_publish_gate(d, cover, DB_PATH)
    if not gate.get("is_publishable"):
        reasons = ",".join(gate.get("reasons") or []) or "quality_gate_blocked"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⛔ 该草稿被发布门槛拦截：`{row['draft_id']}`\n原因：`{reasons}`",
            parse_mode="Markdown",
        )
        return

    album_all = gate.get("album_all") or _album_paths_for_draft(d, cover, DB_PATH)
    if not album_all:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⚠️ 预览失败：无可用实拍图片 `{row['draft_id']}`",
            parse_mode="Markdown",
        )
        return

    caption_variant = "a"
    caption = build_channel_caption(d, album_all, caption_variant=caption_variant)
    # The second preview message must represent the discussion/comment detail,
    # not repeat the channel caption. It is generated from the same canonical
    # draft projection used by the frozen publication package.
    detail = build_discussion_detail_text(d)
    head = (
        "🏠 <b>发布前确认</b>\n"
        "请核对位置、户型、租金和图片。"
    )

    if len(album_all) == 1:
        with open(album_all[0], "rb") as f:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=f,
                caption=caption[:1024],
                parse_mode=ParseMode.HTML,
            )
    else:
        media: list[InputMediaPhoto] = []
        for idx, path in enumerate(album_all):
            with open(path, "rb") as f:
                buf = io.BytesIO(f.read())
                buf.name = f"preview_{idx}.jpg"
            if idx == 0:
                media.append(
                    InputMediaPhoto(
                        media=buf,
                        caption=caption[:1024],
                        parse_mode=ParseMode.HTML,
                    )
                )
            else:
                media.append(InputMediaPhoto(media=buf))
        await context.bot.send_media_group(chat_id=update.effective_chat.id, media=media)

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"{head}\n\n"
            "📌 评论区详情预览（发布后会回复主帖）\n\n"
            f"{detail}"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "✅ 确认这套房源",
                callback_data=f"ap:r:{int(row['id'])}",
            )],
            [
                InlineKeyboardButton("✏️ 补充修改说明", callback_data=f"ap:e:{int(row['id'])}"),
                InlineKeyboardButton("🖼 重新选主图", callback_data=f"ap:c:{int(row['id'])}"),
            ],
        ]),
    )


async def on_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    if not _is_admin(update.effective_user.id):
        return
    parts = (q.data or "").split(":")
    if len(parts) != 3 or parts[0] != "ap" or not parts[2].isdigit():
        return
    act, pk_s = parts[1], int(parts[2])
    row = _draft_row_by_pk(pk_s)
    if not row:
        await q.edit_message_text("草稿已不存在。")
        return
    draft_id = row["draft_id"]

    if act == "f":
        context.user_data["await"] = "fix_area"
        context.user_data["fix_area_pk"] = pk_s
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"📍 请直接回复这套房源的区域名称。\n"
                f"房源：{html.escape(display_title(row['title']))}\n\n"
                "只填来源明确写过的位置；不知道就发送 /cancel。"
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if act == "u":
        selected = _selected_variant_for_draft(row)
        await q.edit_message_reply_markup(reply_markup=_kb_preview(pk_s, selected))
        return

    if act.startswith("v"):
        variant = _variant_from_action(act)
        _save_caption_variant_for_draft(draft_id, variant)
        _log_action(update.effective_user.id, f"preview_{variant}", draft_id)
        await _send_visual_preview(
            update=update,
            context=context,
            row=row,
            caption_variant=variant,
        )
        return

    if act == "r" or (len(act) == 2 and act.startswith("a") and act[1] in {"a", "b", "c"}):
        # One clear admin action, while retaining the internal build -> approve
        # frozen-package contract. No raw DRF/package identifiers are exposed.
        try:
            from publication_package import approve_package, build_package
            variant = "a"
            with _conn() as c:
                frozen = c.execute(
                    """SELECT package_id,status,snapshot_json
                       FROM publication_packages
                       WHERE draft_id=? AND status IN ('approved','published')
                       ORDER BY id DESC LIMIT 1""",
                    (draft_id,),
                ).fetchone()
                source_media_count = c.execute(
                    """SELECT COUNT(*) FROM media_assets m
                       WHERE m.owner_type='source_post'
                         AND CAST(m.owner_ref_id AS TEXT)=CAST(? AS TEXT)
                         AND COALESCE(m.status,'active')='active'""",
                    (row["source_post_id"],),
                ).fetchone()[0]
                pkg = c.execute(
                    "SELECT property_id, status, snapshot_json FROM publication_packages WHERE draft_id=? ORDER BY package_version DESC LIMIT 1",
                    (draft_id,),
                ).fetchone()
            if frozen and str(frozen[1] or "").lower() == "published":
                with _conn() as c:
                    c.execute(
                        "UPDATE drafts SET review_status='published', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                        (draft_id,),
                    )
                    c.commit()
                await q.edit_message_reply_markup(None)
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="✅ 这套房源已经发布，无需再次审核。已从待审队列移除。",
                )
                return
            if frozen and str(frozen[1] or "").lower() == "approved":
                try:
                    frozen_variant = json.loads(frozen[2] or "{}").get("caption_variant") or variant
                except (TypeError, ValueError, json.JSONDecodeError):
                    frozen_variant = variant
                await q.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 发布到频道", callback_data=f"ap:n{frozen_variant}:{pk_s}"),
                    ]])
                )
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="✅ 这套已经确认，请直接点击“发布到频道”。",
                )
                return
            if int(source_media_count or 0) < 2:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=(
                        f"📷 这套目前只有 {int(source_media_count or 0)} 张可用实拍，暂不能审核发布。\n"
                        "请回到“导入房源”补到至少 2 张，系统不会生成空壳帖子。"
                    ),
                )
                return
            _save_caption_variant_for_draft(draft_id, variant)
            if pkg and str(pkg[1] or "").lower() == "approved":
                try:
                    frozen_variant = json.loads(pkg[2] or "{}").get("caption_variant") or variant
                except (TypeError, ValueError, json.JSONDecodeError):
                    frozen_variant = variant
                await q.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📤 发布到频道", callback_data=f"ap:n{frozen_variant}:{pk_s}"),
                    ]])
                )
                return
            package_variant = ""
            if pkg and pkg[2]:
                try:
                    package_variant = str(json.loads(pkg[2]).get("caption_variant") or "").lower()
                except (TypeError, ValueError, json.JSONDecodeError):
                    package_variant = ""
            reusable = bool(
                pkg
                and str(pkg[1] or "").lower() == "package_ready"
                and re.fullmatch(r"(?i)l_\d+", str(pkg[0] or ""))
                and package_variant == variant
            )
            if not reusable:
                await asyncio.to_thread(
                    build_package,
                    DB_PATH,
                    draft_id,
                    caption_variant_override=variant,
                )
            approved = await asyncio.to_thread(approve_package, DB_PATH, draft_id, str(update.effective_user.id))
            with _conn() as c:
                c.execute(
                    "UPDATE drafts SET review_status='approved', approved_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                    (draft_id,),
                )
                c.commit()
            _log_action(update.effective_user.id, "approve_from_preview", draft_id, str(approved.get("package_id") or ""))
            await q.edit_message_reply_markup(
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📤 发布到频道", callback_data=f"ap:n{variant}:{pk_s}"),
                ]])
            )
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    "✅ 这套房源已经确认。\n"
                    "请点击“📤 发布到频道”完成发布。"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.exception("approve from preview failed for %s", draft_id)
            error_text = str(exc)
            friendly = {
                "approved_package_frozen": "这套已经审核冻结，请直接发布，不要重复审核。",
                "canonical_package_gate_blocked:insufficient_media": "可用实拍不足，请回到“导入房源”补图。",
            }.get(error_text, "审核没有完成。系统已保留原草稿，请到“检查问题”查看缺少的内容。")
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"⚠️ {html.escape(friendly)}",
                parse_mode=ParseMode.HTML,
            )
        return

    if act.startswith("n"):
        if not _direct_publish_enabled():
            logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
            await q.edit_message_text("⛔ 当前生产配置已关闭直接发布，未发送房源。")
            return
        # The preview queue is not an approval queue.  This legacy callback must
        # consume exactly the same approved frozen package contract as /send and
        # the scheduler; never call the publisher for pending/ready drafts.
        with _conn() as c:
            approval = c.execute(
                """SELECT d.review_status, pp.status, pp.snapshot_json
                   FROM drafts d
                   LEFT JOIN publication_packages pp
                     ON pp.id = (SELECT pp2.id FROM publication_packages pp2
                                 WHERE pp2.draft_id=d.draft_id
                                 ORDER BY pp2.package_version DESC, pp2.id DESC LIMIT 1)
                   WHERE d.draft_id=?""",
                (draft_id,),
            ).fetchone()
        if not approval or str(approval[0] or "").lower() != "approved" or str(approval[1] or "").lower() != "approved":
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"⛔ 未发布 `{draft_id}`：该预览仍未通过最终审核冻结。\n"
                    f"当前 draft={str(approval[0] or '-') if approval else '-'}，"
                    f"package={str(approval[1] or '-') if approval else '-'}。\n"
                    f"请先执行 `/approve {draft_id}`，确认 package 预览后再发布。"
                ),
                parse_mode="Markdown",
            )
            return
        try:
            variant = str(json.loads(approval[2] or "{}").get("caption_variant") or "").lower()
        except (TypeError, ValueError, json.JSONDecodeError):
            variant = ""
        if variant not in {"a", "b", "c"}:
            variant = _variant_from_action(act)
        from meihua_publisher import MeihuaPublisher

        pub = MeihuaPublisher(DB_PATH)
        ok = await asyncio.to_thread(pub.publish_draft, draft_id, variant)
        if ok:
            _log_action(update.effective_user.id, f"publish_now_{variant}", draft_id)
            await q.edit_message_reply_markup(None)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    "✅ <b>房源已发布到测试频道</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
        else:
            await _notify_publish_failure_once(context, update.effective_chat.id, draft_id)

    elif act == "q":
        from media_consistency import assess_draft_media, mark_draft_media_broken, media_blocks_ready, media_issue_summary

        media_status = assess_draft_media(draft_id, DB_PATH)
        if media_blocks_ready(media_status):
            mark_draft_media_broken(draft_id, media_status, DB_PATH)
            reasons = media_issue_summary(media_status)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"⛔ 无法加入 ready：`{draft_id}`\n"
                    f"原因：`{reasons}`\n"
                    "请先运行媒体恢复工具或丢弃该草稿。"
                ),
                parse_mode="Markdown",
            )
            return
        with _conn() as c:
            c.execute(
                "UPDATE drafts SET review_status='ready', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (pk_s,),
            )
            c.commit()
        _log_action(update.effective_user.id, "queue", draft_id)
        await q.edit_message_reply_markup(None)
        variant = _variant_from_note(row["review_note"])
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🕒 已加入队列 ready：`{draft_id}`（版本 {variant.upper()}）")

    elif act == "d":
        with _conn() as c:
            c.execute(
                "UPDATE drafts SET review_status='rejected', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (pk_s,),
            )
            c.commit()
        _log_action(update.effective_user.id, "reject", draft_id)
        await q.edit_message_reply_markup(None)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"🗑 已丢弃 `{draft_id}`")

    elif act == "c":
        from cover_generator import CoverGenerator

        async def _cov():
            gen = CoverGenerator(DB_PATH)
            return await asyncio.to_thread(gen.generate_for_draft, draft_id)

        asset_id, path = await _cov()
        _log_action(update.effective_user.id, "redo_cover", draft_id, str(path or ""))
        msg = f"🖼 封面已重算：`{draft_id}`" if path else f"⚠️ 封面失败：`{draft_id}`"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)

    elif act == "e":
        context.user_data["note_draft_pk"] = pk_s
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✏️ 请直接回复需要修改的内容，例如：\n区域应为堆谷区；租金改为 $800/月。\n\n发送 /cancel 取消。",
        )


async def on_text_private(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    text = (update.message.text or "").strip()
    mode = context.user_data.get("await")

    if mode == "fix_area":
        pk = context.user_data.pop("fix_area_pk", None)
        context.user_data.pop("await", None)
        row = _draft_row_by_pk(int(pk)) if pk is not None else None
        if not row:
            await update.message.reply_text("这套草稿已不存在，请返回待处理列表。", reply_markup=admin_menu())
            return
        area = text[:40].strip()
        try:
            from qiaolian_dual.area_admin import set_canonical_area
            result = set_canonical_area(
                DB_PATH,
                str(row["listing_id"] or ""),
                area,
                str(update.effective_user.id),
                "管理员在简单发布流程中确认来源位置",
            )
            await update.message.reply_text(
                f"✅ 区域已确认：{html.escape(result['new_area'])}\n\n继续点“处理待发布房源”。",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_menu(),
            )
        except Exception as exc:
            logger.exception("simple area correction failed")
            await update.message.reply_text(
                f"⚠️ 这个区域还不能保存：{html.escape(str(exc))}\n请核对来源文字后重试。",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_menu(),
            )
        return

    if mode == "daily_html":
        context.user_data.pop("await", None)
        _set_setting(KEY_DAILY_TEXT, text[:12000])
        await update.message.reply_text("已保存每日广播正文（HTML）。/daily 查看，/daily_on 开启。")
        return

    if mode == "pin_html":
        context.user_data.pop("await", None)
        _set_setting(KEY_PIN_TEXT, text[:12000])
        await update.message.reply_text("已保存频道置顶帖正文。执行 /post_menu 发到频道。")
        return

    if mode == "intake_text":
        old = str(context.user_data.get("intake_text") or "").strip()
        context.user_data["intake_text"] = (old + "\n" + text).strip()[:12000] if old else text[:12000]
        context.user_data["await"] = "intake_images"
        await update.message.reply_text(
            f"✅ 已收文字（{len(context.user_data['intake_text'])} 字）\n"
            "可继续发图片或补充文字，收齐后点完成。",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ 完成导入", callback_data="cmd:intake_done")]]),
        )
        return

    if mode == "intake_images":
        if text:
            old = str(context.user_data.get("intake_text") or "").strip()
            merged = (old + "\n" + text).strip() if old else text
            context.user_data["intake_text"] = merged[:12000]
            await update.message.reply_text(
                f"✅ 已合并文字（共 {len(context.user_data['intake_text'])} 字）",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ 完成导入", callback_data="cmd:intake_done")]]),
            )
        return

    pk = context.user_data.get("note_draft_pk")
    if pk is not None:
        note = text[:2000]
        with _conn() as c:
            c.execute(
                """UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (note, pk),
            )
            c.commit()
        context.user_data.pop("note_draft_pk", None)
        await update.message.reply_text("✏️ 备注已保存。")
        return

    # 管理员在非导入状态下直接发文字时也必须有反馈，
    # 避免“Bot 没反应”的感受。
    await update.message.reply_text(
        "✅ 发布后台正常。\n\n"
        "发房源图片和文字：点「📥 微信导入」\n"
        "查看待审内容：点「📋 待审预览」\n"
        "检查服务：发送 /status\n"
        "返回管理首页：发送 /start",
        reply_markup=admin_menu(),
    )


async def on_photo_private(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if context.user_data.get("await") not in {"intake_text", "intake_images"}:
        return
    photos = update.message.photo or []
    if not photos:
        return
    best = photos[-1]
    try:
        f = await context.bot.get_file(best.file_id)
        inbox = BASE_DIR / "data" / "wechat_inbox"
        inbox.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        local_path = inbox / f"wx_{ts}_{best.file_unique_id}.jpg"
        await f.download_to_drive(custom_path=str(local_path))
        bucket = context.user_data.setdefault("intake_images", [])
        bucket.append(str(local_path))
        context.user_data["await"] = "intake_images"
        caption = str(update.message.caption or "").strip()
        if caption:
            old = str(context.user_data.get("intake_text") or "").strip()
            context.user_data["intake_text"] = (old + "\n" + caption).strip()[:12000] if old else caption[:12000]
        text_len = len(str(context.user_data.get("intake_text") or ""))
        await update.message.reply_text(
            f"📷 已收图 {len(bucket)} 张" + (f" · 文字 {text_len} 字" if text_len else ""),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ 完成导入", callback_data="cmd:intake_done")]]),
        )
    except Exception:
        logger.exception("save intake photo failed")
        await update.message.reply_text("图片保存失败，请重发，或直接 /intake_done 先导入文本。")


async def scheduled_publish(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Publish one explicitly approved frozen package through the same publisher as /send.

    The scheduler never consumes ready/pending drafts and never rewrites an
    approval after a publisher failure.  The delivery protocol owns the only
    transition to publishing/published or an explicit reconciliation hold.
    """
    if _scheduler_paused():
        logger.info("定时房源帖：暂停，跳过")
        return
    if not _direct_publish_enabled():
        logger.warning("定时房源帖：发布开关关闭，跳过")
        return
    with _conn() as c:
        if c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='publication_packages'"
        ).fetchone() is None:
            orphan = c.execute(
                "SELECT draft_id FROM drafts WHERE review_status IN ('ready','approved') ORDER BY id LIMIT 1"
            ).fetchone()
            if orphan:
                _return_publish_blocked_to_pending(str(orphan["draft_id"]))
            logger.error("定时房源帖：publication_packages 表不存在；请先执行数据库 bootstrap，已安全跳过")
            return
        row = c.execute(
            """SELECT d.id, d.draft_id, d.review_note, d.property_type, pp.snapshot_json
               FROM drafts d
               JOIN publication_packages pp ON pp.draft_id=d.draft_id
               WHERE d.review_status='approved' AND pp.status='approved'
               ORDER BY COALESCE(d.queue_score, 0) DESC, d.id ASC
               LIMIT 1"""
        ).fetchone()
    if not row:
        with _conn() as c:
            orphan = c.execute(
                """SELECT d.draft_id
                   FROM drafts d
                   LEFT JOIN publication_packages pp
                     ON pp.draft_id=d.draft_id AND pp.status='approved'
                   WHERE d.review_status IN ('ready','approved')
                     AND pp.package_id IS NULL
                   ORDER BY d.id LIMIT 1"""
            ).fetchone()
        if orphan:
            _return_publish_blocked_to_pending(str(orphan["draft_id"]))
            logger.warning("定时房源帖：发现无 approved frozen package 的队列项，已退回 pending：%s", orphan["draft_id"])
            return
        logger.info("定时房源帖：没有 approved frozen package")
        return
    draft_id = str(row["draft_id"])
    from meihua_publisher import MeihuaPublisher

    try:
        variant = str(json.loads(row["snapshot_json"] or "{}").get("caption_variant") or "").lower()
    except (TypeError, ValueError, json.JSONDecodeError):
        variant = ""
    if variant not in {"a", "b", "c"}:
        from meihua_publisher import default_caption_variant_for_property
        variant = _variant_from_note(
            row["review_note"],
            default_caption_variant_for_property(row["property_type"]),
        )
    publisher = MeihuaPublisher(DB_PATH)
    ok = await asyncio.to_thread(publisher.publish_draft, draft_id, variant)
    if not ok:
        logger.warning("定时房源帖未完成：%s；审核状态保持，由投递协议决定恢复或对账", draft_id)
        return
    logger.info("定时房源帖已提交：%s 版本 %s", draft_id, variant.upper())


async def scheduled_daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not CHANNEL_ID:
        return
    if _get_setting(KEY_DAILY_ON, "0").strip() not in ("1", "true", "yes"):
        return
    body = _get_setting(KEY_DAILY_TEXT, "").strip()
    if not body:
        logger.info("每日广播：正文为空，跳过")
        return
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        return
    kb = build_channel_menu_keyboard()
    try:
        await context.bot.send_message(
            chat_id=CHANNEL_ID,
            text=body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb if kb.inline_keyboard else None,
        )
        logger.info("每日广播已发送")
    except Exception:
        logger.exception("每日广播发送失败")


async def tick_schedules(context: ContextTypes.DEFAULT_TYPE) -> None:
    """约每 30 秒检查一次，匹配当前「分」的槽位（改 /slots 无需重启）。"""
    now = datetime.now(TZ)
    bd = context.application.bot_data
    hm = (now.hour, now.minute)

    slots = _parse_slots_from_raw(_slots_raw_effective())
    if any((t.hour, t.minute) == hm for t in slots):
        key = ("pub", now.date(), hm[0], hm[1])
        if bd.get("_tick_pub") != key:
            bd["_tick_pub"] = key
            await scheduled_publish(context)

    d_on = _get_setting(KEY_DAILY_ON, "0").strip() in ("1", "true", "yes")
    d_raw = _get_setting(KEY_DAILY_TIME, "").strip()
    if d_on and d_raw:
        parsed = _parse_hhmm(d_raw)
        if parsed and (parsed[0], parsed[1]) == hm:
            dkey = ("daily", now.date())
            if bd.get("_tick_daily") != dkey:
                bd["_tick_daily"] = dkey
                await scheduled_daily_broadcast(context)


def clear_autopilot_input_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    """供 v2 /cancel 联动：退出「等待输入正文/备注」状态。"""
    context.user_data.pop("await", None)
    context.user_data.pop("note_draft_pk", None)


def register_autopilot_features(
    application: Application,
    *,
    include_cancel: bool = True,
    simple_mode: bool = False,
) -> None:
    """
    将队列/预览/定时/运营命令挂到已有 Application（与 v2 共用 meihua666 时调用）。
    group=-1：命令优先于 v2 会话；group=1：仅当会话未消费时处理 autopilot 的自由文本。
    """
    grp_cmd = -1
    grp_txt = 1
    if simple_mode:
        application.add_handler(CommandHandler("pending", cmd_pending), group=grp_cmd)
        application.add_handler(CommandHandler("send", cmd_send), group=grp_cmd)
        application.add_handler(CommandHandler("new", cmd_intake), group=grp_cmd)
        application.add_handler(CommandHandler("intake", cmd_intake), group=grp_cmd)
        application.add_handler(CommandHandler("intake_done", cmd_intake_done), group=grp_cmd)
        if include_cancel:
            application.add_handler(CommandHandler("cancel", cmd_cancel), group=grp_cmd)
        application.add_handler(
            CallbackQueryHandler(on_preview_callback, pattern=r"^ap:[a-z]{1,2}:\d+$"),
            group=grp_cmd,
        )
        application.add_handler(
            MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, on_photo_private),
            group=grp_txt,
        )
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, on_text_private),
            group=grp_txt,
        )
        logger.info("已启用简单发布模式：导入 → 处理 → 手动发布；定时发布关闭")
        return
    application.add_handler(CommandHandler("ops", cmd_ops), group=grp_cmd)
    application.add_handler(CommandHandler("help", cmd_help), group=grp_cmd)
    application.add_handler(CommandHandler("pending", cmd_pending), group=grp_cmd)
    application.add_handler(CommandHandler("status", cmd_status), group=grp_cmd)
    application.add_handler(CommandHandler("stats", cmd_stats), group=grp_cmd)
    application.add_handler(CommandHandler("sources", cmd_sources), group=grp_cmd)
    application.add_handler(CommandHandler("source_add", cmd_source_add), group=grp_cmd)
    application.add_handler(CommandHandler("source_on", cmd_source_on), group=grp_cmd)
    application.add_handler(CommandHandler("source_off", cmd_source_off), group=grp_cmd)
    application.add_handler(CommandHandler("logs", cmd_logs), group=grp_cmd)
    application.add_handler(CommandHandler("analytics", cmd_analytics), group=grp_cmd)
    application.add_handler(CommandHandler("pause", cmd_pause), group=grp_cmd)
    application.add_handler(CommandHandler("resume", cmd_resume), group=grp_cmd)
    application.add_handler(CommandHandler("slots", cmd_slots), group=grp_cmd)
    application.add_handler(CommandHandler("check", cmd_check), group=grp_cmd)
    application.add_handler(CommandHandler("send", cmd_send), group=grp_cmd)
    application.add_handler(CommandHandler("publish", cmd_publish), group=grp_cmd)
    application.add_handler(CommandHandler("approve", cmd_approve), group=grp_cmd)
    application.add_handler(CommandHandler("reject", cmd_reject), group=grp_cmd)
    application.add_handler(CommandHandler("daily", cmd_daily), group=grp_cmd)
    application.add_handler(CommandHandler("daily_on", cmd_daily_on), group=grp_cmd)
    application.add_handler(CommandHandler("daily_off", cmd_daily_off), group=grp_cmd)
    application.add_handler(CommandHandler("daily_time", cmd_daily_time), group=grp_cmd)
    application.add_handler(CommandHandler("daily_text", cmd_daily_text), group=grp_cmd)
    application.add_handler(CommandHandler("tpl", cmd_tpl), group=grp_cmd)
    application.add_handler(CommandHandler("tpl_use", cmd_tpl_use), group=grp_cmd)
    application.add_handler(CommandHandler("tpl_test", cmd_tpl_test), group=grp_cmd)
    application.add_handler(CommandHandler("new", cmd_intake), group=grp_cmd)
    application.add_handler(CommandHandler("intake", cmd_intake), group=grp_cmd)
    application.add_handler(CommandHandler("wxin", cmd_intake), group=grp_cmd)
    application.add_handler(CommandHandler("intake_done", cmd_intake_done), group=grp_cmd)
    application.add_handler(CommandHandler("intake_cancel", cmd_intake_cancel), group=grp_cmd)
    application.add_handler(CommandHandler("intake_pending", cmd_intake_pending), group=grp_cmd)
    application.add_handler(CommandHandler("wx", cmd_intake_pending), group=grp_cmd)
    application.add_handler(CommandHandler("post_menu", cmd_post_menu), group=grp_cmd)
    application.add_handler(CommandHandler("post_index", cmd_post_index), group=grp_cmd)
    application.add_handler(CommandHandler("pin_text", cmd_pin_text), group=grp_cmd)
    if include_cancel:
        application.add_handler(CommandHandler("cancel", cmd_cancel), group=grp_cmd)
    application.add_handler(
        CallbackQueryHandler(on_preview_callback, pattern=r"^ap:[a-z]{1,2}:\d+$"),
        group=grp_cmd,
    )
    application.add_handler(
        MessageHandler(
            filters.PHOTO & filters.ChatType.PRIVATE,
            on_photo_private,
        ),
        group=grp_txt,
    )
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            on_text_private,
        ),
        group=grp_txt,
    )

    jq = application.job_queue
    if jq is None:
        logger.warning("job_queue 不可用：定时 ready 发帖与每日广播不启动（pip install 'python-telegram-bot[job-queue]'）")
    else:
        jq.run_repeating(tick_schedules, interval=30.0, first=8.0, name="tick_schedules")
        logger.info("已挂载调度 tick（30s）时区=%s 槽=%s", TZ_NAME, _slots_raw_effective())


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("请设置 AUTOPILOT_BOT_TOKEN 或 PUBLISHER_BOT_TOKEN")
    if not ADMIN_IDS:
        raise SystemExit("ADMIN_IDS 未设置")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start), group=-1)
    register_autopilot_features(app, include_cancel=True)
    logger.info("Autopilot publish bot 独立启动（未与 v2 合并）")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
