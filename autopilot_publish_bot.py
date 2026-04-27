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

# bot_settings keys
KEY_SLOTS = "publish_slots"
KEY_DAILY_TIME = "daily_broadcast_time"
KEY_DAILY_TEXT = "daily_broadcast_html"
KEY_DAILY_ON = "daily_broadcast_enabled"
KEY_PIN_TEXT = "channel_pin_html"


def _direct_publish_enabled() -> bool:
    return os.getenv("AUTOPILOT_DIRECT_PUBLISH_ENABLED", "").strip().lower() == "yes"

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
    text = (raw_text or "").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    title = lines[0] if lines else "微信笔记房源"

    def _pick(patterns: list[str]) -> str:
        for p in patterns:
            m = re.search(p, text, flags=re.IGNORECASE)
            if m:
                return str(m.group(1) or "").strip()
        return ""

    area = _pick([r"(?:位置|地址|区域)[:：]\s*([^\n]+)", r"(BKK1|BKK2|BKK3|俄罗斯市场|洪森大道|钻石岛|森速|富力城|炳发城)"])
    layout = _pick([r"(?:户型|房型)[:：]\s*([^\n]+)", r"(\d+\s*房(?:\+\d+\s*保姆房)?[^\n]{0,20})"])
    price = _pick([r"(?:租金|月租|价格)[:：]\s*\$?\s*([0-9][0-9,]{2,})", r"\$([0-9][0-9,]{2,})\s*(?:/月|每月)?"]).replace(",", "")
    payment_terms = _pick([r"(?:押金|押付|付款)[:：]\s*(押[^\n]+)", r"(押\s*[一二三四五六七八九十两0-9]+\s*付\s*[一二三四五六七八九十两0-9]+)"])
    contract_term = _pick([r"(?:合同|租期)[:：]\s*([0-9一二三四五六七八九十两]+\s*(?:年|个月|月))", r"([0-9]+\s*(?:year|years|month|months))"])
    contact = _pick([r"(?:飞机|telegram|tg)[:：]\s*(@[A-Za-z0-9_]+)", r"(?:微信|wechat)[:：]\s*([A-Za-z0-9_]+)", r"(?:电话|phone)[:：]\s*([+0-9]{6,})"])

    prop = "公寓"
    lower = text.lower()
    if "别墅" in text or "villa" in lower:
        prop = "别墅"
    elif "排屋" in text or "townhouse" in lower:
        prop = "排屋"
    elif "商铺" in text or "shophouse" in lower:
        prop = "商铺"

    return {
        "title": title,
        "area": area,
        "layout": layout,
        "property_type": prop,
        "price": int(price) if price.isdigit() else None,
        "payment_terms": payment_terms,
        "contract_term": contract_term,
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
    import json

    d = dict(row)
    for f in ("highlights", "drawbacks"):
        if isinstance(d.get(f), str):
            try:
                d[f] = json.loads(d[f])
            except Exception:
                d[f] = []
    return d


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


def _kb_preview(draft_pk: int) -> InlineKeyboardMarkup:
    p = str(draft_pk)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("👁 预览A", callback_data=f"ap:va:{p}"),
                InlineKeyboardButton("👁 预览B", callback_data=f"ap:vb:{p}"),
                InlineKeyboardButton("👁 预览C", callback_data=f"ap:vc:{p}"),
            ],
            [
                InlineKeyboardButton("✅ 发布A", callback_data=f"ap:na:{p}"),
                InlineKeyboardButton("✅ 发布B", callback_data=f"ap:nb:{p}"),
                InlineKeyboardButton("✅ 发布C", callback_data=f"ap:nc:{p}"),
            ],
            [
                InlineKeyboardButton("🕒 加入队列", callback_data=f"ap:q:{p}"),
            ],
            [
                InlineKeyboardButton("✏️ 修改文案", callback_data=f"ap:e:{p}"),
                InlineKeyboardButton("🖼 重做封面", callback_data=f"ap:c:{p}"),
            ],
            [InlineKeyboardButton("🗑 丢弃", callback_data=f"ap:d:{p}")],
        ]
    )


def build_channel_menu_keyboard() -> InlineKeyboardMarkup:
    """频道置顶帖：四个按钮 2×2（按区域｜按预算｜最新房源｜顾问咨询）。"""
    if DEEPLINK_BOT_USERNAME:
        base = f"https://t.me/{DEEPLINK_BOT_USERNAME}?start="
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("按区域找房", url=f"{base}find_area"),
                    InlineKeyboardButton("按预算找房", url=f"{base}find_budget"),
                ],
                [
                    InlineKeyboardButton("看最新房源", url=f"{base}latest"),
                    InlineKeyboardButton("咨询中文顾问", url=f"{base}advisor"),
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
    b = html.escape(BRAND_NAME)
    return (
        f"<b>🏠 {b}</b>\n\n"
        "看中哪套，点帖内「💬 咨询这套」或「📅 预约看房」，"
        "Bot 会直接接住当前入口，不用重新解释是哪套房。\n\n"
        "👇 点下方按钮主动找房"
    )


def channel_index_html() -> str:
    """频道第二条置顶：区域 / 预算导航索引帖，方便新用户快速定位。"""
    b = html.escape(BRAND_NAME)
    return (
        f"<b>📋 {b} · 房源导航</b>\n\n"
        "按下方按钮快速找房 👇"
    )


async def cmd_post_index(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """发布频道索引帖（第二条置顶）。"""
    if not _is_admin(update.effective_user.id):
        return
    if not CHANNEL_ID:
        await update.message.reply_text("未配置 CHANNEL_ID。")
        return
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
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
        "🧭 <b>侨联发布后台（固定流程版）</b>\n\n"
        "<b>高频 6 个</b>\n"
        "/ops — 一屏总览（待审/已发/队列）\n"
        "/new — 新建房源\n"
        "/pending — 预览待审草稿\n"
        "/send DRF_xxx — 立即发布\n"
        "/slots — 定时时段\n"
        "/logs — 最近发布日志\n"
        "/analytics [days] — 运营报表\n\n"
        "<b>采集源管理</b>\n"
        "/sources — 查看采集源\n"
        "/source_add <name> <type> [mode] [url] — 新增采集源\n"
        "/source_on <key> /source_off <key> — 启停采集源\n\n"
        "<b>兼容旧命令</b>\n"
        "/publish /approve /reject /sources\n\n"
        "<b>运营功能</b>\n"
        "/daily /tpl /post_menu /post_index\n"
        "/intake — 开始微信导入\n"
        "/intake_done — 导入并自动生草稿\n"
        "/intake_pending 或 /wx — 查看微信草稿\n\n"
        "点下方按钮也可以完成大多数操作。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    raw = _slots_raw_effective()
    await update.message.reply_text(
        f"时区：<code>{TZ_NAME}</code>\n"
        f"定时槽：<code>{html.escape(raw)}</code>\n\n"
        "<b>最常用</b>\n"
        "/ops /pending /send /slots /logs /analytics\n\n"
        "<b>微信导入</b>\n"
        "/intake → 发文本和图片 → /intake_done\n"
        "/intake_pending 或 /wx 查看已生成草稿\n\n"
        "<b>采集源</b>\n"
        "/sources /source_add /source_on /source_off\n\n"
        "<b>兼容旧命令</b>\n"
        "/publish /approve /reject",
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
        f"ready队列：<b>{ready}</b>\n"
        f"今日已发：<b>{published_today}</b>\n\n"
        f"队列状态：<b>{'暂停' if paused else '运行'}</b>\n"
        f"每日广播：<b>{'开' if daily_on else '关'}</b>  {html.escape(daily_time)}\n"
        f"发帖时段：<code>{html.escape(slots)}</code>\n\n"
        f"待审核Top：{pending_line}\n"
        f"下一个ready：{ready_line}\n\n"
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
    """兼容旧命令：/approve == /send"""
    await cmd_send(update, context)


async def cmd_reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """兼容旧命令：按 draft_id 丢弃草稿。"""
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("用法：<code>/reject DRF_xxx</code>", parse_mode=ParseMode.HTML)
        return
    draft_id = context.args[0].strip()
    if not re.match(r"^[A-Za-z0-9_\-]+$", draft_id):
        await update.message.reply_text("draft_id 格式不对。")
        return
    with _conn() as c:
        row = c.execute("SELECT id, review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if not row:
            await update.message.reply_text(f"未找到草稿：<code>{html.escape(draft_id)}</code>", parse_mode=ParseMode.HTML)
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
    await update.message.reply_text(f"🗑 已丢弃草稿：<code>{html.escape(draft_id)}</code>", parse_mode=ParseMode.HTML)


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


async def cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("用法：<code>/send DRF_xxxxxxxx</code>", parse_mode=ParseMode.HTML)
        return
    draft_id = context.args[0].strip()
    if not re.match(r"^[A-Za-z0-9_\-]+$", draft_id):
        await update.message.reply_text("draft_id 格式不对。")
        return
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        return
    from meihua_publisher import MeihuaPublisher

    pub = MeihuaPublisher(DB_PATH)
    ok = await asyncio.to_thread(pub.publish_draft, draft_id)
    if ok:
        _log_action(update.effective_user.id, "send_one", draft_id)
        await update.message.reply_text(f"✅ 已发布 <code>{html.escape(draft_id)}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ 发布失败（检查草稿、封面、频道权限）：<code>{html.escape(draft_id)}</code>", parse_mode=ParseMode.HTML)


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
        "📝 <b>微信笔记导入模式</b>\n\n"
        "第 1 步：请发送微信笔记文本（先发文字）。\n"
        "第 2 步：再连续发图片（可多张）。\n"
        "第 3 步：发送 <code>/intake_done</code> 完成入库。\n\n"
        "取消：/intake_cancel 或 /cancel",
        parse_mode=ParseMode.HTML,
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
            parse_msg = f"✅ 草稿已生成：<code>{html.escape(draft_id_gen)}</code>\n可用 <code>/send {html.escape(draft_id_gen)}</code> 发布。"
        else:
            parse_msg = f"⚠️ 解析完成但未找到草稿，可用 <code>/intake_pending</code> 查看。"
    except Exception as e:
        logger.exception("auto parse after intake_done failed")
        parse_msg = f"⚠️ 自动解析失败：{html.escape(str(e))}\n可用 <code>/intake_pending</code> 查看。"

    await update.message.reply_text(
        "✅ 已导入微信笔记\n"
        f"source_post_id: <code>{html.escape(source_post_id)}</code>\n"
        f"title: {html.escape(parsed.get('title') or '-')}\n"
        f"images: {len(images)}\n\n"
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
            out.append("\n可用：<code>/send DRF_xxx</code> 立即发布。")
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
    if not _is_admin(update.effective_user.id):
        return
    limit = 5
    with _conn() as c:
        rows = c.execute(
            f"""SELECT * FROM drafts
                WHERE review_status='pending'
                  AND cover_asset_id IS NOT NULL AND cover_asset_id != ''
                  AND COALESCE(queue_score, 0) >= ?
                ORDER BY queue_score DESC, id DESC LIMIT {limit}""",
            (PREVIEW_MIN_SCORE,),
        ).fetchall()
    if not rows:
        await update.message.reply_text("当前没有达到预览门槛的待审核草稿。")
        return
    from meihua_publisher import build_caption

    for row in rows:
        d = _draft_to_caption_dict(row)
        cap = build_caption(d)
        head = (
            f"📋 预览 <b>#{int(row['id'])}</b> · <code>{html.escape(str(row['draft_id'] or '-'))}</code>\n"
            f"{html.escape(str(row['title'] or '（无标题）'))}\n\n"
        )
        text = head + (cap[:3200] if len(cap) > 3200 else cap)
        img = _cover_path_for_draft(row)
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


def _variant_from_note(note: str | None) -> str:
    if not note:
        return "a"
    m = re.search(r"caption_variant:(a|b|c)", str(note))
    return m.group(1) if m else "a"


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
    caption_variant: str,
) -> None:
    from meihua_publisher import (
        _album_paths_for_draft,
        build_channel_caption,
        build_rich_album_caption,
        evaluate_publish_gate,
    )

    d = _draft_to_caption_dict(row)
    cover = _cover_path_for_draft(row) or ""
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

    caption = build_channel_caption(d, album_all, caption_variant=caption_variant)
    detail = build_rich_album_caption(d, caption_variant=caption_variant)
    head = (
        f"🧪 发帖预览（版本 {caption_variant.upper()}）\n"
        f"草稿：`{row['draft_id']}`  模式：`{gate.get('mode', '-')}`"
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
        text=f"{head}\n\n{detail}",
        parse_mode=ParseMode.HTML,
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

    if act.startswith("n"):
        variant = _variant_from_action(act)
        _save_caption_variant_for_draft(draft_id, variant)
        if not _direct_publish_enabled():
            logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
            return
        from meihua_publisher import MeihuaPublisher

        pub = MeihuaPublisher(DB_PATH)
        ok = await asyncio.to_thread(pub.publish_draft, draft_id, variant)
        if ok:
            _log_action(update.effective_user.id, f"publish_now_{variant}", draft_id)
            await q.edit_message_reply_markup(None)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"✅ 已发布 `{draft_id}`（版本 {variant.upper()}）",
                parse_mode="Markdown",
            )
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ 发布失败 `{draft_id}`")

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
            text=f"请直接回复一句「修正说明」（将写入备注）。\n目标草稿 id=`{draft_id}`\n发送 /cancel 取消。",
            parse_mode="Markdown",
        )


async def on_text_private(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    text = (update.message.text or "").strip()
    mode = context.user_data.get("await")

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
        context.user_data["intake_text"] = text
        context.user_data["await"] = "intake_images"
        await update.message.reply_text(
            "已收到微信笔记文本 ✅\n"
            "现在请连续发送图片（可多张），发完后输入 /intake_done。\n"
            "若只导入文本，也可直接 /intake_done。"
        )
        return

    if mode == "intake_images":
        if text:
            old = str(context.user_data.get("intake_text") or "").strip()
            merged = (old + "\n" + text).strip() if old else text
            context.user_data["intake_text"] = merged[:12000]
            await update.message.reply_text("已追加文本。继续发图片，完成后 /intake_done。")
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


async def on_photo_private(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        return
    if context.user_data.get("await") != "intake_images":
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
        await update.message.reply_text(f"📷 已收图 {len(bucket)} 张")
    except Exception:
        logger.exception("save intake photo failed")
        await update.message.reply_text("图片保存失败，请重发，或直接 /intake_done 先导入文本。")


async def scheduled_publish(context: ContextTypes.DEFAULT_TYPE) -> None:
    if _scheduler_paused():
        logger.info("定时房源帖：暂停，跳过")
        return
    with _conn() as c:
        row = c.execute(
            """SELECT id, draft_id, review_note FROM drafts
               WHERE review_status='ready'
               ORDER BY queue_score DESC, id ASC LIMIT 1"""
        ).fetchone()
    if not row:
        logger.info("定时房源帖：ready 为空")
        return
    draft_id = row["draft_id"]
    from media_consistency import assess_draft_media, mark_draft_media_broken, media_blocks_ready, media_issue_summary

    media_status = assess_draft_media(draft_id, DB_PATH)
    if media_blocks_ready(media_status):
        mark_draft_media_broken(draft_id, media_status, DB_PATH)
        with _conn() as c:
            c.execute(
                "UPDATE drafts SET review_status='pending', updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
                (draft_id,),
            )
            c.commit()
        logger.warning(
            "定时房源帖跳过并退回 pending：%s media=%s",
            draft_id,
            media_issue_summary(media_status),
        )
        return
    from meihua_publisher import MeihuaPublisher

    variant = _variant_from_note(row["review_note"])
    if not _direct_publish_enabled():
        logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
        return
    pub = MeihuaPublisher(DB_PATH)
    ok = await asyncio.to_thread(pub.publish_draft, draft_id, variant)
    if not ok:
        _return_publish_blocked_to_pending(draft_id)
        logger.warning(
            "publish gate blocked, returning draft to pending: %s",
            draft_id,
        )
        return
    logger.info("定时房源帖 %s 版本 %s → %s", draft_id, variant.upper(), ok)


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
) -> None:
    """
    将队列/预览/定时/运营命令挂到已有 Application（与 v2 共用 meihua666 时调用）。
    group=-1：命令优先于 v2 会话；group=1：仅当会话未消费时处理 autopilot 的自由文本。
    """
    grp_cmd = -1
    grp_txt = 1
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
