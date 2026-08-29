"""把预约/管理端房态同步到该房源最新的频道主帖。

这里只改状态行和行动按钮，不重新解析或改写任何房源事实。
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from .config import DB_PATH, USER_BOT_USERNAME

logger = logging.getLogger(__name__)

_STATUS_RE = re.compile(
    r"(?m)^[🟢🟡🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*(?:\n+)?"
)


def _status_label(status: str) -> str:
    return {
        "active": "🟢 当前可预约",
        "reserved": "🟡 已有预约 · 仍可预约",
        "pending": "🔵 房态待确认",
        "rented": "🔴 已租出",
        "inactive": "⚫ 已下架",
    }.get(str(status or "").strip().lower(), "🔵 房态待确认")


def _caption_with_status(caption: str, status: str) -> str:
    cleaned = _STATUS_RE.sub("", str(caption or "").strip()).strip()
    label = _status_label(status)
    lines = cleaned.splitlines()
    tag_index = next((i for i, line in enumerate(lines) if line.lstrip().startswith("#")), len(lines))
    before = lines[:tag_index]
    after = lines[tag_index:]
    while before and not before[-1].strip():
        before.pop()
    parts = before + ["", label]
    if after:
        parts += [""] + after
    return "\n".join(parts).strip()[:1024]


def _keyboard(username: str, token: str, listing_id: str, status: str) -> InlineKeyboardMarkup:
    base = f"https://t.me/{username}?start="
    photos = InlineKeyboardButton("📸 更多实拍", url=f"{base}photos_{listing_id}")
    helper = InlineKeyboardButton(
        "🤖 侨联找房助手",
        url=f"{base}view_{listing_id}",
    )
    if status in {"active", "reserved"}:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 预约看房", url=f"{base}book_{listing_id}"), photos],
            [helper],
        ])
    return InlineKeyboardMarkup([[photos], [helper]])


async def sync_channel_listing_status(listing_id: str) -> bool:
    listing_id = str(listing_id or "").strip()
    token = str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip()
    username = str(USER_BOT_USERNAME or "").strip().lstrip("@")
    channel_id = str(os.getenv("CHANNEL_ID") or "").strip()
    if not listing_id or not token or not username or not channel_id:
        return False
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT p.id AS post_row_id, p.channel_message_id, p.post_text,
                          l.status, pp.public_token
                   FROM posts p
                   JOIN listings l ON l.listing_id=p.listing_id
                   LEFT JOIN publication_packages pp ON pp.package_id=p.publication_package_id
                   WHERE p.listing_id=? AND p.publish_status='published'
                     AND CAST(COALESCE(p.channel_message_id,'0') AS INTEGER)>0
                   ORDER BY CAST(p.channel_message_id AS INTEGER) DESC LIMIT 1""",
                (listing_id,),
            ).fetchone()
            if not row or not str(row["public_token"] or "").startswith("ql"):
                return False
            message_id = int(row["channel_message_id"])
            status = str(row["status"] or "pending").strip().lower()
            caption = _caption_with_status(str(row["post_text"] or ""), status)
            markup = _keyboard(username, str(row["public_token"]), listing_id, status)
            await Bot(token=token).edit_message_caption(
                chat_id=channel_id,
                message_id=message_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup,
            )
            conn.execute(
                "UPDATE posts SET post_text=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (caption, int(row["post_row_id"])),
            )
        return True
    except Exception:
        logger.exception("频道房态同步失败: listing_id=%s", listing_id)
        return False
