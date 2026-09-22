"""把预约/管理端房态同步到该房源最新的频道主帖。

只更新现代 publication instance 的频道展示状态，不改 durable inventory truth，
不重新解析或改写任何 frozen publication facts。
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from .channel_links import channel_action_url
from .config import DB_PATH, USER_BOT_USERNAME

logger = logging.getLogger(__name__)

APPOINTMENT_LOCK_COUNT = 5
_ACTIVE_APPOINTMENT_STATUSES = ("pending", "assigned", "contacted", "confirmed")

_STATUS_RE = re.compile(r"(?m)^[🟢🟡🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*")
_PUBLIC_ID_RE = re.compile(r"\b(?:QL-[A-HJ-NP-Z2-9]{2,4}-[A-HJ-NP-Z][2-9][A-HJ-NP-Z][2-9]|QL-[A-HJ-NP-Z2-9]{6}|QC\d{3,8})\b", re.I)


def _active_appointment_count(conn: sqlite3.Connection, listing_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM appointments WHERE listing_id=? AND status IN (?,?,?,?)",
        (listing_id, *_ACTIVE_APPOINTMENT_STATUSES),
    ).fetchone()
    return int(row[0] if row else 0)


def _effective_status(status: str, appointment_count: int) -> str:
    """Derive channel display state without mutating durable inventory_status."""
    status = str(status or "").strip().lower()
    if status in {"rented", "inactive", "offline", "pending"}:
        return status
    if status not in {"active", "reserved"}:
        return status or "pending"
    if appointment_count >= APPOINTMENT_LOCK_COUNT:
        return "pending"
    if appointment_count >= 1:
        return "reserved"
    return status


def _status_label(status: str, appointment_count: int = 0) -> str:
    status = str(status or "").strip().lower()
    if status == "pending" and appointment_count >= APPOINTMENT_LOCK_COUNT:
        return "🔵 已有5份预约看房，房态待确认"
    return {
        "active": "🟢 当前可预约",
        "reserved": "🟡 已有预约 · 仍可预约",
        "pending": "🔵 房态待确认",
        "rented": "🔴 已租出",
        "inactive": "⚫ 已下架",
        "offline": "⚫ 已下架",
    }.get(status, "🔵 房态待确认")


def _caption_with_status(caption: str, status: str, appointment_count: int = 0, public_listing_id: str = "") -> str:
    raw = str(caption or "").strip()
    label = _status_label(status, appointment_count)
    found = _PUBLIC_ID_RE.search(raw)
    public_id = found.group(0).upper() if found else str(public_listing_id or "").strip().upper()
    status_line = f"{label}　{public_id}" if public_id else label
    if _STATUS_RE.search(raw):
        replaced = False
        lines: list[str] = []
        for line in raw.splitlines():
            if _STATUS_RE.match(line):
                if replaced:
                    continue
                lines.append(status_line)
                replaced = True
            else:
                lines.append(line)
        return "\n".join(lines).strip()[:1024]
    lines = raw.splitlines()
    tag_index = next((i for i, line in enumerate(lines) if line.lstrip().startswith("#")), len(lines))
    before = lines[:tag_index]
    after = lines[tag_index:]
    while before and not before[-1].strip():
        before.pop()
    parts = before + ["", status_line]
    if after:
        parts += [""] + after
    return "\n".join(parts).strip()[:1024]


def _keyboard(username: str, public_listing_id: str, status: str) -> InlineKeyboardMarkup:
    details = InlineKeyboardButton("📷 房源详情", url=channel_action_url(username, public_listing_id, "photos"))
    if status in {"active", "reserved"}:
        return InlineKeyboardMarkup([
            [details],
            [InlineKeyboardButton("📅 预约看房", url=channel_action_url(username, public_listing_id, "book"))],
        ])
    return InlineKeyboardMarkup([[details]])


async def sync_channel_listing_status(listing_id: str) -> bool:
    listing_id = str(listing_id or "").strip()
    token = str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip()
    username = str(USER_BOT_USERNAME or "").strip().lstrip("@")
    if not listing_id or not token or not username:
        return False
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT pi.id AS instance_row_id,
                          pi.channel_chat_id,
                          pi.channel_message_id,
                          pi.post_text,
                          l.inventory_status,
                          l.public_listing_id,
                          pp.public_token
                   FROM publication_instances pi
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   JOIN listing_offers o ON o.offer_id=pi.offer_id
                   LEFT JOIN publication_packages_v3 pp ON pp.package_id=pi.package_id
                   WHERE pi.listing_id=?
                     AND pi.platform='telegram'
                     AND pi.publish_status='published'
                     AND o.offer_type='rent'
                     AND o.publication_policy='telegram_rent'
                     AND CAST(COALESCE(pi.channel_message_id,'0') AS INTEGER)>0
                   ORDER BY pi.updated_at DESC, pi.id DESC
                   LIMIT 1""",
                (listing_id,),
            ).fetchone()
            if not row:
                return False

            public_id = str(row["public_listing_id"] or "").strip()
            chat_id = str(row["channel_chat_id"] or os.getenv("CHANNEL_ID") or "").strip()
            if not public_id or not chat_id:
                return False

            message_id = int(row["channel_message_id"])
            appointment_count = _active_appointment_count(conn, listing_id)
            status = _effective_status(
                str(row["inventory_status"] or "pending"),
                appointment_count,
            )
            caption = _caption_with_status(
                str(row["post_text"] or ""),
                status,
                appointment_count,
                public_id,
            )
            markup = _keyboard(username, public_id, status)

            await Bot(token=token).edit_message_caption(
                chat_id=chat_id,
                message_id=message_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=markup,
            )
            conn.execute(
                "UPDATE publication_instances SET post_text=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (caption, int(row["instance_row_id"])),
            )
        return True
    except Exception:
        logger.exception("频道房态同步失败: listing_id=%s", listing_id)
        return False
