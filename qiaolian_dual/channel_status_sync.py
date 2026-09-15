"""把预约/管理端房态同步到该房源最新的频道主帖。

这里只改状态行和行动按钮，不重新解析或改写任何房源事实。
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from .channel_links import channel_action_url, public_qc_code
from .config import DB_PATH, USER_BOT_USERNAME

logger = logging.getLogger(__name__)

APPOINTMENT_LOCK_COUNT = 5
_ACTIVE_APPOINTMENT_STATUSES = ("pending", "assigned", "contacted", "confirmed")

_STATUS_RE = re.compile(
    r"(?m)^[🟢🟡🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*"
)
_PUBLIC_ID_RE = re.compile(r"\b(?:QL-[A-HJ-NP-Z2-9]{6}|QC\d{3,8})\b", re.I)


def _active_appointment_count(conn: sqlite3.Connection, listing_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM appointments WHERE listing_id=? AND status IN (?,?,?,?)",
        (listing_id, *_ACTIVE_APPOINTMENT_STATUSES),
    ).fetchone()
    return int(row[0] if row else 0)


def _apply_appointment_lock(
    conn: sqlite3.Connection,
    listing_id: str,
    status: str,
    appointment_count: int,
) -> str:
    """Derive the bookable channel state from active appointment count.

    Terminal/manual hold states stay authoritative. For a bookable listing the
    public state is deterministic: 0 -> active, 1-4 -> reserved, >=5 -> pending.
    """
    status = str(status or "").strip().lower()
    if status in {"rented", "inactive", "offline"}:
        return status
    if status == "pending" and appointment_count < APPOINTMENT_LOCK_COUNT:
        return status
    if status not in {"active", "reserved", "pending"}:
        return status or "pending"

    if appointment_count >= APPOINTMENT_LOCK_COUNT:
        target = "pending"
    elif appointment_count >= 1:
        target = "reserved"
    else:
        target = "active"

    if target != status:
        conn.execute(
            "UPDATE listings SET status=?, updated_at=datetime('now','localtime') "
            "WHERE listing_id=? AND status IN ('active','reserved','pending')",
            (target, listing_id),
        )
    return target


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


def _caption_with_status(
    caption: str,
    status: str,
    appointment_count: int = 0,
    listing_id: str = "",
) -> str:
    raw = str(caption or "").strip()
    label = _status_label(status, appointment_count)
    found = _PUBLIC_ID_RE.search(raw)
    qc = found.group(0).upper() if found else public_qc_code(listing_id)
    status_line = f"{label}　{qc}" if qc else label

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


def _keyboard(username: str, token: str, listing_id: str, status: str) -> InlineKeyboardMarkup:
    _ = token
    details = InlineKeyboardButton(
        "🏠 房源详情", url=channel_action_url(username, listing_id, "details")
    )
    photos = InlineKeyboardButton(
        "📸 更多实拍", url=channel_action_url(username, listing_id, "photos")
    )
    if status in {"active", "reserved"}:
        return InlineKeyboardMarkup([
            [details, photos],
            [InlineKeyboardButton(
                "📅 预约看房", url=channel_action_url(username, listing_id, "book")
            )],
        ])
    return InlineKeyboardMarkup([[details, photos]])


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
            if not row:
                return False
            message_id = int(row["channel_message_id"])
            appointment_count = _active_appointment_count(conn, listing_id)
            status = _apply_appointment_lock(
                conn,
                listing_id,
                str(row["status"] or "pending").strip().lower(),
                appointment_count,
            )
            caption = _caption_with_status(
                str(row["post_text"] or ""),
                status,
                appointment_count,
                listing_id,
            )
            markup = _keyboard(username, str(row["public_token"] or ""), listing_id, status)
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
