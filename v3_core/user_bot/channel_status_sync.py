"""Best-effort V3 appointment status sync for the exact published Telegram post.

This keeps the locked-production appointment status contract while reading only
V3 tables.  It never searches Telegram history and never edits a guessed post:
the target chat/message identity comes from ``publication_instances``.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from v3_core.publishing.channel_contract import channel_action_url
from v3_core.status_labels import inventory_status_bookable, inventory_status_presentation

from .appointments import ACTIVE_APPOINTMENT_STATUSES

logger = logging.getLogger(__name__)

APPOINTMENT_LOCK_COUNT = 5
_STATUS_RE = re.compile(r"(?m)^[🟢🟡🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*")


def derive_appointment_inventory_status(current_status: str, active_count: int) -> str:
    """Match locked SHA ``_apply_appointment_lock`` semantics."""
    current = str(current_status or "").strip().lower()
    count = max(0, int(active_count or 0))
    if current in {"rented", "inactive", "offline"}:
        return current
    if current == "pending" and count < APPOINTMENT_LOCK_COUNT:
        return current
    if current not in {"active", "reserved", "pending"}:
        return current or "pending"
    if count >= APPOINTMENT_LOCK_COUNT:
        return "pending"
    if count >= 1:
        return "reserved"
    return "active"


def appointment_status_label(status: str, active_count: int = 0) -> str:
    icon, label = inventory_status_presentation(status)
    return f"{icon} {label}"


def caption_with_appointment_status(
    caption: str,
    *,
    status: str,
    active_count: int,
    public_listing_id: str,
) -> str:
    raw = str(caption or "").strip()
    status_line = f"{appointment_status_label(status, active_count)}　{str(public_listing_id or '').strip()}".strip()
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


def appointment_channel_keyboard(
    *,
    username: str,
    public_listing_id: str,
    status: str,
) -> InlineKeyboardMarkup:
    details = InlineKeyboardButton(
        "🏠 租赁详情",
        url=channel_action_url(username, public_listing_id, "details"),
    )
    photos = InlineKeyboardButton(
        "📸 更多实拍",
        url=channel_action_url(username, public_listing_id, "photos"),
    )
    rows = [[details, photos]]
    if inventory_status_bookable(status):
        rows.append(
            [
                InlineKeyboardButton(
                    "📅 预约看房",
                    url=channel_action_url(username, public_listing_id, "book"),
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


@dataclass(frozen=True)
class ChannelStatusSyncResult:
    listing_id: str
    attempted: bool
    synced: bool
    previous_status: str = ""
    next_status: str = ""
    active_appointment_count: int = 0
    channel_chat_id: str = ""
    channel_message_id: str = ""
    error: str = ""


class V3AppointmentChannelSynchronizer:
    def __init__(
        self,
        db_path: str | Path,
        *,
        publisher_bot_token: str,
        user_bot_username: str,
        bot_factory: Callable[[str], Any] | None = None,
    ):
        self.db_path = Path(db_path).expanduser().resolve()
        self.publisher_bot_token = str(publisher_bot_token or "").strip()
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        self.bot_factory = bot_factory or (lambda token: Bot(token=token))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _snapshot(self, listing_id: str) -> tuple[dict[str, Any] | None, int]:
        statuses = tuple(sorted(ACTIVE_APPOINTMENT_STATUSES))
        placeholders = ",".join("?" for _ in statuses)
        with self._connect() as conn:
            row = conn.execute(
                """SELECT pi.id AS publication_row_id,pi.channel_chat_id,
                          pi.channel_message_id,pi.post_text,
                          l.inventory_status,l.public_listing_id
                   FROM publication_instances pi
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   WHERE pi.listing_id=? AND pi.platform='telegram'
                     AND pi.publish_status='published'
                     AND CAST(COALESCE(pi.channel_message_id,'0') AS INTEGER)>0
                   ORDER BY pi.updated_at DESC,pi.id DESC
                   LIMIT 1""",
                (listing_id,),
            ).fetchone()
            count_row = conn.execute(
                f"""SELECT COUNT(*) AS count FROM appointments_v3
                    WHERE listing_id=? AND status IN ({placeholders})""",
                (listing_id, *statuses),
            ).fetchone()
        return (dict(row) if row is not None else None, int(count_row["count"] if count_row else 0))

    async def sync(self, listing_id: str) -> ChannelStatusSyncResult:
        clean = str(listing_id or "").strip()
        if not clean:
            return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="listing_id_required")
        if not self.publisher_bot_token or not self.user_bot_username:
            return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="channel_sync_not_configured")
        try:
            row, active_count = self._snapshot(clean)
            if row is None:
                return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="published_instance_not_found")
            previous = str(row.get("inventory_status") or "").strip().lower()
            target = derive_appointment_inventory_status(previous, active_count)
            public_id = str(row.get("public_listing_id") or "").strip()
            caption = caption_with_appointment_status(
                str(row.get("post_text") or ""),
                status=target,
                active_count=active_count,
                public_listing_id=public_id,
            )
            chat_id = str(row.get("channel_chat_id") or "").strip()
            message_id = int(str(row.get("channel_message_id") or "0"))
            bot = self.bot_factory(self.publisher_bot_token)
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=message_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=appointment_channel_keyboard(
                    username=self.user_bot_username,
                    public_listing_id=public_id,
                    status=target,
                ),
            )
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                if target != previous:
                    conn.execute(
                        "UPDATE listings_v3 SET inventory_status=?,updated_at=CURRENT_TIMESTAMP WHERE listing_id=?",
                        (target, clean),
                    )
                conn.execute(
                    "UPDATE publication_instances SET post_text=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (caption, int(row["publication_row_id"])),
                )
                conn.commit()
            return ChannelStatusSyncResult(
                clean,
                attempted=True,
                synced=True,
                previous_status=previous,
                next_status=target,
                active_appointment_count=active_count,
                channel_chat_id=chat_id,
                channel_message_id=str(message_id),
            )
        except Exception as exc:
            logger.exception("V3 channel appointment status sync failed: listing_id=%s", clean)
            return ChannelStatusSyncResult(clean, attempted=True, synced=False, error=str(exc))


__all__ = [
    "APPOINTMENT_LOCK_COUNT",
    "ChannelStatusSyncResult",
    "V3AppointmentChannelSynchronizer",
    "appointment_channel_keyboard",
    "appointment_status_label",
    "caption_with_appointment_status",
    "derive_appointment_inventory_status",
]
