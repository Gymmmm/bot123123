"""Best-effort V3 appointment status sync for the exact published Telegram post.

This keeps the locked-production appointment status contract while reading only
V3 tables.  It never searches Telegram history and never edits a guessed post:
the target chat/message identity comes from ``publication_instances``.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from v3_core.publishing.channel_contract import (
    assert_channel_identity,
    official_channel_action_urls,
    official_channel_button_spec,
)

from .appointments import ACTIVE_APPOINTMENT_STATUSES

logger = logging.getLogger(__name__)

APPOINTMENT_LOCK_COUNT = 5
_STATUS_RE = re.compile(r"(?m)^[🟢🟡🟠🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*")


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
    clean = str(status or "").strip().lower()
    return {
        "active": "🟢 当前可预约",
        "reserved": "🟡 已有预约，仍可预约",
        "high_demand": "🟠 预约较多",
        "pending": "🔵 房态确认中",
        "rented": "🔴 已租出",
        "inactive": "⚫ 已下架",
        "offline": "⚫ 已下架",
        "withdrawn": "⚫ 已下架",
    }.get(clean, "🔵 房态确认中")


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
    advisor_url: str,
    area: str = "",
) -> InlineKeyboardMarkup:
    actions = official_channel_action_urls(
        username, public_listing_id, advisor_url=advisor_url
    )
    rows = [
        [InlineKeyboardButton(label, url=url) for label, url in row]
        for row in official_channel_button_spec(actions, inventory_status=status, area=area)
    ]
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
        advisor_url: str = "",
        bot_factory: Callable[[str], Any] | None = None,
        appointment_table: str = "appointments_v3",
    ):
        self.db_path = Path(db_path).expanduser().resolve()
        self.publisher_bot_token = str(publisher_bot_token or "").strip()
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        self.advisor_url = str(advisor_url or "").strip()
        self.bot_factory = bot_factory or (lambda token: Bot(token=token))
        clean_table = str(appointment_table or "appointments_v3").strip()
        if clean_table not in {"appointments_v3", "appointments"}:
            raise ValueError("unsupported_appointment_table")
        self.appointment_table = clean_table

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
                          pi.channel_message_id,pi.post_text,pi.listing_id AS publication_listing_id,
                          p.listing_id AS package_listing_id,p.snapshot_json,p.actions_json,
                          l.inventory_status,l.public_listing_id AS live_public_listing_id,\n                          l.public_location_key
                   FROM publication_instances pi
                   JOIN publication_packages_v3 p ON p.package_id=pi.package_id
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   WHERE pi.listing_id=? AND pi.platform='telegram'
                     AND pi.publish_status='published'
                     AND CAST(COALESCE(pi.channel_message_id,'0') AS INTEGER)>0
                   ORDER BY pi.updated_at DESC,pi.id DESC
                   LIMIT 1""",
                (listing_id,),
            ).fetchone()
            count_row = conn.execute(
                f"""SELECT COUNT(*) AS count FROM {self.appointment_table}
                    WHERE listing_id=? AND status IN ({placeholders})""",
                (listing_id, *statuses),
            ).fetchone()
        return (dict(row) if row is not None else None, int(count_row["count"] if count_row else 0))

    async def sync(self, listing_id: str) -> ChannelStatusSyncResult:
        clean = str(listing_id or "").strip()
        if not clean:
            return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="listing_id_required")
        if not self.publisher_bot_token or not self.user_bot_username or not self.advisor_url:
            return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="channel_sync_not_configured")
        try:
            row, active_count = self._snapshot(clean)
            if row is None:
                return ChannelStatusSyncResult(clean, attempted=False, synced=False, error="published_instance_not_found")
            previous = str(row.get("inventory_status") or "").strip().lower()
            snapshot = json.loads(str(row.get("snapshot_json") or "{}"))
            public_id = str(snapshot.get("public_listing_id") or "").strip()
            frozen_listing_id = str(snapshot.get("listing_id") or "").strip()
            if clean != str(row.get("publication_listing_id") or "") or clean != str(row.get("package_listing_id") or "") or clean != frozen_listing_id:
                raise ValueError("channel_identity_listing_id_mismatch")
            if public_id != str(row.get("live_public_listing_id") or "").strip():
                raise ValueError("channel_identity_public_listing_id_mismatch")
            frozen_actions = json.loads(str(row.get("actions_json") or "{}"))
            assert_channel_identity(caption=row.get("post_text"), actions=frozen_actions, public_listing_id=public_id)
            chat_id = str(row.get("channel_chat_id") or "").strip()
            message_id = int(str(row.get("channel_message_id") or "0"))

            # Refresh durable inventory immediately before Telegram I/O so a
            # status change after _snapshot() is reflected in the projection.
            with self._connect() as conn:
                current_row = conn.execute(
                    "SELECT inventory_status FROM listings_v3 WHERE listing_id=?",
                    (clean,),
                ).fetchone()
            target = (
                str(current_row["inventory_status"] or "").strip().lower()
                if current_row is not None
                else previous
            )
            caption = caption_with_appointment_status(
                str(row.get("post_text") or ""),
                status=target,
                active_count=active_count,
                public_listing_id=public_id,
            )
            keyboard = appointment_channel_keyboard(
                username=self.user_bot_username,
                public_listing_id=public_id,
                status=target,
                advisor_url=self.advisor_url,
                area=str(row.get("public_location_key") or ""),
            )
            bot = self.bot_factory(self.publisher_bot_token)
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=message_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
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
