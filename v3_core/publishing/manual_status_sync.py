"""Best-effort sync of an explicit Publisher inventory status to one published post.

The administrator has already chosen the target inventory status before this
module runs. This module never derives or changes that choice, never searches
Telegram history, and never creates a replacement post. The exact Telegram
message identity comes only from ``publication_instances``.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Any

from telegram.constants import ParseMode

from v3_core.status_labels import inventory_status_presentation

from .channel_contract import channel_action_url
from .telegram_adapter import build_channel_keyboard


_STATUS_RE = re.compile(r"(?m)^[🟢🟡🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*")


@dataclass(frozen=True)
class ManualStatusSyncResult:
    listing_id: str
    status: str
    attempted: bool
    synced: bool
    channel_chat_id: str = ""
    channel_message_id: str = ""
    error: str = ""


def caption_with_inventory_status(
    caption: object,
    *,
    status: object,
    public_listing_id: object,
) -> str:
    raw = str(caption or "").strip()
    public_id = str(public_listing_id or "").strip()
    icon, label = inventory_status_presentation(status)
    status_line = f"{icon} {label}　{public_id}".strip()
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
    return (raw + ("\n\n" if raw else "") + status_line).strip()[:1024]


class PublisherManualStatusSynchronizer:
    def __init__(self, db_path: str | Path, *, user_bot_username: str):
        self.db_path = Path(db_path).expanduser().resolve()
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _published_row(self, listing_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT pi.id AS publication_row_id,pi.channel_chat_id,
                          pi.channel_message_id,pi.post_text,l.public_listing_id
                   FROM publication_instances pi
                   JOIN listings_v3 l ON l.listing_id=pi.listing_id
                   WHERE pi.listing_id=?
                     AND pi.platform='telegram'
                     AND pi.publish_status='published'
                     AND CAST(COALESCE(pi.channel_message_id,'0') AS INTEGER)>0
                   ORDER BY pi.updated_at DESC,pi.id DESC
                   LIMIT 1""",
                (str(listing_id or "").strip(),),
            ).fetchone()
        return dict(row) if row is not None else None

    async def sync(self, bot: Any, *, listing_id: str, status: str) -> ManualStatusSyncResult:
        clean_listing = str(listing_id or "").strip()
        clean_status = str(status or "").strip().lower()
        if not clean_listing:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="listing_id_required")
        if not self.user_bot_username:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="user_bot_username_missing")
        row = self._published_row(clean_listing)
        if row is None:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="published_instance_not_found")
        try:
            public_id = str(row.get("public_listing_id") or "").strip()
            if not public_id:
                raise ValueError("public_listing_id_missing")
            chat_id = str(row.get("channel_chat_id") or "").strip()
            message_id = int(str(row.get("channel_message_id") or "0"))
            caption = caption_with_inventory_status(
                row.get("post_text"),
                status=clean_status,
                public_listing_id=public_id,
            )
            actions = {
                action: channel_action_url(self.user_bot_username, public_id, action)
                for action in ("details", "photos", "book")
            }
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=message_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=build_channel_keyboard(actions, inventory_status=clean_status),
            )
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE publication_instances SET post_text=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (caption, int(row["publication_row_id"])),
                )
                conn.commit()
            return ManualStatusSyncResult(
                clean_listing,
                clean_status,
                True,
                True,
                channel_chat_id=chat_id,
                channel_message_id=str(message_id),
            )
        except Exception as exc:
            return ManualStatusSyncResult(
                clean_listing,
                clean_status,
                True,
                False,
                channel_chat_id=str(row.get("channel_chat_id") or ""),
                channel_message_id=str(row.get("channel_message_id") or ""),
                error=str(exc),
            )


__all__ = [
    "ManualStatusSyncResult",
    "PublisherManualStatusSynchronizer",
    "caption_with_inventory_status",
]
