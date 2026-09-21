"""Best-effort sync of an explicit Publisher inventory status to one published post.

The administrator has already chosen the target inventory status before this
module runs. This module never derives or changes that choice, never searches
Telegram history, and never creates a replacement post. The exact Telegram
message identity comes only from ``publication_instances``.
"""
from __future__ import annotations

from dataclasses import dataclass
import asyncio
from pathlib import Path
import json
import re
import sqlite3
import time
from typing import Any

from telegram.constants import ParseMode

from .channel_contract import assert_channel_identity, channel_action_url, official_channel_action_urls
from .channel_renderer import channel_status_presentation
from .telegram_adapter import build_channel_keyboard


_STATUS_RE = re.compile(r"(?m)^[🟢🟡🟠🔵🔴⚫]️?\s*(?:房源状态｜)?[^\n]*")


@dataclass(frozen=True)
class ManualStatusSyncResult:
    listing_id: str
    status: str
    attempted: bool
    synced: bool
    channel_chat_id: str = ""
    channel_message_id: str = ""
    error: str = ""


def _frozen_public_id(row: dict[str, Any], listing_id: str) -> str:
    snapshot = json.loads(str(row.get("snapshot_json") or "{}"))
    public_id = str(snapshot.get("public_listing_id") or "").strip()
    frozen_listing_id = str(snapshot.get("listing_id") or "").strip()
    clean_listing = str(listing_id or "").strip()
    if (
        clean_listing != str(row.get("publication_listing_id") or "")
        or clean_listing != str(row.get("package_listing_id") or "")
        or clean_listing != frozen_listing_id
    ):
        raise ValueError("channel_identity_listing_id_mismatch")
    if public_id != str(row.get("live_public_listing_id") or "").strip():
        raise ValueError("channel_identity_public_listing_id_mismatch")
    frozen_actions = json.loads(str(row.get("actions_json") or "{}"))
    assert_channel_identity(
        caption=row.get("post_text"),
        actions=frozen_actions,
        public_listing_id=public_id,
    )
    return public_id


def caption_with_inventory_status(
    caption: object,
    *,
    status: object,
    public_listing_id: object,
) -> str:
    raw = str(caption or "").strip()
    public_id = str(public_listing_id or "").strip()
    icon, label = channel_status_presentation(status)
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
    def __init__(self, db_path: str | Path, *, user_bot_username: str, advisor_url: str = ""):
        self.db_path = Path(db_path).expanduser().resolve()
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        self.advisor_url = str(advisor_url or "").strip()
        self._flush_lock = asyncio.Lock()
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS publisher_status_sync_outbox_v3 (
                    listing_id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 1,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '');
                CREATE TRIGGER IF NOT EXISTS v3_inventory_status_outbox
                AFTER UPDATE OF inventory_status ON listings_v3
                WHEN OLD.inventory_status IS NOT NEW.inventory_status
                BEGIN
                    INSERT INTO publisher_status_sync_outbox_v3(listing_id) VALUES(NEW.listing_id)
                    ON CONFLICT(listing_id) DO UPDATE SET revision=revision+1,
                        attempts=0,next_attempt=0,last_error='';
                END;
            """)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _published_row(self, listing_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT pi.id AS publication_row_id,pi.channel_chat_id,
                          pi.channel_message_id,pi.post_text,pi.listing_id AS publication_listing_id,
                          p.listing_id AS package_listing_id,p.snapshot_json,p.actions_json,
                          l.public_listing_id AS live_public_listing_id
                   FROM publication_instances pi
                   JOIN publication_packages_v3 p ON p.package_id=pi.package_id
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

    async def sync(self, bot: Any, *, listing_id: str, status: str, _row=None) -> ManualStatusSyncResult:
        if _row is None:
            with self._connect() as conn:
                conn.execute("""INSERT INTO publisher_status_sync_outbox_v3(listing_id) VALUES (?)
                    ON CONFLICT(listing_id) DO UPDATE SET next_attempt=0""", (listing_id,))
            return await self._flush_listing(bot, listing_id)
        clean_listing = str(listing_id or "").strip()
        clean_status = str(status or "").strip().lower()
        if not clean_listing:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="listing_id_required")
        if not self.user_bot_username:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="user_bot_username_missing")
        row = _row
        if row is None:
            return ManualStatusSyncResult(clean_listing, clean_status, False, False, error="published_instance_not_found")
        try:
            public_id = _frozen_public_id(row, clean_listing)
            chat_id = str(row.get("channel_chat_id") or "").strip()
            message_id = int(str(row.get("channel_message_id") or "0"))
            caption = caption_with_inventory_status(
                row.get("post_text"),
                status=clean_status,
                public_listing_id=public_id,
            )
            actions = (
                official_channel_action_urls(
                    self.user_bot_username, public_id, advisor_url=self.advisor_url
                )
                if self.advisor_url
                else {
                    action: channel_action_url(self.user_bot_username, public_id, action)
                    for action in ("details", "photos", "book")
                }
            )
            try:
                await bot.edit_message_caption(
                    chat_id=chat_id, message_id=message_id, caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_channel_keyboard(actions, inventory_status=clean_status),
                )
            except Exception as exc:
                from telegram.error import BadRequest
                if not isinstance(exc, BadRequest) or "message is not modified" not in str(exc).lower():
                    raise
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE publication_instances SET post_text=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND listing_id=?",
                    (caption, int(row["publication_row_id"]), clean_listing),
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

    async def _flush_listing(self, bot, listing_id):
        async with self._flush_lock:
            return await self._flush_listing_unlocked(bot, listing_id)

    async def _flush_listing_unlocked(self, bot, listing_id):
        # Read the current status, never replay a stale administrator choice.
        with self._connect() as conn:
            job = conn.execute("SELECT * FROM publisher_status_sync_outbox_v3 WHERE listing_id=?", (listing_id,)).fetchone()
            listing = conn.execute("SELECT inventory_status FROM listings_v3 WHERE listing_id=?", (listing_id,)).fetchone()
            rows = conn.execute("""SELECT pi.id AS publication_row_id,pi.channel_chat_id,
                pi.channel_message_id,pi.post_text,pi.listing_id AS publication_listing_id,
                p.listing_id AS package_listing_id,p.snapshot_json,p.actions_json,
                l.public_listing_id AS live_public_listing_id
                FROM publication_instances pi
                JOIN publication_packages_v3 p ON p.package_id=pi.package_id
                JOIN listings_v3 l ON l.listing_id=pi.listing_id
                WHERE pi.listing_id=? AND pi.platform='telegram' AND pi.publish_status='published'
                AND CAST(COALESCE(pi.channel_message_id,'0') AS INTEGER)>0 ORDER BY pi.id""", (listing_id,)).fetchall()
        status = str(listing[0]) if listing else ""
        result = ManualStatusSyncResult(listing_id, status, False, False, error="published_instance_not_found")
        try:
            for row in rows:
                _frozen_public_id(dict(row), listing_id)
        except Exception as exc:
            result = ManualStatusSyncResult(
                listing_id, status, True, False, error=str(exc)
            )
        else:
            for row in rows:
                result = await self.sync(bot, listing_id=listing_id, status=status, _row=dict(row))
                if not result.synced:
                    break
        if job:
            with self._connect() as conn:
                if not rows or result.synced:
                    conn.execute("DELETE FROM publisher_status_sync_outbox_v3 WHERE listing_id=? AND revision=?", (listing_id, job["revision"]))
                else:
                    delay = min(600, 10 * 2 ** min(job["attempts"], 6))
                    conn.execute("""UPDATE publisher_status_sync_outbox_v3 SET attempts=attempts+1,
                        next_attempt=?,last_error=? WHERE listing_id=? AND revision=?""",
                        (time.time()+delay, type(result).__name__ + ":sync_failed", listing_id, job["revision"]))
        return result

    async def scheduled_tick(self, context):
        with self._connect() as conn:
            jobs = conn.execute("SELECT listing_id FROM publisher_status_sync_outbox_v3 WHERE next_attempt<=? LIMIT 10", (time.time(),)).fetchall()
        for job in jobs:
            await self._flush_listing(context.bot, job[0])


__all__ = [
    "ManualStatusSyncResult",
    "PublisherManualStatusSynchronizer",
    "caption_with_inventory_status",
]
