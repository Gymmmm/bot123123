"""External publication identity repository.

A delivery attempt records *an attempt to cross the Telegram API boundary*.
A publication instance records *the durable external object that now exists*.
Keeping these responsibilities separate preserves exact message-id edits without
turning the delivery state machine into another listing/post truth table.
Construction never initializes schema; additive DDL belongs to the explicit V3
storage bootstrap.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any
import uuid

from .delivery_state import DeliveryBlocked, validate_result


@dataclass(frozen=True)
class PublicationInstance:
    instance_id: str
    package_id: str
    listing_id: str
    offer_id: str
    platform: str
    channel_chat_id: str
    channel_message_id: str
    caption_message_id: str
    button_message_id: str
    publish_status: str


DDL = """
CREATE TABLE IF NOT EXISTS publication_instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL UNIQUE,
    package_id TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    offer_id TEXT NOT NULL DEFAULT '',
    platform TEXT NOT NULL,
    channel_chat_id TEXT NOT NULL,
    channel_message_id TEXT NOT NULL,
    caption_message_id TEXT NOT NULL DEFAULT '',
    button_message_id TEXT NOT NULL DEFAULT '',
    media_group_id TEXT NOT NULL DEFAULT '',
    publish_status TEXT NOT NULL DEFAULT 'published',
    post_text TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, channel_chat_id, channel_message_id),
    UNIQUE(package_id, platform, channel_chat_id)
);
CREATE INDEX IF NOT EXISTS idx_publication_instances_listing
ON publication_instances(listing_id, publish_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_publication_instances_package
ON publication_instances(package_id, publish_status, updated_at);
"""


class PublicationInstanceRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _model(row: sqlite3.Row) -> PublicationInstance:
        return PublicationInstance(
            instance_id=str(row["instance_id"]),
            package_id=str(row["package_id"]),
            listing_id=str(row["listing_id"]),
            offer_id=str(row["offer_id"] or ""),
            platform=str(row["platform"]),
            channel_chat_id=str(row["channel_chat_id"]),
            channel_message_id=str(row["channel_message_id"]),
            caption_message_id=str(row["caption_message_id"] or ""),
            button_message_id=str(row["button_message_id"] or ""),
            publish_status=str(row["publish_status"]),
        )

    def get(self, instance_id: str) -> PublicationInstance | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM publication_instances WHERE instance_id=?",
                (str(instance_id),),
            ).fetchone()
        return self._model(row) if row else None

    def get_for_package(
        self, package_id: str, *, platform: str, channel_chat_id: str
    ) -> PublicationInstance | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM publication_instances
                   WHERE package_id=? AND platform=? AND channel_chat_id=?""",
                (str(package_id), str(platform), str(channel_chat_id)),
            ).fetchone()
        return self._model(row) if row else None

    def record_telegram_publication(
        self,
        *,
        package_id: str,
        listing_id: str,
        channel_chat_id: str,
        telegram_result: dict[str, Any],
        offer_id: str = "",
        instance_id: str | None = None,
    ) -> PublicationInstance:
        result = validate_result(telegram_result)
        message_ids = [str(value) for value in result["media_message_ids"]]
        channel_message_id = message_ids[0]
        caption_message_id = str(
            result.get("caption_message_id") or channel_message_id
        )
        button_message_id = str(result.get("button_message_id") or "")
        media_group_id = str(result.get("media_group_id") or "")
        post_text = str(result.get("caption") or "")
        identity = (
            str(package_id or "").strip(),
            str(listing_id or "").strip(),
            str(offer_id or "").strip(),
            str(channel_chat_id or "").strip(),
        )
        if not identity[0] or not identity[1] or not identity[3]:
            raise DeliveryBlocked("publication instance identity is incomplete")

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT * FROM publication_instances
                   WHERE package_id=? AND platform='telegram' AND channel_chat_id=?""",
                (identity[0], identity[3]),
            ).fetchone()
            if existing is not None:
                stored = (
                    str(existing["listing_id"]),
                    str(existing["offer_id"] or ""),
                    str(existing["channel_message_id"]),
                )
                expected = (identity[1], identity[2], channel_message_id)
                if stored != expected:
                    raise DeliveryBlocked(
                        "existing publication instance conflicts with Telegram receipt"
                    )
                conn.commit()
                return self._model(existing)

            new_id = str(instance_id or ("PUB_" + uuid.uuid4().hex))
            conn.execute(
                """INSERT INTO publication_instances
                   (instance_id,package_id,listing_id,offer_id,platform,
                    channel_chat_id,channel_message_id,caption_message_id,
                    button_message_id,media_group_id,publish_status,post_text)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'published',?)""",
                (
                    new_id,
                    identity[0],
                    identity[1],
                    identity[2],
                    "telegram",
                    identity[3],
                    channel_message_id,
                    caption_message_id,
                    button_message_id,
                    media_group_id,
                    post_text,
                ),
            )
            row = conn.execute(
                "SELECT * FROM publication_instances WHERE instance_id=?",
                (new_id,),
            ).fetchone()
            conn.commit()
            assert row is not None
            return self._model(row)

    def mark_status(self, instance_id: str, status: str) -> PublicationInstance:
        clean_status = str(status or "").strip()
        if not clean_status:
            raise ValueError("publication status is required")
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE publication_instances
                   SET publish_status=?,updated_at=CURRENT_TIMESTAMP
                   WHERE instance_id=?""",
                (clean_status, str(instance_id)),
            )
            if cur.rowcount != 1:
                raise KeyError(instance_id)
            row = conn.execute(
                "SELECT * FROM publication_instances WHERE instance_id=?",
                (str(instance_id),),
            ).fetchone()
            conn.commit()
        assert row is not None
        return self._model(row)


__all__ = [
    "DDL",
    "PublicationInstance",
    "PublicationInstanceRepository",
]
