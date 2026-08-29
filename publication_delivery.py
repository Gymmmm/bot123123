#!/usr/bin/env python3
"""Durable, idempotent Telegram publication delivery state machine.

The external Telegram send cannot participate in a SQLite transaction.  This
repository records the boundary on both sides of that call and commits every
local publication projection in one transaction after a durable receipt exists.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
import uuid
from typing import Any


class DeliveryBlocked(RuntimeError):
    """Raised when automatic delivery or recovery is unsafe."""


@dataclass(frozen=True)
class DeliveryAttempt:
    attempt_id: str
    package_id: str
    draft_id: str
    listing_id: str
    channel_chat_id: str
    state: str
    telegram_result: dict[str, Any] | None = None
    error_message: str = ""


_DDL = """
CREATE TABLE IF NOT EXISTS publication_delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL UNIQUE,
    package_id TEXT NOT NULL,
    draft_id TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    channel_chat_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'prepared',
    telegram_result_json TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    prepared_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sending_at TEXT,
    sent_at TEXT,
    committed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(package_id, channel_chat_id)
);
CREATE INDEX IF NOT EXISTS idx_publication_delivery_state
ON publication_delivery_attempts(state, updated_at);
"""


def _decode_result(raw: Any) -> dict[str, Any] | None:
    if not str(raw or "").strip():
        return None
    try:
        value = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        raise DeliveryBlocked("durable Telegram receipt is invalid JSON") from exc
    if not isinstance(value, dict):
        raise DeliveryBlocked("durable Telegram receipt is not an object")
    return value


def _validate_result(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DeliveryBlocked("Telegram receipt must be an object")
    message_ids = value.get("media_message_ids")
    if not isinstance(message_ids, list) or not message_ids:
        raise DeliveryBlocked("Telegram receipt has no media_message_ids")
    if any(str(item or "").strip() == "" for item in message_ids):
        raise DeliveryBlocked("Telegram receipt contains an empty message id")
    return dict(value)


class PublicationDeliveryRepository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        with self._connect() as conn:
            conn.executescript(_DDL)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _attempt(row: sqlite3.Row) -> DeliveryAttempt:
        return DeliveryAttempt(
            attempt_id=str(row["attempt_id"]),
            package_id=str(row["package_id"]),
            draft_id=str(row["draft_id"]),
            listing_id=str(row["listing_id"]),
            channel_chat_id=str(row["channel_chat_id"]),
            state=str(row["state"]),
            telegram_result=_decode_result(row["telegram_result_json"]),
            error_message=str(row["error_message"] or ""),
        )

    def prepare(
        self, *, package_id: str, draft_id: str, listing_id: str, channel_chat_id: str
    ) -> DeliveryAttempt:
        identity = tuple(str(v or "").strip() for v in (
            package_id, draft_id, listing_id, channel_chat_id
        ))
        if not all(identity):
            raise DeliveryBlocked("delivery identity contains an empty value")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM publication_delivery_attempts WHERE package_id=? AND channel_chat_id=?",
                (identity[0], identity[3]),
            ).fetchone()
            if row is None:
                attempt_id = "DLV_" + uuid.uuid4().hex
                conn.execute(
                    """INSERT INTO publication_delivery_attempts
                       (attempt_id,package_id,draft_id,listing_id,channel_chat_id,state)
                       VALUES (?,?,?,?,?,'prepared')""",
                    (attempt_id, *identity),
                )
                row = conn.execute(
                    "SELECT * FROM publication_delivery_attempts WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()
            else:
                stored = (
                    str(row["package_id"]), str(row["draft_id"]),
                    str(row["listing_id"]), str(row["channel_chat_id"]),
                )
                if stored != identity:
                    raise DeliveryBlocked("existing delivery attempt identity does not match")
                if str(row["state"]) in {"sending", "unknown"}:
                    raise DeliveryBlocked(
                        f"delivery attempt is {row['state']}; reconcile it before any retry"
                    )
            conn.commit()
            return self._attempt(row)

    def _transition(
        self, attempt_id: str, allowed: set[str], target: str, *, extra_sql: str = "",
        params: tuple[Any, ...] = (), timestamp_column: str | None = None,
    ) -> None:
        stamp = f", {timestamp_column}=CURRENT_TIMESTAMP" if timestamp_column else ""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state FROM publication_delivery_attempts WHERE attempt_id=?",
                (str(attempt_id),),
            ).fetchone()
            if row is None:
                raise DeliveryBlocked("delivery attempt does not exist")
            if str(row["state"]) == target:
                conn.commit()
                return
            if str(row["state"]) not in allowed:
                raise DeliveryBlocked(
                    f"cannot move delivery attempt from {row['state']} to {target}"
                )
            conn.execute(
                f"""UPDATE publication_delivery_attempts
                    SET state=?, updated_at=CURRENT_TIMESTAMP{stamp}{extra_sql}
                    WHERE attempt_id=?""",
                (target, *params, str(attempt_id)),
            )
            conn.commit()

    def mark_sending(self, attempt_id: str) -> None:
        self._transition(
            attempt_id, {"prepared", "failed_before_send"}, "sending",
            extra_sql=", error_message=''", timestamp_column="sending_at",
        )

    def mark_sent(self, attempt_id: str, telegram_result: dict[str, Any]) -> None:
        result = _validate_result(telegram_result)
        self._transition(
            attempt_id, {"sending"}, "sent",
            extra_sql=", telegram_result_json=?, error_message=''",
            params=(json.dumps(result, ensure_ascii=False, sort_keys=True),),
            timestamp_column="sent_at",
        )

    def mark_failed_before_send(self, attempt_id: str, reason: str) -> None:
        self._transition(
            attempt_id, {"prepared", "failed_before_send"}, "failed_before_send",
            extra_sql=", error_message=?", params=(str(reason or "")[:2000],),
        )

    def mark_unknown(
        self, attempt_id: str, error: str, telegram_result: dict[str, Any] | None = None
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state,telegram_result_json FROM publication_delivery_attempts WHERE attempt_id=?",
                (str(attempt_id),),
            ).fetchone()
            if row is None:
                raise DeliveryBlocked("delivery attempt does not exist")
            # A durable sent receipt always wins over a later local commit error.
            if str(row["state"]) in {"sent", "committed"} and str(
                row["telegram_result_json"] or ""
            ).strip():
                conn.execute(
                    """UPDATE publication_delivery_attempts
                       SET error_message=?,updated_at=CURRENT_TIMESTAMP WHERE attempt_id=?""",
                    (str(error or "")[:2000], str(attempt_id)),
                )
                conn.commit()
                return
            receipt = ""
            if telegram_result is not None:
                receipt = json.dumps(
                    _validate_result(telegram_result), ensure_ascii=False, sort_keys=True
                )
            if str(row["state"]) not in {"sending", "unknown"}:
                raise DeliveryBlocked(
                    f"cannot mark {row['state']} delivery attempt as unknown"
                )
            conn.execute(
                """UPDATE publication_delivery_attempts
                   SET state='unknown',telegram_result_json=CASE WHEN ?='' THEN telegram_result_json ELSE ? END,
                       error_message=?,updated_at=CURRENT_TIMESTAMP WHERE attempt_id=?""",
                (receipt, receipt, str(error or "")[:2000], str(attempt_id)),
            )
            conn.commit()

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    def commit_success(
        self, *, attempt_id: str, post_id: str, package_id: str, draft_id: str,
        listing_id: str, channel_chat_id: str, telegram_result: dict[str, Any] | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM publication_delivery_attempts WHERE attempt_id=?",
                (str(attempt_id),),
            ).fetchone()
            if row is None:
                raise DeliveryBlocked("delivery attempt does not exist")
            expected = tuple(str(v or "") for v in (
                package_id, draft_id, listing_id, channel_chat_id
            ))
            stored = tuple(str(row[key]) for key in (
                "package_id", "draft_id", "listing_id", "channel_chat_id"
            ))
            if stored != expected:
                raise DeliveryBlocked("commit identity does not match delivery attempt")
            if str(row["state"]) == "committed":
                conn.commit()
                return
            if str(row["state"]) != "sent":
                raise DeliveryBlocked(f"cannot commit delivery attempt in {row['state']} state")
            durable = _decode_result(row["telegram_result_json"])
            result = _validate_result(durable or telegram_result)
            if telegram_result is not None and _validate_result(telegram_result) != result:
                raise DeliveryBlocked("Telegram result differs from durable receipt")
            media_ids = result["media_message_ids"]
            channel_message_id = str(media_ids[0])
            if not self._table_exists(conn, "posts"):
                raise DeliveryBlocked("posts table is missing")
            conn.execute(
                """INSERT INTO posts
                   (post_id,listing_id,draft_id,platform,channel_chat_id,channel_message_id,
                    media_group_id,caption_message_id,button_message_id,publish_status,
                    post_text,publication_package_id,published_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,'published',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                   ON CONFLICT(post_id) DO NOTHING""",
                (
                    str(post_id), str(listing_id), str(draft_id), "telegram",
                    str(channel_chat_id), channel_message_id,
                    str(result.get("media_group_id") or ""), channel_message_id,
                    str(result.get("button_message_id") or ""),
                    str(result.get("caption") or ""),
                    str(package_id),
                ),
            )
            post = conn.execute(
                "SELECT listing_id,draft_id,channel_chat_id,channel_message_id FROM posts WHERE post_id=?",
                (str(post_id),),
            ).fetchone()
            if post is None or tuple(str(post[k] or "") for k in post.keys()) != (
                str(listing_id), str(draft_id), str(channel_chat_id), channel_message_id
            ):
                raise DeliveryBlocked("existing post record conflicts with delivery receipt")
            if self._table_exists(conn, "drafts"):
                cur = conn.execute(
                    """UPDATE drafts SET review_status='published',listing_id=?,
                       published_at=COALESCE(published_at,CURRENT_TIMESTAMP),updated_at=CURRENT_TIMESTAMP
                       WHERE draft_id=?""",
                    (str(listing_id), str(draft_id)),
                )
                if cur.rowcount != 1:
                    raise DeliveryBlocked("draft record is missing during publication commit")
            if self._table_exists(conn, "publication_packages"):
                cur = conn.execute(
                    """UPDATE publication_packages SET status='published',
                       published_at=COALESCE(published_at,CURRENT_TIMESTAMP),updated_at=CURRENT_TIMESTAMP
                       WHERE package_id=? AND draft_id=? AND status IN ('approved','published')""",
                    (str(package_id), str(draft_id)),
                )
                if cur.rowcount != 1:
                    raise DeliveryBlocked("approved publication package is missing during commit")
            if self._table_exists(conn, "listings"):
                conn.execute(
                    """UPDATE listings SET status='active',updated_at=CURRENT_TIMESTAMP
                       WHERE listing_id=?""",
                    (str(listing_id),),
                )
            conn.execute(
                """UPDATE publication_delivery_attempts
                   SET state='committed',committed_at=CURRENT_TIMESTAMP,
                       error_message='',updated_at=CURRENT_TIMESTAMP WHERE attempt_id=?""",
                (str(attempt_id),),
            )
            conn.commit()

    def commit_saved_result(self, *, attempt: DeliveryAttempt, post_id: str) -> None:
        result = _validate_result(attempt.telegram_result)
        self.commit_success(
            attempt_id=attempt.attempt_id,
            post_id=str(post_id),
            package_id=attempt.package_id,
            draft_id=attempt.draft_id,
            listing_id=attempt.listing_id,
            channel_chat_id=attempt.channel_chat_id,
            telegram_result=result,
        )


__all__ = ["DeliveryAttempt", "DeliveryBlocked", "PublicationDeliveryRepository"]
