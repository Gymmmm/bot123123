"""V3 durable publication delivery state machine.

Extracted from the production ``publication_delivery.py`` contract at
8e4605cf. This module owns only the external-send boundary. It deliberately
does not update listings, review rows, packages, or external publication
instances; those projections are committed by their own repositories.
Construction never initializes schema; additive DDL belongs to the explicit V3
storage bootstrap.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any


class DeliveryBlocked(RuntimeError):
    """Raised when automatic delivery or recovery is unsafe."""


@dataclass(frozen=True)
class DeliveryAttempt:
    attempt_id: str
    package_id: str
    listing_id: str
    offer_id: str
    channel_chat_id: str
    state: str
    telegram_result: dict[str, Any] | None = None
    error_message: str = ""


DDL = """
CREATE TABLE IF NOT EXISTS publication_delivery_attempts_v3 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL UNIQUE,
    package_id TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    offer_id TEXT NOT NULL DEFAULT '',
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
CREATE INDEX IF NOT EXISTS idx_publication_delivery_v3_state
ON publication_delivery_attempts_v3(state, updated_at);
"""


def decode_result(raw: Any) -> dict[str, Any] | None:
    if not str(raw or "").strip():
        return None
    try:
        value = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        raise DeliveryBlocked("durable Telegram receipt is invalid JSON") from exc
    if not isinstance(value, dict):
        raise DeliveryBlocked("durable Telegram receipt is not an object")
    return value


def validate_result(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DeliveryBlocked("Telegram receipt must be an object")
    message_ids = value.get("media_message_ids")
    if not isinstance(message_ids, list) or not message_ids:
        raise DeliveryBlocked("Telegram receipt has no media_message_ids")
    if any(str(item or "").strip() == "" for item in message_ids):
        raise DeliveryBlocked("Telegram receipt contains an empty message id")
    return dict(value)


class PublicationDeliveryStateRepository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @staticmethod
    def _attempt(row: sqlite3.Row) -> DeliveryAttempt:
        return DeliveryAttempt(
            attempt_id=str(row["attempt_id"]),
            package_id=str(row["package_id"]),
            listing_id=str(row["listing_id"]),
            offer_id=str(row["offer_id"] or ""),
            channel_chat_id=str(row["channel_chat_id"]),
            state=str(row["state"]),
            telegram_result=decode_result(row["telegram_result_json"]),
            error_message=str(row["error_message"] or ""),
        )

    def get(self, attempt_id: str) -> DeliveryAttempt:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM publication_delivery_attempts_v3 WHERE attempt_id=?",
                (str(attempt_id),),
            ).fetchone()
        if row is None:
            raise DeliveryBlocked("delivery attempt does not exist")
        return self._attempt(row)

    def prepare(
        self,
        *,
        package_id: str,
        listing_id: str,
        channel_chat_id: str,
        offer_id: str = "",
    ) -> DeliveryAttempt:
        identity = tuple(
            str(v or "").strip()
            for v in (package_id, listing_id, offer_id, channel_chat_id)
        )
        if not identity[0] or not identity[1] or not identity[3]:
            raise DeliveryBlocked("delivery identity contains an empty required value")

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM publication_delivery_attempts_v3 WHERE package_id=? AND channel_chat_id=?",
                (identity[0], identity[3]),
            ).fetchone()
            if row is None:
                attempt_id = "DLV_" + uuid.uuid4().hex
                conn.execute(
                    """INSERT INTO publication_delivery_attempts_v3
                       (attempt_id,package_id,listing_id,offer_id,channel_chat_id,state)
                       VALUES (?,?,?,?,?,'prepared')""",
                    (attempt_id, *identity),
                )
                row = conn.execute(
                    "SELECT * FROM publication_delivery_attempts_v3 WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()
            else:
                stored = (
                    str(row["package_id"]),
                    str(row["listing_id"]),
                    str(row["offer_id"] or ""),
                    str(row["channel_chat_id"]),
                )
                if stored != identity:
                    raise DeliveryBlocked("existing delivery attempt identity does not match")
                if str(row["state"]) in {"sending", "unknown"}:
                    raise DeliveryBlocked(
                        f"delivery attempt is {row['state']}; reconcile it before any retry"
                    )
            conn.commit()
            assert row is not None
            return self._attempt(row)

    def _transition(
        self,
        attempt_id: str,
        allowed: set[str],
        target: str,
        *,
        extra_sql: str = "",
        params: tuple[Any, ...] = (),
        timestamp_column: str | None = None,
    ) -> None:
        stamp = f", {timestamp_column}=CURRENT_TIMESTAMP" if timestamp_column else ""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state FROM publication_delivery_attempts_v3 WHERE attempt_id=?",
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
                f"""UPDATE publication_delivery_attempts_v3
                    SET state=?, updated_at=CURRENT_TIMESTAMP{stamp}{extra_sql}
                    WHERE attempt_id=?""",
                (target, *params, str(attempt_id)),
            )
            conn.commit()

    def mark_sending(self, attempt_id: str) -> None:
        self._transition(
            attempt_id,
            {"prepared", "failed_before_send"},
            "sending",
            extra_sql=", error_message=''",
            timestamp_column="sending_at",
        )

    def mark_sent(self, attempt_id: str, telegram_result: dict[str, Any]) -> None:
        result = validate_result(telegram_result)
        self._transition(
            attempt_id,
            {"sending"},
            "sent",
            extra_sql=", telegram_result_json=?, error_message=''",
            params=(json.dumps(result, ensure_ascii=False, sort_keys=True),),
            timestamp_column="sent_at",
        )

    def mark_failed_before_send(self, attempt_id: str, reason: str) -> None:
        self._transition(
            attempt_id,
            {"prepared", "failed_before_send"},
            "failed_before_send",
            extra_sql=", error_message=?",
            params=(str(reason or "")[:2000],),
        )

    def mark_unknown(
        self,
        attempt_id: str,
        error: str,
        telegram_result: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state,telegram_result_json FROM publication_delivery_attempts_v3 WHERE attempt_id=?",
                (str(attempt_id),),
            ).fetchone()
            if row is None:
                raise DeliveryBlocked("delivery attempt does not exist")
            if str(row["state"]) in {"sent", "committed"} and str(
                row["telegram_result_json"] or ""
            ).strip():
                conn.execute(
                    """UPDATE publication_delivery_attempts_v3
                       SET error_message=?,updated_at=CURRENT_TIMESTAMP WHERE attempt_id=?""",
                    (str(error or "")[:2000], str(attempt_id)),
                )
                conn.commit()
                return
            receipt = ""
            if telegram_result is not None:
                receipt = json.dumps(
                    validate_result(telegram_result),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            if str(row["state"]) not in {"sending", "unknown"}:
                raise DeliveryBlocked(
                    f"cannot mark {row['state']} delivery attempt as unknown"
                )
            conn.execute(
                """UPDATE publication_delivery_attempts_v3
                   SET state='unknown',
                       telegram_result_json=CASE WHEN ?='' THEN telegram_result_json ELSE ? END,
                       error_message=?,updated_at=CURRENT_TIMESTAMP
                   WHERE attempt_id=?""",
                (receipt, receipt, str(error or "")[:2000], str(attempt_id)),
            )
            conn.commit()

    def mark_committed(self, attempt_id: str) -> DeliveryAttempt:
        attempt = self.get(attempt_id)
        if attempt.state == "committed":
            return attempt
        if attempt.state != "sent" or not attempt.telegram_result:
            raise DeliveryBlocked(
                f"cannot commit delivery attempt in {attempt.state} state"
            )
        self._transition(
            attempt_id,
            {"sent"},
            "committed",
            extra_sql=", error_message=''",
            timestamp_column="committed_at",
        )
        return self.get(attempt_id)


__all__ = [
    "DDL",
    "DeliveryAttempt",
    "DeliveryBlocked",
    "PublicationDeliveryStateRepository",
    "decode_result",
    "validate_result",
]
