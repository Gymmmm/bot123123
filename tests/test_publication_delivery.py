from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from publication_delivery import DeliveryBlocked, PublicationDeliveryRepository
from qiaolian_dual.db import SCHEMA as USER_BOT_SCHEMA


def _seed(db_path: Path, suffix: str = "1") -> tuple[str, str, str]:
    root = Path(__file__).resolve().parent.parent
    with sqlite3.connect(db_path) as conn:
        conn.executescript((root / "schema_core.sql").read_text(encoding="utf-8"))
        conn.executescript(USER_BOT_SCHEMA)
        draft_id = f"DRF_{suffix}"
        listing_id = f"l_{suffix}"
        package_id = f"PKG_{suffix}"
        conn.execute(
            """INSERT INTO drafts(draft_id,listing_id,title,project,community,area,property_type,
               price,review_status) VALUES (?,?,?,?,?,?,?,?, 'approved')""",
            (draft_id, listing_id, "BKK1｜1房｜公寓", "", "", "BKK1", "公寓", 600),
        )
        conn.execute(
            """INSERT INTO listings(listing_id,title,property_type,area,community,price,currency,
               status,created_at,updated_at) VALUES (?,?,?,?,?,?,'USD','pending',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (listing_id, "BKK1｜1房｜公寓", "公寓", "BKK1", "", 600),
        )
        conn.execute(
            """INSERT INTO publication_packages
               (package_id,draft_id,property_id,package_version,source_type,listing_type,media_type,
                cover_template,status,cover_path,main_images_json,discussion_images_json,post_text,
                snapshot_json,content_hash)
               VALUES (?,?,?,1,'telegram','公寓','image','minimal_white','approved','cover.jpg','[]','[]','caption','{}','hash')""",
            (package_id, draft_id, listing_id),
        )
    return package_id, draft_id, listing_id


def _receipt(message_id: int) -> dict:
    return {
        "media_message_ids": [message_id, message_id + 1],
        "button_message_id": message_id + 2,
        "media_group_id": f"group-{message_id}",
        "caption": "frozen caption",
    }


def test_delivery_commit_is_atomic_and_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "delivery.db"
    package_id, draft_id, listing_id = _seed(db_path)
    repo = PublicationDeliveryRepository(str(db_path))
    attempt = repo.prepare(
        package_id=package_id, draft_id=draft_id, listing_id=listing_id,
        channel_chat_id="-10088",
    )
    repo.mark_sending(attempt.attempt_id)
    repo.mark_sent(attempt.attempt_id, _receipt(101))
    repo.commit_success(
        attempt_id=attempt.attempt_id, post_id="TG_ONE", package_id=package_id,
        draft_id=draft_id, listing_id=listing_id, channel_chat_id="-10088",
        telegram_result=_receipt(101),
    )
    again = repo.prepare(
        package_id=package_id, draft_id=draft_id, listing_id=listing_id,
        channel_chat_id="-10088",
    )
    assert again.state == "committed"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0] == 1
        assert conn.execute("SELECT review_status FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()[0] == "published"
        assert conn.execute("SELECT status FROM listings WHERE listing_id=?", (listing_id,)).fetchone()[0] == "active"
        assert conn.execute("SELECT status FROM publication_packages WHERE package_id=?", (package_id,)).fetchone()[0] == "published"


def test_durable_sent_receipt_recovers_without_resend(tmp_path: Path) -> None:
    db_path = tmp_path / "recover.db"
    package_id, draft_id, listing_id = _seed(db_path, "2")
    repo = PublicationDeliveryRepository(str(db_path))
    attempt = repo.prepare(
        package_id=package_id, draft_id=draft_id, listing_id=listing_id,
        channel_chat_id="-10099",
    )
    repo.mark_sending(attempt.attempt_id)
    repo.mark_sent(attempt.attempt_id, _receipt(201))
    repo.mark_unknown(attempt.attempt_id, "local commit interrupted", _receipt(201))
    recovered = repo.prepare(
        package_id=package_id, draft_id=draft_id, listing_id=listing_id,
        channel_chat_id="-10099",
    )
    assert recovered.state == "sent"
    repo.commit_saved_result(attempt=recovered, post_id="TG_RECOVER")
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0] == 1
        assert conn.execute(
            "SELECT state FROM publication_delivery_attempts WHERE attempt_id=?",
            (attempt.attempt_id,),
        ).fetchone()[0] == "committed"


def test_ambiguous_external_state_blocks_automatic_retry(tmp_path: Path) -> None:
    db_path = tmp_path / "unknown.db"
    package_id, draft_id, listing_id = _seed(db_path, "3")
    repo = PublicationDeliveryRepository(str(db_path))
    attempt = repo.prepare(
        package_id=package_id, draft_id=draft_id, listing_id=listing_id,
        channel_chat_id="-10077",
    )
    repo.mark_sending(attempt.attempt_id)
    repo.mark_unknown(attempt.attempt_id, "timeout after Telegram call")
    with pytest.raises(DeliveryBlocked, match="reconcile"):
        repo.prepare(
            package_id=package_id, draft_id=draft_id, listing_id=listing_id,
            channel_chat_id="-10077",
        )
