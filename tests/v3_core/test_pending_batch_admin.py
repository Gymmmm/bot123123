from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3

from v3_core.publishing.pending_batch_admin import (
    PENDING_BATCH_SIZE,
    PendingBatchOperatorPublisherAdminController,
)


class _Message:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def reply_text(self, text, **kwargs):
        self.calls.append({"text": text, **kwargs})


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]


def _build_db(path: Path, pending_count: int = 12) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE listings_v3 (
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                project_name TEXT,
                inventory_status TEXT,
                updated_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE listing_offers (
                offer_id TEXT PRIMARY KEY,
                listing_id TEXT,
                offer_type TEXT,
                offer_status TEXT,
                monthly_rent_usd REAL,
                publication_policy TEXT,
                publishable INTEGER,
                publish_block_reason TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE publication_instances (
                id INTEGER PRIMARY KEY,
                listing_id TEXT, offer_id TEXT, platform TEXT, publish_status TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE publication_packages_v3 (
                id INTEGER PRIMARY KEY, package_id TEXT, listing_id TEXT, offer_id TEXT, status TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE publisher_auto_items_v3 (
                offer_id TEXT PRIMARY KEY, listing_id TEXT, state TEXT
            )"""
        )
        for index in range(1, pending_count + 1):
            listing_id = f"l_{index:02d}"
            conn.execute(
                """INSERT INTO listings_v3
                   (listing_id,public_listing_id,display_title,project_name,inventory_status,updated_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    listing_id,
                    f"QL-{index:04d}",
                    f"测试房源{index}",
                    "测试项目",
                    "pending",
                    f"2026-09-15 00:{index:02d}:00",
                ),
            )
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,offer_status,monthly_rent_usd,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (f"off_{index:02d}", listing_id, "rent", "active", 500 + index, "telegram_rent", 1, ""),
            )
        conn.commit()


def _controller(db: Path) -> PendingBatchOperatorPublisherAdminController:
    controller = object.__new__(PendingBatchOperatorPublisherAdminController)
    controller.db_path = str(db)
    return controller


def test_pending_queue_is_fixed_to_ten_per_group(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=12)
    controller = _controller(db)

    first = controller._pending_batch_rows(0)
    second = controller._pending_batch_rows(1)

    assert PENDING_BATCH_SIZE == 10
    assert [row["listing_id"] for row in first] == [f"l_{i:02d}" for i in range(1, 11)]
    assert [row["listing_id"] for row in second] == ["l_11", "l_12"]


def test_room_status_hub_puts_pending_batch_first(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=12)
    controller = _controller(db)
    message = _Message()

    asyncio.run(controller.show_listing_categories(message))

    markup = message.calls[-1]["reply_markup"]
    labels = _labels(markup)
    callbacks = _callbacks(markup)
    assert labels[0] == "🔵 待确认 12｜10套一组"
    assert callbacks[0] == "v3smp|pbat|0"
    assert "🟢 可预约 0" in labels
    assert "🔴 已租出 0" in labels
    assert "⚫ 已下架 0" in labels


def test_pending_batch_screen_has_one_tap_group_actions(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=12)
    controller = _controller(db)
    message = _Message()

    asyncio.run(controller.show_pending_batch(message, 0))

    call = message.calls[-1]
    assert "第 1 组｜1–10" in call["text"]
    assert "待确认共 12 套" in call["text"]
    labels = _labels(call["reply_markup"])
    callbacks = _callbacks(call["reply_markup"])
    assert "✅ 这10套全部可租" in labels
    assert "⚫ 这10套全部下架" in labels
    assert "🔎 单独处理本组" in labels
    assert "v3smp|pbapply|active|0" in callbacks
    assert "v3smp|pbat|1" in callbacks


def test_batch_confirm_moves_first_ten_then_remaining_two(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=12)
    controller = _controller(db)

    updated, skipped = controller._apply_pending_batch("active", 0)
    assert updated == [f"l_{i:02d}" for i in range(1, 11)]
    assert skipped == []
    assert controller._pending_count() == 2

    updated2, skipped2 = controller._apply_pending_batch("active", 0)
    assert updated2 == ["l_11", "l_12"]
    assert skipped2 == []
    assert controller._pending_count() == 0

    with sqlite3.connect(db) as conn:
        active = conn.execute(
            "SELECT COUNT(*) FROM listings_v3 WHERE inventory_status='active'"
        ).fetchone()[0]
    assert active == 12


def test_batch_does_not_overwrite_listing_that_is_no_longer_pending(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=2)
    controller = _controller(db)

    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE listings_v3 SET inventory_status='rented' WHERE listing_id='l_02'")
        conn.commit()

    stale_rows = [
        {"listing_id": "l_01"},
        {"listing_id": "l_02"},
    ]
    controller._pending_batch_rows = lambda page=0: stale_rows

    updated, skipped = controller._apply_pending_batch("active", 0)

    assert updated == ["l_01"]
    assert skipped == ["l_02"]
    with sqlite3.connect(db) as conn:
        statuses = dict(conn.execute("SELECT listing_id,inventory_status FROM listings_v3"))
    assert statuses["l_01"] == "active"
    assert statuses["l_02"] == "rented"


def test_pending_queue_excludes_listing_with_successful_publication(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=2)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO publication_instances(listing_id,offer_id,platform,publish_status) VALUES (?,?,?,?)",
            ("l_01", "off_01", "telegram", "published"),
        )
        conn.commit()
    controller = _controller(db)

    assert [row["listing_id"] for row in controller._pending_batch_rows(0)] == ["l_02"]
    assert controller._pending_count() == 1


def test_pending_queue_excludes_sale_only_store_only(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=1)
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM listing_offers WHERE listing_id='l_01'")
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,offer_status,monthly_rent_usd,publication_policy,publishable,publish_block_reason)
               VALUES ('sale_01','l_01','sale','active',NULL,'store_only',0,'sale_not_enabled_for_telegram')"""
        )
        conn.commit()
    controller = _controller(db)

    assert controller._pending_batch_rows(0) == []
    assert controller._pending_count() == 0


def test_pending_queue_excludes_review_required_exception(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=1)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE listing_offers SET publishable=0,publish_block_reason='review_required' WHERE offer_id='off_01'"
        )
        conn.execute(
            "INSERT INTO publisher_auto_items_v3(offer_id,listing_id,state) VALUES ('off_01','l_01','exception')"
        )
        conn.commit()
    controller = _controller(db)

    assert controller._pending_batch_rows(0) == []
    assert controller._pending_count() == 0


def test_new_unpublished_publishable_rental_enters_pending_queue(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=1)
    controller = _controller(db)

    rows = controller._pending_batch_rows(0)
    assert len(rows) == 1
    assert rows[0]["listing_id"] == "l_01"
    assert rows[0]["monthly_rent_usd"] == 501


def test_old_pending_detail_callback_no_longer_treats_published_listing_as_actionable(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=1)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO publication_instances(listing_id,offer_id,platform,publish_status) VALUES ('l_01','off_01','telegram','published')"
        )
        conn.commit()
    controller = _controller(db)
    controller._listing_detail = lambda listing_id: {
        "listing_id": listing_id,
        "public_listing_id": "QL-0001",
        "display_title": "测试房源1",
        "inventory_status": "pending",
    }
    message = _Message()

    asyncio.run(controller.show_pending_one(message, "l_01", 0))

    assert "已不属于普通待确认队列" in message.calls[-1]["text"]
    assert "pboneapply" not in " ".join(_callbacks(message.calls[-1]["reply_markup"]))


def test_pending_batch_cas_still_skips_status_changed_after_screen_opened(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=2)
    controller = _controller(db)
    stale_rows = controller._pending_batch_rows(0)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE listings_v3 SET inventory_status='rented' WHERE listing_id='l_02'")
        conn.commit()
    controller._pending_batch_rows = lambda page=0: stale_rows

    updated, skipped = controller._apply_pending_batch("active", 0)

    assert updated == ["l_01"]
    assert skipped == ["l_02"]


def test_eleven_eligible_pending_split_into_ten_and_one(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    _build_db(db, pending_count=11)
    controller = _controller(db)

    assert len(controller._pending_batch_rows(0)) == 10
    assert [row["listing_id"] for row in controller._pending_batch_rows(1)] == ["l_11"]
