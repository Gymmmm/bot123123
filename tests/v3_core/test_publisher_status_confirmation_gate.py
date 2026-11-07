from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from v3_core.publishing.simple_admin import SimplePublisherAdminController


def _db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3(
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                inventory_status TEXT,
                updated_at TEXT
            );
            CREATE TABLE listing_offers(
                offer_id TEXT, listing_id TEXT, monthly_rent_usd REAL,
                offer_type TEXT, offer_status TEXT
            );
            INSERT INTO listings_v3
              (listing_id,public_listing_id,display_title,inventory_status)
              VALUES ('l_01','QL-0001','测试房源','offline');
            """
        )


def _controller(path: Path) -> SimplePublisherAdminController:
    obj = object.__new__(SimplePublisherAdminController)
    obj.db_path = str(path)
    obj.user_bot_username = "testbot"
    return obj


class _Message:
    def __init__(self):
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append((text, kwargs))


def _status(path):
    with sqlite3.connect(path) as conn:
        return conn.execute(
            "SELECT inventory_status FROM listings_v3 WHERE listing_id='l_01'"
        ).fetchone()[0]


def _callback(controller, message, data):
    update = SimpleNamespace(callback_query=SimpleNamespace(data=data, message=message))
    return asyncio.run(controller.handle_callback(update, SimpleNamespace(user_data={})))


def test_restore_requires_confirmation_and_rechecks_original_status(tmp_path):
    path = tmp_path / "isolated.sqlite3"
    _db(path)
    controller = _controller(path)
    message = _Message()
    assert _callback(controller, message, "v3smp|status|active|l_01")
    assert _status(path) == "offline"
    keyboard = message.calls[-1][1]["reply_markup"]
    confirmation = keyboard.inline_keyboard[0][0].callback_data
    assert confirmation == "v3smp|status_apply|active|l_01|offline"

    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE listings_v3 SET inventory_status='rented' WHERE listing_id='l_01'")
    assert not _callback(controller, message, confirmation)
    assert _status(path) == "rented"
    assert "没有覆盖" in message.calls[-1][0]


def test_legacy_confirmation_does_not_modify_status(tmp_path):
    path = tmp_path / "isolated.sqlite3"
    _db(path)
    controller = _controller(path)
    message = _Message()
    assert _callback(controller, message, "v3smp|status_apply|active|l_01")
    assert _status(path) == "offline"
    assert "已经更新" in message.calls[-1][0]


def test_confirmed_status_updates_once_and_replay_is_safe(tmp_path):
    path = tmp_path / "isolated.sqlite3"
    _db(path)
    controller = _controller(path)
    message = _Message()
    callback = "v3smp|status_apply|active|l_01|offline"
    assert _callback(controller, message, callback)
    assert _status(path) == "active"
    assert not _callback(controller, message, callback)
    assert _status(path) == "active"


def test_villa_exterior_exception_exposes_safe_add_photo_exit():
    controller = object.__new__(SimplePublisherAdminController)
    controller._exception_detail = lambda offer_id: {
        "reason_code": "villa_exterior_review_required",
        "reason_text": "缺少合格外立面",
        "review_id": "review_1",
        "public_listing_id": "QL-0009",
    }
    controller._listing_summary = lambda row: "测试别墅"
    message = _Message()
    asyncio.run(controller.show_exception(message, "offer_9"))
    keyboard = message.calls[-1][1]["reply_markup"]
    actions = {
        btn.text: btn.callback_data
        for row in keyboard.inline_keyboard
        for btn in row if btn.callback_data
    }
    assert actions["补充别墅外观图"] == "v3smp|addmedia|offer_9"
    assert "重新检查并发布" in actions
