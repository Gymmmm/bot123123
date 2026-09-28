from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3

from v3_core.publishing.inventory_operator_ui import PublisherInventoryAdminController
from v3_core.publishing.simple_admin_production import ProductionSimplePublisherAdminController


class _Message:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def reply_text(self, text, **kwargs):
        self.calls.append({"text": text, **kwargs})


class _Repo:
    def __init__(self, exceptions: int = 0) -> None:
        self._exceptions = exceptions

    def exception_count(self) -> int:
        return self._exceptions

    def config(self):
        from dataclasses import dataclass

        @dataclass
        class _Cfg:
            enabled: bool = True
            min_media: int = 1
            timezone_name: str = "Asia/Phnom_Penh"
            interval_seconds: int = 60

        return _Cfg()


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_active_operator_home_shows_only_attention_entries():
    """The active operator home is built from PublisherInventoryAdminController.

    V3PublisherApplication delegates ``show_home`` to ``self.simple`` which is
    a PublisherAdviserAdminController.  We assert against the actually
    rendered keyboard so the test pins down what operators see.
    """
    controller = object.__new__(PublisherInventoryAdminController)
    controller.db_path = ":memory:"
    controller.repository = type(
        "_StubRepo",
        (),
        {
            "exception_count": lambda self: 0,
            "queue_count": lambda self: 0,
            "held_count": lambda self: 0,
        },
    )()
    controller.workflow = type(
        "_StubWorkflow",
        (),
        {"queue_counts": lambda self: type("_C", (), {"unknown": 0})()},
    )()
    controller._pending_count = lambda: 0  # type: ignore[assignment]
    keyboard = controller.home_keyboard()
    rows = [[button.text for button in row] for row in keyboard.inline_keyboard]
    # New compact layout: high-attention entries only, no orphan zero badges.
    assert rows[0] == ["➕ 新建房源", "📢 频道运营"]
    assert rows[1][0].startswith("🔵 房态管理")
    assert rows[1][1].startswith("⚠️ 异常房源")
    last = rows[-1]
    assert last[0] == "🕘 最近发布"
    assert last[1] == "⚙️ 更多"
    callbacks = _callbacks(keyboard)
    assert "v3smp|new" in callbacks
    assert "v3bc" in callbacks
    assert any(cb.startswith("v3smp|listings") for cb in callbacks)
    assert any(cb.startswith("v3smp|exceptions|") for cb in callbacks)
    assert "v3smp|inv_rows|recent" in callbacks
    assert "v3smp|settings" in callbacks
    # Low-frequency entries must NOT appear on the home screen anymore.
    forbidden_home_labels = {
        "🚀 自动待发",
        "📦 旧库存",
        "📤 待发预览",
        "📡 采集源",
        "🟢 运行状态",
        "🕒 发帖时段",
        "📊 今日统计",
    }
    flat = [label for row in rows for label in row]
    for forbidden in forbidden_home_labels:
        assert forbidden not in flat, f"{forbidden} should not be on home"


def test_low_frequency_entries_live_behind_settings_page():
    """`⚙️ 更多` (`v3smp|settings`) re-exposes everything we moved off the home."""
    controller = object.__new__(PublisherInventoryAdminController)
    controller.db_path = ":memory:"
    controller.repository = type(
        "_StubRepo",
        (),
        {
            "exception_count": lambda self: 0,
            "queue_count": lambda self: 0,
            "held_count": lambda self: 0,
        },
    )()
    controller.workflow = type(
        "_StubWorkflow",
        (),
        {"queue_counts": lambda self: type("_C", (), {"unknown": 0})()},
    )()
    controller._pending_count = lambda: 0  # type: ignore[assignment]
    home = controller.home_keyboard()
    home_callbacks = set(_callbacks(home))
    moved_callbacks = {
        "v3smp|auto_queue",
        "v3smp|held_queue",
        "v3smp|preview_ready",
        "v3smp|sources",
        "v3smp|runtime",
        "v3smp|windows",
        "v3smp|stats",
    }
    for moved in moved_callbacks:
        assert moved not in home_callbacks, f"{moved} should live behind ⚙️ 更多"


def test_settings_page_keyboard_lists_every_moved_entry():
    """Confirm the ⚙️ 更多 page keeps every moved callback reachable."""
    controller = object.__new__(ProductionSimplePublisherAdminController)
    controller.repository = _Repo(exceptions=0)
    message = _Message()
    asyncio.run(controller.show_settings(message))
    markup = message.calls[-1]["reply_markup"]
    callbacks = set(_callbacks(markup))
    expected = {
        "v3smp|auto_queue",
        "v3smp|held_queue",
        "v3smp|preview_ready",
        "v3smp|sources",
        "v3smp|runtime",
        "v3smp|windows",
        "v3smp|stats",
    }
    missing = expected - callbacks
    assert not missing, f"⚙️ 更多 is missing callbacks: {sorted(missing)}"


def test_room_status_hub_exposes_reserved_as_first_class_state():
    controller = object.__new__(ProductionSimplePublisherAdminController)
    controller.repository = _Repo(exceptions=38)
    message = _Message()

    asyncio.run(controller.show_listing_categories(message))

    markup = message.calls[-1]["reply_markup"]
    labels = _labels(markup)
    assert "🟢 当前可预约" in labels
    assert "🟡 已有预约" in labels
    assert "🔴 已租出" in labels
    assert "⚫ 已下架" in labels
    assert "⚠️ 待处理异常 (38)" in labels
    assert "🕘 最近发布" in labels


def test_production_listing_rows_keep_active_and_reserved_separate(tmp_path: Path):
    db = tmp_path / "qiaolian.db"
    with sqlite3.connect(db) as conn:
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
        conn.executemany(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,display_title,project_name,inventory_status,updated_at)
               VALUES (?,?,?,?,?,?)""",
            [
                ("l_active", "QL-A", "可预约房", "A", "active", "2026-09-11 01:00:00"),
                ("l_reserved", "QL-R", "已有预约房", "R", "reserved", "2026-09-11 02:00:00"),
            ],
        )
        conn.commit()

    controller = object.__new__(ProductionSimplePublisherAdminController)
    controller.db_path = str(db)

    active = controller._production_listing_rows("active")
    reserved = controller._production_listing_rows("reserved")

    assert [row["listing_id"] for row in active] == ["l_active"]
    assert [row["listing_id"] for row in reserved] == ["l_reserved"]


def test_listing_detail_has_direct_reserved_transition():
    controller = object.__new__(ProductionSimplePublisherAdminController)
    controller.user_bot_username = "qiaolian_test_bot"
    controller._listing_detail = lambda listing_id: {
        "listing_id": listing_id,
        "public_listing_id": "QL-PP-A2B3",
        "display_title": "炳发城｜2房",
        "project_name": "炳发城",
        "inventory_status": "active",
        "monthly_rent_usd": 800,
        "offer_id": "OFF_1",
    }
    message = _Message()

    asyncio.run(controller.show_listing(message, "l_1"))

    markup = message.calls[-1]["reply_markup"]
    callbacks = _callbacks(markup)
    assert "v3smp|status|reserved|l_1" in callbacks
    assert "v3smp|status|rented|l_1" in callbacks
    assert "v3smp|status|offline|l_1" in callbacks
    assert "v3smp|recheck|OFF_1" in callbacks
