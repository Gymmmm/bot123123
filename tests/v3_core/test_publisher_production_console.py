from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3

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


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]


def test_production_home_restores_compact_operator_layout():
    keyboard = ProductionSimplePublisherAdminController.home_keyboard()
    rows = [[button.text for button in row] for row in keyboard.inline_keyboard]
    assert rows == [
        ["➕ 发布房源", "🔵 房态管理"],
        ["📢 广播中心", "📡 采集源"],
        ["🧪 查看发布效果", "📚 发布记录"],
        ["⚙️ 发布设置"],
    ]
    callbacks = _callbacks(keyboard)
    assert callbacks == [
        "v3smp|new",
        "v3smp|listings",
        "v3bc",
        "v3smp|sources",
        "v3smp|preview",
        "v3smp|logs",
        "v3smp|settings",
    ]


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
