from __future__ import annotations

import ast
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from v3_core.status_labels import (
    APPOINTMENT_STATUS_LABELS,
    DELIVERY_STATE_LABELS,
    PACKAGE_STATUS_LABELS,
    REVIEW_STATUS_LABELS,
)
from v3_core.user_bot.admin_appointments import admin_home_keyboard, handle_admin_callback
from qiaolian_dual.admin_consult import admin_home_keyboard as complete_admin_home_keyboard
from qiaolian_dual import admin_consult, attribution_store


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row]


class FakeReader:
    def list_today(self):
        return ({"id": 7, "display_name": "张女士", "appointment_time": "下午", "public_listing_id": "QL-RF-A2B3"},)

    def get(self, appointment_id):
        assert appointment_id == 7
        return {"display_name": "张女士", "public_listing_id": "QL-RF-A2B3", "appointment_date": "2026-09-09", "appointment_time": "下午", "viewing_mode": "offline", "contact_value": "@guest", "status": "pending"}


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.answers = 0
        self.edits = []

    async def answer(self, *args, **kwargs):
        self.answers += 1

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


@pytest.mark.asyncio
async def test_admin_today_appointment_list_detail_and_returns_stay_on_adminq():
    assert _callbacks(admin_home_keyboard()) == ["adminq:appointments"]
    query = FakeQuery("adminq:appointments")
    await handle_admin_callback(SimpleNamespace(callback_query=query), FakeReader())
    assert _callbacks(query.edits[-1][1]["reply_markup"]) == ["adminq:appointment:7", "adminq:home"]

    query.data = "adminq:appointment:7"
    await handle_admin_callback(SimpleNamespace(callback_query=query), FakeReader())
    assert _callbacks(query.edits[-1][1]["reply_markup"]) == ["adminq:appointments", "adminq:home"]
    assert "我的预约" not in query.edits[-1][0]


def test_v3_application_registers_admin_command_and_adminq_callbacks():
    source = Path("v3_core/user_bot/app.py").read_text(encoding="utf-8")
    assert 'CommandHandler("admin", admin)' in source
    assert 'pattern=r"^adminq:"' in source
    assert 'BotCommand("admin", "咨询后台")' in source
    assert "BotCommandScopeChat(chat_id=admin_id)" in source
    assert ".post_init(configure_command_menu)" in source
    entrypoint = Path("run_v3_user_bot.py").read_text(encoding="utf-8")
    assert "admin_home_handler=cmd_admin_home" in entrypoint
    assert "admin_query_handler=handle_admin_query" in entrypoint
    assert "admin_authorizer=_is_admin_user" in entrypoint


def test_v3_reuses_complete_existing_admin_console():
    assert _callbacks(complete_admin_home_keyboard()) == [
        "adminq:new",
        "adminq:appointments",
        "adminq:listings",
        "adminq:services",
        "adminq:sources",
        "adminq:history",
    ]


def test_v3_statuses_are_centralized():
    assert REVIEW_STATUS_LABELS["pending"] == "待审核"
    assert PACKAGE_STATUS_LABELS["published"] == "已发布"
    assert DELIVERY_STATE_LABELS["unknown"] == "状态未知"
    assert APPOINTMENT_STATUS_LABELS["confirmed"] == ("🟢", "预约已确认")
    admin_source = Path("v3_core/publishing/admin_bot.py").read_text(encoding="utf-8")
    assert "REVIEW_STATUS_LABELS = {" not in admin_source


def test_v3_public_string_literals_have_no_obvious_informal_pronouns():
    forbidden = ("你的", "帮你", "联系你", "收到你的")
    for path in Path("v3_core/user_bot").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not any(token in node.value for token in forbidden), f"{path}:{node.lineno}"


class _AdminDb:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_names(self):
        with self.connect() as conn:
            return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_complete_admin_console_reads_real_v3_appointments_and_service_tickets(tmp_path, monkeypatch):
    path = tmp_path / "admin-v3.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE appointments(id INTEGER PRIMARY KEY, appointment_date TEXT, created_at TEXT);
            CREATE TABLE appointments_v3(
                id INTEGER PRIMARY KEY,user_id INTEGER,username TEXT,display_name TEXT,
                listing_id TEXT,appointment_date TEXT,appointment_time TEXT,status TEXT,created_at TEXT
            );
            CREATE TABLE listings_v3(listing_id TEXT PRIMARY KEY,public_listing_id TEXT);
            CREATE TABLE repair_tickets(id INTEGER PRIMARY KEY);
            CREATE TABLE repair_tickets_v3(
                id INTEGER PRIMARY KEY,issue_type TEXT,status TEXT,created_at TEXT
            );
            INSERT INTO listings_v3 VALUES ('l_15','QL-PP-D2W2');
            INSERT INTO appointments_v3 VALUES
                (1,123,'gym','Gym','l_15','09-10','evening','pending','2026-09-09 11:10:34');
            INSERT INTO repair_tickets_v3 VALUES
                (1,'物业协调','new','2026-09-09 11:12:00');
        """)
    monkeypatch.setattr(attribution_store, "db", _AdminDb(path))

    appointments = attribution_store.list_today_appointments("2026-09-09")
    services = attribution_store.list_service_tickets()

    assert len(appointments) == 1
    assert appointments[0]["public_listing_id"] == "QL-PP-D2W2"
    assert len(services) == 1
    assert services[0]["issue_type"] == "物业协调"


def test_admin_deeplinks_and_recorded_actions_are_all_readable_chinese():
    details = admin_consult._deeplink_zh("property_QC0004_details", "l_4")
    photos = admin_consult._deeplink_zh("property_QC0006_photos", "l_6")
    assert details.startswith("频道房源详情｜房源详情｜") and "property_" not in details
    assert photos.startswith("频道更多实拍｜更多实拍｜") and "property_" not in photos
    assert admin_consult._deeplink_zh("advisor") == "频道联系顾问｜联系中文顾问"
    assert admin_consult.entry_action_zh("search_pref_submit") == "提交找房条件"
