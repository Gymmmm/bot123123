from __future__ import annotations

import ast
from pathlib import Path
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
