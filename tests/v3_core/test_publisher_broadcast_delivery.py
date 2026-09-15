from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from v3_core.publishing.broadcast import BroadcastService, BroadcastSettingsRepository
from v3_core.publishing.broadcast_admin import BroadcastAdminController
from v3_core.storage.bootstrap import initialize_v3_storage


class FakeLiveBuilder:
    def build(self, *, fx_offset=-0.20, now=None):
        return "<b>daily</b>"


class FakeBot:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("telegram_uncertain")
        return SimpleNamespace(message_id=7788)


def _build(tmp_path: Path):
    db = tmp_path / "broadcast.db"
    initialize_v3_storage(db)
    service = BroadcastService(
        repository=BroadcastSettingsRepository(db),
        repo_root=Path(__file__).resolve().parents[2],
        user_bot_username="qiaolian_rent_bot",
        live_builder=FakeLiveBuilder(),
    )
    service.ensure_defaults()
    controller = BroadcastAdminController(
        service=service,
        channel_chat_id="-1001234567890",
        timezone_name="Asia/Phnom_Penh",
    )
    return db, service, controller


def _logs(db: Path):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            "SELECT trigger_type, template_key, channel_chat_id, channel_message_id, status, local_date, error_text "
            "FROM publisher_broadcast_log_v3 ORDER BY id"
        )]


@pytest.mark.asyncio
async def test_manual_send_persists_success_receipt(tmp_path):
    db, service, controller = _build(tmp_path)
    service.set_button("find")
    bot = FakeBot()
    context = SimpleNamespace(bot=bot)

    result = await controller._send_channel(context, "<b>manual</b>", trigger_type="manual")

    assert result.message_id == 7788
    assert len(bot.calls) == 1
    call = bot.calls[0]
    assert call["chat_id"] == "-1001234567890"
    assert call["text"] == "<b>manual</b>"
    assert call["disable_web_page_preview"] is True
    button = call["reply_markup"].inline_keyboard[0][0]
    assert button.url.endswith("?start=find_home")
    rows = _logs(db)
    assert len(rows) == 1
    assert rows[0]["trigger_type"] == "manual"
    assert rows[0]["status"] == "sent"
    assert rows[0]["channel_message_id"] == 7788


@pytest.mark.asyncio
async def test_scheduled_success_claims_then_marks_sent(tmp_path):
    db, service, controller = _build(tmp_path)
    service.set_enabled(True)
    service.set_time("09:30")
    fixed = datetime(2026, 9, 9, 9, 30, 20, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    service.local_now = lambda: fixed
    bot = FakeBot()
    context = SimpleNamespace(bot=bot)

    await controller.scheduled_tick(context)

    assert len(bot.calls) == 1
    config = service.config()
    assert config.last_scheduled_attempt_date == "2026-09-09"
    assert config.last_scheduled_sent_date == "2026-09-09"
    rows = _logs(db)
    assert [(row["trigger_type"], row["status"], row["channel_message_id"]) for row in rows] == [
        ("scheduled", "sent", 7788)
    ]


@pytest.mark.asyncio
async def test_scheduled_uncertain_failure_is_logged_and_never_auto_resent_same_day(tmp_path):
    db, service, controller = _build(tmp_path)
    service.set_enabled(True)
    service.set_time("09:30")
    fixed = datetime(2026, 9, 9, 9, 30, 5, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    service.local_now = lambda: fixed
    bot = FakeBot(fail=True)
    context = SimpleNamespace(bot=bot)

    await controller.scheduled_tick(context)
    await controller.scheduled_tick(context)

    assert len(bot.calls) == 1
    config = service.config()
    assert config.last_scheduled_attempt_date == "2026-09-09"
    assert config.last_scheduled_sent_date == ""
    rows = _logs(db)
    assert len(rows) == 1
    assert rows[0]["trigger_type"] == "scheduled"
    assert rows[0]["status"] == "failed_or_unknown"
    assert "telegram_uncertain" in rows[0]["error_text"]


@pytest.mark.asyncio
async def test_scheduler_never_touches_network_when_disabled_or_wrong_minute(tmp_path):
    _, service, controller = _build(tmp_path)
    fixed = datetime(2026, 9, 9, 9, 29, 0, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    service.local_now = lambda: fixed
    bot = FakeBot()
    context = SimpleNamespace(bot=bot)

    await controller.scheduled_tick(context)
    assert bot.calls == []

    service.set_enabled(True)
    service.set_time("09:30")
    await controller.scheduled_tick(context)
    assert bot.calls == []


@pytest.mark.asyncio
async def test_daily_copy_and_button_can_be_changed_from_admin(tmp_path):
    _, service, controller = _build(tmp_path)
    message = SimpleNamespace(text="今天的新文案", reply_text=AsyncMock())
    context = SimpleNamespace(user_data={"v3_publisher_broadcast_edit": {"kind": "daily_copy"}})

    assert await controller.handle_text(SimpleNamespace(effective_message=message), context) is True
    assert service.config().template_key == "custom"
    assert service.body() == "今天的新文案"

    query = SimpleNamespace(data="v3bc|daily_btn|combo", message=message)
    assert await controller.handle_callback(SimpleNamespace(callback_query=query), context) is True
    assert service.config().button_key == "combo"

    bot = FakeBot()
    await controller._send_weather_now(SimpleNamespace(bot=bot))
    assert bot.calls[0]["text"] == "今天的新文案"


@pytest.mark.asyncio
async def test_each_weekday_copy_and_button_can_be_changed(tmp_path):
    _, _, controller = _build(tmp_path)
    message = SimpleNamespace(text="周三的新文案", reply_text=AsyncMock())
    context = SimpleNamespace(user_data={"v3_publisher_broadcast_edit": {"kind": "marketing_copy", "weekday": 2}})

    assert await controller.handle_text(SimpleNamespace(effective_message=message), context) is True
    assert controller.marketing.template(2).body == "周三的新文案"

    query = SimpleNamespace(data="v3bc|m_btn|2|contact", message=message)
    assert await controller.handle_callback(SimpleNamespace(callback_query=query), context) is True
    assert controller.marketing.button_key(2) == "contact"
    rows = controller.marketing.footer_rows(controller.marketing.template(2))
    assert rows[0][0].label == "💬 联系中文顾问"


@pytest.mark.asyncio
async def test_temporary_broadcast_uses_its_own_selected_button(tmp_path):
    _, _, controller = _build(tmp_path)
    message = SimpleNamespace(reply_text=AsyncMock())
    bot = FakeBot()
    context = SimpleNamespace(
        bot=bot,
        user_data={"v3_publisher_broadcast_edit": {"kind": "custom_ready", "text": "临时通知", "button_key": "latest"}},
    )
    query = SimpleNamespace(data="v3bc|custom_send", message=message)

    assert await controller.handle_callback(SimpleNamespace(callback_query=query), context) is True
    assert bot.calls[0]["text"] == "临时通知"
    assert bot.calls[0]["reply_markup"].inline_keyboard[0][0].text == "🏠 最新房源"
