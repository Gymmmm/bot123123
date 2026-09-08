from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.service_repository import SQLiteTenantServiceRepository
from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.lead_service import LeadRecordResult
from v3_core.user_bot.service_effects import ServiceEffectExecutor
from v3_core.user_bot.service_flow import TenantService
from v3_core.user_bot.telegram_service_handler import (
    SERVICE_GENERAL_WAIT_KEY,
    SERVICE_REQUEST_SESSION_KEY,
    handle_v3_service_callback,
    handle_v3_service_text,
)


class FakeQuery:
    def __init__(self, data, *, fail_edit=False):
        self.data = data
        self.fail_edit = fail_edit
        self.message = SimpleNamespace(photo=[])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))
        if self.fail_edit:
            raise RuntimeError("telegram_edit_failed")

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))
        if self.fail_edit:
            raise RuntimeError("telegram_edit_failed")


class FakeMessage:
    def __init__(self, text):
        self.text = text
        self.calls = []

    async def reply_text(self, *args, **kwargs):
        self.calls.append(("reply_text", args, kwargs))


class CountingLeadService:
    def __init__(self):
        self.calls = []

    def record(self, *, user, request, created_at):
        self.calls.append((user, request, created_at))
        return LeadRecordResult(
            lead_id=len(self.calls),
            action=request.action,
            source=request.source,
        )


class CountingAdmins:
    def __init__(self):
        self.calls = []

    async def send(self, bot, notification):
        self.calls.append((bot, notification))
        return AdminNotificationResult((10,), (10,), ())


def _user():
    return SimpleNamespace(
        id=123,
        username="alice",
        full_name="Alice",
        first_name="Alice",
        last_name="",
    )


def _callback_update(query):
    return SimpleNamespace(callback_query=query, effective_user=_user())


def _text_update(message):
    return SimpleNamespace(effective_message=message, effective_user=_user())


def _context(user_data=None):
    return SimpleNamespace(bot=object(), user_data={} if user_data is None else user_data)


def _service(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    return TenantService(
        SQLiteTenantServiceRepository(db),
        now=lambda: "2026-09-09 03:14:00",
    )


@pytest.mark.asyncio
async def test_issue_state_is_written_only_after_successful_telegram_render(tmp_path):
    service = _service(tmp_path)
    context = _context()
    query = FakeQuery("v3u:service:issue:repair_ac", fail_edit=True)

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_service_callback(
            _callback_update(query),
            context,
            service=service,
        )

    assert SERVICE_REQUEST_SESSION_KEY not in context.user_data


@pytest.mark.asyncio
async def test_repair_detail_keeps_token_and_renders_slot_choices(tmp_path):
    service = _service(tmp_path)
    context = _context()
    query = FakeQuery("v3u:service:issue:repair_ac")
    await handle_v3_service_callback(_callback_update(query), context, service=service)
    token = context.user_data[SERVICE_REQUEST_SESSION_KEY]["request_token"]

    message = FakeMessage("空调可以启动，但一直不制冷。")
    outcome = await handle_v3_service_text(
        _text_update(message),
        context,
        service=service,
    )

    assert outcome.handled and outcome.rendered
    assert context.user_data[SERVICE_REQUEST_SESSION_KEY]["request_token"] == token
    assert context.user_data[SERVICE_REQUEST_SESSION_KEY]["detail"] == "空调可以启动，但一直不制冷。"
    markup = message.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == [
        "v3u:service:slot:today",
        "v3u:service:slot:tomorrow_am",
        "v3u:service:slot:tomorrow_pm",
        "v3u:home:service",
    ]


@pytest.mark.asyncio
async def test_retry_after_success_page_failure_reuses_ticket_and_does_not_repeat_effects(tmp_path):
    service = _service(tmp_path)
    context = _context()
    await handle_v3_service_callback(
        _callback_update(FakeQuery("v3u:service:issue:repair_door")),
        context,
        service=service,
    )
    await handle_v3_service_text(
        _text_update(FakeMessage("门禁刷卡没有反应。")),
        context,
        service=service,
    )

    leads = CountingLeadService()
    admins = CountingAdmins()
    effects = ServiceEffectExecutor(
        leads=leads,
        admins=admins,
        now=lambda: "2026-09-09 03:14:01",
    )

    first_query = FakeQuery("v3u:service:slot:today", fail_edit=True)
    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_service_callback(
            _callback_update(first_query),
            context,
            service=service,
            effects=effects,
        )

    assert SERVICE_REQUEST_SESSION_KEY in context.user_data
    assert len(leads.calls) == 1
    assert len(admins.calls) == 1

    second_query = FakeQuery("v3u:service:slot:today")
    outcome = await handle_v3_service_callback(
        _callback_update(second_query),
        context,
        service=service,
        effects=effects,
    )

    assert outcome.handled and outcome.rendered
    assert outcome.ticket_id is not None
    assert len(leads.calls) == 1
    assert len(admins.calls) == 1
    assert SERVICE_REQUEST_SESSION_KEY not in context.user_data


@pytest.mark.asyncio
async def test_general_text_is_consumed_only_when_explicit_wait_state_exists(tmp_path):
    service = _service(tmp_path)
    message = FakeMessage("需要帮忙联系物业")
    context = _context()

    untouched = await handle_v3_service_text(
        _text_update(message),
        context,
        service=service,
    )
    assert not untouched.handled
    assert message.calls == []

    query = FakeQuery("v3u:service:general")
    await handle_v3_service_callback(_callback_update(query), context, service=service)
    assert context.user_data[SERVICE_GENERAL_WAIT_KEY] is True

    handled = await handle_v3_service_text(
        _text_update(message),
        context,
        service=service,
    )
    assert handled.handled and handled.rendered
    assert SERVICE_GENERAL_WAIT_KEY not in context.user_data
