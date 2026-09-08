from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.contact_effects import ContactEffectResult
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.telegram_home_handler import handle_v3_home_callback


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.message = SimpleNamespace(photo=[])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))


class FakeHistory:
    def __init__(self):
        self.calls = []

    def build(self, user_id):
        self.calls.append(user_id)
        return AppointmentHistoryView(
            text="📅 <b>我的预约</b>\n\n🆔 QL-RF-A2B3",
            items=(),
            history_count=0,
        )


class FakeContactEffects:
    def __init__(self):
        self.calls = []

    async def execute_general(self, *, bot, user, source="hub"):
        self.calls.append((bot, user, source))
        return ContactEffectResult(
            lead=LeadEffectResult(status="recorded"),
            admin=AdminNotificationResult(
                attempted_admin_ids=(10,),
                sent_admin_ids=(10,),
                failed_admin_ids=(),
            ),
        )


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
    )


def _context():
    return SimpleNamespace(bot=object(), user_data={})


@pytest.mark.asyncio
async def test_appointments_home_action_renders_public_history_with_v3_navigation():
    query = FakeQuery("v3u:home:appointments")
    history = FakeHistory()

    outcome = await handle_v3_home_callback(
        _update(query),
        _context(),
        appointment_history=history,
    )

    assert outcome.handled and outcome.rendered and not outcome.deferred
    assert history.calls == [123]
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    text = query.calls[-1][1][0]
    assert "QL-RF-A2B3" in text
    assert "l_" not in text.lower()
    markup = query.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:home:search", "v3u:t:home"]


@pytest.mark.asyncio
async def test_contact_home_action_runs_effects_before_rendering_handoff():
    query = FakeQuery("v3u:home:contact")
    effects = FakeContactEffects()

    outcome = await handle_v3_home_callback(
        _update(query),
        _context(),
        appointment_history=FakeHistory(),
        contact_effects=effects,
        advisor_url="https://t.me/advisor",
    )

    assert outcome.handled and outcome.rendered
    assert outcome.contact_effect is not None
    assert len(effects.calls) == 1
    _, user, source = effects.calls[0]
    assert user.user_id == 123 and source == "hub"
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert "有什么需要" in query.calls[-1][1][0]
    assert query.calls[-1][2]["reply_markup"].inline_keyboard[0][0].url == "https://t.me/advisor"


@pytest.mark.asyncio
async def test_contact_without_effect_executor_is_deferred_and_never_claims_success():
    query = FakeQuery("v3u:home:contact")

    outcome = await handle_v3_home_callback(
        _update(query),
        _context(),
        appointment_history=FakeHistory(),
        contact_effects=None,
    )

    assert outcome.handled and outcome.deferred and not outcome.rendered
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
async def test_unfinished_home_actions_are_explicitly_deferred():
    for action in ("search", "rental", "service"):
        query = FakeQuery(f"v3u:home:{action}")
        outcome = await handle_v3_home_callback(
            _update(query),
            _context(),
            appointment_history=FakeHistory(),
        )
        assert outcome.handled and outcome.deferred and not outcome.rendered
        assert [call[0] for call in query.calls] == ["answer"]
