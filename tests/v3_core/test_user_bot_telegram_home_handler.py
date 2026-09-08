from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.contact_effects import ContactEffectResult
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.telegram_home_handler import handle_v3_home_callback
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionViewService


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


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


def _search_views():
    return TransitionViewService(EmptyInventory())


@pytest.mark.asyncio
async def test_search_home_action_renders_locked_entry_then_sets_keyword_waiting_state():
    query = FakeQuery("v3u:home:search")
    context = _context()

    outcome = await handle_v3_home_callback(
        _update(query),
        context,
        appointment_history=FakeHistory(),
        search_views=_search_views(),
    )

    assert outcome.handled and outcome.rendered and not outcome.deferred
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert "想找什么样的房子" in query.calls[-1][1][0]
    assert context.user_data[AWAITING_KEYWORD_SESSION_KEY] == {"source": "user_search"}
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"] == "user_search"
    markup = query.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == [
        "v3u:t:search_area",
        "v3u:t:search_budget",
        "v3u:t:search_layout",
        "v3u:t:search_available",
        "v3u:t:home",
    ]
    assert not any(value.startswith("hub:") for value in callbacks)


@pytest.mark.asyncio
async def test_search_home_render_failure_does_not_create_keyword_waiting_state():
    query = FakeQuery("v3u:home:search", fail_edit=True)
    context = _context()

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_home_callback(
            _update(query),
            context,
            appointment_history=FakeHistory(),
            search_views=_search_views(),
        )

    assert context.user_data == {}


@pytest.mark.asyncio
async def test_search_without_view_runtime_is_explicitly_deferred():
    query = FakeQuery("v3u:home:search")
    outcome = await handle_v3_home_callback(
        _update(query),
        _context(),
        appointment_history=FakeHistory(),
        search_views=None,
    )
    assert outcome.handled and outcome.deferred and not outcome.rendered
    assert [call[0] for call in query.calls] == ["answer"]


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
async def test_unfinished_rental_and_service_home_actions_are_explicitly_deferred():
    for action in ("rental", "service"):
        query = FakeQuery(f"v3u:home:{action}")
        outcome = await handle_v3_home_callback(
            _update(query),
            _context(),
            appointment_history=FakeHistory(),
            search_views=_search_views(),
        )
        assert outcome.handled and outcome.deferred and not outcome.rendered
        assert [call[0] for call in query.calls] == ["answer"]
