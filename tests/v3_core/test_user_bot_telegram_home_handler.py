from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
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
    def __init__(self, db_path=None):
        self.calls = []
        self.reader = SimpleNamespace(db_path=db_path) if db_path is not None else None

    def build(self, user_id):
        self.calls.append(user_id)
        return AppointmentHistoryView(text="📅 <b>我的预约</b>\n\n🆔 QL-RF-A2B3", items=(), history_count=0)


class FakeContactEffects:
    def __init__(self):
        self.calls = []

    async def execute_general(self, *, bot, user, source="hub"):
        self.calls.append((bot, user, source))
        return ContactEffectResult(
            lead=LeadEffectResult(status="recorded"),
            admin=AdminNotificationResult(attempted_admin_ids=(10,), sent_admin_ids=(10,), failed_admin_ids=()),
        )


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice", first_name="Alice", last_name=""),
    )


def _context():
    return SimpleNamespace(bot=object(), user_data={})


def _search_views():
    return TransitionViewService(EmptyInventory())


def _tenant_db(tmp_path, *, bound=False):
    db = tmp_path / "home-tenant.db"
    initialize_v3_storage(db)
    if bound:
        with sqlite3.connect(str(db)) as conn:
            conn.execute(
                """INSERT INTO tenant_bindings_v3
                   (user_id,binding_code,property_name,status,created_at)
                   VALUES (123,'BIND-123','富力城 A3-1208','active','2026-09-09 03:00:00')"""
            )
            conn.commit()
    return db


@pytest.mark.asyncio
async def test_search_home_action_renders_locked_entry_then_sets_keyword_waiting_state():
    query = FakeQuery("v3u:home:search")
    context = _context()
    outcome = await handle_v3_home_callback(_update(query), context, appointment_history=FakeHistory(), search_views=_search_views())
    assert outcome.handled and outcome.rendered and not outcome.deferred
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert "想住什么样的房子" in query.calls[-1][1][0]
    assert context.user_data[AWAITING_KEYWORD_SESSION_KEY] == {"source": "user_search"}
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"] == "user_search"
    markup = query.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:t:search_area", "v3u:t:search_budget", "v3u:t:search_layout", "v3u:t:home"]
    assert "v3u:t:search_available" not in callbacks
    assert not any(value.startswith("hub:") for value in callbacks)


@pytest.mark.asyncio
async def test_search_home_render_failure_does_not_create_keyword_waiting_state():
    query = FakeQuery("v3u:home:search", fail_edit=True)
    context = _context()
    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_home_callback(_update(query), context, appointment_history=FakeHistory(), search_views=_search_views())
    assert context.user_data == {}


@pytest.mark.asyncio
async def test_search_without_view_runtime_is_explicitly_deferred():
    query = FakeQuery("v3u:home:search")
    outcome = await handle_v3_home_callback(_update(query), _context(), appointment_history=FakeHistory(), search_views=None)
    assert outcome.handled and outcome.deferred and not outcome.rendered
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
async def test_appointments_home_action_renders_public_history_with_v3_navigation():
    query = FakeQuery("v3u:home:appointments")
    history = FakeHistory()
    outcome = await handle_v3_home_callback(_update(query), _context(), appointment_history=history)
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
        _update(query), _context(), appointment_history=FakeHistory(),
        contact_effects=effects, advisor_url="https://t.me/advisor",
    )
    assert outcome.handled and outcome.rendered
    assert outcome.contact_effect is not None
    assert len(effects.calls) == 1
    _, user, source = effects.calls[0]
    assert user.user_id == 123 and source == "hub"
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert "中文顾问" in query.calls[-1][1][0]
    assert "顾问帮我找" not in query.calls[-1][1][0]
    assert query.calls[-1][2]["reply_markup"].inline_keyboard[0][0].url.startswith("https://t.me/advisor?text=")


@pytest.mark.asyncio
async def test_contact_without_effect_executor_is_deferred_and_never_claims_success():
    query = FakeQuery("v3u:home:contact")
    outcome = await handle_v3_home_callback(_update(query), _context(), appointment_history=FakeHistory(), contact_effects=None)
    assert outcome.handled and outcome.deferred and not outcome.rendered
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "expected_text", "expected_callbacks"),
    [
        (
            "rental",
            "租到房，不代表服务就结束了",
            {"v3u:assure:handover", "v3u:home:search", "v3u:home:contact", "v3u:t:home"},
        ),
        (
            "service",
            "入住服务",
            {"v3u:home:contact", "v3u:home:rental", "v3u:home:search", "v3u:t:home"},
        ),
    ],
)
async def test_assurance_and_service_home_actions_are_complete_v3_surfaces(action, expected_text, expected_callbacks):
    query = FakeQuery(f"v3u:home:{action}")
    outcome = await handle_v3_home_callback(
        _update(query), _context(), appointment_history=FakeHistory(), search_views=_search_views(),
    )
    assert outcome.handled and outcome.rendered and not outcome.deferred
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert expected_text in query.calls[-1][1][0]
    markup = query.calls[-1][2]["reply_markup"]
    callbacks = {button.callback_data for row in markup.inline_keyboard for button in row}
    assert callbacks == expected_callbacks
    assert not any(value.startswith(("hub:", "service:")) for value in callbacks)
    labels = [button.text for row in markup.inline_keyboard for button in row]
    if action == "service":
        assert "报修" not in "".join(labels)
        assert "租约" not in "".join(labels)
        assert "物业" not in "".join(labels)


@pytest.mark.asyncio
async def test_home_service_unbound_user_goes_through_binding_gate(tmp_path):
    query = FakeQuery("v3u:home:service")
    history = FakeHistory(_tenant_db(tmp_path, bound=False))
    outcome = await handle_v3_home_callback(_update(query), _context(), appointment_history=history)
    assert outcome.handled and outcome.rendered
    text = query.calls[-1][1][0]
    labels = [button.text for row in query.calls[-1][2]["reply_markup"].inline_keyboard for button in row]
    assert "这边还没有显示你的住房信息" in text
    assert labels == ["💬 中文顾问", "🛡 看租后服务", "🔍 开始找房", "⬅️ 回首页"]
    assert all(word not in "".join(labels) for word in ("报修", "租约", "物业"))


@pytest.mark.asyncio
async def test_home_service_bound_user_opens_resident_home(tmp_path):
    query = FakeQuery("v3u:home:service")
    history = FakeHistory(_tenant_db(tmp_path, bound=True))
    outcome = await handle_v3_home_callback(_update(query), _context(), appointment_history=history)
    assert outcome.handled and outcome.rendered
    text = query.calls[-1][1][0]
    labels = [button.text for row in query.calls[-1][2]["reply_markup"].inline_keyboard for button in row]
    assert "富力城 A3-1208" in text
    assert "📋 我的租约" in labels
    assert "🔧 报修" in labels
    assert "🏢 物业协调" in labels
    assert "📍 周边服务" in labels
