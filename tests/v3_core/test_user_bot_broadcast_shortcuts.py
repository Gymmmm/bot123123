from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.appointment_history import AppointmentHistoryView
from v3_core.user_bot.contact_effects import ContactEffectResult
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.listing_responses import PublicDetailsResponse, SemanticAction
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.telegram_start_handler import BROADCAST_START_SHORTCUTS, handle_v3_start
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionViewService


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


class FakeMessage:
    def __init__(self):
        self.calls = []

    async def reply_text(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return SimpleNamespace(message_id=len(self.calls))


class FakeListings:
    def __init__(self, result=None):
        self.calls = []
        self.result = result

    def resolve(self, payload):
        self.calls.append(payload)
        if self.result is not None:
            return self.result
        return SimpleNamespace(ok=False)


class FakeSearchExecutor:
    def __init__(self):
        self.calls = []

    def execute(self, intent, *, limit=5):
        self.calls.append((intent, limit))
        result = SearchFlowResult(
            criteria=SearchCriteria(raw_text=""), mode="strict", has_similar=False,
            cards=(), public_listing_ids=(),
        )
        return SimpleNamespace(intent=intent, result=result)


class FakeContactEffects:
    def __init__(self):
        self.calls = []

    async def execute_general(self, *, bot, user, source="hub"):
        self.calls.append((bot, user, source))
        return ContactEffectResult(
            lead=LeadEffectResult(status="recorded"),
            admin=AdminNotificationResult((10,), (10,), ()),
        )


class FakeAppointmentHistory:
    def __init__(self):
        self.calls = []

    def build(self, user_id):
        self.calls.append(user_id)
        return AppointmentHistoryView(text="📅 <b>目前还没有看房预约</b>", items=(), history_count=0)


class FakeTenantService:
    def active_binding(self, user_id):
        assert user_id == 123
        return SimpleNamespace(property_name="富力城 A3-1208")


def _update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=100),
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice", first_name="Alice", last_name=""),
        callback_query=None,
    )


def _context(payload):
    return SimpleNamespace(args=[payload], user_data={"stale": True}, bot=object())


def _views():
    return TransitionViewService(EmptyInventory())


@pytest.mark.asyncio
async def test_find_home_shortcut_enters_guided_search_without_property_resolution():
    assert BROADCAST_START_SHORTCUTS == frozenset({"find_home", "latest", "advisor", "budget", "appointments", "assurance", "service"})
    message = FakeMessage()
    listings = FakeListings()
    context = _context("find_home")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views())
    assert outcome.handled and outcome.kind == "broadcast_find_home"
    assert listings.calls == []
    assert "想住什么样的房子" in message.calls[0][0][0]
    assert context.user_data[AWAITING_KEYWORD_SESSION_KEY] == {"source": "daily_broadcast"}
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"] == "daily_broadcast"
    assert "stale" not in context.user_data


@pytest.mark.asyncio
async def test_budget_shortcut_opens_budget_filter_directly():
    message = FakeMessage()
    listings = FakeListings()
    context = _context("budget")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views())
    assert outcome.handled and outcome.kind == "broadcast_budget"
    assert listings.calls == []
    assert "每月预算大概多少" in message.calls[-1][0][0]
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"] == "daily_broadcast"


@pytest.mark.asyncio
async def test_latest_shortcut_uses_published_search_executor_not_property_resolution():
    message = FakeMessage()
    listings = FakeListings()
    search = FakeSearchExecutor()
    context = _context("latest")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views(), search_executor=search)
    assert outcome.handled and outcome.kind == "broadcast_latest"
    assert listings.calls == []
    assert len(search.calls) == 1
    intent, limit = search.calls[0]
    assert limit == 5
    assert intent.source == "daily_broadcast_latest"
    assert intent.touch_payload == {"daily_broadcast": True, "latest": True}
    assert "这组条件暂时没有对上的房源" in message.calls[-1][0][0]


@pytest.mark.asyncio
async def test_appointments_shortcut_uses_real_appointment_history():
    message = FakeMessage()
    listings = FakeListings()
    history = FakeAppointmentHistory()
    context = _context("appointments")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views(), appointment_history=history)
    assert outcome.handled and outcome.kind == "broadcast_appointments"
    assert listings.calls == []
    assert history.calls == [123]
    assert "目前还没有看房预约" in message.calls[-1][0][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_kind", "expected_text"),
    [("assurance", "broadcast_assurance", "租到房，不代表服务就结束了"), ("service", "broadcast_service", "入住服务")],
)
async def test_service_shortcuts_land_on_real_user_surfaces(payload, expected_kind, expected_text):
    message = FakeMessage()
    listings = FakeListings()
    context = _context(payload)
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views())
    assert outcome.handled and outcome.kind == expected_kind
    assert listings.calls == []
    assert expected_text in message.calls[-1][0][0]
    if payload == "assurance":
        assert "入住时" in message.calls[-1][0][0]
        assert "关于侨联" not in message.calls[-1][0][0]


@pytest.mark.asyncio
async def test_service_shortcut_uses_bound_tenant_home_without_extra_gate():
    message = FakeMessage()
    outcome = await handle_v3_start(
        _update(message),
        _context("service"),
        listings=FakeListings(),
        transition_views=_views(),
        tenant_service=FakeTenantService(),
    )
    assert outcome.handled and outcome.kind == "broadcast_service"
    text = message.calls[-1][0][0]
    markup = message.calls[-1][1]["reply_markup"]
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data
    ]
    assert "富力城 A3-1208" in text
    assert "v3u:service:tenant_lease" in callbacks
    assert "v3u:service:tenant" not in callbacks


@pytest.mark.asyncio
async def test_advisor_shortcut_records_contact_effect_and_renders_advisor_handoff():
    message = FakeMessage()
    listings = FakeListings()
    effects = FakeContactEffects()
    context = _context("advisor")
    outcome = await handle_v3_start(
        _update(message), context, listings=listings, transition_views=_views(),
        contact_effects=effects, advisor_url="https://t.me/advisor",
    )
    assert outcome.handled and outcome.kind == "broadcast_advisor"
    assert listings.calls == []
    assert len(effects.calls) == 1
    _, user, source = effects.calls[0]
    assert user.user_id == 123
    assert source == "daily_broadcast"
    assert "中文顾问" in message.calls[-1][0][0]
    assert "顾问帮我找" not in message.calls[-1][0][0]
    markup = message.calls[-1][1]["reply_markup"]
    assert markup.inline_keyboard[0][0].url.startswith("https://t.me/advisor?text=")


@pytest.mark.asyncio
async def test_unknown_start_payload_without_reason_still_falls_through_without_attribute_error():
    message = FakeMessage()
    listings = FakeListings()
    context = _context("not_a_real_shortcut")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views())
    assert outcome.handled and outcome.kind == "invalid_link"
    assert listings.calls == ["not_a_real_shortcut"]
    assert message.calls[-1][0][0] == (
        "这套房的入口已经失效，或信息刚刚更新过。\n\n可以重新找房，或让顾问按你的条件接着看。"
    )


@pytest.mark.asyncio
async def test_unbookable_property_book_deeplink_shows_lock_copy_then_contextual_details():
    public_id = "QL-RF-A2B3"
    details = PublicDetailsResponse(
        text="🏠 <b>房源详情</b>\n\n🔴 房态：已租出",
        action_rows=(
            (SemanticAction("💬 问这套房", "consult", public_id),),
            (SemanticAction("📸 更多实拍", "photos", public_id),),
            (SemanticAction("✏️ 换个条件找", "change_search"),),
        ),
    )
    listings = FakeListings(SimpleNamespace(ok=False, reason="listing_not_bookable", details=details, action="book", public_listing_id=public_id))
    message = FakeMessage()
    context = _context(f"property_{public_id}_book")
    outcome = await handle_v3_start(_update(message), context, listings=listings, transition_views=_views())
    assert outcome.handled and outcome.kind == "unbookable"
    assert listings.calls == [f"property_{public_id}_book"]
    assert message.calls[0][0][0] == "这套房现在暂时不能预约。\n\n可以先看详情，或让顾问帮你看别的选择。"
    assert "🔴 房态：已租出" in message.calls[1][0][0]
    markup = message.calls[1][1]["reply_markup"]
    labels = [button.text for row in markup.inline_keyboard for button in row]
    assert "📅 预约看房" not in labels
    assert "✏️ 换个条件找" in labels
    assert "看相近房源" not in " ".join(labels)
