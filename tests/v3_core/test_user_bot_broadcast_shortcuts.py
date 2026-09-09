from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.contact_effects import ContactEffectResult
from v3_core.user_bot.lead_effects import LeadEffectResult
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
    def __init__(self):
        self.calls = []

    def resolve(self, payload):
        self.calls.append(payload)
        return SimpleNamespace(ok=False)


class FakeSearchExecutor:
    def __init__(self):
        self.calls = []

    def execute(self, intent, *, limit=5):
        self.calls.append((intent, limit))
        result = SearchFlowResult(
            criteria=SearchCriteria(raw_text=""),
            mode="strict",
            has_similar=False,
            cards=(),
            public_listing_ids=(),
        )
        return SimpleNamespace(intent=intent, result=result)


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


def _update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=100),
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
        callback_query=None,
    )


def _context(payload):
    return SimpleNamespace(args=[payload], user_data={"stale": True}, bot=object())


def _views():
    return TransitionViewService(EmptyInventory())


@pytest.mark.asyncio
async def test_find_home_shortcut_enters_guided_search_without_property_resolution():
    assert BROADCAST_START_SHORTCUTS == frozenset({"find_home", "latest", "advisor"})
    message = FakeMessage()
    listings = FakeListings()
    context = _context("find_home")

    outcome = await handle_v3_start(
        _update(message),
        context,
        listings=listings,
        transition_views=_views(),
    )

    assert outcome.handled and outcome.kind == "broadcast_find_home"
    assert listings.calls == []
    assert "想找什么样的房子" in message.calls[0][0][0]
    assert context.user_data[AWAITING_KEYWORD_SESSION_KEY] == {"source": "daily_broadcast"}
    assert context.user_data[SEARCH_PREF_SESSION_KEY]["source"] == "daily_broadcast"
    assert "stale" not in context.user_data


@pytest.mark.asyncio
async def test_latest_shortcut_uses_published_search_executor_not_property_resolution():
    message = FakeMessage()
    listings = FakeListings()
    search = FakeSearchExecutor()
    context = _context("latest")

    outcome = await handle_v3_start(
        _update(message),
        context,
        listings=listings,
        transition_views=_views(),
        search_executor=search,
    )

    assert outcome.handled and outcome.kind == "broadcast_latest"
    assert listings.calls == []
    assert len(search.calls) == 1
    intent, limit = search.calls[0]
    assert limit == 5
    assert intent.source == "daily_broadcast_latest"
    assert intent.touch_payload == {"daily_broadcast": True, "latest": True}
    assert "暂时没有完全符合条件的房源" in message.calls[-1][0][0]


@pytest.mark.asyncio
async def test_advisor_shortcut_records_contact_effect_and_renders_advisor_handoff():
    message = FakeMessage()
    listings = FakeListings()
    effects = FakeContactEffects()
    context = _context("advisor")

    outcome = await handle_v3_start(
        _update(message),
        context,
        listings=listings,
        transition_views=_views(),
        contact_effects=effects,
        advisor_url="https://t.me/advisor",
    )

    assert outcome.handled and outcome.kind == "broadcast_advisor"
    assert listings.calls == []
    assert len(effects.calls) == 1
    _, user, source = effects.calls[0]
    assert user.user_id == 123
    assert source == "daily_broadcast"
    assert "有什么需要" in message.calls[-1][0][0]
    markup = message.calls[-1][1]["reply_markup"]
    assert markup.inline_keyboard[0][0].url == "https://t.me/advisor"


@pytest.mark.asyncio
async def test_unknown_start_payload_still_falls_through_to_public_property_validation():
    message = FakeMessage()
    listings = FakeListings()
    context = _context("not_a_real_shortcut")

    outcome = await handle_v3_start(
        _update(message),
        context,
        listings=listings,
        transition_views=_views(),
    )

    assert outcome.handled and outcome.kind == "invalid_link"
    assert listings.calls == ["not_a_real_shortcut"]
    assert message.calls[-1][0][0] == (
        "这个链接已经失效或房源信息已更新。\n\n"
        "您可以重新找房，或直接联系我们。"
    )
