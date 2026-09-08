from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_submit_executor import SearchSubmitExecution
from v3_core.user_bot.telegram_callback_handler import SEARCH_SESSION_KEY
from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import LAST_SEARCH_PREF_KEY, TransitionActionService
from v3_core.user_bot.transition_session import SEARCH_PREF_SESSION_KEY


PUBLIC_ID = "QL-RF-A2B3"


class FakeQuery:
    def __init__(self, data, *, fail_edit=False):
        self.data = data
        self.message = SimpleNamespace(photo=[])
        self.fail_edit = fail_edit
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


class ViewsStub:
    inventory = None

    def appointment_date(self, draft):
        raise AssertionError("search must not render appointment date")

    def appointment_time(self, draft):
        raise AssertionError("search must not render appointment time")

    def custom_date_prompt(self):
        raise AssertionError("search must not render custom date")

    def custom_time_prompt(self):
        raise AssertionError("search must not render custom time")


class FakeSearchExecutor:
    def __init__(self, *, matched=True, fail=False):
        self.matched = matched
        self.fail = fail
        self.calls = []

    def execute(self, intent, *, limit=5):
        self.calls.append((intent, limit))
        if self.fail:
            raise RuntimeError("search_failed")
        cards = ()
        ids = ()
        mode = "no_match"
        if self.matched:
            cards = (
                SearchCardResponse(
                    public_listing_id=PUBLIC_ID,
                    text="🏠 <b>BKK1｜1房</b>\n💰 <b>$500/月</b>",
                    photo_path="",
                    action_rows=(
                        (
                            SemanticAction(
                                "🏠 房源详情",
                                "details",
                                target_public_listing_id=PUBLIC_ID,
                            ),
                        ),
                    ),
                    index=0,
                    total=1,
                ),
            )
            ids = (PUBLIC_ID,)
            mode = "strict"
        flow = SearchFlowResult(
            criteria=intent.criteria,
            mode=mode,
            has_similar=False,
            cards=cards,
            public_listing_ids=ids,
        )
        return SearchSubmitExecution(intent=intent, result=flow)


def _user_data():
    return {
        SEARCH_PREF_SESSION_KEY: {
            "source": "similar_listing",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {"from_public_listing_id": PUBLIC_ID},
        }
    }


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
    )


def _context(user_data):
    return SimpleNamespace(user_data=user_data, bot=SimpleNamespace())


@pytest.mark.asyncio
async def test_matched_search_renders_public_card_then_consumes_guided_pref():
    query = FakeQuery("v3u:t:budget_choice:b2")
    user_data = _user_data()
    executor = FakeSearchExecutor(matched=True)

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
        search_executor=executor,
    )

    assert outcome.handled
    assert outcome.search_execution is not None
    assert outcome.search_presentation is not None
    assert outcome.search_presentation.matched
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    assert SEARCH_PREF_SESSION_KEY not in user_data
    assert user_data[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": ["BKK1"],
        "budget_min": 400,
        "budget_max": 600,
    }
    assert user_data[SEARCH_SESSION_KEY] == [PUBLIC_ID]
    assert "LST_" not in repr(user_data)
    markup = query.calls[-1][2]["reply_markup"]
    assert markup.inline_keyboard[0][0].callback_data == f"v3u:listing:details:{PUBLIC_ID}"


@pytest.mark.asyncio
async def test_no_match_renders_locked_copy_with_v3_only_followups():
    query = FakeQuery("v3u:t:budget_choice:b2")
    user_data = _user_data()

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=ViewsStub(),
        search_executor=FakeSearchExecutor(matched=False),
    )

    assert outcome.search_presentation is not None
    assert not outcome.search_presentation.matched
    assert SEARCH_PREF_SESSION_KEY not in user_data
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    text = query.calls[-1][1][0]
    assert "暂时没有完全符合条件的房源" in text
    assert "BKK1｜$400–600" in text
    markup = query.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:change_search", "v3u:t:home"]
    assert not any(value.startswith("find") for value in callbacks)
    assert not any(value.startswith("appointment_menu:") for value in callbacks)


@pytest.mark.asyncio
async def test_search_executor_failure_keeps_guided_pref_unchanged():
    query = FakeQuery("v3u:t:budget_choice:b2")
    user_data = _user_data()
    before = repr(user_data)

    with pytest.raises(RuntimeError, match="search_failed"):
        await handle_v3_transition_action(
            _update(query),
            _context(user_data),
            actions=TransitionActionService(),
            views=ViewsStub(),
            search_executor=FakeSearchExecutor(fail=True),
        )

    assert repr(user_data) == before
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
async def test_search_telegram_failure_keeps_guided_pref_for_retry():
    query = FakeQuery("v3u:t:budget_choice:b2", fail_edit=True)
    user_data = _user_data()

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_transition_action(
            _update(query),
            _context(user_data),
            actions=TransitionActionService(),
            views=ViewsStub(),
            search_executor=FakeSearchExecutor(matched=True),
        )

    assert SEARCH_PREF_SESSION_KEY in user_data
    assert LAST_SEARCH_PREF_KEY not in user_data
    assert SEARCH_SESSION_KEY not in user_data
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
