from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_submit_executor import SearchSubmitExecution
from v3_core.user_bot.telegram_callback_handler import SEARCH_SESSION_KEY
from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import (
    LAST_SEARCH_PREF_KEY,
    SEARCH_AWAITING_AREA_KEY,
    TransitionActionService,
)
from v3_core.user_bot.transition_session import SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_views import TransitionViewService


PUBLIC_ID = "QL-RF-A2B3"


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


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


class FakeSearchExecutor:
    def __init__(self, *, matched=True):
        self.matched = matched
        self.calls = []

    def execute(self, intent, *, limit=5):
        self.calls.append(intent)
        if self.matched:
            cards = (
                SearchCardResponse(
                    public_listing_id=PUBLIC_ID,
                    text="🏠 <b>BKK1｜2房</b>",
                    photo_path="",
                    action_rows=((SemanticAction("房源详情", "details", PUBLIC_ID),),),
                    index=0,
                    total=1,
                ),
            )
            mode = "strict"
            ids = (PUBLIC_ID,)
        else:
            cards = ()
            mode = "no_match"
            ids = ()
        return SearchSubmitExecution(
            intent=intent,
            result=SearchFlowResult(
                criteria=intent.criteria,
                mode=mode,
                has_similar=False,
                cards=cards,
                public_listing_ids=ids,
            ),
        )


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
    )


def _context(user_data):
    return SimpleNamespace(user_data=user_data, bot=SimpleNamespace())


def _views():
    return TransitionViewService(EmptyInventory())


def _callbacks(query):
    markup = query.calls[-1][2]["reply_markup"]
    return [button.callback_data for row in markup.inline_keyboard for button in row]


@pytest.mark.asyncio
async def test_search_area_renders_v3_only_choices_then_initializes_pref():
    query = FakeQuery("v3u:t:search_area")
    user_data = {}

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.navigation == "search_area"
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]
    callbacks = _callbacks(query)
    assert "v3u:t:area_choice:bkk1" in callbacks
    assert "v3u:t:area_choice:rf" in callbacks
    assert "v3u:t:area_other" in callbacks
    assert "v3u:change_search" in callbacks
    assert not any(value.startswith(("findarea:", "hub:")) for value in callbacks)
    assert user_data[SEARCH_PREF_SESSION_KEY]["source"] == "home_area"
    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == []


@pytest.mark.asyncio
async def test_area_choice_renders_budget_with_canonical_location_after_edit():
    query = FakeQuery("v3u:t:area_choice:bkk1")
    user_data = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "home_area",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        }
    }

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.navigation == "search_budget"
    assert "已选：BKK1" in query.calls[-1][1][0]
    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == ["BKK1"]
    assert user_data[SEARCH_PREF_SESSION_KEY]["area_display"] == "BKK1"
    callbacks = _callbacks(query)
    assert "v3u:t:budget_choice:b2" in callbacks
    assert not any(value.startswith("findbudget:") for value in callbacks)


@pytest.mark.asyncio
async def test_area_render_failure_does_not_advance_real_pref():
    query = FakeQuery("v3u:t:area_choice:bkk1", fail_edit=True)
    user_data = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "home_area",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        }
    }

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_transition_action(
            _update(query),
            _context(user_data),
            actions=TransitionActionService(),
            views=_views(),
        )

    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == []
    assert user_data[SEARCH_PREF_SESSION_KEY]["area_display"] == ""


@pytest.mark.asyncio
async def test_other_area_prompt_sets_waiting_flag_only_after_render():
    query = FakeQuery("v3u:t:area_other")
    user_data = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "home_area",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        }
    }

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "search_custom_area"
    assert "其他位置" in query.calls[-1][1][0]
    assert query.calls[-1][2]["reply_markup"] is None
    assert user_data[SEARCH_AWAITING_AREA_KEY] is True


@pytest.mark.asyncio
async def test_search_layout_renders_v3_layout_callbacks_only():
    query = FakeQuery("v3u:t:search_layout")
    user_data = {}

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.navigation == "search_layout"
    callbacks = _callbacks(query)
    assert "v3u:t:layout_choice:studio" in callbacks
    assert "v3u:t:layout_choice:2br" in callbacks
    assert "v3u:t:layout_choice:any" in callbacks
    assert not any(value.startswith("roompick:") for value in callbacks)


@pytest.mark.asyncio
async def test_layout_choice_crosses_existing_search_executor_boundary():
    query = FakeQuery("v3u:t:layout_choice:2br")
    user_data = {}
    executor = FakeSearchExecutor(matched=True)

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
        search_executor=executor,
    )

    assert outcome.search_execution is not None
    assert outcome.search_presentation is not None and outcome.search_presentation.matched
    assert executor.calls[0].source == "home_layout"
    assert executor.calls[0].criteria.room_type == "2房"
    assert user_data[SEARCH_SESSION_KEY] == [PUBLIC_ID]
    assert user_data[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": [],
        "budget_min": None,
        "budget_max": None,
    }
    assert "LST_" not in repr(user_data)


@pytest.mark.asyncio
async def test_current_available_directly_uses_published_only_search_executor():
    query = FakeQuery("v3u:t:search_available")
    user_data = {}
    executor = FakeSearchExecutor(matched=True)

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=_views(),
        search_executor=executor,
    )

    assert outcome.search_execution is not None
    assert outcome.search_presentation is not None and outcome.search_presentation.matched
    intent = executor.calls[0]
    assert intent.source == "home_available"
    assert intent.criteria.has_filter is False
    assert intent.touch_payload == {"current_available": True}
    assert user_data[SEARCH_SESSION_KEY] == [PUBLIC_ID]
    assert "LST_" not in repr(user_data)
