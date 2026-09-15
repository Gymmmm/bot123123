from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.telegram_transition_action_handler import handle_v3_transition_action
from v3_core.user_bot.transition_actions import TransitionActionService
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY
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
        raise AssertionError


def _update(query):
    return SimpleNamespace(callback_query=query)


def _context(user_data):
    return SimpleNamespace(user_data=user_data)


@pytest.mark.asyncio
async def test_guided_area_entry_clears_keyword_waiting_only_after_successful_render():
    query = FakeQuery("v3u:t:search_area")
    user_data = {AWAITING_KEYWORD_SESSION_KEY: {"source": "user_search"}}

    outcome = await handle_v3_transition_action(
        _update(query),
        _context(user_data),
        actions=TransitionActionService(),
        views=TransitionViewService(EmptyInventory()),
    )

    assert outcome.handled
    assert AWAITING_KEYWORD_SESSION_KEY not in user_data
    assert [call[0] for call in query.calls] == ["answer", "edit_text"]


@pytest.mark.asyncio
async def test_guided_area_render_failure_preserves_keyword_waiting_for_retry():
    query = FakeQuery("v3u:t:search_area", fail_edit=True)
    user_data = {AWAITING_KEYWORD_SESSION_KEY: {"source": "user_search"}}

    with pytest.raises(RuntimeError, match="telegram_edit_failed"):
        await handle_v3_transition_action(
            _update(query),
            _context(user_data),
            actions=TransitionActionService(),
            views=TransitionViewService(EmptyInventory()),
        )

    assert user_data[AWAITING_KEYWORD_SESSION_KEY] == {"source": "user_search"}
