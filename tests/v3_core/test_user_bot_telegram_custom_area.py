from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.telegram_transition_text_handler import handle_v3_transition_text
from v3_core.user_bot.transition_actions import (
    SEARCH_AWAITING_AREA_KEY,
    TransitionActionService,
)
from v3_core.user_bot.transition_session import SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_text_actions import TransitionTextActionService
from v3_core.user_bot.transition_views import TransitionViewService


class EmptyInventory:
    def resolve(self, public_listing_id):
        return None


class FakeMessage:
    def __init__(self, text, *, fail_reply=False):
        self.text = text
        self.fail_reply = fail_reply
        self.calls = []

    async def reply_text(self, *args, **kwargs):
        self.calls.append(("reply_text", args, kwargs))
        if self.fail_reply:
            raise RuntimeError("telegram_reply_failed")
        return SimpleNamespace(chat_id=12345, message_id=88)


def _update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
        effective_chat=SimpleNamespace(id=12345),
        callback_query=None,
    )


def _context(user_data):
    return SimpleNamespace(user_data=user_data, bot=SimpleNamespace())


def _session():
    return {
        SEARCH_AWAITING_AREA_KEY: True,
        SEARCH_PREF_SESSION_KEY: {
            "source": "home_area",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        },
    }


@pytest.mark.asyncio
async def test_custom_area_reply_renders_budget_then_advances_canonical_pref():
    message = FakeMessage("BKK1")
    user_data = _session()

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=TransitionViewService(EmptyInventory()),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.next_step == "search_budget"
    assert SEARCH_AWAITING_AREA_KEY not in user_data
    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == ["BKK1"]
    assert user_data[SEARCH_PREF_SESSION_KEY]["area_display"] == "BKK1"
    rendered = message.calls[-1][1][0]
    assert "每月预算大概多少" in rendered
    assert "已选：BKK1" in rendered
    markup = message.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert "v3u:t:budget_choice:b2" in callbacks
    assert not any(value.startswith("findbudget:") for value in callbacks)


@pytest.mark.asyncio
async def test_custom_area_reply_failure_preserves_waiting_state_and_old_pref():
    message = FakeMessage("BKK1", fail_reply=True)
    user_data = _session()

    with pytest.raises(RuntimeError, match="telegram_reply_failed"):
        await handle_v3_transition_text(
            _update(message),
            _context(user_data),
            actions=TransitionTextActionService(),
            views=TransitionViewService(EmptyInventory()),
        )

    assert user_data[SEARCH_AWAITING_AREA_KEY] is True
    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == []
    assert user_data[SEARCH_PREF_SESSION_KEY]["area_display"] == ""


@pytest.mark.asyncio
async def test_invalid_custom_area_keeps_waiting_state_and_returns_retry_copy():
    message = FakeMessage("完全不存在的地方")
    user_data = _session()

    outcome = await handle_v3_transition_text(
        _update(message),
        _context(user_data),
        actions=TransitionTextActionService(),
        views=TransitionViewService(EmptyInventory()),
    )

    assert outcome.handled and outcome.result is not None
    assert outcome.result.status == "invalid"
    assert SEARCH_AWAITING_AREA_KEY in user_data
    assert user_data[SEARCH_PREF_SESSION_KEY]["location_keys"] == []
    assert "BKK1" in message.calls[-1][1][0]
