from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.telegram_listing_callback import handle_v3_listing_callback


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.message = SimpleNamespace(photo=[])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, text, **kwargs):
        self.calls.append(("edit", text, kwargs.get("reply_markup")))


def _update(raw):
    return SimpleNamespace(
        callback_query=FakeQuery(raw),
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    "hub:home",
    "listing:QC001:photos",
    "service:repair",
    "contract:view",
    "appointment_menu:QC001",
    "apdate:2026-09-20",
    "aptime:18:00",
    "find_available",
    "repair",
    "renewal",
    "termination",
    "change_home",
    "advisor:old",
])
async def test_historical_user_callbacks_do_not_crash_or_restore_old_business(raw):
    update = _update(raw)
    outcome = await handle_v3_listing_callback(
        update,
        SimpleNamespace(user_data={}, bot=object()),
        router=None,
        inventory=None,
        transition_views=None,
        contact_effects=None,
        advisor_url="",
        channel_url="",
    )
    assert outcome.handled
    assert [call[0] for call in update.callback_query.calls] == ["answer", "edit"]
    text = update.callback_query.calls[-1][1]
    assert text == "⚠️ 这个入口已经更新\n请使用下面的最新服务入口。"
    markup = update.callback_query.calls[-1][2]
    labels = [button.text for row in markup.inline_keyboard for button in row]
    assert labels == ["🏠 返回首页", "💬 中文顾问"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row if button.callback_data]
    assert callbacks == ["v3u:t:home", "v3u:home:contact"]
