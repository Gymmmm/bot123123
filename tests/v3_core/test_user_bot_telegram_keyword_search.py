from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.keyword_search_actions import KeywordSearchActionService
from v3_core.user_bot.lead_effects import LeadEffectResult
from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_submit_executor import SearchSubmitExecution
from v3_core.user_bot.telegram_callback_handler import SEARCH_SESSION_KEY
from v3_core.user_bot.telegram_keyword_search_handler import handle_v3_keyword_search_text
from v3_core.user_bot.transition_actions import LAST_SEARCH_PREF_KEY
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY


PUBLIC_ID = "QL-RF-A2B3"


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


class FakeBot:
    def __init__(self, *, fail_send=False):
        self.fail_send = fail_send
        self.calls = []

    async def send_message(self, *args, **kwargs):
        self.calls.append(("send_message", args, kwargs))
        if self.fail_send:
            raise RuntimeError("telegram_send_failed")
        return SimpleNamespace(chat_id=kwargs.get("chat_id", 12345), message_id=99)

    async def send_photo(self, *args, **kwargs):
        raise AssertionError("keyword search test cards have no photo")


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
                    text="🏠 <b>BKK1｜1房</b>",
                    photo_path="",
                    action_rows=((SemanticAction("房源详情", "details", PUBLIC_ID),),),
                    index=0,
                    total=1,
                ),
            )
            ids = (PUBLIC_ID,)
            mode = "strict"
        else:
            cards = ()
            ids = ()
            mode = "no_match"
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


class FakeLeadEffects:
    def __init__(self):
        self.calls = []

    def record_keyword_search(self, *, user, intent, match_mode):
        self.calls.append((user, intent, match_mode))
        return LeadEffectResult(status="recorded")


def _session():
    return {
        AWAITING_KEYWORD_SESSION_KEY: {"source": "user_search"},
        SEARCH_PREF_SESSION_KEY: {
            "source": "user_search",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        },
    }


def _update(message):
    return SimpleNamespace(
        effective_message=message,
        callback_query=None,
        effective_chat=SimpleNamespace(id=12345),
        effective_user=SimpleNamespace(
            id=123,
            username="alice",
            full_name="Alice",
            first_name="Alice",
            last_name="",
        ),
    )


def _context(user_data, bot):
    return SimpleNamespace(user_data=user_data, bot=bot)


@pytest.mark.asyncio
async def test_keyword_search_presents_public_card_then_records_keyword_lead_and_consumes_waiting():
    message = FakeMessage("BKK1 一房 800以内")
    user_data = _session()
    bot = FakeBot()
    effects = FakeLeadEffects()

    outcome = await handle_v3_keyword_search_text(
        _update(message),
        _context(user_data, bot),
        actions=KeywordSearchActionService(),
        search_executor=FakeSearchExecutor(matched=True),
        lead_effects=effects,
    )

    assert outcome.handled and outcome.presentation is not None and outcome.presentation.matched
    assert [call[0] for call in bot.calls] == ["send_message"]
    assert user_data[SEARCH_SESSION_KEY] == [PUBLIC_ID]
    assert AWAITING_KEYWORD_SESSION_KEY not in user_data
    assert SEARCH_PREF_SESSION_KEY not in user_data
    assert user_data[LAST_SEARCH_PREF_KEY]["budget_max"] == 800
    assert len(effects.calls) == 1
    user, intent, match_mode = effects.calls[0]
    assert user.user_id == 123
    assert intent.touch_payload["message"] == "BKK1 一房 800以内"
    assert match_mode == "strict"


@pytest.mark.asyncio
async def test_keyword_search_telegram_failure_preserves_waiting_and_skips_lead():
    message = FakeMessage("BKK1 一房 800以内")
    user_data = _session()
    effects = FakeLeadEffects()

    with pytest.raises(RuntimeError, match="telegram_send_failed"):
        await handle_v3_keyword_search_text(
            _update(message),
            _context(user_data, FakeBot(fail_send=True)),
            actions=KeywordSearchActionService(),
            search_executor=FakeSearchExecutor(matched=True),
            lead_effects=effects,
        )

    assert AWAITING_KEYWORD_SESSION_KEY in user_data
    assert SEARCH_PREF_SESSION_KEY in user_data
    assert LAST_SEARCH_PREF_KEY not in user_data
    assert effects.calls == []


@pytest.mark.asyncio
async def test_keyword_no_match_renders_v3_followup_then_records_no_match_lead():
    message = FakeMessage("BKK1 一房 800以内")
    user_data = _session()
    effects = FakeLeadEffects()

    outcome = await handle_v3_keyword_search_text(
        _update(message),
        _context(user_data, FakeBot()),
        actions=KeywordSearchActionService(),
        search_executor=FakeSearchExecutor(matched=False),
        lead_effects=effects,
    )

    assert outcome.presentation is not None and not outcome.presentation.matched
    assert "暂时没有完全符合条件" in message.calls[-1][1][0]
    markup = message.calls[-1][2]["reply_markup"]
    callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
    assert callbacks == ["v3u:change_search", "v3u:home:contact", "v3u:t:home"]
    assert effects.calls[0][2] == "no_match"
    assert AWAITING_KEYWORD_SESSION_KEY not in user_data


@pytest.mark.asyncio
async def test_keyword_no_match_reply_failure_preserves_waiting_and_skips_lead():
    message = FakeMessage("BKK1 一房 800以内", fail_reply=True)
    user_data = _session()
    effects = FakeLeadEffects()

    with pytest.raises(RuntimeError, match="telegram_reply_failed"):
        await handle_v3_keyword_search_text(
            _update(message),
            _context(user_data, FakeBot()),
            actions=KeywordSearchActionService(),
            search_executor=FakeSearchExecutor(matched=False),
            lead_effects=effects,
        )

    assert AWAITING_KEYWORD_SESSION_KEY in user_data
    assert SEARCH_PREF_SESSION_KEY in user_data
    assert effects.calls == []
