from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.listing_responses import SemanticAction
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_flow import SearchFlowResult
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.telegram_callback_handler import SEARCH_ANCHOR_KEY, SEARCH_SESSION_KEY
from v3_core.user_bot.telegram_search_results import present_search_flow_result


class FakeBot:
    def __init__(self):
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=700)

    async def send_photo(self, **kwargs):
        self.calls.append(("send_photo", kwargs))
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=701)


class FakeQuery:
    def __init__(self, *, has_photo=False):
        self.message = SimpleNamespace(photo=[object()] if has_photo else [])
        self.calls = []

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))

    async def edit_message_media(self, *args, **kwargs):
        self.calls.append(("edit_media", args, kwargs))


def _update(query=None):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
    )


def _context(user_data=None):
    return SimpleNamespace(bot=FakeBot(), user_data=dict(user_data or {}))


def _card(public_id, *, index, total, photo_path=""):
    return SearchCardResponse(
        public_listing_id=public_id,
        text=f"card-{public_id}",
        photo_path=photo_path,
        action_rows=(
            (
                SemanticAction(
                    "🏠 房源详情",
                    "details",
                    target_public_listing_id=public_id,
                ),
            ),
        ),
        index=index,
        total=total,
    )


def _result(cards, *, mode="strict", has_similar=False):
    return SearchFlowResult(
        criteria=SearchCriteria(location_keys=("BKK1",), budget_max=900),
        mode=mode,
        has_similar=has_similar,
        cards=tuple(cards),
        public_listing_ids=tuple(card.public_listing_id for card in cards),
    )


@pytest.mark.asyncio
async def test_initial_search_sends_first_card_and_saves_full_public_id_session():
    result = _result(
        [
            _card("QL-RF-A2B3", index=0, total=2),
            _card("QL-BK-C4D5", index=1, total=2),
        ],
        has_similar=True,
    )
    context = _context()

    presented = await present_search_flow_result(_update(), context, result)

    assert presented.matched
    assert presented.mode == "strict"
    assert presented.has_similar
    assert presented.public_listing_ids == ("QL-RF-A2B3", "QL-BK-C4D5")
    assert presented.response is not None
    assert presented.response.text == "card-QL-RF-A2B3"
    assert [call[0] for call in context.bot.calls] == ["send_message"]
    assert context.user_data[SEARCH_SESSION_KEY] == ["QL-RF-A2B3", "QL-BK-C4D5"]
    assert context.user_data[SEARCH_ANCHOR_KEY] == {
        "chat_id": 12345,
        "message_id": 700,
    }


@pytest.mark.asyncio
async def test_initial_search_from_callback_reuses_same_text_card_renderer():
    result = _result([_card("QL-RF-A2B3", index=0, total=1)])
    query = FakeQuery(has_photo=False)
    context = _context()

    await present_search_flow_result(_update(query), context, result)

    assert [call[0] for call in query.calls] == ["edit_text"]
    assert context.bot.calls == []
    assert context.user_data[SEARCH_SESSION_KEY] == ["QL-RF-A2B3"]


@pytest.mark.asyncio
async def test_no_match_clears_old_v3_search_session_without_inventing_copy():
    result = _result([], mode="no_match")
    context = _context(
        {
            SEARCH_SESSION_KEY: ["QL-RF-A2B3"],
            SEARCH_ANCHOR_KEY: {"chat_id": 1, "message_id": 2},
        }
    )

    presented = await present_search_flow_result(_update(), context, result)

    assert not presented.matched
    assert presented.status == "no_match"
    assert presented.response is None
    assert SEARCH_SESSION_KEY not in context.user_data
    assert SEARCH_ANCHOR_KEY not in context.user_data
    assert context.bot.calls == []


@pytest.mark.asyncio
async def test_mismatched_or_duplicate_identity_fails_before_telegram_or_session_write():
    card = _card("QL-RF-A2B3", index=0, total=1)
    mismatch = SearchFlowResult(
        criteria=SearchCriteria(),
        mode="strict",
        has_similar=False,
        cards=(card,),
        public_listing_ids=("QL-BK-C4D5",),
    )
    duplicate_cards = (
        _card("QL-RF-A2B3", index=0, total=2),
        _card("QL-RF-A2B3", index=1, total=2),
    )
    duplicate = SearchFlowResult(
        criteria=SearchCriteria(),
        mode="strict",
        has_similar=False,
        cards=duplicate_cards,
        public_listing_ids=("QL-RF-A2B3", "QL-RF-A2B3"),
    )

    for result, error in (
        (mismatch, "search_flow_card_identity_mismatch"),
        (duplicate, "search_flow_contains_duplicate_public_id"),
    ):
        context = _context()
        with pytest.raises(ValueError, match=error):
            await present_search_flow_result(_update(), context, result)
        assert context.bot.calls == []
        assert context.user_data == {}
