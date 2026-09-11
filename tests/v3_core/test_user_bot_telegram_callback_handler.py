from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.callback_router import CallbackDispatchResult
from v3_core.user_bot.callbacks import parse_callback
from v3_core.user_bot.consult import ConsultIntent, ConsultResult
from v3_core.user_bot.listing_responses import PublicDetailsResponse, PublicPhotosResponse, SemanticAction
from v3_core.user_bot.public_flow import PublicListingFlowResult
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_session import SearchSessionNavigation
from v3_core.user_bot.telegram_callback_handler import (
    SEARCH_ANCHOR_KEY,
    SEARCH_SESSION_KEY,
    handle_v3_callback,
)


class RouterStub:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def dispatch(self, raw, *, session_public_listing_ids=()):
        self.calls.append((raw, tuple(session_public_listing_ids)))
        return self.result


class FakeQuery:
    def __init__(self, data, *, has_photo=False):
        self.data = data
        self.message = SimpleNamespace(photo=[object()] if has_photo else [])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, *args, **kwargs):
        self.calls.append(("edit_text", args, kwargs))

    async def edit_message_caption(self, *args, **kwargs):
        self.calls.append(("edit_caption", args, kwargs))

    async def edit_message_media(self, *args, **kwargs):
        self.calls.append(("edit_media", args, kwargs))


class FakeBot:
    def __init__(self):
        self.calls = []
        self.next_message_id = 900

    async def send_photo(self, **kwargs):
        self.calls.append(("send_photo", kwargs))
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=self.next_message_id)

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", kwargs))
        return []

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=self.next_message_id)


def _update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=12345),
    )


def _context(*, session_ids=()):
    return SimpleNamespace(
        bot=FakeBot(),
        user_data={SEARCH_SESSION_KEY: list(session_ids)} if session_ids else {},
    )


def _details_dispatch():
    return CallbackDispatchResult(
        status="ok",
        callback=parse_callback("v3u:listing:details:QL-RF-A2B3"),
        action="details",
        listing=PublicListingFlowResult(
            status="ok",
            action="details",
            public_listing_id="QL-RF-A2B3",
            details=PublicDetailsResponse(
                text="details",
                action_rows=(
                    (
                        SemanticAction(
                            "📸 更多实拍",
                            "photos",
                            target_public_listing_id="QL-RF-A2B3",
                        ),
                    ),
                ),
            ),
        ),
    )


@pytest.mark.asyncio
async def test_non_v3_callback_is_ignored_without_answering_or_dispatching():
    query = FakeQuery("listing:detail:LST_1")
    router = RouterStub(_details_dispatch())
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert not outcome.handled
    assert outcome.response is None
    assert router.calls == []
    assert query.calls == []
    assert context.bot.calls == []


@pytest.mark.asyncio
async def test_details_on_photo_message_edits_caption_and_answers_once():
    query = FakeQuery("v3u:listing:details:QL-RF-A2B3", has_photo=True)
    router = RouterStub(_details_dispatch())
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert outcome.handled and outcome.response is not None
    assert outcome.response.kind == "details"
    assert router.calls == [("v3u:listing:details:QL-RF-A2B3", ())]
    assert [call[0] for call in query.calls] == ["answer", "edit_caption"]
    assert context.bot.calls == []


@pytest.mark.asyncio
async def test_details_on_text_message_edits_text():
    query = FakeQuery("v3u:listing:details:QL-RF-A2B3")
    router = RouterStub(_details_dispatch())
    context = _context()

    await handle_v3_callback(_update(query), context, router=router)

    assert [call[0] for call in query.calls] == ["answer", "edit_text"]


@pytest.mark.asyncio
async def test_details_from_search_session_offer_one_tap_return_to_same_card():
    query = FakeQuery("v3u:listing:details:QL-RF-A2B3", has_photo=True)
    router = RouterStub(_details_dispatch())
    context = _context(session_ids=("QL-BK-C4D5", "QL-RF-A2B3"))

    await handle_v3_callback(_update(query), context, router=router)

    markup = query.calls[-1][2]["reply_markup"]
    buttons = [button for row in markup.inline_keyboard for button in row]
    back = next(button for button in buttons if button.text == "⬅️ 返回搜索结果")
    assert back.callback_data == "v3u:card:1:QL-RF-A2B3"


@pytest.mark.asyncio
async def test_photos_send_frozen_media_then_action_message(tmp_path):
    one = tmp_path / "1.jpg"
    two = tmp_path / "2.jpg"
    three = tmp_path / "3.jpg"
    for path in (one, two, three):
        path.write_bytes(path.name.encode())
    result = CallbackDispatchResult(
        status="ok",
        action="photos",
        listing=PublicListingFlowResult(
            status="ok",
            action="photos",
            public_listing_id="QL-RF-A2B3",
            photos=PublicPhotosResponse(
                media_groups=((str(one),), (str(two), str(three))),
                text="photos",
                action_rows=(
                    (
                        SemanticAction(
                            "🏠 房源详情",
                            "details",
                            target_public_listing_id="QL-RF-A2B3",
                        ),
                    ),
                ),
            ),
        ),
    )
    query = FakeQuery("v3u:listing:photos:QL-RF-A2B3")
    router = RouterStub(result)
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert outcome.handled and outcome.response is not None
    assert outcome.response.kind == "photos"
    assert [call[0] for call in context.bot.calls] == [
        "send_photo",
        "send_media_group",
        "send_message",
    ]
    assert [call[0] for call in query.calls] == ["answer"]


@pytest.mark.asyncio
async def test_card_edit_refreshes_public_id_session_only_after_render(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    navigation = SearchSessionNavigation(
        status="ok",
        public_listing_ids=("QL-RF-A2B3", "QL-BK-C4D5"),
        card=SearchCardResponse(
            public_listing_id="QL-BK-C4D5",
            text="card",
            photo_path=str(cover),
            action_rows=(
                (
                    SemanticAction(
                        "⬅️ 上一套",
                        "previous",
                        target_public_listing_id="QL-RF-A2B3",
                        target_index=0,
                    ),
                ),
            ),
            index=1,
            total=2,
        ),
    )
    result = CallbackDispatchResult(
        status="ok",
        action="show_card",
        navigation=navigation,
    )
    query = FakeQuery("v3u:card:1:QL-BK-C4D5", has_photo=True)
    router = RouterStub(result)
    context = _context(session_ids=("QL-RF-A2B3", "QL-BK-C4D5"))

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert outcome.handled
    assert router.calls == [
        (
            "v3u:card:1:QL-BK-C4D5",
            ("QL-RF-A2B3", "QL-BK-C4D5"),
        )
    ]
    assert [call[0] for call in query.calls] == ["answer", "edit_media"]
    assert context.user_data[SEARCH_SESSION_KEY] == [
        "QL-RF-A2B3",
        "QL-BK-C4D5",
    ]


@pytest.mark.asyncio
async def test_text_card_with_cover_sends_new_photo_and_records_anchor(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    result = CallbackDispatchResult(
        status="ok",
        action="show_card",
        navigation=SearchSessionNavigation(
            status="ok",
            public_listing_ids=("QL-RF-A2B3",),
            card=SearchCardResponse(
                public_listing_id="QL-RF-A2B3",
                text="card",
                photo_path=str(cover),
                action_rows=(
                    (
                        SemanticAction(
                            "🏠 房源详情",
                            "details",
                            target_public_listing_id="QL-RF-A2B3",
                        ),
                    ),
                ),
                index=0,
                total=1,
            ),
        ),
    )
    query = FakeQuery("v3u:card:0:QL-RF-A2B3")
    router = RouterStub(result)
    context = _context(session_ids=("QL-RF-A2B3",))

    await handle_v3_callback(_update(query), context, router=router)

    assert [call[0] for call in context.bot.calls] == ["send_photo"]
    assert context.user_data[SEARCH_ANCHOR_KEY] == {
        "chat_id": 12345,
        "message_id": 900,
    }


@pytest.mark.asyncio
async def test_transition_is_answered_but_not_executed_without_transition_runtime():
    consult = CallbackDispatchResult(
        status="ok",
        action="consult",
        consult=ConsultResult(
            status="ok",
            public_listing_id="QL-RF-A2B3",
            intent=ConsultIntent(
                listing_id="LST_1",
                public_listing_id="QL-RF-A2B3",
                source="listing_callback",
                inventory_status="rented",
                offer_status="inactive",
                publication_instance_id="PUB_1",
            ),
        ),
    )
    query = FakeQuery("v3u:listing:consult:QL-RF-A2B3")
    router = RouterStub(consult)
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert outcome.handled and outcome.response is not None
    assert outcome.response.kind == "transition"
    assert [call[0] for call in query.calls] == ["answer"]
    assert context.bot.calls == []


@pytest.mark.asyncio
async def test_expired_listing_or_card_action_shows_visible_alert():
    result = CallbackDispatchResult(
        status="expired",
        action="show_card",
        reason="search_session_expired",
    )
    query = FakeQuery("v3u:card:0:QL-RF-A2B3")
    router = RouterStub(result)
    context = _context()

    outcome = await handle_v3_callback(_update(query), context, router=router)

    assert outcome.handled and outcome.response is not None
    assert outcome.response.kind == "error"
    assert [call[0] for call in query.calls] == ["answer"]
    assert query.calls[0][1] == ("搜索结果已更新，请重新查找。",)
    assert query.calls[0][2] == {"show_alert": True}
    assert context.bot.calls == []
