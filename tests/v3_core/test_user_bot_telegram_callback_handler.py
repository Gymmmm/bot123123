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
    LISTING_SOURCE_KEY,
    PHOTOS_ALBUM_KEY,
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
        self.returned_ids: list[int] = []

    def _alloc_id(self) -> int:
        current = self.next_message_id
        self.next_message_id += 1
        return current

    async def send_photo(self, **kwargs):
        self.calls.append(("send_photo", kwargs))
        mid = self._alloc_id()
        self.returned_ids.append(mid)
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid)

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", kwargs))
        frames = list(kwargs.get("media") or [])
        ids = []
        for _ in frames:
            mid = self._alloc_id()
            ids.append(mid)
            self.returned_ids.append(mid)
        return [
            SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid) for mid in ids
        ]

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs))
        mid = self._alloc_id()
        self.returned_ids.append(mid)
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=mid)

    async def delete_message(self, **kwargs):
        self.calls.append(("delete_message", kwargs))


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
    back = next(button for button in buttons if button.text == "⬅️ 返回房源")
    assert back.callback_data == "v3u:card:1:QL-RF-A2B3"


@pytest.mark.asyncio
async def test_channel_details_return_home_without_channel_or_change_conditions_button():
    query = FakeQuery("v3u:listing:details:QL-RF-A2B3", has_photo=True)
    context = _context()
    context.user_data[LISTING_SOURCE_KEY] = "channel_deeplink"

    await handle_v3_callback(
        _update(query), context, router=RouterStub(_details_dispatch()),
        channel_url="https://t.me/example_channel",
    )

    buttons = [button for row in query.calls[-1][2]["reply_markup"].inline_keyboard for button in row]
    assert any(button.text == "🏠 返回首页" and button.callback_data == "v3u:t:home" for button in buttons)
    assert all(button.text not in {"返回频道", "换条件"} for button in buttons)


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
                media_groups=((str(one), str(two), str(three)),),
                text="🟢 当前可预约",
                media_caption="📷 QL-RF-A2B3｜实拍相册\n共 3 张",
                photo_path=str(one),
                photo_total=3,
                action_rows=(
                    (
                        SemanticAction(
                            "🏠 房源详情",
                            "details",
                            target_public_listing_id="QL-RF-A2B3",
                        ),
                        SemanticAction(
                            "📅 预约看房",
                            "book",
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
    assert [call[0] for call in context.bot.calls] == ["send_media_group", "send_message"]
    media = context.bot.calls[0][1]["media"]
    assert len(media) == 3
    assert getattr(media[0], "caption", None)
    assert not getattr(media[1], "caption", None)
    assert "🟢 当前可预约" in context.bot.calls[1][1]["text"]
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



def test_details_from_album_edits_caption_or_text(tmp_path):
    """Opening 🏠 房源详情 edits the current message to text details."""
    import asyncio

    result = CallbackDispatchResult(
        status="ok",
        callback=parse_callback("v3u:listing:details:QL-RF-A2B3"),
        action="details",
        listing=PublicListingFlowResult(
            status="ok",
            action="details",
            public_listing_id="QL-RF-A2B3",
            details=PublicDetailsResponse(
                text="details-body",
                action_rows=(
                    (
                        SemanticAction(
                            "📅 预约看房",
                            "book",
                            target_public_listing_id="QL-RF-A2B3",
                        ),
                    ),
                ),
            ),
        ),
    )
    query = FakeQuery("v3u:listing:details:QL-RF-A2B3", has_photo=True)
    router = RouterStub(result)
    context = _context()

    outcome = asyncio.run(handle_v3_callback(_update(query), context, router=router))
    assert outcome.handled and outcome.response is not None
    assert outcome.response.kind == "details"
    assert [call[0] for call in query.calls] == ["answer", "edit_caption"]
    assert context.bot.calls == []


def test_photos_expand_sends_remaining_without_action_bar(tmp_path):
    import asyncio

    frame_a = tmp_path / "5.jpg"
    frame_b = tmp_path / "6.jpg"
    frame_a.write_bytes(b"five")
    frame_b.write_bytes(b"six")
    result = CallbackDispatchResult(
        status="ok",
        callback=parse_callback("v3u:listing:photos:QL-RF-A2B3:4"),
        action="photos",
        listing=PublicListingFlowResult(
            status="ok",
            action="photos",
            public_listing_id="QL-RF-A2B3",
            photos=PublicPhotosResponse(
                media_groups=((str(frame_a), str(frame_b)),),
                text="",
                media_caption="📷 QL-RF-A2B3｜全部实拍\n续 2 张",
                photo_path=str(frame_a),
                photo_index=4,
                photo_total=6,
                action_rows=(),
                expand_only=True,
            ),
        ),
    )
    query = FakeQuery("v3u:listing:photos:QL-RF-A2B3:4", has_photo=False)
    context = _context()

    outcome = asyncio.run(
        handle_v3_callback(_update(query), context, router=RouterStub(result))
    )
    assert outcome.handled and outcome.response is not None
    assert outcome.response.expand_only
    assert [call[0] for call in query.calls] == ["answer"]
    assert [call[0] for call in context.bot.calls] == ["send_media_group"]
    assert context.user_data.get("v3_listing_touchpoint") == "listing_photos_expand"


@pytest.mark.asyncio
async def test_photos_paging_deletes_prior_4_frames_and_old_card_then_sends_new_4_plus_new_card(tmp_path):
    """Page 1 → Page 2 must clear the previous 4 photo frames + 1 action card,
    then send exactly 4 new frames + 1 new action card. No old messages leak."""
    frames = []
    for index in range(8):
        path = tmp_path / f"frame_{index}.jpg"
        path.write_bytes(b"jpg")
        frames.append(str(path))
    page_frames = (tuple(frames[:4]), tuple(frames[4:]))

    def _dispatch(page: int) -> CallbackDispatchResult:
        return CallbackDispatchResult(
            status="ok",
            callback=parse_callback(f"v3u:listing:photos:QL-RF-A2B3:pg:{page}"),
            action="photos",
            listing=PublicListingFlowResult(
                status="ok",
                action="photos",
                public_listing_id="QL-RF-A2B3",
                photos=PublicPhotosResponse(
                    media_groups=(page_frames[page],),
                    text=f"📷 实拍房源 QL-RF-A2B3\n第 {page + 1}/2 页 · 共 8 张",
                    media_caption=f"📷 实拍 第 {page + 1}/2 页",
                    photo_path=page_frames[page][0],
                    photo_total=8,
                    action_rows=(
                        (SemanticAction("📅 预约看房", "book", "QL-RF-A2B3"),),
                        (SemanticAction("⬅️ 返回房源", "details", "QL-RF-A2B3"),),
                    ),
                ),
            ),
        )

    ctx = _context()
    first = await handle_v3_callback(
        _update(FakeQuery("v3u:listing:photos:QL-RF-A2B3:pg:0")),
        ctx,
        router=RouterStub(_dispatch(page=0)),
    )
    assert first.handled

    prior_ids = list(ctx.bot.returned_ids)
    assert len(prior_ids) == 5  # 4 album frames + 1 action card.

    await handle_v3_callback(
        _update(FakeQuery("v3u:listing:photos:QL-RF-A2B3:pg:1")),
        ctx,
        router=RouterStub(_dispatch(page=1)),
    )

    deleted = [call[1]["message_id"] for call in ctx.bot.calls if call[0] == "delete_message"]
    assert deleted == prior_ids, (deleted, prior_ids)

    second_calls = [call for call in ctx.bot.calls if call[0] != "delete_message"][2:]
    kinds = [call[0] for call in second_calls]
    assert kinds == ["send_media_group", "send_message"], kinds
    assert len(second_calls[0][1]["media"]) == 4
    assert "第 2/2 页" in second_calls[1][1]["text"]

    state = ctx.user_data[PHOTOS_ALBUM_KEY]["12345::QL-RF-A2B3"]
    new_ids = list(state["message_ids"])
    assert len(new_ids) == 5
    assert set(new_ids).isdisjoint(set(prior_ids))


@pytest.mark.asyncio
async def test_first_photos_entry_without_pg_suffix_is_remembered_for_next_page(tmp_path):
    """Real 查看实拍 entry must be tracked before the first 下一页 press."""
    frames = []
    for index in range(8):
        path = tmp_path / f"real_entry_{index}.jpg"
        path.write_bytes(b"jpg")
        frames.append(str(path))

    def _dispatch(raw: str, page: int) -> CallbackDispatchResult:
        return CallbackDispatchResult(
            status="ok",
            callback=parse_callback(raw),
            action="photos",
            listing=PublicListingFlowResult(
                status="ok",
                action="photos",
                public_listing_id="QL-RF-A2B3",
                photos=PublicPhotosResponse(
                    media_groups=(tuple(frames[page * 4:(page + 1) * 4]),),
                    text=f"第 {page + 1}/2 页 · 共 8 张",
                    media_caption=f"实拍 {page + 1}/2",
                    photo_path=frames[page * 4],
                    photo_total=8,
                    action_rows=((SemanticAction("⬅️ 返回房源", "details", "QL-RF-A2B3"),),),
                ),
            ),
        )

    ctx = _context()
    first_raw = "v3u:listing:photos:QL-RF-A2B3"
    await handle_v3_callback(
        _update(FakeQuery(first_raw)),
        ctx,
        router=RouterStub(_dispatch(first_raw, 0)),
    )
    prior_ids = list(ctx.bot.returned_ids)
    assert len(prior_ids) == 5
    assert ctx.user_data[PHOTOS_ALBUM_KEY]["12345::QL-RF-A2B3"]["message_ids"] == prior_ids

    next_raw = "v3u:listing:photos:QL-RF-A2B3:pg:1"
    await handle_v3_callback(
        _update(FakeQuery(next_raw)),
        ctx,
        router=RouterStub(_dispatch(next_raw, 1)),
    )
    deleted = [
        call[1]["message_id"]
        for call in ctx.bot.calls
        if call[0] == "delete_message"
    ]
    assert deleted == prior_ids
