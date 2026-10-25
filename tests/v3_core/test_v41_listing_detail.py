"""V4.1 listing detail: channel deep link → detail with 3–4 real photos, no internal IDs."""
from __future__ import annotations

import itertools
import json
from datetime import date
from types import SimpleNamespace

import pytest
from PIL import Image

from v3_core.publishing.channel_contract import channel_start_payload
from v3_core.user_bot.appointment_success_view import build_appointment_success_view
from v3_core.user_bot.callback_router import CallbackRouter
from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.deeplink import parse_channel_start_payload
from v3_core.user_bot.listing_contact import build_listing_contact_view
from v3_core.user_bot.listing_responses import (
    build_details_response,
    build_photos_overview_response,
    build_photos_page_response,
    build_photos_response,
)
from v3_core.user_bot.public_appointment import PublicAppointmentDraft
from v3_core.user_bot.public_flow import PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService
from v3_core.user_bot.telegram_callback_handler import (
    DETAILS_ALBUM_PAGE,
    PHOTOS_ALBUM_KEY,
    handle_v3_callback,
)
from v3_core.user_bot.telegram_start_handler import handle_v3_start
from v3_core.user_bot.transition_views import TransitionViewService

PUBLIC_ID = "QL-RF-A2B3"


class Inventory:
    def __init__(self, view):
        self.view = view

    def resolve(self, public_listing_id):
        return self.view if str(public_listing_id) == PUBLIC_ID else None


def _photos(tmp_path, n):
    paths = []
    for i in range(n):
        path = tmp_path / f"room_{i}.jpg"
        Image.new("RGB", (64, 48), (10 * i, 80, 120)).save(path)
        paths.append(str(path))
    return paths


def _view(*, gallery=(), status="active", cover_path=""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_INTERNAL_1",
        "public_listing_id": PUBLIC_ID,
        "listing": {"project_name": "富力城", "property_type": "公寓", "layout": "2房1厅",
                    "public_location_display": "钻石岛", "floor": "18"},
        "offer": {"offer_type": "rent", "monthly_rent_usd": 800, "publication_policy": "telegram_rent"},
    }
    return PublishedListingView(
        listing={"listing_id": "LST_INTERNAL_1", "public_listing_id": PUBLIC_ID, "inventory_status": status},
        offer={"offer_id": "OFF_1", "offer_type": "rent", "offer_status": "active" if status == "active" else "inactive",
               "publication_policy": "telegram_rent"},
        publication={"instance_id": "PUB_1"},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False),
                 "gallery_json": json.dumps(list(gallery)), "cover_path": cover_path},
    )


class FakeBot:
    def __init__(self):
        self.calls = []
        self._ids = itertools.count(500)

    def _msg(self, chat_id):
        return SimpleNamespace(message_id=next(self._ids), chat_id=chat_id)

    async def send_media_group(self, **kw):
        msgs = [self._msg(kw["chat_id"]) for _ in kw["media"]]
        self.calls.append(("send_media_group", kw, [m.message_id for m in msgs]))
        return msgs

    async def send_photo(self, **kw):
        msg = self._msg(kw["chat_id"])
        self.calls.append(("send_photo", kw, [msg.message_id]))
        return msg

    async def send_message(self, **kw):
        msg = self._msg(kw["chat_id"])
        self.calls.append(("send_message", kw, [msg.message_id]))
        return msg

    async def edit_message_media(self, **kw):
        self.calls.append(("edit_message_media", kw, [kw["message_id"]]))
        return SimpleNamespace(message_id=kw["message_id"], chat_id=kw["chat_id"])

    async def delete_message(self, **kw):
        self.calls.append(("delete_message", kw, [kw["message_id"]]))


class FakeMessage:
    def __init__(self, message_id=1, photo=None):
        self.message_id = message_id
        self.photo = photo or []
        self.calls = []

    async def reply_text(self, text, **kw):
        self.calls.append(("reply_text", text, kw))
        return SimpleNamespace(message_id=99)


class FakeQuery:
    def __init__(self, data, message):
        self.data = data
        self.message = message
        self.calls = []

    async def answer(self, *a, **k):
        self.calls.append(("answer", a, k))

    async def edit_message_text(self, *a, **k):
        self.calls.append(("edit_text", a, k))

    async def edit_message_caption(self, *a, **k):
        self.calls.append(("edit_caption", a, k))

    async def edit_message_media(self, *a, **k):
        self.calls.append(("edit_media", a, k))


def _start_update(message):
    return SimpleNamespace(effective_message=message, effective_chat=SimpleNamespace(id=1001),
                           effective_user=SimpleNamespace(id=7, username="u", full_name="U"))


def _cb_update(query):
    return SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=1001),
                           effective_user=SimpleNamespace(id=7, username="u", full_name="U"))


def _flow(view):
    inventory = Inventory(view)
    return PublicListingFlowService(PublicRouteService(inventory)), TransitionViewService(inventory)


def _kinds(bot):
    return [c[0] for c in bot.calls]


def _all_text(bot, message=None):
    parts = []
    for name, kw, _ids in bot.calls:
        parts.append(str(kw.get("text") or kw.get("caption") or ""))
        for item in kw.get("media") or ():
            parts.append(str(getattr(item, "caption", "") or ""))
    for call in getattr(message, "calls", []):
        parts.append(str(call[1]))
    return "\n".join(parts)


def _buttons(markup):
    return [[b.text for b in row] for row in markup.inline_keyboard]


async def _start(payload, view, *, context=None):
    listings, views = _flow(view)
    message = FakeMessage()
    context = context or SimpleNamespace(args=[payload], user_data={}, bot=FakeBot())
    context.args = [payload]
    outcome = await handle_v3_start(_start_update(message), context, listings=listings,
                                    transition_views=views, channel_url="https://t.me/qiaolian")
    return outcome, message, context


# ---- Fix 1 + 2: channel deep link → detail with curated real photos ----

@pytest.mark.asyncio
async def test_channel_details_deeplink_end_to_end_opens_detail_with_photos(tmp_path):
    payload = channel_start_payload(PUBLIC_ID, "details", source_code="ch")
    route = parse_channel_start_payload(payload)
    assert route.action == "details" and route.public_listing_id == PUBLIC_ID

    view = _view(gallery=_photos(tmp_path, 6))
    outcome, message, context = await _start(payload, view)

    assert outcome.kind == "details"
    assert message.calls == []  # never the home screen
    bot = context.bot
    assert _kinds(bot) == ["send_photo"]
    detail = bot.calls[0][1]
    assert detail["caption"].startswith("📷 实拍 1–4/6")
    assert "富力城" in detail["caption"]
    assert len(detail["caption"]) <= 1024
    labels = _buttons(detail["reply_markup"])
    assert labels[0] == ["1/2", "下一组 ➡️"]
    assert ["📅 在线预约", "💬 咨询这套"] in labels
    assert PUBLIC_ID not in _all_text(bot)
    state = context.user_data[PHOTOS_ALBUM_KEY][f"1001::{PUBLIC_ID}"]
    assert state["page"] == DETAILS_ALBUM_PAGE and len(state["message_ids"]) == 1


@pytest.mark.asyncio
async def test_retapping_channel_button_replaces_previous_detail(tmp_path):
    view = _view(gallery=_photos(tmp_path, 3))
    payload = channel_start_payload(PUBLIC_ID, "details", source_code="ch")
    _o, _m, context = await _start(payload, view)
    first_ids = list(context.user_data[PHOTOS_ALBUM_KEY][f"1001::{PUBLIC_ID}"]["message_ids"])
    context.bot.calls.clear()

    await _start(payload, view, context=context)
    kinds = _kinds(context.bot)
    assert kinds.count("send_photo") == 1
    deleted = [str(c[1]["message_id"]) for c in context.bot.calls if c[0] == "delete_message"]
    assert deleted == first_ids


@pytest.mark.asyncio
async def test_detail_without_photos_is_text_only_and_hides_photo_button(tmp_path):
    view = _view(gallery=(str(tmp_path / "missing.jpg"),))
    payload = channel_start_payload(PUBLIC_ID, "details")
    outcome, message, context = await _start(payload, view)
    assert outcome.kind == "details"
    assert context.bot.calls == []
    text, kw = message.calls[-1][1], message.calls[-1][2]
    assert "富力城" in text
    assert not any("实拍" in label for row in _buttons(kw["reply_markup"]) for label in row)


def test_detail_curates_only_real_listing_photos(tmp_path):
    real = _photos(tmp_path, 5)
    cover = tmp_path / "cover.jpg"
    Image.new("RGB", (10, 10)).save(cover)
    gallery = [str(tmp_path / "gone.jpg"), str(cover), *real]
    response = build_details_response(_view(gallery=gallery, cover_path=str(cover)))
    shown = list(response.media_groups[0])
    assert 3 <= len(shown) <= 4
    assert str(cover) not in shown and str(tmp_path / "gone.jpg") not in shown
    assert set(shown) <= set(real)
    assert response.photo_total == 5
    assert response.action_rows[0][0].label == "📸 下一组实拍"


def test_not_bookable_detail_hides_booking():
    response = build_details_response(_view(status="rented"))
    labels = [a.label for row in response.action_rows for a in row]
    assert "📅 在线预约" not in labels and "💬 咨询这套" in labels


@pytest.mark.asyncio
async def test_details_callback_sends_photos_once_and_replaces_search_card(tmp_path):
    view = _view(gallery=_photos(tmp_path, 9))
    listings, views = _flow(view)
    router = CallbackRouter(listings=listings, search_sessions=SimpleNamespace(
        navigate=lambda *_a, **_k: SimpleNamespace(status="ok", card=None)))
    context = SimpleNamespace(user_data={}, bot=FakeBot())
    card = FakeMessage(message_id=42, photo=[object()])
    query = FakeQuery(f"v3u:listing:details:{PUBLIC_ID}", card)
    await handle_v3_callback(_cb_update(query), context, router=router, transition_views=views)

    kinds = _kinds(context.bot)
    assert kinds == ["edit_message_media"]
    assert context.bot.calls[0][1]["message_id"] == 42
    assert not [c for c in query.calls if c[0] in {"edit_text", "edit_caption"}]

    # Re-entering the same detail does not re-send or edit an unchanged photo.
    context.bot.calls.clear()
    detail_id = context.user_data[PHOTOS_ALBUM_KEY][f"1001::{PUBLIC_ID}"]["message_ids"][-1]
    again = FakeQuery(
        f"v3u:listing:details:{PUBLIC_ID}",
        FakeMessage(message_id=int(detail_id), photo=[object()]),
    )
    await handle_v3_callback(_cb_update(again), context, router=router, transition_views=views)
    assert context.bot.calls == []

    # Actual next-photo control edits the same message in place.
    next_query = FakeQuery(
        f"v3u:detailphoto:{PUBLIC_ID}:1",
        FakeMessage(message_id=int(detail_id), photo=[object()]),
    )
    await handle_v3_callback(_cb_update(next_query), context, router=router, transition_views=views)
    assert [call[0] for call in next_query.calls] == ["answer", "edit_media"]
    media = next_query.calls[-1][2]["media"]
    assert "实拍 5–8/9" in media.caption
    assert "富力城" in media.caption
    assert context.bot.calls == []  # no send, delete, or extra message
    state = context.user_data[PHOTOS_ALBUM_KEY][f"1001::{PUBLIC_ID}"]
    assert state["message_ids"] == [str(detail_id)] and state["detail_index"] == 1

    # After an app restart, button callbacks still resolve from the database.
    restarted = SimpleNamespace(user_data={}, bot=context.bot)
    later = FakeQuery(
        f"v3u:detailphoto:{PUBLIC_ID}:2",
        FakeMessage(message_id=int(detail_id), photo=[object()]),
    )
    await handle_v3_callback(_cb_update(later), restarted, router=router, transition_views=views)
    assert [call[0] for call in later.calls] == ["answer", "edit_media"]
    assert "实拍 9–9/9" in later.calls[-1][2]["media"].caption


@pytest.mark.asyncio
async def test_detail_to_search_card_and_back_stays_on_one_message(tmp_path):
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton
    from v3_core.user_bot.telegram_callback_response import TelegramCallbackResponse
    from v3_core.user_bot.telegram_callback_handler import render_search_card_response

    raw = _photos(tmp_path, 3)
    view = _view(gallery=raw)
    payload = channel_start_payload(PUBLIC_ID, "details")
    _, _, ctx = await _start(payload, view)
    state = ctx.user_data[PHOTOS_ALBUM_KEY][f"1001::{PUBLIC_ID}"]
    msg_id = int(state["message_ids"][0])
    query = FakeQuery("v3u:card:0:" + PUBLIC_ID,
                      FakeMessage(message_id=msg_id, photo=[object()]))
    card = TelegramCallbackResponse(
        kind="card", status="ok", text="🏠 富力城",
        photo_path=raw[0],
        keyboard=InlineKeyboardMarkup([[InlineKeyboardButton(
            "📋 房源详情", callback_data=f"v3u:listing:details:{PUBLIC_ID}"
        )]]),
    )
    await render_search_card_response(_cb_update(query), ctx, card, query=query)
    assert state.get("message_ids") is None
    assert state.get("page") is None

    listings, views = _flow(view)
    router = CallbackRouter(listings=listings, search_sessions=SimpleNamespace(
        navigate=lambda *_a, **_k: SimpleNamespace(status="ok", card=None)))
    ctx.bot.calls.clear()
    back = FakeQuery(f"v3u:listing:details:{PUBLIC_ID}",
                     FakeMessage(message_id=msg_id, photo=[object()]))
    await handle_v3_callback(_cb_update(back), ctx, router=router, transition_views=views)
    assert _kinds(ctx.bot) == ["edit_message_media"]
    assert ctx.bot.calls[0][1]["message_id"] == msg_id


# ---- Fix 3: internal listing IDs never reach user-visible text ----

def test_internal_ids_hidden_on_album_detail_consult_and_booking(tmp_path):
    view = _view(gallery=_photos(tmp_path, 6))
    inventory = Inventory(view)
    texts = []
    for response in (
        build_details_response(view),
        build_photos_response(view),
        build_photos_response(view, offset=4),
        build_photos_page_response(view, page=0),
        build_photos_overview_response(view),
    ):
        texts.extend([response.text, getattr(response, "media_caption", "")])
    texts.append(build_listing_contact_view(
        ConsultIntent(public_listing_id=PUBLIC_ID, listing_id="LST_INTERNAL_1", inventory_status="active",
                      offer_status="active", publication_instance_id="PUB_1", source="listing_callback"), inventory).text)
    views = TransitionViewService(inventory)
    draft = PublicAppointmentDraft(PUBLIC_ID, mode="offline", date="2026-10-15", time="pm")
    texts.append(views.appointment_mode(draft).text)
    texts.append(views.appointment_date(draft, today=date(2026, 10, 10)).text)
    texts.append(views.appointment_time(draft).text)
    texts.append(build_appointment_success_view(draft, inventory).text)
    joined = "\n".join(texts)
    assert PUBLIC_ID not in joined
    assert "QL-" not in joined
    assert "LST_INTERNAL_1" not in joined
