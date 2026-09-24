from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.service_repository import SQLiteTenantServiceRepository
from v3_core.user_bot.public_flow import PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService
from v3_core.user_bot.search_cards import build_search_card
from v3_core.user_bot.service_flow import TenantService
from v3_core.user_bot.service_product_views import service_home_view
from v3_core.user_bot.service_views import local_life_view
from v3_core.user_bot.telegram_callback_handler import LISTING_SOURCE_KEY
from v3_core.user_bot.telegram_service_handler import (
    SERVICE_REQUEST_SESSION_KEY,
    handle_v3_service_callback,
    handle_v3_service_text,
)
from v3_core.user_bot.telegram_start_handler import handle_v3_start
from v3_core.user_bot.transition_views import TransitionViewService


PUBLIC_ID = "QL-RF-A2B3"


class MemoryInventory:
    def __init__(self, view):
        self.view = view

    def resolve(self, public_listing_id):
        return self.view if str(public_listing_id) == self.view.public_listing_id else None


def _view(*, gallery=(), cover_path=""):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_INTERNAL_1",
        "public_listing_id": PUBLIC_ID,
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "富力城",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_INTERNAL_1",
            "public_listing_id": PUBLIC_ID,
            "inventory_status": "active",
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
            "cover_path": cover_path,
        },
    )


class FakeMessage:
    def __init__(self, text=""):
        self.text = text
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append(("reply_text", text, kwargs))
        return SimpleNamespace(delete=None)


class FakeBot:
    def __init__(self):
        self.calls = []

    async def send_photo(self, **kwargs):
        photo = kwargs.get("photo")
        self.calls.append(("send_photo", getattr(photo, "name", ""), kwargs))
        return SimpleNamespace(message_id=1, chat_id=kwargs.get("chat_id"))

    async def send_media_group(self, **kwargs):
        self.calls.append(("send_media_group", "", kwargs))

    async def send_message(self, **kwargs):
        self.calls.append(("send_message", kwargs.get("text", ""), kwargs))
        return SimpleNamespace(message_id=2, chat_id=kwargs.get("chat_id"))


def _start_update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1001),
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
    )


def _flow(view):
    inventory = MemoryInventory(view)
    return PublicListingFlowService(PublicRouteService(inventory)), TransitionViewService(inventory)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_kind"),
    [
        (f"property_{PUBLIC_ID}_details__ch", "details"),
        (f"property_{PUBLIC_ID}_photos__ch", "photos"),
        (f"property_{PUBLIC_ID}_book__ch", "book"),
    ],
)
async def test_channel_deeplink_start_handler_keeps_listing_context(tmp_path, payload, expected_kind):
    photo = tmp_path / "room.jpg"
    photo.write_bytes(b"room")
    view = _view(gallery=(str(photo),))
    listings, transition_views = _flow(view)
    message = FakeMessage()
    bot = FakeBot()
    context = SimpleNamespace(
        args=[payload],
        user_data={"old_listing_session": "QL-OLD-XXXX"},
        bot=bot,
    )

    outcome = await handle_v3_start(
        _start_update(message),
        context,
        listings=listings,
        transition_views=transition_views,
        channel_url="https://t.me/qiaolian",
    )

    assert outcome.handled and outcome.kind == expected_kind
    assert outcome.payload == payload
    assert outcome.result is not None
    assert outcome.result.public_listing_id == PUBLIC_ID
    assert outcome.result.source == "channel_listing"
    assert context.user_data[LISTING_SOURCE_KEY] == "channel_listing"
    assert "old_listing_session" not in context.user_data

    if expected_kind in {"details", "photos"}:
        assert [call[0] for call in bot.calls] == ["send_photo"]
        caption = bot.calls[0][2]["caption"]
        assert "🏡 富力城｜2房1厅" in caption
        assert "📸 1/1" in caption
        assert PUBLIC_ID not in caption
    else:
        rendered = message.calls[-1][1]
        assert "预约看房" in rendered
        assert "富力城｜2房1厅" in rendered
        assert PUBLIC_ID not in rendered

    public_text = repr((message.calls, bot.calls, context.user_data))
    assert "LST_INTERNAL_1" not in public_text


def test_search_card_uses_frozen_gallery_when_cover_path_is_stale(tmp_path):
    missing_cover = tmp_path / "old-cover.jpg"
    missing_gallery = tmp_path / "missing-room.jpg"
    existing_gallery = tmp_path / "room.jpg"
    existing_gallery.write_bytes(b"room")
    view = _view(
        cover_path=str(missing_cover),
        gallery=(str(missing_gallery), str(existing_gallery)),
    )

    card = build_search_card((view,), 0)

    assert card.photo_path == str(existing_gallery)
    assert Path(card.photo_path).is_file()
    assert PUBLIC_ID == card.public_listing_id
    assert "LST_INTERNAL_1" not in card.text


def test_public_service_home_matches_frozen_product():
    view = service_home_view()
    labels = [choice.label for row in view.rows for choice in row]
    callbacks = [choice.callback_data for row in view.rows for choice in row]
    assert "<b>侨联服务</b>" in view.text
    assert labels == [
        "📋 我的租约", "🛡️ 入住服务", "🏠 安心租房",
        "💬 中文顾问", "⬅️ 返回首页",
    ]
    assert callbacks == [
        "v3u:service:tenant_lease", "v3u:service:concierge",
        "v3u:home:rental", "v3u:home:contact", "v3u:t:home",
    ]
    assert "没有显示你的住房信息" not in view.text


@pytest.mark.asyncio
async def test_service_start_ignores_previous_listing_session_and_never_renders_listing_details():
    view = _view()
    listings, transition_views = _flow(view)
    message = FakeMessage("/service")
    context = SimpleNamespace(
        args=["service"],
        user_data={
            "v3_listing_source": "search_result",
            "v3_listing_touchpoint": "listing_details",
            "v3_find_card_public_ids": [PUBLIC_ID],
        },
        bot=FakeBot(),
    )
    service = SimpleNamespace(active_binding=lambda user_id: SimpleNamespace(property_name="旧绑定住房"))

    outcome = await handle_v3_start(
        _start_update(message),
        context,
        listings=listings,
        transition_views=transition_views,
        tenant_service=service,
    )

    assert outcome.kind == "broadcast_service"
    rendered = message.calls[-1][1]
    assert "<b>侨联服务</b>" in rendered
    assert "QL-" not in rendered
    assert PUBLIC_ID not in rendered
    assert context.user_data == {}


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.message = SimpleNamespace(photo=[])
        self.calls = []

    async def answer(self, *args, **kwargs):
        self.calls.append(("answer", args, kwargs))

    async def edit_message_text(self, text, **kwargs):
        self.calls.append(("edit_text", text, kwargs))

    async def edit_message_caption(self, caption, **kwargs):
        self.calls.append(("edit_caption", caption, kwargs))


def _callback_update(query):
    return SimpleNamespace(
        callback_query=query,
        effective_chat=SimpleNamespace(id=1001),
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
    )


def _text_update(message):
    return SimpleNamespace(
        effective_message=message,
        effective_user=SimpleNamespace(id=123, username="alice", full_name="Alice"),
    )


def _unbound_service(tmp_path):
    db = tmp_path / "service.db"
    initialize_v3_storage(db)
    return TenantService(SQLiteTenantServiceRepository(db))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("callback", "expected"),
    [
        ("v3u:service:repair", "房屋报修"),
        ("v3u:service:property", "物业协调"),
        ("v3u:service:local", "周边生活"),
    ],
)
async def test_public_service_actions_do_not_require_binding(tmp_path, callback, expected):
    service = _unbound_service(tmp_path)
    query = FakeQuery(callback)
    bot = FakeBot()
    outcome = await handle_v3_service_callback(
        _callback_update(query),
        SimpleNamespace(user_data={}, bot=bot),
        service=service,
    )
    assert outcome.handled and outcome.rendered
    rendered = bot.calls[-1][1] if bot.calls else query.calls[-1][1]
    assert expected in rendered
    assert "没有显示你的住房信息" not in rendered


@pytest.mark.asyncio
async def test_missing_binding_is_only_a_lease_gate(tmp_path):
    service = _unbound_service(tmp_path)
    query = FakeQuery("v3u:service:tenant_lease")
    bot = FakeBot()
    outcome = await handle_v3_service_callback(
        _callback_update(query),
        SimpleNamespace(user_data={}, bot=bot),
        service=service,
    )
    assert outcome.handled and outcome.rendered
    assert bot.calls == []
    text = query.calls[-1][1]
    assert text == (
        "📋 <b>我的租约</b>\n"
        "目前没有查到已绑定的租约。如果你已经通过侨联入住，但这里暂时没有显示，可以联系中文顾问帮你核对。"
    )


class FakeEffects:
    def __init__(self):
        self.general_calls = []

    async def general(self, **kwargs):
        self.general_calls.append(kwargs)
        return SimpleNamespace(kind="general", lead=SimpleNamespace(status="recorded"), admin=SimpleNamespace(sent_admin_ids=()))


@pytest.mark.asyncio
async def test_unbound_repair_flow_reuses_general_service_effect_without_fake_binding(tmp_path):
    service = _unbound_service(tmp_path)
    context = SimpleNamespace(user_data={}, bot=FakeBot())
    effects = FakeEffects()

    await handle_v3_service_callback(
        _callback_update(FakeQuery("v3u:service:issue:repair_ac")),
        context,
        service=service,
        effects=effects,
    )
    assert SERVICE_REQUEST_SESSION_KEY in context.user_data

    message = FakeMessage("空调可以启动，但一直不制冷。")
    detail = await handle_v3_service_text(
        _text_update(message),
        context,
        service=service,
        effects=effects,
    )
    assert detail.handled and detail.rendered
    assert context.user_data[SERVICE_REQUEST_SESSION_KEY]["detail"] == "空调可以启动，但一直不制冷。"

    await handle_v3_service_callback(
        _callback_update(FakeQuery("v3u:service:repair_media_skip")),
        context,
        service=service,
        effects=effects,
    )
    await handle_v3_service_callback(
        _callback_update(FakeQuery("v3u:service:slot:today")),
        context,
        service=service,
        effects=effects,
    )
    query = FakeQuery("v3u:service:repair_confirm")
    submitted = await handle_v3_service_callback(
        _callback_update(query),
        context,
        service=service,
        effects=effects,
    )
    assert submitted.handled and submitted.rendered
    assert submitted.ticket_id is None
    assert len(effects.general_calls) == 1
    details = effects.general_calls[0]["details"]
    assert "报修：空调" in details
    assert "空调可以启动，但一直不制冷。" in details
    assert "希望时间：今天内" in details
    assert SERVICE_REQUEST_SESSION_KEY not in context.user_data

    with sqlite3.connect(str(service.repository.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tenant_bindings_v3").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM repair_tickets_v3").fetchone()[0] == 0


def test_local_life_is_phnom_penh_hub_with_real_rfcity_and_other_area_paths():
    view = local_life_view()
    labels = [choice.label for row in view.rows for choice in row]
    callbacks = [choice.callback_data for row in view.rows for choice in row]
    assert "<b>周边生活</b>" in view.text
    assert "富力城" in view.text
    assert labels == ["富力城周边", "问问其他区域", "返回侨联服务"]
    assert callbacks == ["v3u:service:rfcity", "v3u:home:contact", "v3u:home:service"]
    assert "LST_" not in repr(view)
