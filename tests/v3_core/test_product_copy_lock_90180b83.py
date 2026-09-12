from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.publishing.telegram_adapter import build_channel_keyboard
from v3_core.user_bot.channel_status_sync import appointment_channel_keyboard
from v3_core.user_bot.home_views import build_contact_view, build_home_view
from v3_core.user_bot.listing_responses import build_details_response
from v3_core.user_bot.public_flow import PublicListingFlowResult, PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService
from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.telegram_start_handler import handle_v3_start
from v3_core.user_bot.transition_actions import SearchSubmitIntent

import json


ACTIONS = {
    "details": "https://t.me/TestBot?start=property_QL-RF-A2B3_details",
    "photos": "https://t.me/TestBot?start=property_QL-RF-A2B3_photos",
    "book": "https://t.me/TestBot?start=property_QL-RF-A2B3_book",
}


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


def _view(*, status="rented", offer_status="inactive"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "canonical_facts": {},
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "1房",
            "public_location_display": "富力城",
            "size_sqm": 45,
            "floor": "8",
        },
        "offer": {
            "monthly_rent_usd": 680,
            "payment_terms": "押1付1",
            "contract_term": "1年",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "l_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": status,
        },
        offer={
            "offer_status": offer_status,
            "offer_type": "rent",
            "publication_policy": "telegram_rent",
        },
        publication={},
        package={"snapshot_json": json.dumps(snapshot, ensure_ascii=False), "gallery_json": "[]"},
    )


class MemoryPublishedInventory:
    def __init__(self, view):
        self.view = view

    def resolve(self, public_listing_id):
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def test_channel_and_sync_keyboards_use_locked_details_label():
    publish = _labels(build_channel_keyboard(dict(ACTIONS), inventory_status="active"))
    sync = _labels(
        appointment_channel_keyboard(
            username="qiaolian_rent_bot",
            public_listing_id="QL-RF-A2B3",
            status="reserved",
        )
    )
    assert publish == ["🏠 租赁详情", "📸 更多实拍", "📅 预约看房"]
    assert sync == ["🏠 租赁详情", "📸 更多实拍", "📅 预约看房"]
    assert "🏠 房源详情" not in publish + sync


def test_home_contact_and_no_match_use_advisor_label():
    home = [choice.label for row in build_home_view(channel_url="https://t.me/x").rows for choice in row]
    contact = [choice.label for row in build_contact_view().rows for choice in row]
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(location_keys=("BKK1",), budget_max=800),
        source="user_search",
        goal="any",
        area_display="BKK1",
        budget_label="$800以内",
        touch_payload={},
    )
    no_match = [choice.label for row in build_search_no_match_view(intent).rows for choice in row]
    assert "💬 联系中文顾问" in home
    assert "💬 联系中文顾问" in contact
    assert "💬 联系中文顾问" in no_match
    assert "💬 联系我们" not in home + contact + no_match


def test_unbookable_book_payload_keeps_details_instead_of_dead_link():
    service = PublicListingFlowService(PublicRouteService(MemoryPublishedInventory(_view())))
    result = service.resolve("property_QL-RF-A2B3_book")
    assert result.status == "blocked"
    assert result.reason == "listing_not_bookable"
    assert result.book is None
    assert result.details is not None
    assert "🏠 <b>区域：</b>" in result.details.text
    assert result.details.action_rows[0][0].label == "📸 更多实拍"
    assert result.details.action_rows[0][1].label == "💬 联系侨联"


class _FakeMessage:
    def __init__(self):
        self.texts = []
        self.markups = []

    async def reply_text(self, text, **kwargs):
        self.texts.append(text)
        self.markups.append(kwargs.get("reply_markup"))


@pytest.mark.asyncio
async def test_start_handler_renders_chinese_unbookable_copy_then_details():
    details = build_details_response(_view())
    result = PublicListingFlowResult(
        status="blocked",
        action="book",
        public_listing_id="QL-RF-A2B3",
        reason="listing_not_bookable",
        details=details,
    )

    class _Listings:
        def resolve(self, payload):
            assert payload == "property_QL-RF-A2B3_book"
            return result

    message = _FakeMessage()
    update = SimpleNamespace(
        effective_message=message,
        effective_chat=SimpleNamespace(id=1),
        effective_user=SimpleNamespace(id=9, username="u", full_name="U", first_name="U", last_name=""),
    )
    context = SimpleNamespace(args=["property_QL-RF-A2B3_book"], user_data={}, bot=None)
    outcome = await handle_v3_start(
        update,
        context,
        listings=_Listings(),
        transition_views=SimpleNamespace(build=lambda plan: None),
    )
    assert outcome.kind == "unbookable"
    assert outcome.handled is True
    assert message.texts[0] == "这套房暂时不能预约，可以看相近房源或联系中文顾问。"
    assert "🏠 <b>区域：</b>" in message.texts[1]
    support = [button.text for row in message.markups[0].inline_keyboard for button in row]
    assert support == ["🔍 帮我找房", "💬 联系中文顾问", "🏠 返回首页"]
