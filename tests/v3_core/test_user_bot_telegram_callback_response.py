from __future__ import annotations

import pytest

from v3_core.user_bot.callback_router import CallbackDispatchResult
from v3_core.user_bot.consult import ConsultIntent, ConsultResult
from v3_core.user_bot.listing_responses import (
    PublicDetailsResponse,
    PublicPhotosResponse,
    SemanticAction,
)
from v3_core.user_bot.public_flow import PublicBookIntent, PublicListingFlowResult
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_session import SearchSessionNavigation
from v3_core.user_bot.similar_intent import SimilarIntentResult, SimilarSearchIntent
from v3_core.user_bot.telegram_callback_response import adapt_callback_response


def _callbacks(markup):
    return [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
    ]


def test_details_dispatch_becomes_keyboarded_telegram_response_with_public_ids_only():
    listing = PublicListingFlowResult(
        status="ok",
        action="details",
        public_listing_id="QL-RF-A2B3",
        details=PublicDetailsResponse(
            text="details",
            action_rows=(
                (
                    SemanticAction(
                        "📅 预约看房",
                        "book",
                        target_public_listing_id="QL-RF-A2B3",
                    ),
                    SemanticAction(
                        "📸 更多实拍",
                        "photos",
                        target_public_listing_id="QL-RF-A2B3",
                    ),
                ),
            ),
        ),
    )
    response = adapt_callback_response(
        CallbackDispatchResult(status="ok", action="details", listing=listing)
    )

    assert response.kind == "details"
    assert response.text == "details"
    assert response.keyboard is not None
    callbacks = _callbacks(response.keyboard)
    assert callbacks == [
        "v3u:listing:book:QL-RF-A2B3",
        "v3u:listing:photos:QL-RF-A2B3",
    ]
    assert all("LST_" not in value and "QC" not in value for value in callbacks)


def test_photos_dispatch_preserves_media_groups_without_starting_any_transition():
    listing = PublicListingFlowResult(
        status="ok",
        action="photos",
        public_listing_id="QL-RF-A2B3",
        photos=PublicPhotosResponse(
            media_groups=(("/frozen/1.jpg", "/frozen/2.jpg"),),
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
    )
    response = adapt_callback_response(
        CallbackDispatchResult(status="ok", action="photos", listing=listing)
    )

    assert response.kind == "photos"
    assert response.media_groups == (("/frozen/1.jpg", "/frozen/2.jpg"),)
    assert response.text == "photos"
    assert response.transition is None


def test_live_search_card_response_carries_refreshed_session_ids():
    card = SearchCardResponse(
        public_listing_id="QL-BK-C4D5",
        text="card",
        photo_path="/frozen/cover.jpg",
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
    )
    navigation = SearchSessionNavigation(
        status="ok",
        public_listing_ids=("QL-RF-A2B3", "QL-BK-C4D5"),
        card=card,
        requested_public_listing_id="QL-BK-E6F7",
        requested_removed=True,
    )
    response = adapt_callback_response(
        CallbackDispatchResult(
            status="ok",
            action="show_card",
            navigation=navigation,
        )
    )

    assert response.kind == "card"
    assert response.text == "card"
    assert response.photo_path == "/frozen/cover.jpg"
    assert response.session_public_listing_ids == (
        "QL-RF-A2B3",
        "QL-BK-C4D5",
    )
    assert response.requested_removed
    assert response.keyboard is not None
    assert _callbacks(response.keyboard) == ["v3u:card:0:QL-RF-A2B3"]


def test_book_consult_similar_and_change_search_remain_transition_intents():
    book_intent = PublicBookIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="listing_callback",
        start_payload="",
    )
    book = adapt_callback_response(
        CallbackDispatchResult(
            status="ok",
            action="book",
            listing=PublicListingFlowResult(
                status="ok",
                action="book",
                public_listing_id="QL-RF-A2B3",
                book=book_intent,
            ),
        )
    )
    consult_intent = ConsultIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="listing_callback",
        inventory_status="rented",
        offer_status="inactive",
        publication_instance_id="PUB_1",
    )
    consult = adapt_callback_response(
        CallbackDispatchResult(
            status="ok",
            action="consult",
            consult=ConsultResult(
                status="ok",
                public_listing_id="QL-RF-A2B3",
                intent=consult_intent,
            ),
        )
    )
    similar_intent = SimilarSearchIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="similar_listing",
        goal="any",
        location_keys=("BKK1",),
        area_display="BKK1",
        next_step="budget",
    )
    similar = adapt_callback_response(
        CallbackDispatchResult(
            status="ok",
            action="similar",
            similar=SimilarIntentResult(
                status="ok",
                public_listing_id="QL-RF-A2B3",
                intent=similar_intent,
            ),
        )
    )
    change = adapt_callback_response(
        CallbackDispatchResult(
            status="ok",
            action="change_search",
            change_search=True,
        )
    )

    assert book.transition == "book" and book.book_intent is book_intent
    assert consult.transition == "consult" and consult.consult_intent is consult_intent
    assert similar.transition == "similar" and similar.similar_intent is similar_intent
    assert change.transition == "change_search"
    for response in (book, consult, similar, change):
        assert response.kind == "transition"
        assert response.keyboard is None
        assert response.text == ""


def test_non_ok_dispatch_becomes_error_without_inventing_user_copy():
    response = adapt_callback_response(
        CallbackDispatchResult(
            status="expired",
            action="show_card",
            reason="search_session_expired",
        )
    )

    assert response.kind == "error"
    assert response.status == "expired"
    assert response.reason == "search_session_expired"
    assert response.text == ""
    assert response.keyboard is None


def test_success_without_required_payload_fails_closed():
    with pytest.raises(ValueError, match="successful_details_dispatch_missing_response"):
        adapt_callback_response(
            CallbackDispatchResult(
                status="ok",
                action="details",
                listing=PublicListingFlowResult(
                    status="ok",
                    action="details",
                    public_listing_id="QL-RF-A2B3",
                ),
            )
        )
