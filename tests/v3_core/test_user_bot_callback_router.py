from __future__ import annotations

from v3_core.user_bot.callback_router import CallbackRouter
from v3_core.user_bot.consult import ConsultIntent, ConsultResult
from v3_core.user_bot.public_flow import (
    PublicBookIntent,
    PublicListingFlowResult,
)
from v3_core.user_bot.search_cards import SearchCardResponse
from v3_core.user_bot.search_session import SearchSessionNavigation


class ListingFlowStub:
    def __init__(self, result=None):
        self.result = result or PublicListingFlowResult(
            status="ok",
            action="details",
            public_listing_id="QL-RF-A2B3",
        )
        self.calls = []

    def resolve_action(self, public_listing_id, action, *, source="listing_callback"):
        self.calls.append((public_listing_id, action, source))
        return self.result


class SessionStub:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def navigate(self, callback, ids):
        self.calls.append((callback, tuple(ids)))
        return self.result


class ConsultStub:
    def __init__(self, result=None):
        self.result = result or ConsultResult(
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
        )
        self.calls = []

    def resolve(self, public_listing_id, *, source="listing_callback"):
        self.calls.append((public_listing_id, source))
        return self.result


def _router(*, listing_result=None, navigation=None, consult_result=None, wire_consult=True):
    listing = ListingFlowStub(listing_result)
    session = SessionStub(
        navigation
        or SearchSessionNavigation(
            status="ok",
            public_listing_ids=("QL-RF-A2B3",),
            card=SearchCardResponse(
                public_listing_id="QL-RF-A2B3",
                text="card",
                photo_path="",
                action_rows=(),
                index=0,
                total=1,
            ),
        )
    )
    consult = ConsultStub(consult_result) if wire_consult else None
    router = CallbackRouter(
        listings=listing,
        search_sessions=session,
        consults=consult,
    )
    return router, listing, session, consult


def test_details_callback_delegates_to_shared_listing_flow_with_callback_source():
    result = PublicListingFlowResult(
        status="ok",
        action="details",
        public_listing_id="QL-RF-A2B3",
    )
    router, listing, session, consult = _router(listing_result=result)

    dispatched = router.dispatch("v3u:listing:details:QL-RF-A2B3")

    assert dispatched.ok
    assert dispatched.action == "details"
    assert dispatched.listing is result
    assert listing.calls == [("QL-RF-A2B3", "details", "listing_callback")]
    assert session.calls == []
    assert consult is not None and consult.calls == []


def test_book_callback_returns_book_intent_without_executing_appointment():
    result = PublicListingFlowResult(
        status="ok",
        action="book",
        public_listing_id="QL-RF-A2B3",
        book=PublicBookIntent(
            listing_id="LST_1",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            start_payload="",
        ),
    )
    router, listing, _, _ = _router(listing_result=result)

    dispatched = router.dispatch("v3u:listing:book:QL-RF-A2B3")

    assert dispatched.ok
    assert dispatched.listing is not None
    assert dispatched.listing.book is not None
    assert dispatched.listing.book.source == "listing_callback"
    assert listing.calls == [("QL-RF-A2B3", "book", "listing_callback")]


def test_consult_callback_returns_pure_intent_even_for_rented_published_listing():
    router, listing, session, consult = _router()

    dispatched = router.dispatch("v3u:listing:consult:QL-RF-A2B3")

    assert dispatched.ok
    assert dispatched.action == "consult"
    assert dispatched.consult is not None
    assert dispatched.consult.intent is not None
    assert dispatched.consult.intent.public_listing_id == "QL-RF-A2B3"
    assert dispatched.consult.intent.inventory_status == "rented"
    assert dispatched.consult.intent.source == "listing_callback"
    assert consult is not None
    assert consult.calls == [("QL-RF-A2B3", "listing_callback")]
    assert listing.calls == []
    assert session.calls == []


def test_missing_consult_listing_preserves_not_found_without_falling_through():
    result = ConsultResult(
        status="not_found",
        public_listing_id="QL-RF-A2B3",
        reason="listing_not_publicly_published",
    )
    router, listing, session, consult = _router(consult_result=result)

    dispatched = router.dispatch("v3u:listing:consult:QL-RF-A2B3")

    assert dispatched.status == "not_found"
    assert dispatched.reason == "listing_not_publicly_published"
    assert dispatched.consult is result
    assert consult is not None and len(consult.calls) == 1
    assert listing.calls == []
    assert session.calls == []


def test_blocked_or_missing_listing_status_is_preserved():
    blocked_router, _, _, _ = _router(
        listing_result=PublicListingFlowResult(
            status="blocked",
            action="book",
            public_listing_id="QL-RF-A2B3",
            reason="listing_not_bookable",
        )
    )
    missing_router, _, _, _ = _router(
        listing_result=PublicListingFlowResult(
            status="not_found",
            action="details",
            public_listing_id="QL-RF-A2B3",
            reason="listing_not_publicly_published",
        )
    )

    blocked = blocked_router.dispatch("v3u:listing:book:QL-RF-A2B3")
    missing = missing_router.dispatch("v3u:listing:details:QL-RF-A2B3")

    assert blocked.status == "blocked"
    assert blocked.reason == "listing_not_bookable"
    assert missing.status == "not_found"
    assert missing.reason == "listing_not_publicly_published"


def test_card_callback_delegates_only_to_live_session_navigation():
    navigation = SearchSessionNavigation(
        status="ok",
        public_listing_ids=("QL-RF-A2B3", "QL-BK-C4D5"),
        card=SearchCardResponse(
            public_listing_id="QL-BK-C4D5",
            text="next card",
            photo_path="",
            action_rows=(),
            index=1,
            total=2,
        ),
    )
    router, listing, session, consult = _router(navigation=navigation)

    dispatched = router.dispatch(
        "v3u:card:1:QL-BK-C4D5",
        session_public_listing_ids=("QL-RF-A2B3", "QL-BK-C4D5"),
    )

    assert dispatched.ok
    assert dispatched.action == "show_card"
    assert dispatched.navigation is navigation
    assert dispatched.navigation.card is not None
    assert dispatched.navigation.card.public_listing_id == "QL-BK-C4D5"
    assert listing.calls == []
    assert consult is not None and consult.calls == []
    assert len(session.calls) == 1


def test_stale_and_expired_search_callbacks_have_distinct_fail_closed_results():
    stale_router, _, _, _ = _router(
        navigation=SearchSessionNavigation(status="invalid_callback")
    )
    expired_router, _, _, _ = _router(
        navigation=SearchSessionNavigation(
            status="expired",
            requested_public_listing_id="QL-RF-A2B3",
        )
    )

    stale = stale_router.dispatch(
        "v3u:card:0:QL-RF-A2B3",
        session_public_listing_ids=("QL-RF-A2B3",),
    )
    expired = expired_router.dispatch(
        "v3u:card:0:QL-RF-A2B3",
        session_public_listing_ids=("QL-RF-A2B3",),
    )

    assert stale.status == "invalid_callback"
    assert stale.reason == "stale_or_tampered_search_callback"
    assert expired.status == "expired"
    assert expired.reason == "search_session_expired"


def test_change_search_is_an_explicit_intent_not_a_side_effect():
    router, listing, session, consult = _router()

    dispatched = router.dispatch("v3u:change_search")

    assert dispatched.ok
    assert dispatched.action == "change_search"
    assert dispatched.change_search
    assert listing.calls == []
    assert session.calls == []
    assert consult is not None and consult.calls == []


def test_similar_remains_unsupported_and_missing_consult_service_fails_closed():
    router, listing, session, consult = _router(wire_consult=False)

    consult_result = router.dispatch("v3u:listing:consult:QL-RF-A2B3")
    similar = router.dispatch("v3u:listing:similar:QL-RF-A2B3")

    for result in (consult_result, similar):
        assert result.status == "unsupported"
        assert result.reason == "unsupported_not_wired"
    assert consult_result.action == "consult"
    assert similar.action == "similar"
    assert consult is None
    assert listing.calls == []
    assert session.calls == []


def test_legacy_or_malformed_callback_never_falls_through_to_services():
    router, listing, session, consult = _router()

    for raw in (
        "findcard:1:LST_2",
        "listing:detail:LST_1",
        "v3u:listing:details:QC0350",
        "garbage",
    ):
        result = router.dispatch(raw)
        assert result.status == "invalid_callback"
        assert result.reason == "unsupported_or_malformed_callback"

    assert listing.calls == []
    assert session.calls == []
    assert consult is not None and consult.calls == []
