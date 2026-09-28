from __future__ import annotations

import pytest

from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.public_flow import PublicBookIntent
from v3_core.user_bot.similar_intent import SimilarSearchIntent
from v3_core.user_bot.telegram_callback_response import TelegramCallbackResponse
from v3_core.user_bot.transition_plan import build_transition_plan


def test_book_transition_starts_public_id_appointment_at_mode_without_side_effects():
    response = TelegramCallbackResponse(
        kind="transition",
        status="ok",
        transition="book",
        book_intent=PublicBookIntent(
            listing_id="LST_1",
            public_listing_id="QL-RF-A2B3",
            source="listing_callback",
            start_payload="",
        ),
    )

    plan = build_transition_plan(response)

    assert plan.kind == "book"
    assert plan.next_step == "appointment_mode"
    assert plan.effects == ("render_appointment_mode",)
    assert not plan.includes("record_lead")
    assert plan.book is not None
    assert plan.book.public_listing_id == "QL-RF-A2B3"
    assert plan.book.draft.public_listing_id == "QL-RF-A2B3"
    assert not hasattr(plan.book.draft, "listing_id")
    assert "LST_1" not in repr(plan.book.draft)
    assert plan.book.draft.mode == "offline"
    assert plan.book.draft.source == "listing_callback"
    assert plan.book.draft.step == "date"


def test_consult_transition_declares_but_does_not_execute_lead_and_admin_effects():
    intent = ConsultIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="listing_callback",
        inventory_status="rented",
        offer_status="inactive",
        publication_instance_id="PUB_1",
    )
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="consult",
            consult_intent=intent,
        )
    )

    assert plan.kind == "consult"
    assert plan.next_step == "contact_handoff"
    assert plan.effects == (
        "record_lead",
        "notify_admin",
        "render_contact_handoff",
    )
    assert plan.consult is not None
    assert plan.consult.intent is intent


def test_similar_transition_asks_budget_for_bookable_listing():
    """Bookable listings should ask for budget preference."""
    intent = SimilarSearchIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="similar_listing",
        goal="budget",
        location_keys=("BKK1",),
        area_display="BKK1",
        next_step="budget",
    )
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="similar",
            similar_intent=intent,
        )
    )

    assert plan.kind == "similar"
    assert plan.next_step == "search_budget"
    assert plan.effects == ("render_search_budget",)
    assert plan.similar is not None
    assert plan.similar.intent is intent
    assert plan.similar.intent.goal == "budget"
    assert plan.similar.intent.location_keys == ("BKK1",)
    # Intent now has property_type field (may be empty for bookable)
    assert hasattr(plan.similar.intent, "property_type")


def test_similar_transition_runs_direct_search_for_rented_offline():
    """Rented/offline listings should run similar search directly."""
    intent = SimilarSearchIntent(
        listing_id="LST_1",
        public_listing_id="QL-RF-A2B3",
        source="similar_listing",
        goal="any",
        location_keys=("BKK1",),
        area_display="BKK1",
        next_step="search_submit",
        budget_min=800,
        budget_max=800,
        budget_label="$800/月",
        room_type="2房1厅",
        property_type="公寓",
    )
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="similar",
            similar_intent=intent,
        )
    )

    assert plan.kind == "similar"
    assert plan.next_step == "search_submit"
    assert plan.effects == ("run_similar_search",)
    assert plan.similar is not None
    assert plan.similar.intent.budget_min == 800
    assert plan.similar.intent.room_type == "2房1厅"


def test_change_search_resets_to_normal_search_entry_contract():
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="change_search",
        )
    )

    assert plan.kind == "change_search"
    assert plan.next_step == "search_entry"
    assert plan.effects == ("render_search_entry",)
    assert plan.change_search is not None
    assert plan.change_search.source == "user_search"
    assert plan.change_search.goal == "any"


def test_transition_planner_rejects_non_transition_and_incomplete_intents():
    """Non-transition responses and missing intents are rejected."""
    with pytest.raises(ValueError, match="transition_plan_requires_successful_transition_response"):
        build_transition_plan(TelegramCallbackResponse(kind="error", status="blocked"))

    with pytest.raises(ValueError, match="book_transition_missing_intent"):
        build_transition_plan(
            TelegramCallbackResponse(
                kind="transition",
                status="ok",
                transition="book",
            )
        )

    # Invalid goal (not "budget", "any", or "") should raise
    with pytest.raises(ValueError, match="similar_transition_contract_mismatch"):
        build_transition_plan(
            TelegramCallbackResponse(
                kind="transition",
                status="ok",
                transition="similar",
                similar_intent=SimilarSearchIntent(
                    listing_id="LST_1",
                    public_listing_id="QL-RF-A2B3",
                    source="similar_listing",
                    goal="住宅",  # Invalid - should be "budget", "any", or ""
                    location_keys=("BKK1",),
                    area_display="BKK1",
                    next_step="budget",
                ),
            )
        )
