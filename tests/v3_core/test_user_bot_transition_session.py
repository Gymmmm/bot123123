from __future__ import annotations

import json

from v3_core.user_bot.consult import ConsultIntent
from v3_core.user_bot.public_flow import PublicBookIntent
from v3_core.user_bot.similar_intent import SimilarSearchIntent
from v3_core.user_bot.telegram_callback_response import TelegramCallbackResponse
from v3_core.user_bot.transition_plan import build_transition_plan
from v3_core.user_bot.transition_session import (
    APPOINTMENT_SESSION_KEY,
    AWAITING_KEYWORD_SESSION_KEY,
    CONTACT_SESSION_KEY,
    SEARCH_CARD_ANCHOR_KEY,
    SEARCH_CARD_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    apply_session_mutation,
    build_transition_session,
)


def _serialized(mutation) -> str:
    return json.dumps(
        {
            "set_values": mutation.set_values,
            "delete_keys": mutation.delete_keys,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def test_book_session_contains_only_public_listing_identity():
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="book",
            book_intent=PublicBookIntent(
                listing_id="LST_SECRET_1",
                public_listing_id="QL-RF-A2B3",
                source="listing_callback",
                start_payload="",
            ),
        )
    )

    mutation = build_transition_session(plan)

    assert mutation.delete_keys == ()
    assert mutation.set_values == {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": "QL-RF-A2B3",
            "mode": "offline",
            "date": "",
            "time": "",
            "source": "listing_callback",
        }
    }
    assert "LST_SECRET_1" not in _serialized(mutation)


def test_consult_session_projects_away_internal_listing_id():
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="consult",
            consult_intent=ConsultIntent(
                listing_id="LST_SECRET_2",
                public_listing_id="QL-BK-C4D5",
                source="listing_callback",
                inventory_status="rented",
                offer_status="inactive",
                publication_instance_id="PUB_2",
            ),
        )
    )

    mutation = build_transition_session(plan)

    assert mutation.set_values == {CONTACT_SESSION_KEY: "QL-BK-C4D5"}
    serialized = _serialized(mutation)
    assert "LST_SECRET_2" not in serialized
    assert "PUB_2" not in serialized


def test_similar_session_keeps_frozen_area_and_public_origin_only():
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="similar",
            similar_intent=SimilarSearchIntent(
                listing_id="LST_SECRET_3",
                public_listing_id="QL-RF-A2B3",
                source="similar_listing",
                goal="any",
                location_keys=("BKK1",),
                area_display="BKK1",
                next_step="budget",
            ),
        )
    )

    mutation = build_transition_session(plan)

    assert mutation.delete_keys == (AWAITING_KEYWORD_SESSION_KEY,)
    assert mutation.set_values[SEARCH_PREF_SESSION_KEY] == {
        "source": "similar_listing",
        "goal": "any",
        "location_keys": ["BKK1"],
        "area_display": "BKK1",
        "touch_payload": {"from_public_listing_id": "QL-RF-A2B3"},
    }
    assert "LST_SECRET_3" not in _serialized(mutation)


def test_change_search_resets_search_pref_and_clears_old_card_session():
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="change_search",
        )
    )
    mutation = build_transition_session(plan)

    assert mutation.delete_keys == (SEARCH_CARD_SESSION_KEY, SEARCH_CARD_ANCHOR_KEY)
    assert mutation.set_values[SEARCH_PREF_SESSION_KEY] == {
        "source": "user_search",
        "goal": "any",
        "location_keys": [],
        "area_display": "",
        "touch_payload": {},
    }
    assert mutation.set_values[AWAITING_KEYWORD_SESSION_KEY] == {
        "source": "user_search"
    }


def test_apply_session_mutation_only_applies_declared_set_and_delete_operations():
    user_data = {
        SEARCH_CARD_SESSION_KEY: ["QL-RF-A2B3"],
        SEARCH_CARD_ANCHOR_KEY: {"chat_id": 1, "message_id": 2},
        "unrelated": {"keep": True},
    }
    plan = build_transition_plan(
        TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="change_search",
        )
    )

    apply_session_mutation(user_data, build_transition_session(plan))

    assert SEARCH_CARD_SESSION_KEY not in user_data
    assert SEARCH_CARD_ANCHOR_KEY not in user_data
    assert user_data["unrelated"] == {"keep": True}
    assert user_data[SEARCH_PREF_SESSION_KEY]["goal"] == "any"
    assert user_data[AWAITING_KEYWORD_SESSION_KEY]["source"] == "user_search"
