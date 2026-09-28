from __future__ import annotations

from v3_core.user_bot.keyword_search_actions import KeywordSearchActionService
from v3_core.user_bot.transition_actions import LAST_SEARCH_PREF_KEY
from v3_core.user_bot.transition_session import AWAITING_KEYWORD_SESSION_KEY, SEARCH_PREF_SESSION_KEY


def _session(source="user_search"):
    return {
        AWAITING_KEYWORD_SESSION_KEY: {"source": source},
        SEARCH_PREF_SESSION_KEY: {
            "source": source,
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        },
    }


def test_clear_rental_text_is_claimed_without_keyword_waiting_state():
    result = KeywordSearchActionService().apply("BKK1 一房 800以内", {})

    assert result.ok and result.intent is not None
    assert result.intent.source == "direct_text"
    assert result.intent.criteria.location_keys == ("BKK1",)
    assert result.intent.criteria.room_type == "1房"
    assert result.intent.criteria.budget_max == 800
    assert result.mutation is not None
    assert result.mutation.set_values[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": ["BKK1"],
        "budget_min": None,
        "budget_max": 800,
    }


def test_unrelated_plain_text_is_not_claimed_as_search():
    result = KeywordSearchActionService().apply("你好", {})
    assert result.status == "not_applicable"
    assert result.intent is None


def test_room_type_alone_is_not_claimed_because_it_is_not_a_strict_search_filter():
    result = KeywordSearchActionService().apply("一房", {})
    assert result.status == "not_applicable"
    assert result.intent is None


def test_empty_keyword_text_preserves_fixed_sha_retry_prompt_without_mutation():
    result = KeywordSearchActionService().apply("   ", _session())
    assert result.status == "invalid"
    assert "BKK1 预算800内 一房 安静" in result.prompt
    assert result.mutation is None


def test_keyword_search_parses_existing_v3_criteria_and_builds_public_session_cleanup():
    result = KeywordSearchActionService().apply(
        "BKK1 一房 800以内",
        _session(source="smart_find_play"),
    )

    assert result.ok and result.intent is not None
    assert result.intent.source == "smart_find_play"
    assert result.intent.criteria.location_keys == ("BKK1",)
    assert result.intent.criteria.room_type == "1房"
    assert result.intent.criteria.budget_min is None
    assert result.intent.criteria.budget_max == 800
    assert result.intent.area_display == "BKK1"
    assert result.intent.budget_label == "<= 800 USD/月"
    assert result.intent.touch_payload == {
        "message": "BKK1 一房 800以内",
        "room_type": "1房",
    }
    assert result.mutation is not None
    assert result.mutation.set_values[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": ["BKK1"],
        "budget_min": None,
        "budget_max": 800,
    }
    assert AWAITING_KEYWORD_SESSION_KEY in result.mutation.delete_keys
    assert SEARCH_PREF_SESSION_KEY in result.mutation.delete_keys
    assert "room_type" not in result.mutation.set_values[LAST_SEARCH_PREF_KEY]
