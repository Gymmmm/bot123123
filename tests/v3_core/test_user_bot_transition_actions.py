from __future__ import annotations

from v3_core.user_bot.transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    APPOINTMENT_AWAITING_TIME_KEY,
    LAST_SEARCH_PREF_KEY,
    SEARCH_AWAITING_BUDGET_KEY,
    TransitionActionService,
)
from v3_core.user_bot.transition_callbacks import TransitionCallback
from v3_core.user_bot.transition_session import (
    APPOINTMENT_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    apply_session_mutation,
)


PUBLIC_ID = "QL-RF-A2B3"


def _appt_session(*, mode="offline", date="", time=""):
    return {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": mode,
            "date": date,
            "time": time,
            "source": "listing_callback",
        }
    }


def test_appointment_mode_and_date_are_public_only_state_transitions():
    service = TransitionActionService()

    mode = service.apply(
        TransitionCallback(kind="appointment_mode", value="video"),
        _appt_session(),
    )
    assert mode.ok and mode.next_step == "appointment_date"
    assert mode.appointment is not None
    assert mode.appointment.public_listing_id == PUBLIC_ID
    assert mode.appointment.mode == "video"
    assert "LST_" not in repr(mode)

    session = _appt_session()
    assert mode.mutation is not None
    apply_session_mutation(session, mode.mutation)
    picked = service.apply(
        TransitionCallback(kind="appointment_date", value="09-10"),
        session,
    )
    assert picked.ok and picked.next_step == "appointment_time"
    assert picked.appointment is not None
    assert picked.appointment.date == "09-10"
    assert picked.appointment.time == ""


def test_other_date_and_time_set_explicit_text_waiting_flags():
    service = TransitionActionService()

    other_date = service.apply(
        TransitionCallback(kind="appointment_other_date"),
        _appt_session(),
    )
    assert other_date.ok and other_date.next_step == "appointment_custom_date"
    assert other_date.mutation is not None
    assert other_date.mutation.set_values == {APPOINTMENT_AWAITING_DATE_KEY: True}

    other_time = service.apply(
        TransitionCallback(kind="appointment_other_time"),
        _appt_session(date="09-10"),
    )
    assert other_time.ok and other_time.next_step == "appointment_custom_time"
    assert other_time.mutation is not None
    assert other_time.mutation.set_values == {APPOINTMENT_AWAITING_TIME_KEY: True}


def test_time_choice_returns_submit_intent_but_never_submits():
    service = TransitionActionService()
    result = service.apply(
        TransitionCallback(kind="appointment_time", value="pm"),
        _appt_session(date="09-10"),
    )

    assert result.ok and result.next_step == "appointment_submit"
    assert result.appointment is not None and result.appointment.ready
    assert result.appointment.time == "pm"
    assert result.mutation is not None
    assert result.mutation.set_values[APPOINTMENT_SESSION_KEY]["public_listing_id"] == PUBLIC_ID
    assert "listing_id" not in result.mutation.set_values[APPOINTMENT_SESSION_KEY]


def test_missing_or_invalid_appointment_session_fails_closed():
    service = TransitionActionService()
    missing = service.apply(
        TransitionCallback(kind="appointment_date", value="09-10"),
        {},
    )
    invalid = service.apply(
        TransitionCallback(kind="appointment_date", value="09-10"),
        {APPOINTMENT_SESSION_KEY: {"public_listing_id": "LST_1"}},
    )

    assert missing.status == "expired"
    assert invalid.status == "expired"
    assert missing.reason == "appointment_session_expired"


def test_budget_choice_matches_fixed_sha_immediate_search_boundary():
    service = TransitionActionService()
    session = {
        SEARCH_PREF_SESSION_KEY: {
            "source": "similar_listing",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {"from_public_listing_id": PUBLIC_ID},
        }
    }

    result = service.apply(
        TransitionCallback(kind="budget_choice", value="b2"),
        session,
    )

    assert result.ok and result.next_step == "search_submit"
    assert result.search is not None
    assert result.search.criteria.property_type == ""
    assert result.search.criteria.location_keys == ("BKK1",)
    assert result.search.criteria.budget_min == 400
    assert result.search.criteria.budget_max == 600
    assert result.search.budget_label == "$400–600"
    assert result.search.source == "similar_listing"
    assert result.search.touch_payload == {"from_public_listing_id": PUBLIC_ID}
    assert result.mutation is not None
    assert result.mutation.set_values[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": ["BKK1"],
        "budget_min": 400,
        "budget_max": 600,
    }
    assert SEARCH_PREF_SESSION_KEY in result.mutation.delete_keys


def test_custom_budget_waits_for_text_instead_of_searching():
    service = TransitionActionService()
    result = service.apply(
        TransitionCallback(kind="budget_custom"),
        {SEARCH_PREF_SESSION_KEY: {"source": "user_search", "goal": "any"}},
    )

    assert result.ok and result.next_step == "search_custom_budget"
    assert result.search is None
    assert result.mutation is not None
    assert result.mutation.set_values == {SEARCH_AWAITING_BUDGET_KEY: True}


def test_budget_requires_live_search_pref_and_navigation_is_explicit():
    service = TransitionActionService()
    expired = service.apply(TransitionCallback(kind="budget_choice", value="b1"), {})
    assert expired.status == "expired"
    assert expired.reason == "search_session_expired"

    for kind in ("search_area", "search_budget", "search_layout", "search_available", "home"):
        result = service.apply(TransitionCallback(kind=kind), {})
        assert result.ok and result.next_step == "navigation"
        assert result.navigation == kind
