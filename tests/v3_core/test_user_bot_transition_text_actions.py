from __future__ import annotations

from v3_core.user_bot.transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    APPOINTMENT_AWAITING_TIME_KEY,
    LAST_SEARCH_PREF_KEY,
    SEARCH_AWAITING_AREA_KEY,
    SEARCH_AWAITING_BUDGET_KEY,
)
from v3_core.user_bot.transition_session import APPOINTMENT_SESSION_KEY, SEARCH_PREF_SESSION_KEY
from v3_core.user_bot.transition_text_actions import TransitionTextActionService


PUBLIC_ID = "QL-RF-A2B3"


def _appt_session(*, awaiting_date=False, awaiting_time=False, date=""):
    data = {
        APPOINTMENT_SESSION_KEY: {
            "public_listing_id": PUBLIC_ID,
            "mode": "offline",
            "date": date,
            "time": "",
            "source": "listing_callback",
        }
    }
    if awaiting_date:
        data[APPOINTMENT_AWAITING_DATE_KEY] = True
    if awaiting_time:
        data[APPOINTMENT_AWAITING_TIME_KEY] = True
    return data


def test_custom_date_normalizes_fixed_sha_formats_and_advances_to_time():
    service = TransitionTextActionService()
    for raw, expected in (("0905", "9月5日"), ("9月5日", "9月5日"), ("下周三", "下周三")):
        result = service.apply(raw, _appt_session(awaiting_date=True))
        assert result.ok and result.next_step == "appointment_time"
        assert result.appointment is not None
        assert result.appointment.public_listing_id == PUBLIC_ID
        assert result.appointment.date == expected
        assert "LST_" not in repr(result)
        assert result.mutation is not None
        assert APPOINTMENT_AWAITING_DATE_KEY in result.mutation.delete_keys


def test_invalid_custom_date_preserves_waiting_contract_and_fixed_sha_error_copy():
    service = TransitionTextActionService()
    result = service.apply("hello", _appt_session(awaiting_date=True))

    assert result.status == "invalid"
    assert result.reason == "appointment_date_unrecognized"
    assert "0905" in result.prompt
    assert "下周三" in result.prompt
    assert result.mutation is None


def test_custom_time_validates_then_stops_at_submit_executor_boundary():
    service = TransitionTextActionService()
    for raw in ("20:00", "晚上8点"):
        result = service.apply(raw, _appt_session(awaiting_time=True, date="9月5日"))
        assert result.ok and result.next_step == "appointment_submit"
        assert result.appointment is not None and result.appointment.ready
        assert result.appointment.time == raw
        assert result.mutation is not None
        assert APPOINTMENT_AWAITING_TIME_KEY in result.mutation.delete_keys


def test_invalid_custom_time_returns_fixed_sha_retry_copy():
    result = TransitionTextActionService().apply(
        "随便",
        _appt_session(awaiting_time=True, date="9月5日"),
    )

    assert result.status == "invalid"
    assert result.reason == "appointment_time_unrecognized"
    assert "20:00" in result.prompt
    assert "晚上8点" in result.prompt
    assert result.mutation is None


def test_custom_area_resolves_canonical_keys_then_advances_to_budget():
    session = {
        SEARCH_AWAITING_AREA_KEY: True,
        SEARCH_PREF_SESSION_KEY: {
            "source": "home_area",
            "goal": "any",
            "location_keys": [],
            "area_display": "",
            "touch_payload": {},
        },
    }

    result = TransitionTextActionService().apply("永旺1附近", session)

    assert result.ok and result.next_step == "search_budget"
    assert result.search is None
    assert result.mutation is not None
    pref = result.mutation.set_values[SEARCH_PREF_SESSION_KEY]
    assert pref["area_display"] == "永旺1附近"
    assert pref["location_keys"]
    assert SEARCH_AWAITING_AREA_KEY in result.mutation.delete_keys
    assert "LST_" not in repr(result)


def test_custom_area_failure_keeps_waiting_state_for_retry():
    session = {
        SEARCH_AWAITING_AREA_KEY: True,
        SEARCH_PREF_SESSION_KEY: {"source": "home_area", "goal": "any"},
    }

    result = TransitionTextActionService().apply("完全不存在的地方", session)

    assert result.status == "invalid"
    assert result.reason == "search_area_unrecognized"
    assert "BKK1" in result.prompt
    assert "永旺1附近" in result.prompt
    assert result.mutation is None


def test_custom_budget_success_returns_search_intent_and_public_only_last_pref():
    session = {
        SEARCH_AWAITING_BUDGET_KEY: True,
        SEARCH_PREF_SESSION_KEY: {
            "source": "similar_listing",
            "goal": "any",
            "location_keys": ["BKK1"],
            "area_display": "BKK1",
            "touch_payload": {"from_public_listing_id": PUBLIC_ID},
        },
    }

    result = TransitionTextActionService().apply("600-900", session)

    assert result.ok and result.next_step == "search_submit"
    assert result.search is not None
    assert result.search.criteria.location_keys == ("BKK1",)
    assert result.search.criteria.budget_min == 600
    assert result.search.criteria.budget_max == 900
    assert result.search.budget_label == "600-900 USD/月"
    assert result.search.touch_payload == {
        "message": "600-900",
        "from_public_listing_id": PUBLIC_ID,
    }
    assert result.mutation is not None
    assert result.mutation.set_values[LAST_SEARCH_PREF_KEY] == {
        "property_type": "",
        "location_keys": ["BKK1"],
        "budget_min": 600,
        "budget_max": 900,
    }
    assert SEARCH_PREF_SESSION_KEY in result.mutation.delete_keys
    assert SEARCH_AWAITING_BUDGET_KEY in result.mutation.delete_keys


def test_custom_budget_failure_keeps_waiting_state_for_retry():
    session = {
        SEARCH_AWAITING_BUDGET_KEY: True,
        SEARCH_PREF_SESSION_KEY: {"source": "user_search", "goal": "any"},
    }
    result = TransitionTextActionService().apply("不知道", session)

    assert result.status == "invalid"
    assert result.reason == "budget_unrecognized"
    assert "800以内" in result.prompt
    assert "600-900" in result.prompt
    assert "1500以上" in result.prompt
    assert result.mutation is None


def test_missing_backing_session_expires_and_plain_text_is_not_claimed():
    service = TransitionTextActionService()

    expired_date = service.apply("0905", {APPOINTMENT_AWAITING_DATE_KEY: True})
    expired_area = service.apply("BKK1", {SEARCH_AWAITING_AREA_KEY: True})
    expired_budget = service.apply("800以内", {SEARCH_AWAITING_BUDGET_KEY: True})
    plain = service.apply("BKK1 一房 600", {})

    assert expired_date.status == "expired"
    assert expired_area.status == "expired"
    assert expired_budget.status == "expired"
    assert plain.status == "not_applicable"
