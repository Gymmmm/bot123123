"""V4.1: 「⬅️ 返回上一步」follows the real navigation history and keeps filters."""
from __future__ import annotations

from v3_core.user_bot.transition_actions import LAST_SEARCH_PREF_KEY, TransitionActionService
from v3_core.user_bot.transition_callbacks import encode_transition_choice, parse_transition_callback
from v3_core.user_bot.transition_session import SEARCH_PREF_SESSION_KEY, apply_session_mutation
from v3_core.user_bot.transition_text_actions import TransitionTextActionService
from v3_core.user_bot.transition_views import (
    search_area_for_pref,
    search_budget_for_pref,
    search_layout_for_pref,
)
from v3_core.user_bot.transition_actions import SEARCH_AWAITING_AREA_KEY


def _apply(session, raw):
    result = TransitionActionService().apply(parse_transition_callback(raw), session)
    assert result.ok, result.reason
    if result.mutation is not None:
        apply_session_mutation(session, result.mutation)
    return result


def _back_callback(view):
    return encode_transition_choice(view.rows[-1][-1])


def _page(result, session):
    pref = session.get(SEARCH_PREF_SESSION_KEY)
    return {
        "search_area": search_area_for_pref,
        "search_budget": search_budget_for_pref,
        "search_layout": search_layout_for_pref,
    }[result.navigation](pref)


def test_budget_reached_from_area_goes_back_to_area_and_keeps_area():
    s = {}
    _apply(s, "v3u:t:search_area")
    r = _apply(s, "v3u:t:area_choice:koh")
    assert r.navigation == "search_budget"
    page = _page(r, s)
    assert "已选：📍 钻石岛" in page.text
    assert _back_callback(page) == "v3u:t:search_back"
    back = _apply(s, "v3u:t:search_back")
    assert back.navigation == "search_area"
    assert s[SEARCH_PREF_SESSION_KEY]["area_display"] == "钻石岛"


def test_budget_reached_from_entry_goes_back_to_entry():
    s = {}
    r = _apply(s, "v3u:t:search_budget")
    page = _page(r, s)
    assert _back_callback(page) == "v3u:t:search_back"
    back = _apply(s, "v3u:t:search_back")
    assert back.navigation == "search_entry"


def test_layout_from_budget_from_entry_back_chain_keeps_budget():
    s = {}
    _apply(s, "v3u:t:search_budget")
    r = _apply(s, "v3u:t:budget_choice:b3")
    assert r.navigation == "search_layout"
    page = _page(r, s)
    assert "$600–800" in page.text
    back = _apply(s, "v3u:t:search_back")
    assert back.navigation == "search_budget"
    assert s[SEARCH_PREF_SESSION_KEY]["budget_label"] == "$600–800"
    back = _apply(s, "v3u:t:search_back")
    assert back.navigation == "search_entry"
    # Picking another filter from the entry keeps the budget already chosen.
    r = _apply(s, "v3u:t:search_area")
    assert s[SEARCH_PREF_SESSION_KEY]["budget_label"] == "$600–800"
    assert "$600–800" in _page(r, s).text


def test_full_chain_area_budget_layout_back_twice():
    s = {}
    _apply(s, "v3u:t:search_area")
    _apply(s, "v3u:t:area_choice:bkk1")
    _apply(s, "v3u:t:budget_choice:b2")
    assert _apply(s, "v3u:t:search_back").navigation == "search_budget"
    assert _apply(s, "v3u:t:search_back").navigation == "search_area"
    assert _apply(s, "v3u:t:search_back").navigation == "search_entry"
    pref = s[SEARCH_PREF_SESSION_KEY]
    assert pref["area_display"] and pref["budget_label"] == "$400–600"


def test_custom_area_text_records_history():
    s = {}
    _apply(s, "v3u:t:search_area")
    _apply(s, "v3u:t:area_other")
    assert s.get(SEARCH_AWAITING_AREA_KEY)
    result = TransitionTextActionService().apply("BKK1", s)
    assert result.ok
    apply_session_mutation(s, result.mutation)
    assert _apply(s, "v3u:t:search_back").navigation == "search_area"


def test_adjust_from_no_match_back_returns_to_previous_results():
    s = {LAST_SEARCH_PREF_KEY: {
        "property_type": "", "location_keys": ["BKK1"], "budget_min": None, "budget_max": 400,
        "room_type": "3房", "area_display": "BKK1", "budget_label": "$400 以下", "layout_label": "三房",
    }}
    r = _apply(s, "v3u:t:adjust_budget")
    assert r.navigation == "search_budget"
    assert _back_callback(_page(r, s)) == "v3u:t:search_back"
    back = _apply(s, "v3u:t:search_back")
    assert back.next_step == "search_submit"
    assert back.search.criteria.budget_max == 400
    assert back.search.criteria.location_keys == ("BKK1",)


def test_legacy_sessions_without_history_keep_v4_back_targets():
    assert _back_callback(search_budget_for_pref({"area_display": "BKK1"})) == "v3u:t:search_back_area"
    assert _back_callback(search_layout_for_pref({"budget_label": "$600–800"})) == "v3u:t:search_back_budget"
