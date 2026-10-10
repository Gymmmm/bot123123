"""V4.0 guided-search contract: entry copy, 「已选：」 summary, back keeps state."""
from __future__ import annotations

from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.transition_actions import (
    LAST_SEARCH_PREF_KEY,
    SearchSubmitIntent,
    TransitionActionService,
)
from v3_core.user_bot.transition_callbacks import encode_transition_choice, parse_transition_callback
from v3_core.user_bot.transition_session import SEARCH_PREF_SESSION_KEY, apply_session_mutation
from v3_core.user_bot.transition_views import (
    TransitionViewService,
    filter_summary,
    search_budget_for_pref,
    search_layout_for_pref,
    searching_view,
)


def _labels(view):
    return [c.label for row in view.rows for c in row]


def _callbacks(view):
    return [encode_transition_choice(c) for row in view.rows for c in row]


def _apply(session, raw):
    result = TransitionActionService().apply(parse_transition_callback(raw), session)
    assert result.ok, result.reason
    if result.mutation is not None:
        apply_session_mutation(session, result.mutation)
    return result


def test_search_entry_matches_v4_copy():
    view = TransitionViewService.search_entry()
    assert view.text.startswith("🔍 <b>想找什么样的房子？</b>")
    assert "直接告诉我你的要求就行" in view.text
    assert "1V1" not in view.text
    assert _labels(view) == ["📍 选区域", "💰 选预算", "🏠 选户型", "💬 帮我找房", "⬅️ 返回首页"]


def test_filter_summary_is_unified():
    assert filter_summary("BKK1", "$600–800", "两房") == "已选：📍 BKK1｜💰 $600–800｜🏠 两房"
    assert filter_summary() == ""


def test_budget_and_layout_pages_show_summary_and_back_one_step():
    budget = search_budget_for_pref({"area_display": "BKK1"})
    assert "每月租金预算多少" in budget.text
    assert "已选：📍 BKK1" in budget.text
    assert "✏️ 自己填预算" in _labels(budget)
    assert _labels(budget)[-1] == "⬅️ 返回上一步"
    assert _callbacks(budget)[-1] == "v3u:t:search_back_area"
    assert "$1,500 以上" in _labels(budget)

    layout = search_layout_for_pref({"area_display": "BKK1", "budget_label": "$600–800"})
    assert "需要几间卧室" in layout.text
    assert "已选：📍 BKK1｜💰 $600–800" in layout.text
    assert _callbacks(layout)[-1] == "v3u:t:search_back_budget"

    direct = search_budget_for_pref({})
    assert _callbacks(direct)[-1] == "v3u:change_search"


def test_back_from_layout_keeps_area_and_back_from_budget_keeps_area():
    session: dict = {}
    _apply(session, "v3u:t:search_area")
    _apply(session, "v3u:t:area_choice:bkk1")
    _apply(session, "v3u:t:budget_choice:b3")
    assert session[SEARCH_PREF_SESSION_KEY]["budget_label"] == "$600–800"
    back = _apply(session, "v3u:t:search_back_budget")
    assert back.navigation == "search_budget"
    pref = session[SEARCH_PREF_SESSION_KEY]
    assert pref["area_display"].startswith("BKK1")
    assert "budget_label" not in pref
    back = _apply(session, "v3u:t:search_back_area")
    assert back.navigation == "search_area"
    assert session[SEARCH_PREF_SESSION_KEY]["area_display"].startswith("BKK1")


def test_searching_view_is_interim_panel():
    view = searching_view()
    assert "正在帮你找房" in view.text
    assert view.rows == ()


def test_no_match_v4_buttons_and_adjust_keeps_other_conditions():
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(location_keys=("BKK1",), budget_min=600, budget_max=800, room_type="2房"),
        source="home_area", goal="两房", area_display="BKK1", budget_label="$600–800", touch_payload={},
    )
    view = build_search_no_match_view(intent)
    assert "还没找到完全符合的房子" in view.text
    assert "你的要求：BKK1｜两房｜$600–800" in view.text
    assert _labels(view)[-1] == "⬅️ 返回找房"
    assert _callbacks(view)[:3] == ["v3u:t:adjust_budget", "v3u:t:adjust_area", "v3u:t:adjust_layout"]

    session = {LAST_SEARCH_PREF_KEY: {
        "property_type": "", "location_keys": ["BKK1"], "budget_min": 600, "budget_max": 800,
        "room_type": "2房", "area_display": "BKK1", "budget_label": "$600–800", "layout_label": "两房",
    }}
    nav = _apply(session, "v3u:t:adjust_budget")
    assert nav.navigation == "search_budget"
    submit = _apply(session, "v3u:t:budget_choice:b4")
    assert submit.next_step == "search_submit"
    assert submit.search.criteria.location_keys == ("BKK1",)
    assert submit.search.criteria.room_type == "2房"
    assert (submit.search.criteria.budget_min, submit.search.criteria.budget_max) == (800, 1200)
    assert submit.search.budget_label == "$800–1,200"


def test_adjust_without_previous_search_degrades_to_fresh_flow():
    session: dict = {}
    nav = _apply(session, "v3u:t:adjust_area")
    assert nav.navigation == "search_area"
    assert not session[SEARCH_PREF_SESSION_KEY].get("resume")
