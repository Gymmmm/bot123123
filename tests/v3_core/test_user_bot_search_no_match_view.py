from __future__ import annotations

from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.transition_actions import SearchSubmitIntent


def test_no_match_keeps_conditions_strict_and_offers_adjust_or_advisor_only():
    intent = SearchSubmitIntent(
        criteria=SearchCriteria(
            property_type="",
            location_keys=("BKK1",),
            budget_min=None,
            budget_max=800,
            room_type="",
            raw_text="BKK1 800以内",
        ),
        source="direct_text",
        goal="any",
        area_display="BKK1",
        budget_label="<= 800 USD/月",
        touch_payload={},
    )
    view = build_search_no_match_view(intent)
    assert view.kind == "search_no_match"
    assert "BKK1｜&lt;= 800 USD/月" in view.text
    assert "暂时没有完全符合的房源" in view.text
    assert "相近房源" not in view.text
    labels = [choice.label for row in view.rows for choice in row]
    assert labels == ["💰 调整预算", "📍 换个区域", "🏠 调整户型", "💬 中文顾问", "⬅️ 返回首页"]
    assert "当前可约" not in " ".join(labels)
    assert [choice.kind for row in view.rows for choice in row] == [
        "search_budget", "search_area", "search_layout", "home", "home",
    ]