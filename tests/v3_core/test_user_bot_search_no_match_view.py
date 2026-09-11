from __future__ import annotations

from v3_core.user_bot.search_no_match_view import build_search_no_match_view
from v3_core.user_bot.search_query import SearchCriteria
from v3_core.user_bot.transition_actions import SearchSubmitIntent


def test_no_match_offers_current_inventory_before_restart_or_advisor():
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
    assert "BKK1｜<= 800 USD/月" in view.text
    assert [choice.label for row in view.rows for choice in row] == [
        "🏘 查看当前可约",
        "✏️ 调整条件",
        "💬 联系中文顾问",
        "🏠 返回首页",
    ]
    assert [choice.kind for row in view.rows for choice in row] == [
        "search_available",
        "change_search",
        "home",
        "home",
    ]
