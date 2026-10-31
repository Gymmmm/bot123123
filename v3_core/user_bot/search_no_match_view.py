"""Strict no-match view for guided V3 search."""
from __future__ import annotations

from html import escape as he

from .transition_actions import SearchSubmitIntent
from .transition_views import TransitionChoice, TransitionView


def build_search_no_match_view(intent: SearchSubmitIntent) -> TransitionView:
    goal = str(intent.goal or "any").strip()
    summary = "｜".join(
        value
        for value in (
            str(intent.area_display or "").strip(),
            "" if goal in {"", "any", "住宅"} else goal,
            str(intent.budget_label or "").strip(),
        )
        if value
    )
    summary_block = f"\n\n你的要求：{he(summary)}" if summary else ""
    return TransitionView(
        kind="search_no_match",
        text=(
            "🔍 <b>暂时没有完全符合的房源</b>"
            f"{summary_block}\n\n"
            "调整一项条件即可继续搜索，其他条件会保留。\n也可以把需求交给中文顾问。"
        ),
        rows=(
            (
                TransitionChoice("💰 调整预算", "adjust_budget"),
                TransitionChoice("📍 换个区域", "adjust_area"),
            ),
            (
                TransitionChoice("🏠 调整户型", "adjust_layout"),
                TransitionChoice("💬 中文顾问", "home", value="contact"),
            ),
            (TransitionChoice("⬅️ 返回找房", "change_search"),),
        ),
    )


__all__ = ["build_search_no_match_view"]