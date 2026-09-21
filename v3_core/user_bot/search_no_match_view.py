"""Strict no-match view for guided V3 search."""
from __future__ import annotations

from html import escape as he

from .transition_actions import SearchSubmitIntent
from .transition_views import TransitionChoice, TransitionView


def build_search_no_match_view(intent: SearchSubmitIntent) -> TransitionView:
    goal = str(intent.goal or "any").strip()
    summary = " · ".join(
        value
        for value in (
            str(intent.area_display or "").strip(),
            "" if goal in {"", "any", "住宅"} else goal,
            str(intent.budget_label or "").strip(),
        )
        if value
    )
    summary_block = f"\n\n条件｜{he(summary)}" if summary else ""
    return TransitionView(
        kind="search_no_match",
        text=(
            "🔍 <b>暂时没找到完全符合的房源</b>"
            f"{summary_block}\n\n"
            "我没有自动放宽你的条件。\n\n"
            "可以调整条件再找，\n"
            "也可以让中文顾问按原条件继续帮你找。"
        ),
        rows=(
            (TransitionChoice("🔄 调整条件", "change_search"), TransitionChoice("💬 中文顾问", "home", value="contact")),
            (TransitionChoice("⬅️ 返回首页", "home"),),
        ),
    )


__all__ = ["build_search_no_match_view"]