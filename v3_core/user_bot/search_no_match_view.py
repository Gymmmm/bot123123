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
    summary_block = f"\n\n{he(summary)}" if summary else ""
    return TransitionView(
        kind="search_no_match",
        text=(
            "<b>这组条件暂时没有对上的房源</b>"
            f"{summary_block}\n\n"
            "没有自动放宽条件。\n"
            "可以换条件再找，或把原条件发给中文顾问。"
        ),
        rows=(
            (TransitionChoice("换条件", "change_search"),),
            (TransitionChoice("中文顾问", "home", value="contact"),),
            (TransitionChoice("返回首页", "home"),),
        ),
    )


__all__ = ["build_search_no_match_view"]
