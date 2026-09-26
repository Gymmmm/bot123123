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
            "🔍 <b>暂时没有完全符合的房源</b>"
            f"{summary_block}\n\n"
            "可以调整条件继续找，也可以让中文顾问按这组条件帮你找。"
        ),
        rows=(
            (
                TransitionChoice("💰 调整预算", "search_budget"),
                TransitionChoice("📍 换个区域", "search_area"),
            ),
            (TransitionChoice("🏠 调整户型", "search_layout"),),
            (TransitionChoice("💬 中文顾问", "home", value="contact"),),
            (TransitionChoice("⬅️ 返回首页", "home"),),
        ),
    )


__all__ = ["build_search_no_match_view"]