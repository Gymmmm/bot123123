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
            "🔍 <b>还没找到完全符合的房子</b>"
            f"{summary_block}\n\n"
            "可以放宽一点条件再看看，或者让中文顾问继续帮你找。"
        ),
        rows=(
            (
                TransitionChoice("💰 调整预算", "adjust_budget"),
                TransitionChoice("📍 换个区域", "adjust_area"),
            ),
            (
                TransitionChoice("🏠 调整户型", "adjust_layout"),
                TransitionChoice("💬 帮我找房", "home", value="contact"),
            ),
            (TransitionChoice("⬅️ 返回找房", "change_search"),),
        ),
    )


__all__ = ["build_search_no_match_view"]