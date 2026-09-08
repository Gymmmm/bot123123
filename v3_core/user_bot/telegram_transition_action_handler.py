"""Async Telegram adapter for ``v3u:t:*`` transition callbacks.

The pure ``TransitionActionService`` owns state decisions. This adapter only
claims callbacks from the transition namespace, renders side-effect-free local
steps, and applies their public-only session mutation *after* the Telegram edit
succeeds.

Executor boundaries are deliberately returned untouched:
- appointment_submit -> appointment persistence/effects executor;
- search_submit -> search/result/lead orchestration;
- navigation -> home/guided-search orchestration.
No DB writes, lead creation, admin notification, or production registration
happens here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram.constants import ParseMode

from .telegram_transition_ui import build_transition_keyboard
from .transition_actions import TransitionActionResult, TransitionActionService
from .transition_callbacks import parse_transition_callback
from .transition_session import apply_session_mutation
from .transition_views import TransitionView, TransitionViewService


@dataclass(frozen=True)
class TelegramTransitionActionOutcome:
    handled: bool
    result: TransitionActionResult | None = None


async def _edit_view(query: Any, view: TransitionView) -> None:
    keyboard = build_transition_keyboard(view) if view.rows else None
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return
    await query.edit_message_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


def _view_for_result(
    views: TransitionViewService,
    result: TransitionActionResult,
) -> TransitionView | None:
    if result.next_step == "appointment_date":
        if result.appointment is None:
            raise ValueError("appointment_date_action_missing_draft")
        return views.appointment_date(result.appointment)
    if result.next_step == "appointment_time":
        if result.appointment is None:
            raise ValueError("appointment_time_action_missing_draft")
        return views.appointment_time(result.appointment)
    if result.next_step == "appointment_custom_date":
        return views.custom_date_prompt()
    if result.next_step == "appointment_custom_time":
        return views.custom_time_prompt()
    if result.next_step == "search_custom_budget":
        return TransitionView(
            kind="search_custom_budget",
            text=(
                "💰 <b>自己输入预算</b>\n\n"
                "直接发每月预算，例如：<code>800以内</code> 或 <code>600-900</code>。"
            ),
            rows=(),
        )
    return None


async def handle_v3_transition_action(
    update: Any,
    context: Any,
    *,
    actions: TransitionActionService,
    views: TransitionViewService,
) -> TelegramTransitionActionOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    callback = parse_transition_callback(raw)
    if query is None or callback is None:
        return TelegramTransitionActionOutcome(handled=False)

    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_transition_action")

    result = actions.apply(callback, user_data)
    await query.answer()

    if not result.ok:
        # The callback belongs to this handler, but stale/invalid copy belongs to
        # the future orchestration/error layer. Do not mutate local session here.
        return TelegramTransitionActionOutcome(handled=True, result=result)

    view = _view_for_result(views, result)
    if view is not None:
        await _edit_view(query, view)
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        return TelegramTransitionActionOutcome(handled=True, result=result)

    # Submission, search, and navigation need another executor. Their mutation
    # is intentionally deferred so a later failure cannot leave session state
    # pretending that the external action succeeded.
    return TelegramTransitionActionOutcome(handled=True, result=result)


__all__ = [
    "TelegramTransitionActionOutcome",
    "handle_v3_transition_action",
]
