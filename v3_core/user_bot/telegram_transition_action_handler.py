"""Async Telegram adapter for ``v3u:t:*`` transition callbacks.

The pure ``TransitionActionService`` owns state decisions. This adapter claims
callbacks from the transition namespace, renders local steps, and can optionally
execute V3 appointment persistence at the ``appointment_submit`` boundary.

Search submit and navigation remain explicit outer-orchestration boundaries.
Lead creation, admin notification, listing availability recomputation and channel
sync are never hidden here; the appointment executor returns those as an effect
plan for a later layer.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram.constants import ParseMode

from .appointment_service import AppointmentUser
from .appointment_submit_executor import (
    AppointmentSubmitExecution,
    AppointmentSubmitExecutor,
)
from .appointment_success_view import build_appointment_success_view
from .telegram_transition_ui import build_transition_keyboard
from .transition_actions import (
    APPOINTMENT_AWAITING_DATE_KEY,
    APPOINTMENT_AWAITING_TIME_KEY,
    TransitionActionResult,
    TransitionActionService,
)
from .transition_callbacks import parse_transition_callback
from .transition_session import (
    APPOINTMENT_SESSION_KEY,
    SessionMutationPlan,
    apply_session_mutation,
)
from .transition_views import TransitionView, TransitionViewService


@dataclass(frozen=True)
class TelegramTransitionActionOutcome:
    handled: bool
    result: TransitionActionResult | None = None
    appointment_execution: AppointmentSubmitExecution | None = None


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


def _telegram_appointment_user(update: Any) -> AppointmentUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_appointment_submit")
    username = str(getattr(user, "username", "") or "")
    display_name = str(getattr(user, "full_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(
            part
            for part in (
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
            )
            if part
        )
    return AppointmentUser(
        user_id=int(user.id),
        username=username,
        display_name=display_name,
    )


def _appointment_success_cleanup() -> SessionMutationPlan:
    return SessionMutationPlan(
        set_values={},
        delete_keys=(
            APPOINTMENT_SESSION_KEY,
            APPOINTMENT_AWAITING_DATE_KEY,
            APPOINTMENT_AWAITING_TIME_KEY,
        ),
    )


async def handle_v3_transition_action(
    update: Any,
    context: Any,
    *,
    actions: TransitionActionService,
    views: TransitionViewService,
    appointment_executor: AppointmentSubmitExecutor | None = None,
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

    if result.next_step == "appointment_submit" and appointment_executor is not None:
        if result.appointment is None:
            raise ValueError("appointment_submit_action_missing_draft")
        execution = appointment_executor.execute(
            user=_telegram_appointment_user(update),
            draft=result.appointment,
        )
        success_view = build_appointment_success_view(
            result.appointment,
            views.inventory,
            submission_kind=execution.submission.kind,
        )
        # Persist first, then show success. If Telegram editing fails, keep the
        # public session intact; retry is safe because the executor reuses the
        # exact unfinished appointment instead of inserting another row.
        await _edit_view(query, success_view)
        apply_session_mutation(user_data, _appointment_success_cleanup())
        return TelegramTransitionActionOutcome(
            handled=True,
            result=result,
            appointment_execution=execution,
        )

    # Search submission, navigation, or an appointment boundary without an
    # injected executor remains deferred. Do not apply its session mutation yet.
    return TelegramTransitionActionOutcome(handled=True, result=result)


__all__ = [
    "TelegramTransitionActionOutcome",
    "handle_v3_transition_action",
]
