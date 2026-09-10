"""Async Telegram adapter for V3 custom date/time/area/budget text input.

The pure ``TransitionTextActionService`` owns awaiting-state decisions. Issue
#25 keeps custom-time input on the same confirmation boundary as button time
choices; persistence occurs only after the explicit submit callback.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from telegram.constants import ParseMode

from .appointment_confirmation_view import build_appointment_confirmation_view
from .appointment_runtime_effects import (
    AppointmentRuntimeEffectExecutor,
    AppointmentRuntimeEffectResult,
)
from .appointment_service import AppointmentUser
from .appointment_submit_executor import AppointmentSubmitExecution, AppointmentSubmitExecutor
from .lead_effects import LeadEffectExecutor, LeadEffectResult
from .lead_service import LeadUser
from .search_no_match_view import build_search_no_match_view
from .search_submit_executor import SearchSubmitExecution, SearchSubmitExecutor
from .telegram_search_results import TelegramSearchPresentation, present_search_flow_result
from .telegram_transition_ui import build_transition_keyboard
from .transition_actions import APPOINTMENT_AWAITING_DATE_KEY, APPOINTMENT_AWAITING_TIME_KEY
from .transition_session import (
    APPOINTMENT_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    apply_session_mutation,
)
from .transition_text_actions import TransitionTextActionResult, TransitionTextActionService
from .transition_views import TransitionView, TransitionViewService


@dataclass(frozen=True)
class TelegramTransitionTextOutcome:
    handled: bool
    result: TransitionTextActionResult | None = None
    appointment_execution: AppointmentSubmitExecution | None = None
    appointment_effects: AppointmentRuntimeEffectResult | None = None
    search_execution: SearchSubmitExecution | None = None
    search_presentation: TelegramSearchPresentation | None = None
    lead_effect: LeadEffectResult | None = None


async def _reply_view(message: Any, view: TransitionView) -> None:
    keyboard = build_transition_keyboard(view) if view.rows else None
    await message.reply_text(view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


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
    return AppointmentUser(user_id=int(user.id), username=username, display_name=display_name)


def _lead_user(user: AppointmentUser) -> LeadUser:
    return LeadUser(
        user_id=int(user.user_id),
        username=str(user.username or ""),
        display_name=str(user.display_name or ""),
    )


async def handle_v3_transition_text(
    update: Any,
    context: Any,
    *,
    actions: TransitionTextActionService,
    views: TransitionViewService,
    appointment_executor: AppointmentSubmitExecutor | None = None,
    search_executor: SearchSubmitExecutor | None = None,
    lead_effects: LeadEffectExecutor | None = None,
    appointment_runtime_effects: AppointmentRuntimeEffectExecutor | None = None,
) -> TelegramTransitionTextOutcome:
    message = getattr(update, "effective_message", None)
    text = str(getattr(message, "text", "") or "") if message is not None else ""
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        return TelegramTransitionTextOutcome(handled=False)

    result = actions.apply(text, user_data)
    if result.status == "not_applicable":
        return TelegramTransitionTextOutcome(handled=False, result=result)
    if result.status == "invalid":
        if result.prompt:
            await message.reply_text(result.prompt, parse_mode=ParseMode.HTML)
        return TelegramTransitionTextOutcome(handled=True, result=result)
    if not result.ok:
        return TelegramTransitionTextOutcome(handled=True, result=result)

    if result.next_step == "appointment_time":
        if result.appointment is None:
            raise ValueError("appointment_time_text_action_missing_draft")
        view = views.appointment_time(result.appointment)
        await _reply_view(message, view)
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        return TelegramTransitionTextOutcome(handled=True, result=result)

    if result.next_step == "search_budget":
        preview = deepcopy(user_data)
        if result.mutation is not None:
            apply_session_mutation(preview, result.mutation)
        pref = preview.get(SEARCH_PREF_SESSION_KEY)
        area_display = ""
        if isinstance(pref, dict):
            area_display = str(pref.get("area_display") or "").strip()
        await _reply_view(message, views.search_budget(area_display))
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        return TelegramTransitionTextOutcome(handled=True, result=result)

    # TransitionTextActionService historically names a ready custom-time draft
    # appointment_submit. Issue #25 requires one confirmation screen first.
    # Keep the existing pure service/session contract and move persistence behind
    # the explicit callback handled by telegram_transition_action_handler.
    if result.next_step == "appointment_submit":
        if result.appointment is None:
            raise ValueError("appointment_submit_text_action_missing_draft")
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        user_data.pop(APPOINTMENT_AWAITING_DATE_KEY, None)
        user_data.pop(APPOINTMENT_AWAITING_TIME_KEY, None)
        await _reply_view(
            message,
            build_appointment_confirmation_view(result.appointment, views.inventory),
        )
        return TelegramTransitionTextOutcome(handled=True, result=result)

    if result.next_step == "search_submit" and search_executor is not None:
        if result.search is None:
            raise ValueError("search_submit_text_action_missing_intent")
        execution = search_executor.execute(result.search)
        presentation = await present_search_flow_result(update, context, execution.result)
        if not presentation.matched:
            await _reply_view(message, build_search_no_match_view(result.search))
        lead_effect = None
        if lead_effects is not None:
            lead_effect = lead_effects.record_search(
                user=_lead_user(_telegram_appointment_user(update)),
                intent=result.search,
            )
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        return TelegramTransitionTextOutcome(
            handled=True,
            result=result,
            search_execution=execution,
            search_presentation=presentation,
            lead_effect=lead_effect,
        )

    return TelegramTransitionTextOutcome(handled=True, result=result)


__all__ = ["TelegramTransitionTextOutcome", "handle_v3_transition_text"]
