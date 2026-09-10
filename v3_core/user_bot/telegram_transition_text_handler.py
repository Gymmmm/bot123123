"""Async Telegram adapter for V3 custom date/time/area/budget text input.

The pure ``TransitionTextActionService`` owns awaiting-state decisions. At the
appointment boundary the ordering is durable appointment -> lead ->
availability/channel/admin effects -> user success page.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .appointment_runtime_effects import (
    AppointmentRuntimeEffectExecutor,
    AppointmentRuntimeEffectResult,
)
from .appointment_service import AppointmentUser
from .appointment_submit_executor import AppointmentSubmitExecution, AppointmentSubmitExecutor
from .appointment_success_view import build_appointment_success_view
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
    SessionMutationPlan,
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


def _view_keyboard(view: TransitionView, channel_url: str = ""):
    keyboard = build_transition_keyboard(view) if view.rows else None
    clean_channel = str(channel_url or "").strip()
    if not clean_channel or not view.kind.startswith("appointment"):
        return keyboard
    rows = [list(row) for row in (keyboard.inline_keyboard if keyboard else ())]
    if not any(str(button.text or "") == "📣 返回房源频道" for row in rows for button in row):
        rows.append([InlineKeyboardButton("📣 返回房源频道", url=clean_channel)])
    return InlineKeyboardMarkup(rows)


async def _reply_view(message: Any, view: TransitionView, *, channel_url: str = "") -> None:
    keyboard = _view_keyboard(view, channel_url)
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


def _appointment_success_cleanup() -> SessionMutationPlan:
    return SessionMutationPlan(
        set_values={},
        delete_keys=(
            APPOINTMENT_SESSION_KEY,
            APPOINTMENT_AWAITING_DATE_KEY,
            APPOINTMENT_AWAITING_TIME_KEY,
        ),
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
    channel_url: str = "",
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
        await _reply_view(message, view, channel_url=channel_url)
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
        await _reply_view(message, views.search_budget(area_display), channel_url=channel_url)
        if result.mutation is not None:
            apply_session_mutation(user_data, result.mutation)
        return TelegramTransitionTextOutcome(handled=True, result=result)

    if result.next_step == "appointment_submit" and appointment_executor is not None:
        if result.appointment is None:
            raise ValueError("appointment_submit_text_action_missing_draft")
        appointment_user = _telegram_appointment_user(update)
        lead_user = _lead_user(appointment_user)
        execution = appointment_executor.execute(user=appointment_user, draft=result.appointment)
        lead_effect = None
        if lead_effects is not None:
            lead_effect = lead_effects.record_appointment(
                user=lead_user,
                execution=execution,
                draft=result.appointment,
            )
        runtime_effect = None
        if appointment_runtime_effects is not None:
            runtime_effect = await appointment_runtime_effects.execute(
                bot=getattr(context, "bot", None),
                user=lead_user,
                execution=execution,
                draft=result.appointment,
            )
        success_view = build_appointment_success_view(
            result.appointment,
            views.inventory,
            submission_kind=execution.submission.kind,
        )
        await _reply_view(message, success_view, channel_url=channel_url)
        apply_session_mutation(user_data, _appointment_success_cleanup())
        return TelegramTransitionTextOutcome(
            handled=True,
            result=result,
            appointment_execution=execution,
            appointment_effects=runtime_effect,
            lead_effect=lead_effect,
        )

    if result.next_step == "search_submit" and search_executor is not None:
        if result.search is None:
            raise ValueError("search_submit_text_action_missing_intent")
        execution = search_executor.execute(result.search)
        presentation = await present_search_flow_result(update, context, execution.result)
        if not presentation.matched:
            await _reply_view(message, build_search_no_match_view(result.search), channel_url=channel_url)
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
