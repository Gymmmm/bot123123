"""Async Telegram adapter for ``v3u:t:*`` transition callbacks.

The pure ``TransitionActionService`` owns state decisions. This adapter claims
callbacks from the transition namespace, renders local steps, and executes the
V3 appointment/search boundaries when their dependencies are injected.

Issue #25 keeps persistence behind an explicit confirmation callback:
date -> time -> confirmation -> submit. Durable ordering after submit remains
appointment -> lead -> availability/channel/admin effects -> user success page.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping

from telegram.constants import ParseMode

from .appointment_confirmation_view import build_appointment_confirmation_view
from .appointment_runtime_effects import (
    AppointmentRuntimeEffectExecutor,
    AppointmentRuntimeEffectResult,
)
from .appointment_service import AppointmentUser
from .appointment_submit_executor import (
    AppointmentSubmitExecution,
    AppointmentSubmitExecutor,
)
from .appointment_success_view import build_appointment_success_view
from .lead_effects import LeadEffectExecutor, LeadEffectResult
from .lead_service import LeadUser
from .public_appointment import PublicAppointmentDraft
from .search_no_match_view import build_search_no_match_view
from .search_submit_executor import SearchSubmitExecution, SearchSubmitExecutor
from .telegram_search_results import TelegramSearchPresentation, present_search_flow_result
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
    AWAITING_KEYWORD_SESSION_KEY,
    SEARCH_PREF_SESSION_KEY,
    SessionMutationPlan,
    apply_session_mutation,
)
from .transition_views import TransitionView, TransitionViewService


_GUIDED_SEARCH_CALLBACKS = frozenset(
    {
        "search_area",
        "area_choice",
        "area_other",
        "search_budget",
        "budget_choice",
        "budget_custom",
        "search_layout",
        "layout_choice",
        "search_available",
    }
)


@dataclass(frozen=True)
class TelegramTransitionActionOutcome:
    handled: bool
    result: TransitionActionResult | None = None
    appointment_execution: AppointmentSubmitExecution | None = None
    appointment_effects: AppointmentRuntimeEffectResult | None = None
    search_execution: SearchSubmitExecution | None = None
    search_presentation: TelegramSearchPresentation | None = None
    lead_effect: LeadEffectResult | None = None


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


def _view_for_result(views: TransitionViewService, result: TransitionActionResult) -> TransitionView | None:
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
    if result.next_step == "search_custom_area":
        return views.custom_area_prompt()
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


def _navigation_view(
    views: TransitionViewService,
    result: TransitionActionResult,
    user_data: dict[str, Any],
) -> TransitionView | None:
    navigation = result.navigation
    if not navigation or navigation == "home":
        return None
    preview = deepcopy(user_data)
    if result.mutation is not None:
        apply_session_mutation(preview, result.mutation)
    if navigation == "search_area":
        return views.search_area()
    if navigation == "search_layout":
        return views.search_layout()
    if navigation == "search_budget":
        pref = preview.get(SEARCH_PREF_SESSION_KEY)
        area_display = ""
        if isinstance(pref, dict):
            area_display = str(pref.get("area_display") or "").strip()
        return views.search_budget(area_display)
    raise ValueError(f"unsupported_transition_navigation:{navigation}")


def _apply_success_mutation(
    user_data: dict[str, Any],
    result: TransitionActionResult,
    callback_kind: str,
) -> None:
    if result.mutation is not None:
        apply_session_mutation(user_data, result.mutation)
    if str(callback_kind or "") in _GUIDED_SEARCH_CALLBACKS:
        user_data.pop(AWAITING_KEYWORD_SESSION_KEY, None)


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


def _load_confirmation_draft(user_data: Mapping[str, Any]) -> PublicAppointmentDraft | None:
    raw = user_data.get(APPOINTMENT_SESSION_KEY)
    if not isinstance(raw, Mapping):
        return None
    try:
        draft = PublicAppointmentDraft(
            public_listing_id=raw.get("public_listing_id", ""),
            mode=raw.get("mode", "offline"),
            date=str(raw.get("date") or ""),
            time=str(raw.get("time") or ""),
            source=str(raw.get("source") or "user_bot"),
        )
    except (TypeError, ValueError):
        return None
    return draft if draft.ready else None


async def _submit_confirmed_appointment(
    update: Any,
    context: Any,
    query: Any,
    *,
    draft: PublicAppointmentDraft,
    views: TransitionViewService,
    user_data: dict[str, Any],
    appointment_executor: AppointmentSubmitExecutor,
    lead_effects: LeadEffectExecutor | None,
    appointment_runtime_effects: AppointmentRuntimeEffectExecutor | None,
) -> TelegramTransitionActionOutcome:
    appointment_user = _telegram_appointment_user(update)
    lead_user = _lead_user(appointment_user)
    execution = appointment_executor.execute(user=appointment_user, draft=draft)
    lead_effect = None
    if lead_effects is not None:
        lead_effect = lead_effects.record_appointment(
            user=lead_user,
            execution=execution,
            draft=draft,
        )
    runtime_effect = None
    if appointment_runtime_effects is not None:
        runtime_effect = await appointment_runtime_effects.execute(
            bot=getattr(context, "bot", None),
            user=lead_user,
            execution=execution,
            draft=draft,
        )
    success_view = build_appointment_success_view(
        draft,
        views.inventory,
        submission_kind=execution.submission.kind,
    )
    await _edit_view(query, success_view)
    apply_session_mutation(user_data, _appointment_success_cleanup())
    return TelegramTransitionActionOutcome(
        handled=True,
        appointment_execution=execution,
        appointment_effects=runtime_effect,
        lead_effect=lead_effect,
    )


async def handle_v3_transition_action(
    update: Any,
    context: Any,
    *,
    actions: TransitionActionService,
    views: TransitionViewService,
    appointment_executor: AppointmentSubmitExecutor | None = None,
    search_executor: SearchSubmitExecutor | None = None,
    lead_effects: LeadEffectExecutor | None = None,
    appointment_runtime_effects: AppointmentRuntimeEffectExecutor | None = None,
) -> TelegramTransitionActionOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    callback = parse_transition_callback(raw)
    if query is None or callback is None:
        return TelegramTransitionActionOutcome(handled=False)

    user_data = getattr(context, "user_data", None)
    if not isinstance(user_data, dict):
        raise ValueError("telegram_user_data_missing_for_transition_action")

    # These two callbacks are the explicit confirmation boundary added by
    # Issue #25. They operate on the same public appointment session and do not
    # introduce a second controller or persistence path.
    if callback.kind in {"appointment_submit", "appointment_back_time"}:
        await query.answer()
        draft = _load_confirmation_draft(user_data)
        if draft is None:
            return TelegramTransitionActionOutcome(handled=True)
        if callback.kind == "appointment_back_time":
            await _edit_view(query, views.appointment_time(draft.with_date(draft.date)))
            return TelegramTransitionActionOutcome(handled=True)
        if appointment_executor is None:
            return TelegramTransitionActionOutcome(handled=True)
        return await _submit_confirmed_appointment(
            update,
            context,
            query,
            draft=draft,
            views=views,
            user_data=user_data,
            appointment_executor=appointment_executor,
            lead_effects=lead_effects,
            appointment_runtime_effects=appointment_runtime_effects,
        )

    result = actions.apply(callback, user_data)
    await query.answer()
    if not result.ok:
        return TelegramTransitionActionOutcome(handled=True, result=result)

    view = _view_for_result(views, result)
    if view is not None:
        await _edit_view(query, view)
        _apply_success_mutation(user_data, result, callback.kind)
        return TelegramTransitionActionOutcome(handled=True, result=result)

    if result.next_step == "navigation":
        navigation_view = _navigation_view(views, result, user_data)
        if navigation_view is not None:
            await _edit_view(query, navigation_view)
            _apply_success_mutation(user_data, result, callback.kind)
            return TelegramTransitionActionOutcome(handled=True, result=result)

    # Existing action service historically names this boundary appointment_submit.
    # Under Issue #25 it now means "draft ready for confirmation"; persistence is
    # performed only by the explicit appointment_submit callback above.
    if result.next_step == "appointment_submit":
        if result.appointment is None:
            raise ValueError("appointment_submit_action_missing_draft")
        _apply_success_mutation(user_data, result, callback.kind)
        await _edit_view(query, build_appointment_confirmation_view(result.appointment, views.inventory))
        return TelegramTransitionActionOutcome(handled=True, result=result)

    if result.next_step == "search_submit" and search_executor is not None:
        if result.search is None:
            raise ValueError("search_submit_action_missing_intent")
        execution = search_executor.execute(result.search)
        presentation = await present_search_flow_result(update, context, execution.result)
        if not presentation.matched:
            await _edit_view(query, build_search_no_match_view(result.search))
        lead_effect = None
        if lead_effects is not None:
            lead_effect = lead_effects.record_search(
                user=_lead_user(_telegram_appointment_user(update)),
                intent=result.search,
            )
        _apply_success_mutation(user_data, result, callback.kind)
        return TelegramTransitionActionOutcome(
            handled=True,
            result=result,
            search_execution=execution,
            search_presentation=presentation,
            lead_effect=lead_effect,
        )

    return TelegramTransitionActionOutcome(handled=True, result=result)


__all__ = ["TelegramTransitionActionOutcome", "handle_v3_transition_action"]
