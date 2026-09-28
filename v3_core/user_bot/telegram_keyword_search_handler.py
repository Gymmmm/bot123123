"""Telegram adapter for explicit V3 free-text keyword search."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram.constants import ParseMode

from .keyword_search_actions import KeywordSearchActionResult, KeywordSearchActionService
from .lead_effects import LeadEffectExecutor, LeadEffectResult
from .lead_service import LeadUser
from .search_no_match_view import build_search_no_match_view
from .search_submit_executor import SearchSubmitExecution, SearchSubmitExecutor
from .telegram_search_results import TelegramSearchPresentation, present_search_flow_result
from .telegram_transition_ui import build_transition_keyboard
from .transition_session import apply_session_mutation


@dataclass(frozen=True)
class TelegramKeywordSearchOutcome:
    handled: bool
    result: KeywordSearchActionResult | None = None
    execution: SearchSubmitExecution | None = None
    presentation: TelegramSearchPresentation | None = None
    lead_effect: LeadEffectResult | None = None


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_keyword_search")
    display_name = str(getattr(user, "full_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(
            value
            for value in (
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
            )
            if value
        )
    return LeadUser(
        user_id=int(user.id),
        username=str(getattr(user, "username", "") or ""),
        display_name=display_name,
    )


async def handle_v3_keyword_search_text(
    update: Any,
    context: Any,
    *,
    actions: KeywordSearchActionService,
    search_executor: SearchSubmitExecutor,
    lead_effects: LeadEffectExecutor | None = None,
) -> TelegramKeywordSearchOutcome:
    message = getattr(update, "effective_message", None)
    user_data = getattr(context, "user_data", None)
    if message is None or not isinstance(user_data, dict):
        return TelegramKeywordSearchOutcome(handled=False)

    result = actions.apply(getattr(message, "text", ""), user_data)
    if result.status == "not_applicable":
        return TelegramKeywordSearchOutcome(handled=False, result=result)
    if result.status == "invalid":
        if result.prompt:
            await message.reply_text(result.prompt, parse_mode=ParseMode.HTML)
        return TelegramKeywordSearchOutcome(handled=True, result=result)
    if not result.ok or result.intent is None:
        return TelegramKeywordSearchOutcome(handled=True, result=result)

    execution = search_executor.execute(result.intent)
    presentation = await present_search_flow_result(update, context, execution.result)
    if not presentation.matched:
        view = build_search_no_match_view(result.intent)
        await message.reply_text(
            view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_transition_keyboard(view),
        )

    lead_effect = None
    if lead_effects is not None:
        lead_effect = lead_effects.record_keyword_search(
            user=_lead_user(update),
            intent=result.intent,
            match_mode=execution.result.mode,
        )

    if result.mutation is not None:
        apply_session_mutation(user_data, result.mutation)
    return TelegramKeywordSearchOutcome(
        handled=True,
        result=result,
        execution=execution,
        presentation=presentation,
        lead_effect=lead_effect,
    )


__all__ = ["TelegramKeywordSearchOutcome", "handle_v3_keyword_search_text"]
