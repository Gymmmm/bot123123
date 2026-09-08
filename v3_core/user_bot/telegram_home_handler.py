"""Telegram adapter for V3 home-surface callbacks.

Only actions with complete V3 behavior are rendered here. Search remains deferred
until its free-text awaiting handler is extracted; rental/service remain deferred
until their fixed-SHA subflows are extracted. This prevents dead home buttons.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram.constants import ParseMode

from .appointment_history import AppointmentHistoryService, AppointmentHistoryView
from .contact_effects import ContactEffectExecutor, ContactEffectResult
from .home_callbacks import HomeAction, parse_home_callback
from .home_views import build_appointment_history_home_view, build_contact_view
from .lead_service import LeadUser
from .telegram_home_ui import build_home_keyboard


@dataclass(frozen=True)
class TelegramHomeOutcome:
    handled: bool
    action: HomeAction | None = None
    rendered: bool = False
    deferred: bool = False
    appointment_history: AppointmentHistoryView | None = None
    contact_effect: ContactEffectResult | None = None


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_home_action")
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


async def _edit_home_view(query: Any, view) -> None:
    markup = build_home_keyboard(view)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
        return
    await query.edit_message_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


async def handle_v3_home_callback(
    update: Any,
    context: Any,
    *,
    appointment_history: AppointmentHistoryService,
    contact_effects: ContactEffectExecutor | None = None,
    advisor_url: str = "",
) -> TelegramHomeOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    callback = parse_home_callback(raw)
    if query is None or callback is None:
        return TelegramHomeOutcome(handled=False)

    await query.answer()
    action = callback.action

    if action == "appointments":
        user = _lead_user(update)
        history = appointment_history.build(user.user_id)
        await _edit_home_view(query, build_appointment_history_home_view(history))
        return TelegramHomeOutcome(
            handled=True,
            action=action,
            rendered=True,
            appointment_history=history,
        )

    if action == "contact":
        if contact_effects is None:
            return TelegramHomeOutcome(
                handled=True,
                action=action,
                deferred=True,
            )
        user = _lead_user(update)
        effect = await contact_effects.execute_general(
            bot=getattr(context, "bot", None),
            user=user,
            source="hub",
        )
        await _edit_home_view(query, build_contact_view(advisor_url=advisor_url))
        return TelegramHomeOutcome(
            handled=True,
            action=action,
            rendered=True,
            contact_effect=effect,
        )

    # Search/rental/service are intentionally not rendered until their complete
    # V3 subflows exist. Full home will not be exposed before these are live.
    return TelegramHomeOutcome(
        handled=True,
        action=action,
        deferred=True,
    )


__all__ = ["TelegramHomeOutcome", "handle_v3_home_callback"]
