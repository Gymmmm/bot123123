"""Thin Telegram renderer for V3 transition view models.

All labels, ordering, and semantic choices are decided before this layer. This
module only encodes choices into the V3 callback contract and builds Telegram
keyboard objects. It never touches session state or executes transition effects.
"""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .home_callbacks import encode_home_callback
from .telegram_ui import TELEGRAM_CALLBACK_MAX_BYTES
from .transition_callbacks import TRANSITION_PREFIX, encode_transition_choice
from .transition_views import TransitionChoice, TransitionView

_HOME_VALUE_ACTIONS = frozenset({"contact", "appointments", "search"})
_LOCAL_FLAG_KINDS = frozenset({"appointment_submit", "appointment_back_time"})


def encode_appointment_flow_choice(choice: TransitionChoice) -> str:
    """Encode Issue #25 appointment confirm/success buttons.

    ``transition_callbacks`` does not yet own ``appointment_submit`` /
    ``appointment_back_time`` or home ``appointments``. Window D keeps those
    encodings in this allowed renderer so views can ship; Window E must teach
    the parser/handler the same callback strings.
    """
    kind = str(choice.kind or "").strip()
    value = str(choice.value or "").strip()
    if kind == "home" and value in _HOME_VALUE_ACTIONS:
        return encode_home_callback(value)
    if kind in _LOCAL_FLAG_KINDS:
        if value:
            raise ValueError("flag_transition_callback_must_not_have_value")
        return f"{TRANSITION_PREFIX}:{kind}"
    return encode_transition_choice(choice)


def build_transition_keyboard(view: TransitionView) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for semantic_row in view.rows:
        buttons: list[InlineKeyboardButton] = []
        for choice in semantic_row:
            label = str(choice.label or "").strip()
            if not label:
                raise ValueError("telegram_transition_button_label_missing")
            callback_data = encode_appointment_flow_choice(choice)
            if len(callback_data.encode("utf-8")) > TELEGRAM_CALLBACK_MAX_BYTES:
                raise ValueError("telegram_transition_callback_data_too_long")
            buttons.append(
                InlineKeyboardButton(label, callback_data=callback_data)
            )
        if buttons:
            rows.append(buttons)
    if not rows:
        raise ValueError("telegram_transition_keyboard_requires_choices")
    return InlineKeyboardMarkup(rows)


__all__ = ["build_transition_keyboard", "encode_appointment_flow_choice"]
