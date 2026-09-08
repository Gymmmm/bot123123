"""Thin Telegram renderer for V3 transition view models.

All labels, ordering, and semantic choices are decided before this layer. This
module only encodes choices into the V3 callback contract and builds Telegram
keyboard objects. It never touches session state or executes transition effects.
"""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .telegram_ui import TELEGRAM_CALLBACK_MAX_BYTES
from .transition_callbacks import encode_transition_choice
from .transition_views import TransitionView


def build_transition_keyboard(view: TransitionView) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for semantic_row in view.rows:
        buttons: list[InlineKeyboardButton] = []
        for choice in semantic_row:
            label = str(choice.label or "").strip()
            if not label:
                raise ValueError("telegram_transition_button_label_missing")
            callback_data = encode_transition_choice(choice)
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


__all__ = ["build_transition_keyboard"]
