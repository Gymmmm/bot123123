"""Thin python-telegram-bot UI adapter for V3 User Bot semantic actions.

Business services decide which actions exist and which public listing each one
targets. This module only converts those frozen decisions into Telegram button
objects and enforces Telegram callback-data size limits.
"""
from __future__ import annotations

from collections.abc import Iterable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .callbacks import encode_semantic_action
from .listing_responses import SemanticAction


TELEGRAM_CALLBACK_MAX_BYTES = 64


def build_action_keyboard(
    action_rows: Iterable[Iterable[SemanticAction]],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for semantic_row in action_rows:
        buttons: list[InlineKeyboardButton] = []
        for action in semantic_row:
            label = str(action.label or "").strip()
            if not label:
                raise ValueError("telegram_button_label_missing")
            callback_data = encode_semantic_action(action)
            if len(callback_data.encode("utf-8")) > TELEGRAM_CALLBACK_MAX_BYTES:
                raise ValueError("telegram_callback_data_too_long")
            buttons.append(
                InlineKeyboardButton(
                    label,
                    callback_data=callback_data,
                )
            )
        if buttons:
            rows.append(buttons)
    if not rows:
        raise ValueError("telegram_keyboard_requires_actions")
    return InlineKeyboardMarkup(rows)


__all__ = ["TELEGRAM_CALLBACK_MAX_BYTES", "build_action_keyboard"]
