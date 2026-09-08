"""Telegram renderer for V3 home views."""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .home_callbacks import encode_home_callback
from .home_views import HomeChoice, HomeView


def encode_home_choice(choice: HomeChoice) -> InlineKeyboardButton:
    url = str(choice.url or "").strip()
    if url:
        return InlineKeyboardButton(choice.label, url=url)
    if choice.kind == "root":
        return InlineKeyboardButton(choice.label, callback_data="v3u:t:home")
    return InlineKeyboardButton(
        choice.label,
        callback_data=encode_home_callback(choice.kind),
    )


def build_home_keyboard(view: HomeView) -> InlineKeyboardMarkup | None:
    if not view.rows:
        return None
    return InlineKeyboardMarkup(
        [[encode_home_choice(choice) for choice in row] for row in view.rows]
    )


__all__ = ["build_home_keyboard", "encode_home_choice"]
