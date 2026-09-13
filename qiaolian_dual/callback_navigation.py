"""Navigation compatibility layer for the integrated User Bot."""
from __future__ import annotations

from . import callback_navigation_base as _base
from .callback_navigation_base import *  # noqa: F401,F403


def matches(data: str) -> bool:
    return data == 'hub:about' or _base.matches(data)


async def handle_navigation_callback(update, context, query, data: str, user):
    if data == 'hub:about':
        from telegram.constants import ParseMode
        from .common import MAIN
        from .keyboards_common import main_keyboard
        from .messages import about_text
        from .texts import render_panel
        await render_panel(
            update,
            text=about_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
            context=context,
        )
        return MAIN
    return await _base.handle_navigation_callback(update, context, query, data, user)
