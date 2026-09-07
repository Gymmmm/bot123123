"""归因入口包装：在 /start 前记录来源，不改 start_routes 的前台行为。"""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes


async def start_with_attribution(update: Update, context: ContextTypes.DEFAULT_TYPE, **kwargs):
    from .attribution import remember_touch
    from .start_routes import start

    user = update.effective_user
    arg = str((getattr(context, "args", None) or [""])[0] or "")
    remember_touch(
        user,
        action="direct_start" if not arg else "",
        source="bot_direct_start" if not arg else "channel_deeplink",
        start_arg=arg,
    )
    return await start(update, context, **kwargs)
