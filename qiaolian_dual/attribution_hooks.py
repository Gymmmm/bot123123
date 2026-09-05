"""把归因写入 /start，避免直接改 start_routes 与前台冲突。"""
from __future__ import annotations

from telegram.ext import ContextTypes
from telegram import Update


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
