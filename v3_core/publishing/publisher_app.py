"""Final V3 Publisher application composition.

The core review/publish admin bot remains unchanged; this explicit subclass
adds the V3-owned broadcast controller without runtime monkey patching.
"""
from __future__ import annotations

from html import escape
import os

from telegram import InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes

from .admin_bot import PublisherAdminBot, PublisherAdminSettings, REPO_ROOT, load_settings
from .broadcast import BroadcastService, BroadcastSettingsRepository
from .broadcast_admin import BROADCAST_EDIT_STATE_KEY, BroadcastAdminController


class V3PublisherApplication(PublisherAdminBot):
    def __init__(self, settings: PublisherAdminSettings):
        super().__init__(settings)
        timezone_name = str(os.getenv("AUTOPILOT_TIMEZONE", "Asia/Phnom_Penh")).strip() or "Asia/Phnom_Penh"
        service = BroadcastService(
            repository=BroadcastSettingsRepository(settings.db_path),
            repo_root=REPO_ROOT,
            user_bot_username=settings.user_bot_username,
            timezone_name=timezone_name,
        )
        self.broadcast = BroadcastAdminController(
            service=service,
            channel_chat_id=settings.channel_chat_id,
            timezone_name=timezone_name,
        )

    def _dashboard_keyboard(self) -> InlineKeyboardMarkup:
        base = super()._dashboard_keyboard()
        rows = [list(row) for row in base.inline_keyboard]
        rows.append([self.broadcast.dashboard_button()])
        return InlineKeyboardMarkup(rows)

    async def daily(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
        await self.broadcast.show_center(update.effective_message)

    async def on_callback(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if raw == "v3bc" or raw.startswith("v3bc|"):
            if query is None:
                return
            if not await self._require_admin(update):
                await query.answer()
                return
            await query.answer()
            try:
                await self.broadcast.handle_callback(update, context)
            except Exception as exc:
                await query.message.reply_text(
                    "广播操作失败，未自动绕过：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([self._home_button()]),
                )
            return
        await super().on_callback(update, context)

    async def on_text(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if isinstance(context.user_data.get(BROADCAST_EDIT_STATE_KEY), dict):
            if not await self._require_admin(update):
                return
            try:
                if await self.broadcast.handle_text(update, context):
                    return
            except Exception as exc:
                await update.effective_message.reply_text(
                    "广播设置保存失败，可直接重新输入：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                    parse_mode=ParseMode.HTML,
                )
                return
        await super().on_text(update, context)

    async def cancel(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop("v3_publisher_edit", None)
        context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
        await update.effective_message.reply_text(
            "已取消当前编辑。",
            reply_markup=InlineKeyboardMarkup([self._home_button()]),
        )

    def build_application(self):
        app = super().build_application()
        app.add_handler(CommandHandler("daily", self.daily), group=0)
        if app.job_queue is None:
            raise RuntimeError("python-telegram-bot JobQueue support is required for V3 Publisher")
        app.job_queue.run_repeating(
            self.broadcast.scheduled_tick,
            interval=20,
            first=5,
            name="v3_daily_broadcast_tick",
        )
        return app


def run() -> None:
    settings = load_settings()
    V3PublisherApplication(settings).build_application().run_polling(drop_pending_updates=False)


__all__ = ["V3PublisherApplication", "run"]
