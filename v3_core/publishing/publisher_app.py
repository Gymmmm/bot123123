"""Final V3 Publisher application composition.

The application presents a seven-entry administrator console while retaining the
existing review/package/delivery machinery underneath.  Automatic listings use
the same frozen-package and durable Telegram delivery path as manual listings.
"""
from __future__ import annotations

from html import escape
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes, MessageHandler, filters

from v3_core.ops.runtime_state import RuntimeStateRepository
from .admin_bot import PublisherAdminBot, PublisherAdminSettings, REPO_ROOT, load_settings
from .autopilot_anomalies import FinalAutoPublishRepository, FinalAutoPublishService
from .broadcast import BroadcastService, BroadcastSettingsRepository
from .broadcast_admin import BROADCAST_EDIT_STATE_KEY, BroadcastAdminController
from .simple_admin import NEW_LISTING_STATE_KEY, SIMPLE_EDIT_STATE_KEY
from .simple_admin_production import ProductionSimplePublisherAdminController


class V3PublisherApplication(PublisherAdminBot):
    def __init__(self, settings: PublisherAdminSettings):
        super().__init__(settings)
        timezone_name = (
            str(os.getenv("V3_AUTO_PUBLISH_TIMEZONE") or os.getenv("AUTOPILOT_TIMEZONE") or "Asia/Phnom_Penh").strip()
            or "Asia/Phnom_Penh"
        )
        broadcast_service = BroadcastService(
            repository=BroadcastSettingsRepository(settings.db_path),
            repo_root=REPO_ROOT,
            user_bot_username=settings.user_bot_username,
            timezone_name=timezone_name,
        )
        self.broadcast = BroadcastAdminController(
            service=broadcast_service,
            channel_chat_id=settings.channel_chat_id,
            timezone_name=timezone_name,
        )
        self.auto_repository = FinalAutoPublishRepository(settings.db_path)
        self.auto_repository.ensure_defaults()
        self.runtime = RuntimeStateRepository(settings.db_path)
        self.autopilot = FinalAutoPublishService(
            workflow=self.workflow,
            repository=self.auto_repository,
            channel_chat_id=settings.channel_chat_id,
        )
        self.simple = ProductionSimplePublisherAdminController(
            db_path=settings.db_path,
            repo_root=REPO_ROOT,
            workflow=self.workflow,
            autopilot=self.autopilot,
            repository=self.auto_repository,
            runtime=self.runtime,
            user_bot_username=settings.user_bot_username,
            channel_chat_id=settings.channel_chat_id,
            cover_output_dir=settings.cover_output_dir,
        )

    @staticmethod
    def _home_button() -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")]

    def _dashboard_keyboard(self) -> InlineKeyboardMarkup:
        return self.simple.home_keyboard()

    async def _send_dashboard(self, message) -> None:
        await self.simple.show_home(message)

    async def daily(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
        await self.broadcast.show_center(update.effective_message)

    async def on_callback(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if raw.startswith("v3smp|"):
            if query is None:
                return
            if not await self._require_admin(update):
                await query.answer()
                return
            await query.answer()
            try:
                await self.simple.handle_callback(update, context)
            except Exception as exc:
                await query.message.reply_text(
                    "操作失败，房源没有被强行发布：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([self._home_button()]),
                )
            return
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
                    "广播操作失败：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([self._home_button()]),
                )
            return
        await super().on_callback(update, context)

    async def on_text(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        try:
            if isinstance(context.user_data.get(NEW_LISTING_STATE_KEY), dict):
                if await self.simple.consume_new_listing_message(update, context):
                    return
            if isinstance(context.user_data.get(SIMPLE_EDIT_STATE_KEY), dict):
                if await self.simple.handle_text(update, context):
                    return
            if isinstance(context.user_data.get(BROADCAST_EDIT_STATE_KEY), dict):
                if await self.broadcast.handle_text(update, context):
                    return
        except Exception as exc:
            await update.effective_message.reply_text(
                "处理失败，未发布到频道：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self._home_button()]),
            )
            return
        await super().on_text(update, context)

    async def on_photo(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        if not isinstance(context.user_data.get(NEW_LISTING_STATE_KEY), dict):
            await update.effective_message.reply_text(
                "请先从首页点击“➕ 新建房源”，再发送房源图片。",
                reply_markup=InlineKeyboardMarkup([self._home_button()]),
            )
            return
        try:
            await self.simple.consume_new_listing_message(update, context)
        except Exception as exc:
            await update.effective_message.reply_text(
                "图片处理失败，未发布到频道：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self._home_button()]),
            )

    async def cancel(self, update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop("v3_publisher_edit", None)
        context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
        context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
        context.user_data.pop(NEW_LISTING_STATE_KEY, None)
        await update.effective_message.reply_text(
            "已取消当前操作。",
            reply_markup=InlineKeyboardMarkup([self._home_button()]),
        )

    def build_application(self):
        app = super().build_application()
        app.add_handler(CommandHandler("daily", self.daily), group=0)
        app.add_handler(MessageHandler(filters.PHOTO, self.on_photo), group=0)
        if app.job_queue is None:
            raise RuntimeError("python-telegram-bot JobQueue support is required for V3 Publisher")
        app.job_queue.run_repeating(
            self.autopilot.scheduled_tick,
            interval=20,
            first=3,
            name="v3_auto_publish_tick",
        )
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
