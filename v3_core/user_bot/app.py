"""Independent python-telegram-bot Application wiring for the V3 User Bot.

Nothing in this module initializes schema or changes production services. It
only composes V3-owned handlers against an already initialized additive V3 DB.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from v3_core.ops.runtime_state import RuntimeStateRepository
from v3_core.storage.appointment_repository import SQLiteAppointmentRepository

from .admin_notifications import TelegramAdminNotifier
from .admin_appointments import AdminAppointmentReader, handle_admin_callback, show_admin_home
from .appointment_availability import AppointmentAvailabilityService
from .appointment_runtime_effects import AppointmentRuntimeEffectExecutor
from .channel_status_sync import V3AppointmentChannelSynchronizer
from .contact_effects import ContactEffectExecutor
from .home_views import build_booking_view, build_home_view
from .listing_contact import ListingContactEffectExecutor
from .runtime import UserBotReadRuntime, build_read_runtime
from .service_effects import ServiceEffectExecutor
from .telegram_assurance_handler import handle_v3_assurance_callback
from .telegram_home_handler import handle_v3_home_action, handle_v3_home_callback
from .telegram_home_ui import build_home_keyboard
from .telegram_keyword_search_handler import handle_v3_keyword_search_text
from .telegram_listing_callback import handle_v3_listing_callback
from .telegram_service_handler import handle_v3_service_callback, handle_v3_service_text
from .telegram_start_handler import handle_v3_start
from .telegram_transition_action_handler import handle_v3_transition_action
from .telegram_transition_text_handler import handle_v3_transition_text
from .transition_runtime import UserBotTransitionRuntime, build_transition_runtime

logger = logging.getLogger(__name__)


PUBLIC_BOT_COMMANDS = (
    BotCommand("start", "打开侨联小管家"),
    BotCommand("find", "开始找房"),
    BotCommand("appointments", "我的预约"),
    BotCommand("service", "入住服务"),
    BotCommand("rental", "租后服务"),
    BotCommand("advisor", "中文顾问"),
)

ADMIN_BOT_COMMANDS = PUBLIC_BOT_COMMANDS + (
    BotCommand("admin", "管理后台"),
    BotCommand("contracts", "租客与合同"),
)

LEGACY_REPLY_TEXT_ROUTES = {
    "开始找房": "find_home",
    "帮我找房": "find_home",
    "精准筛选": "find_home",
    "我的预约": "appointments",
    "我的租约": "service",
    "联系顾问": "advisor",
    "联系我们": "advisor",
    "售后服务": "assurance",
    "服务保障": "assurance",
    "关于侨联": "assurance",
    "使用说明": "",
    "首页": "",
    "回到首页": "",
    "我的收藏": "",
    "预约看房": "__booking_guide__",
}

LEGACY_CALLBACK_ACTIONS = {
    "home": "root",
    "home_brand": "rental",
    "home_consult": "contact",
    "home_living": "service",
    "home_nearby": "service",
    "home_appoint": "search",
    "home_smart_search": "search",
    "keyword:handoff": "contact",
    "hub:advisor": "contact",
    "hub:appointments": "appointments",
    "hub:appoint": "search",
    "hub:contract": "service",
    "hub:favorites": "root",
    "hub:account": "root",
    "hub:find": "search",
    "hub:help": "root",
    "hub:latest": "root",
    "hub:precise": "search",
    "hub:promise": "rental",
    "hub:rental": "rental",
    "hub:service": "service",
    "appointment_menu:contact": "contact",
    "appointment_menu:list": "appointments",
    "service:checkin_tips": "rental",
    "service:promise": "rental",
}


def legacy_callback_action(raw: object) -> str:
    clean = str(raw or "").strip()
    if clean in LEGACY_CALLBACK_ACTIONS:
        return LEGACY_CALLBACK_ACTIONS[clean]
    if clean.startswith(("findmode:", "smart_")):
        return "search"
    if clean == "service:change":
        return "search"
    if clean.startswith("service:"):
        return "service"
    if clean.startswith("contract:"):
        return "service"
    if clean.startswith("appointment_menu:"):
        return "appointments"
    if clean.startswith("hub:"):
        return "root"
    return ""


async def remove_legacy_reply_keyboard(update: Any) -> None:
    message = getattr(update, "effective_message", None)
    if message is None:
        return
    cleanup = await message.reply_text("\u2063", reply_markup=ReplyKeyboardRemove())
    delete = getattr(cleanup, "delete", None)
    if callable(delete):
        try:
            await delete()
        except Exception:
            logger.debug("legacy reply keyboard cleanup message could not be deleted", exc_info=True)


@dataclass(frozen=True)
class V3UserBotConfig:
    repo_root: Path
    db_path: Path
    user_bot_token: str
    publisher_bot_token: str
    user_bot_username: str
    channel_url: str
    advisor_url: str
    admin_ids: tuple[int, ...]

    @classmethod
    def from_env(cls, repo_root: str | Path | None = None) -> "V3UserBotConfig":
        root = Path(repo_root or Path(__file__).resolve().parents[2]).expanduser().resolve()
        load_dotenv(root / ".env")
        data_dir = Path(os.getenv("DATA_DIR", root / "data")).expanduser().resolve()
        db_path = Path(os.getenv("DB_PATH", data_dir / "qiaolian_dual_bot.db")).expanduser().resolve()
        advisor = str(os.getenv("ADVISOR_TG") or os.getenv("SUPPORT_USERNAME") or "").strip()
        if advisor.startswith("@"):
            advisor = f"https://t.me/{advisor[1:]}"
        elif advisor and not advisor.startswith(("http://", "https://")):
            advisor = f"https://t.me/{advisor.lstrip('@')}"
        admin_ids = tuple(
            sorted(
                {
                    int(value.strip())
                    for value in str(os.getenv("ADMIN_IDS") or "").split(",")
                    if value.strip().isdigit() and int(value.strip()) > 0
                }
            )
        )
        return cls(
            repo_root=root,
            db_path=db_path,
            user_bot_token=str(os.getenv("USER_BOT_TOKEN") or "").strip(),
            publisher_bot_token=str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip(),
            user_bot_username=str(os.getenv("USER_BOT_USERNAME") or "qiaolian_rent_bot").strip().lstrip("@"),
            channel_url=str(os.getenv("CHANNEL_URL") or "").strip(),
            advisor_url=advisor,
            admin_ids=admin_ids,
        )

    def validate(self) -> None:
        if not self.user_bot_token:
            raise RuntimeError("USER_BOT_TOKEN is required")
        if not self.db_path.is_file():
            raise RuntimeError(f"V3 database does not exist: {self.db_path}")


@dataclass(frozen=True)
class V3UserBotDependencies:
    read: UserBotReadRuntime
    transition: UserBotTransitionRuntime
    admins: TelegramAdminNotifier
    contact_effects: ContactEffectExecutor
    listing_contact_effects: ListingContactEffectExecutor
    service_effects: ServiceEffectExecutor
    appointment_effects: AppointmentRuntimeEffectExecutor


def build_v3_user_bot_dependencies(config: V3UserBotConfig) -> V3UserBotDependencies:
    read = build_read_runtime(config.db_path)
    transition = build_transition_runtime(config.db_path)
    admins = TelegramAdminNotifier(config.admin_ids)
    contact_effects = ContactEffectExecutor(leads=transition.lead_effects, admins=admins)
    listing_contact_effects = ListingContactEffectExecutor(
        leads=transition.lead_effects,
        admins=admins,
    )
    service_effects = ServiceEffectExecutor(leads=transition.leads, admins=admins)
    appointment_effects = AppointmentRuntimeEffectExecutor(
        availability=AppointmentAvailabilityService(
            repository=SQLiteAppointmentRepository(config.db_path)
        ),
        channel=V3AppointmentChannelSynchronizer(
            config.db_path,
            publisher_bot_token=config.publisher_bot_token,
            user_bot_username=config.user_bot_username,
            advisor_url=config.advisor_url,
        ),
        admins=admins,
        inventory=read.inventory,
    )
    return V3UserBotDependencies(
        read=read,
        transition=transition,
        admins=admins,
        contact_effects=contact_effects,
        listing_contact_effects=listing_contact_effects,
        service_effects=service_effects,
        appointment_effects=appointment_effects,
    )


async def _render_home_callback(update: Any, config: V3UserBotConfig) -> None:
    query = update.callback_query
    await query.answer()
    home = build_home_view(channel_url=config.channel_url, advisor_url=config.advisor_url)
    message = getattr(query, "message", None)
    markup = build_home_keyboard(home)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=home.text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    else:
        await query.edit_message_text(
            home.text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )


def build_v3_user_bot_application(
    config: V3UserBotConfig,
    *,
    dependencies: V3UserBotDependencies | None = None,
    admin_command_ids: tuple[int, ...] | None = None,
    admin_authorizer: Callable[[int], bool] | None = None,
    admin_home_handler: Callable[[Any, Any], Awaitable[None]] | None = None,
    admin_query_handler: Callable[[Any, Any], Awaitable[None]] | None = None,
    admin_contract_command_handler: Callable[[Any, Any], Awaitable[Any]] | None = None,
    admin_contract_callback_handler: Callable[[Any, Any, Any, str, Any], Awaitable[Any]] | None = None,
    admin_contract_text_handler: Callable[[Any, Any], Awaitable[Any]] | None = None,
    admin_workflow_callback_handler: Callable[[Any, Any, Any, str, Any], Awaitable[Any]] | None = None,
) -> Application:
    config.validate()
    deps = dependencies or build_v3_user_bot_dependencies(config)
    admin_appointments = AdminAppointmentReader(config.db_path)
    runtime_state = RuntimeStateRepository(config.db_path)

    async def configure_command_menu(application: Application) -> None:
        public_scopes = (BotCommandScopeDefault(), BotCommandScopeAllPrivateChats())
        for scope in public_scopes:
            await application.bot.delete_my_commands(scope=scope)
            await application.bot.delete_my_commands(scope=scope, language_code="zh")
            await application.bot.set_my_commands(PUBLIC_BOT_COMMANDS, scope=scope)
            await application.bot.set_my_commands(PUBLIC_BOT_COMMANDS, scope=scope, language_code="zh")
        for admin_id in sorted(set(admin_command_ids or config.admin_ids)):
            scope = BotCommandScopeChat(chat_id=admin_id)
            await application.bot.delete_my_commands(scope=scope)
            await application.bot.delete_my_commands(scope=scope, language_code="zh")
            await application.bot.set_my_commands(ADMIN_BOT_COMMANDS, scope=scope)
            await application.bot.set_my_commands(ADMIN_BOT_COMMANDS, scope=scope, language_code="zh")
        runtime_state.heartbeat("user", state="running", event=True)

    app = (
        ApplicationBuilder()
        .token(config.user_bot_token)
        .post_init(configure_command_menu)
        .build()
    )

    def is_admin(update: Any) -> bool:
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)
        authorized = admin_authorizer or (lambda user_id: user_id in config.admin_ids)
        return bool(user and authorized(int(user.id)) and chat and chat.type == "private")

    async def admin(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        if not is_admin(update):
            return
        if admin_home_handler is not None:
            await admin_home_handler(update, context)
        else:
            await show_admin_home(update.effective_message)

    async def contracts(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        if not is_admin(update):
            return
        if admin_contract_command_handler is not None:
            await admin_contract_command_handler(update, context)

    async def admin_callbacks(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        query = getattr(update, "callback_query", None)
        if query is None:
            return
        if not is_admin(update):
            await query.answer("无管理员权限", show_alert=True)
            return
        raw = str(query.data or "")
        if raw in {"adminq:appointments", "adminq:pending"} or raw.startswith(
            "adminq:appointment:"
        ):
            await handle_admin_callback(
                update,
                admin_appointments,
                context=context,
                availability=deps.appointment_effects.availability,
                channel_sync=deps.appointment_effects.channel,
                advisor_url=config.advisor_url,
                channel_url=config.channel_url,
            )
            return
        if admin_query_handler is not None:
            await admin_query_handler(update, context)
        else:
            await handle_admin_callback(update, admin_appointments)

    async def legacy_admin_callbacks(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        query = getattr(update, "callback_query", None)
        user = getattr(update, "effective_user", None)
        if query is None or user is None:
            return
        if not is_admin(update):
            await query.answer("无管理员权限", show_alert=True)
            return
        raw = str(query.data or "")
        if raw.startswith("admincontract:") and admin_contract_callback_handler is not None:
            await admin_contract_callback_handler(update, context, query, raw, user)
            return
        if raw.startswith(("adminlead:", "adminrepair:")) and admin_workflow_callback_handler is not None:
            await admin_workflow_callback_handler(update, context, query, raw, user)

    async def _run_start(update, context, *, payload: str | None = None, preserve_args: bool = False):
        await remove_legacy_reply_keyboard(update)
        original_args = getattr(context, "args", None)
        if not preserve_args:
            context.args = [payload] if payload else []
        try:
            return await handle_v3_start(
                update,
                context,
                listings=deps.read.listings,
                transition_views=deps.transition.views,
                channel_url=config.channel_url,
                search_executor=deps.transition.searches,
                appointment_history=deps.transition.appointment_history,
                tenant_service=deps.transition.tenant_service,
                contact_effects=deps.contact_effects,
                advisor_url=config.advisor_url,
            )
        finally:
            if not preserve_args:
                context.args = original_args

    async def start(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, preserve_args=True)

    async def find(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="find_home")

    async def appointments(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="appointments")

    async def service(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="service")

    async def rental(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="assurance")

    async def advisor(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="advisor")

    async def about(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="assurance")

    async def contact(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context, payload="advisor")

    async def help_command(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await _run_start(update, context)

    async def callbacks(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if raw == "v3u:t:home":
            await _render_home_callback(update, config)
            return
        if raw.startswith("v3u:t:"):
            await handle_v3_transition_action(
                update,
                context,
                actions=deps.transition.callback_actions,
                views=deps.transition.views,
                appointment_executor=deps.transition.appointments,
                search_executor=deps.transition.searches,
                lead_effects=deps.transition.lead_effects,
                appointment_runtime_effects=deps.appointment_effects,
            )
            return
        if raw.startswith("v3u:home:"):
            await handle_v3_home_callback(
                update,
                context,
                appointment_history=deps.transition.appointment_history,
                search_views=deps.transition.views,
                contact_effects=deps.contact_effects,
                advisor_url=config.advisor_url,
            )
            return
        if raw.startswith("v3u:service:"):
            await handle_v3_service_callback(
                update,
                context,
                service=deps.transition.tenant_service,
                effects=deps.service_effects,
                advisor_url=config.advisor_url,
            )
            return
        if raw.startswith("v3u:assure:"):
            await handle_v3_assurance_callback(
                update,
                context,
                repo_root=config.repo_root,
                advisor_url=config.advisor_url,
            )
            return
        await handle_v3_listing_callback(
            update,
            context,
            router=deps.read.callbacks,
            inventory=deps.read.inventory,
            transition_views=deps.transition.views,
            contact_effects=deps.listing_contact_effects,
            advisor_url=config.advisor_url,
            channel_url=config.channel_url,
        )

    async def legacy_public_callbacks(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        action = legacy_callback_action(raw)
        if action == "root":
            await _render_home_callback(update, config)
            return
        if action:
            await handle_v3_home_action(
                update,
                context,
                action=action,
                appointment_history=deps.transition.appointment_history,
                search_views=deps.transition.views,
                contact_effects=deps.contact_effects,
                advisor_url=config.advisor_url,
            )
            return
        await handle_v3_listing_callback(
            update,
            context,
            router=deps.read.callbacks,
            inventory=deps.read.inventory,
            transition_views=deps.transition.views,
            contact_effects=deps.listing_contact_effects,
            advisor_url=config.advisor_url,
            channel_url=config.channel_url,
        )

    async def text(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        message = getattr(update, "effective_message", None)
        raw_text = str(getattr(message, "text", "") or "").strip()
        legacy_route = LEGACY_REPLY_TEXT_ROUTES.get(raw_text)
        if raw_text in LEGACY_REPLY_TEXT_ROUTES:
            if legacy_route == "__booking_guide__":
                await remove_legacy_reply_keyboard(update)
                view = build_booking_view(advisor_url=config.advisor_url)
                await message.reply_text(
                    view.text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_home_keyboard(view),
                )
            else:
                await _run_start(update, context, payload=legacy_route or None)
            return
        if is_admin(update) and admin_contract_text_handler is not None:
            admin_result = await admin_contract_text_handler(update, context)
            if admin_result is not None:
                return
        outcome = await handle_v3_transition_text(
            update,
            context,
            actions=deps.transition.text_actions,
            views=deps.transition.views,
            appointment_executor=deps.transition.appointments,
            search_executor=deps.transition.searches,
            lead_effects=deps.transition.lead_effects,
            appointment_runtime_effects=deps.appointment_effects,
        )
        if outcome.handled:
            return
        service = await handle_v3_service_text(
            update,
            context,
            service=deps.transition.tenant_service,
            effects=deps.service_effects,
            advisor_url=config.advisor_url,
        )
        if service.handled:
            return
        await handle_v3_keyword_search_text(
            update,
            context,
            actions=deps.transition.keyword_actions,
            search_executor=deps.transition.searches,
            lead_effects=deps.transition.lead_effects,
        )

    async def heartbeat(context):
        runtime_state.heartbeat("user", state="running")

    async def errors(update, context):
        runtime_state.heartbeat("user", state="running", error=f"{type(context.error).__name__}: {context.error}")
        logger.exception("V3 User Bot update failed", exc_info=context.error)

    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(CommandHandler("find", find), group=0)
    app.add_handler(CommandHandler("appointments", appointments), group=0)
    app.add_handler(CommandHandler("service", service), group=0)
    app.add_handler(CommandHandler("rental", rental), group=0)
    app.add_handler(CommandHandler("advisor", advisor), group=0)
    app.add_handler(CommandHandler("about", about), group=0)
    app.add_handler(CommandHandler("contact", contact), group=0)
    app.add_handler(CommandHandler("help", help_command), group=0)
    app.add_handler(CommandHandler("admin", admin), group=0)
    app.add_handler(CommandHandler("contracts", contracts), group=0)
    app.add_handler(CallbackQueryHandler(admin_callbacks, pattern=r"^adminq:"), group=0)
    app.add_handler(
        CallbackQueryHandler(
            legacy_admin_callbacks,
            pattern=r"^(?:admincontract|adminlead|adminrepair):",
        ),
        group=0,
    )
    app.add_handler(CallbackQueryHandler(callbacks, pattern=r"^v3u:"), group=0)
    app.add_handler(
        CallbackQueryHandler(
            legacy_public_callbacks,
            pattern=r"^(?:home(?:_|$)|hub:|keyword:handoff$|appointment_menu:|service:|contract:|listing:|find|smart_|repair|renewal|termination|change_home|change:|advisor:|adviser:|apdate:|aptime:)",
        ),
        group=0,
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text), group=0)
    app.add_error_handler(errors)
    if app.job_queue is not None:
        app.job_queue.run_repeating(heartbeat, interval=30, first=5, name="v3_user_runtime_heartbeat")
    return app


def run_v3_user_bot(
    config: V3UserBotConfig | None = None,
    **application_options: Any,
) -> None:
    resolved = config or V3UserBotConfig.from_env()
    application = build_v3_user_bot_application(resolved, **application_options)
    application.run_polling(allowed_updates=["message", "callback_query"])


__all__ = [
    "ADMIN_BOT_COMMANDS",
    "LEGACY_CALLBACK_ACTIONS",
    "LEGACY_REPLY_TEXT_ROUTES",
    "PUBLIC_BOT_COMMANDS",
    "V3UserBotConfig",
    "V3UserBotDependencies",
    "build_v3_user_bot_application",
    "build_v3_user_bot_dependencies",
    "legacy_callback_action",
    "remove_legacy_reply_keyboard",
    "run_v3_user_bot",
]
