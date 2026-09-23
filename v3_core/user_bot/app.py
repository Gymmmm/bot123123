"""Independent python-telegram-bot Application wiring for the V3 User Bot.

Nothing in this module initializes schema or changes production services. It
only composes V3-owned handlers against an already initialized additive V3 DB.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import time as dt_time
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

from dotenv import load_dotenv
from telegram import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat, ReplyKeyboardRemove
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
from .home_views import build_home_view
from .listing_contact import ListingContactEffectExecutor
from .legacy_routes import legacy_home_action, legacy_reply_text_action, legacy_start_payload
from .runtime import UserBotReadRuntime, build_read_runtime
from .service_effects import ServiceEffectExecutor
from .telegram_assurance_handler import handle_v3_assurance_callback
from .telegram_home_handler import handle_v3_home_callback
from .telegram_home_ui import build_home_keyboard
from .telegram_keyword_search_handler import handle_v3_keyword_search_text
from .telegram_listing_callback import handle_v3_listing_callback
from .telegram_service_handler import handle_v3_service_callback, handle_v3_service_media, handle_v3_service_text
from .telegram_start_handler import handle_v3_start
from .telegram_transition_action_handler import handle_v3_transition_action
from .telegram_transition_text_handler import handle_v3_transition_text
from .transition_runtime import UserBotTransitionRuntime, build_transition_runtime
from .takeover_runtime import build_takeover_transition_runtime
from .takeover_storage import ProductionAdminAppointmentReader, ProductionAppointmentRepository

logger = logging.getLogger(__name__)


_REPLY_KEYBOARD_REMOVED_KEY = "v3_reply_keyboard_removed"


def public_command_menu() -> tuple[BotCommand, ...]:
    return (
        BotCommand("start", "回到首页"),
        BotCommand("find", "开始找房"),
        BotCommand("appointments", "我的预约"),
        BotCommand("service", "侨联服务"),
    )


def admin_command_menu() -> tuple[BotCommand, ...]:
    return public_command_menu() + (
        BotCommand("admin", "管理后台"),
        BotCommand("contracts", "租客与合同"),
    )


async def remove_legacy_reply_keyboard(update: Any, context: Any) -> bool:
    """Remove the retired persistent reply keyboard once per active user session."""
    user_data = getattr(context, "user_data", None)
    if isinstance(user_data, dict) and user_data.get(_REPLY_KEYBOARD_REMOVED_KEY):
        return False
    message = getattr(update, "effective_message", None)
    if message is None or not callable(getattr(message, "reply_text", None)):
        return False
    notice = await message.reply_text(
        "菜单已更新",
        reply_markup=ReplyKeyboardRemove(),
        disable_notification=True,
    )
    if isinstance(user_data, dict):
        user_data[_REPLY_KEYBOARD_REMOVED_KEY] = True
    delete = getattr(notice, "delete", None)
    if callable(delete):
        try:
            await delete()
        except Exception:
            logger.debug("Could not delete reply-keyboard removal notice", exc_info=True)
    return True



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


def build_takeover_user_bot_dependencies(
    config: V3UserBotConfig,
    *,
    repair_reply_markup_factory: Callable[[int], Any] | None = None,
) -> V3UserBotDependencies:
    """Use V3 UX with the live 3858 appointment/lead/tenant data boundaries."""
    read = build_read_runtime(config.db_path)
    transition = build_takeover_transition_runtime(config.db_path)
    admins = TelegramAdminNotifier(config.admin_ids)
    contact_effects = ContactEffectExecutor(leads=transition.lead_effects, admins=admins)
    listing_contact_effects = ListingContactEffectExecutor(
        leads=transition.lead_effects,
        admins=admins,
    )
    service_effects = ServiceEffectExecutor(
        leads=transition.leads,
        admins=admins,
        repair_reply_markup_factory=repair_reply_markup_factory,
    )
    appointment_effects = AppointmentRuntimeEffectExecutor(
        availability=AppointmentAvailabilityService(
            repository=ProductionAppointmentRepository(config.db_path)
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
    lease_reminder_handler: Callable[[Any], Awaitable[Any]] | None = None,
    compat_command_handlers: dict[str, Callable[[Any, Any], Awaitable[Any]]] | None = None,
    admin_appointment_reader: AdminAppointmentReader | None = None,
    compat_start_handler: Callable[[Any, Any, str], Awaitable[Any]] | None = None,
    compat_callback_handler: Callable[[Any, Any], Awaitable[Any]] | None = None,
) -> Application:
    config.validate()
    deps = dependencies or build_v3_user_bot_dependencies(config)
    admin_appointments = admin_appointment_reader or AdminAppointmentReader(config.db_path)
    runtime_state = RuntimeStateRepository(config.db_path)

    async def configure_command_menu(application: Application) -> None:
        public_commands = list(public_command_menu())
        private_scope = BotCommandScopeAllPrivateChats()
        await application.bot.set_my_commands(public_commands)
        await application.bot.set_my_commands(public_commands, language_code="zh")
        await application.bot.set_my_commands(public_commands, scope=private_scope)
        await application.bot.set_my_commands(public_commands, scope=private_scope, language_code="zh")
        admin_commands = list(admin_command_menu())
        for admin_id in sorted(set(admin_command_ids or config.admin_ids)):
            scope = BotCommandScopeChat(chat_id=admin_id)
            await application.bot.set_my_commands(admin_commands, scope=scope)
            await application.bot.set_my_commands(admin_commands, scope=scope, language_code="zh")
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

    async def _run_start_payload(update, context, payload: str | None = None):
        previous_args = tuple(getattr(context, "args", None) or ())
        if payload is not None:
            context.args = [payload] if payload else []
        try:
            active_args = tuple(getattr(context, "args", None) or ())
            active_payload = str(active_args[0] or "").strip() if active_args else ""
            legacy_only = active_payload.startswith(
                ("t_bind_", "detail__", "photos__", "book__", "ch_", "l_")
            )
            if legacy_only and compat_start_handler is not None:
                handled_state = await compat_start_handler(update, context, active_payload)
                if handled_state is not None:
                    return
            await handle_v3_start(
                update,
                context,
                listings=deps.read.listings,
                transition_views=deps.transition.views,
                channel_url=config.channel_url,
                search_executor=deps.transition.searches,
                appointment_history=deps.transition.appointment_history,
                tenant_service=deps.transition.tenant_service,
                contact_effects=deps.contact_effects,
                listing_contact_effects=deps.listing_contact_effects,
                advisor_url=config.advisor_url,
            )
        finally:
            context.args = list(previous_args)

    async def start(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context)

    async def find(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "find_home")

    async def appointments(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "appointments")

    async def service(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "service")

    async def legacy_about(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "assurance")

    async def legacy_contact(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "advisor")

    async def legacy_help(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        await _run_start_payload(update, context, "")

    async def _legacy_callback(update, context, raw: str) -> bool:
        action = legacy_home_action(raw)
        if action is None:
            return False
        if action == "home":
            await _render_home_callback(update, config)
            return True
        await handle_v3_home_callback(
            update,
            context,
            appointment_history=deps.transition.appointment_history,
            search_views=deps.transition.views,
            contact_effects=deps.contact_effects,
            advisor_url=config.advisor_url,
            action_override=action,
        )
        return True

    async def callbacks(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if await _legacy_callback(update, context, raw):
            return
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
        legacy_prefixes = (
            "hub:", "service:", "contract:", "appointment_menu:", "apdate:",
            "aptime:", "apfocus:", "findarea:", "findbudget:", "findmode:",
            "repair", "renewal", "termination", "change_home", "change:",
            "advisor:", "adviser:",
        )
        if compat_callback_handler is not None and raw.startswith(legacy_prefixes):
            await compat_callback_handler(update, context)
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
        await remove_legacy_reply_keyboard(update, context)
        message = getattr(update, "effective_message", None)
        legacy_action = legacy_reply_text_action(getattr(message, "text", "") if message is not None else "")
        if legacy_action is not None:
            await _run_start_payload(update, context, legacy_start_payload(legacy_action))
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

    async def media(update, context):
        runtime_state.heartbeat("user", state="running", event=True)
        await remove_legacy_reply_keyboard(update, context)
        service = await handle_v3_service_media(
            update,
            context,
            service=deps.transition.tenant_service,
            effects=deps.service_effects,
            advisor_url=config.advisor_url,
        )
        if service.handled:
            return

    async def heartbeat(context):
        runtime_state.heartbeat("user", state="running")

    async def errors(update, context):
        runtime_state.heartbeat("user", state="running", error=f"{type(context.error).__name__}: {context.error}")
        logger.exception("V3 User Bot update failed", exc_info=context.error)

    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(CommandHandler("find", find), group=0)
    app.add_handler(CommandHandler("appointments", appointments), group=0)
    app.add_handler(CommandHandler("service", service), group=0)
    app.add_handler(CommandHandler("about", legacy_about), group=0)
    app.add_handler(CommandHandler("contact", legacy_contact), group=0)
    app.add_handler(CommandHandler("help", legacy_help), group=0)
    app.add_handler(CommandHandler("admin", admin), group=0)
    app.add_handler(CommandHandler("contracts", contracts), group=0)
    for command, handler in sorted((compat_command_handlers or {}).items()):
        clean = str(command or "").strip().lstrip("/")
        if clean and callable(handler):
            app.add_handler(CommandHandler(clean, handler), group=0)
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
            callbacks,
            pattern=r"^(?:hub:|listing:|service:|contract:|appointment_menu:|apdate:|aptime:|find|repair|renewal|termination|change_home|change:|advisor:|adviser:|home$|home_brand$|home_smart_search$|home_appoint$|home_consult$|home_living$|home_nearby$|menu_about$|menu_human$|menu_service$)",
        ),
        group=0,
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text), group=0)
    app.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & ~filters.COMMAND, media), group=0)
    app.add_error_handler(errors)
    if app.job_queue is not None:
        app.job_queue.run_repeating(heartbeat, interval=30, first=5, name="v3_user_runtime_heartbeat")
        if lease_reminder_handler is not None:
            app.job_queue.run_daily(
                lease_reminder_handler,
                time=dt_time(hour=9, minute=5),
                name="lease_reminder_job",
            )
    elif lease_reminder_handler is not None:
        logger.warning("job_queue unavailable: lease reminder job not started")
    return app


def run_v3_user_bot(
    config: V3UserBotConfig | None = None,
    **application_options: Any,
) -> None:
    resolved = config or V3UserBotConfig.from_env()
    application = build_v3_user_bot_application(resolved, **application_options)
    application.run_polling(allowed_updates=["message", "callback_query"])


__all__ = [
    "V3UserBotConfig",
    "admin_command_menu",
    "public_command_menu",
    "remove_legacy_reply_keyboard",
    "V3UserBotDependencies",
    "build_takeover_user_bot_dependencies",
    "build_v3_user_bot_application",
    "build_v3_user_bot_dependencies",
    "run_v3_user_bot",
]
