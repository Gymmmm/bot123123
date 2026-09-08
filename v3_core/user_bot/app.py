"""Independent python-telegram-bot Application wiring for the V3 User Bot.

Nothing in this module initializes schema or changes production services.  It
only composes V3-owned handlers against an already initialized additive V3 DB.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from v3_core.storage.appointment_repository import SQLiteAppointmentRepository

from .admin_notifications import TelegramAdminNotifier
from .appointment_availability import AppointmentAvailabilityService
from .appointment_runtime_effects import AppointmentRuntimeEffectExecutor
from .channel_status_sync import V3AppointmentChannelSynchronizer
from .contact_effects import ContactEffectExecutor
from .home_views import build_home_view
from .listing_contact import ListingContactEffectExecutor
from .runtime import UserBotReadRuntime, build_read_runtime
from .service_effects import ServiceEffectExecutor
from .telegram_assurance_handler import handle_v3_assurance_callback
from .telegram_home_handler import handle_v3_home_callback
from .telegram_home_ui import build_home_keyboard
from .telegram_keyword_search_handler import handle_v3_keyword_search_text
from .telegram_listing_callback import handle_v3_listing_callback
from .telegram_service_handler import handle_v3_service_callback, handle_v3_service_text
from .telegram_start_handler import handle_v3_start
from .telegram_transition_action_handler import handle_v3_transition_action
from .telegram_transition_text_handler import handle_v3_transition_text
from .transition_runtime import UserBotTransitionRuntime, build_transition_runtime

logger = logging.getLogger(__name__)


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
    home = build_home_view(channel_url=config.channel_url)
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
) -> Application:
    config.validate()
    deps = dependencies or build_v3_user_bot_dependencies(config)
    app = ApplicationBuilder().token(config.user_bot_token).build()

    async def start(update, context):
        await handle_v3_start(
            update,
            context,
            listings=deps.read.listings,
            transition_views=deps.transition.views,
            channel_url=config.channel_url,
        )

    async def callbacks(update, context):
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
            )
            return
        if raw.startswith("v3u:assure:"):
            await handle_v3_assurance_callback(
                update,
                context,
                repo_root=config.repo_root,
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
        )

    async def text(update, context):
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

    async def errors(update, context):
        logger.exception("V3 User Bot update failed", exc_info=context.error)

    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(CallbackQueryHandler(callbacks, pattern=r"^v3u:"), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text), group=0)
    app.add_error_handler(errors)
    return app


def run_v3_user_bot(config: V3UserBotConfig | None = None) -> None:
    resolved = config or V3UserBotConfig.from_env()
    application = build_v3_user_bot_application(resolved)
    application.run_polling(allowed_updates=["message", "callback_query"])


__all__ = [
    "V3UserBotConfig",
    "V3UserBotDependencies",
    "build_v3_user_bot_application",
    "build_v3_user_bot_dependencies",
    "run_v3_user_bot",
]
