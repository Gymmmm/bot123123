"""Outer Telegram orchestration for V3 listing/card callbacks."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .callback_router import CallbackRouter
from .callbacks import encode_listing_callback
from .lead_service import LeadUser
from .listing_contact import ListingContactEffectExecutor, ListingContactEffectResult, build_listing_contact_view
from .public_inventory import PublicInventoryReader
from .telegram_callback_handler import TelegramCallbackHandlerOutcome, handle_v3_callback
from .telegram_edit import edit_query_panel
from .telegram_navigation import advisor_handoff_url
from .transition_views import TransitionViewService


@dataclass(frozen=True)
class TelegramListingCallbackOutcome:
    handled: bool
    callback: TelegramCallbackHandlerOutcome | None = None
    contact_effect: ListingContactEffectResult | None = None


def _lead_user(update: Any) -> LeadUser:
    user = getattr(update, "effective_user", None)
    if user is None or getattr(user, "id", None) is None:
        raise ValueError("telegram_effective_user_missing_for_listing_contact")
    display_name = str(getattr(user, "full_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(
            value for value in (
                str(getattr(user, "first_name", "") or "").strip(),
                str(getattr(user, "last_name", "") or "").strip(),
            ) if value
        )
    return LeadUser(
        user_id=int(user.id),
        username=str(getattr(user, "username", "") or ""),
        display_name=display_name,
    )


async def _render_contact(query: Any, *, text: str, public_listing_id: str, advisor_url: str) -> None:
    direct_advisor = advisor_handoff_url(advisor_url, public_listing_id=public_listing_id)
    contact_button = (
        InlineKeyboardButton("💬 中文顾问", url=direct_advisor)
        if direct_advisor else InlineKeyboardButton("💬 中文顾问", callback_data="v3u:home:contact")
    )
    markup = InlineKeyboardMarkup([
        [contact_button],
        [InlineKeyboardButton("⬅️ 返回房源", callback_data=encode_listing_callback("details", public_listing_id))],
    ])
    await edit_query_panel(
        query,
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


def _is_historical_callback(raw: str) -> bool:
    clean = str(raw or "").strip().lower()
    prefixes = (
        "hub:", "listing:", "service:", "contract:", "appointment_menu:",
        "apdate:", "aptime:", "find", "repair", "renewal", "termination",
        "change_home", "change:", "advisor:", "adviser:",
    )
    return any(clean.startswith(prefix) for prefix in prefixes)


async def _render_updated_entry(query: Any, *, advisor_url: str = "") -> None:
    direct = advisor_handoff_url(advisor_url)
    advisor_button = (
        InlineKeyboardButton("💬 中文顾问", url=direct)
        if direct else InlineKeyboardButton("💬 中文顾问", callback_data="v3u:home:contact")
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ 返回首页", callback_data="v3u:t:home")],
        [advisor_button],
    ])
    text = "⚠️ <b>这套房的信息已经更新</b>\n\n请使用下面的最新入口。"
    await edit_query_panel(query, text=text, reply_markup=markup)


async def handle_v3_listing_callback(
    update: Any,
    context: Any,
    *,
    router: CallbackRouter,
    inventory: PublicInventoryReader,
    transition_views: TransitionViewService,
    contact_effects: ListingContactEffectExecutor,
    advisor_url: str = "",
    channel_url: str = "",
) -> TelegramListingCallbackOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    if query is not None and _is_historical_callback(raw):
        await query.answer()
        await _render_updated_entry(query, advisor_url=advisor_url)
        return TelegramListingCallbackOutcome(handled=True)

    outcome = await handle_v3_callback(
        update,
        context,
        router=router,
        transition_views=transition_views,
        advisor_url=advisor_url,
        channel_url=channel_url,
    )
    if not outcome.handled:
        return TelegramListingCallbackOutcome(handled=False, callback=outcome)

    response = outcome.response
    if response is None or response.kind != "transition" or response.transition != "consult" or response.consult_intent is None:
        return TelegramListingCallbackOutcome(handled=True, callback=outcome)

    effect = await contact_effects.execute(
        bot=getattr(context, "bot", None),
        user=_lead_user(update),
        intent=response.consult_intent,
    )
    view = build_listing_contact_view(response.consult_intent, inventory, advisor_url=advisor_url)
    await _render_contact(
        update.callback_query,
        text=view.text,
        public_listing_id=view.public_listing_id,
        advisor_url=view.advisor_url,
    )
    return TelegramListingCallbackOutcome(handled=True, callback=outcome, contact_effect=effect)


__all__ = ["TelegramListingCallbackOutcome", "handle_v3_listing_callback"]