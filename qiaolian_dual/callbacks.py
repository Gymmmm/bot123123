"""Central callback dispatcher.

Domain logic lives in callback_* modules. Keep cross-cutting preprocessing here.
"""
from __future__ import annotations

from .common import *
from .search import upsert_user_profile
from .callback_admin import handle_admin_callback, matches as matches_admin
from .callback_navigation import handle_navigation_callback, matches as matches_navigation
from .callback_search import handle_search_callback, matches as matches_search
from .callback_contract import handle_contract_callback, matches as matches_contract
from .callback_appointment import handle_appointment_callback, matches as matches_appointment
from .callback_preference import handle_preference_callback, matches as matches_preference
from .callback_rental import handle_rental_callback, matches as matches_rental
from .callback_service import handle_service_callback, matches as matches_service
from .callback_listing import handle_listing_callback, matches as matches_listing
from .admin_contract_ui import handle_callback as handle_admin_contract_callback, matches as matches_admin_contract


async def handle_ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, *, hooks: dict | None = None) -> int:
    query = update.callback_query
    data = query.data or ""
    user = update.effective_user
    hooks = hooks or {}
    try:
        (hooks.get('upsert_user_profile') or upsert_user_profile)(user)
        if data.startswith("listing:appoint:"):
            context.user_data.pop("appt", None)
        logger.info("[CALLBACK] user_id=%s data=%s", user.id, data)

        routes = (
            (matches_admin_contract, handle_admin_contract_callback),
            (matches_admin, handle_admin_callback),
            (matches_navigation, handle_navigation_callback),
            (matches_search, None),
            (matches_contract, handle_contract_callback),
            (matches_appointment, handle_appointment_callback),
            (matches_preference, handle_preference_callback),
            # Rental service comes before legacy service so old
            # service:handover/service:deposit buttons resolve to the new flow.
            (matches_rental, handle_rental_callback),
            (matches_service, handle_service_callback),
            (matches_listing, handle_listing_callback),
        )
        for matcher, handler in routes:
            if not matcher(data):
                continue
            if handler is None:
                result = await handle_search_callback(update, context, query, data, user, hooks=hooks)
            else:
                result = await handler(update, context, query, data, user)
            return MAIN if result is None else result

        logger.warning("[CALLBACK] unhandled user_id=%s data=%s", user.id, data)
        text = "这个操作已失效，请返回首页继续。"
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回首页", callback_data="home")]])
        try:
            await query.edit_message_text(text, reply_markup=reply_markup)
        except Exception:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup)
        return MAIN
    finally:
        await answer_callback_once(query)
