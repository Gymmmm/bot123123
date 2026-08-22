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
from .callback_service import handle_service_callback, matches as matches_service
from .callback_listing import handle_listing_callback, matches as matches_listing


async def handle_ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, *, hooks: dict | None = None) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = update.effective_user
    hooks = hooks or {}
    (hooks.get('upsert_user_profile') or upsert_user_profile)(user)

    if data.startswith("listing:"):
        context.user_data.pop("appt", None)

    logger.info("[CALLBACK] user_id=%s data=%s", user.id, data)

    if matches_admin(data):
        result = await handle_admin_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_navigation(data):
        result = await handle_navigation_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_search(data):
        result = await handle_search_callback(update, context, query, data, user, hooks=hooks)
        return MAIN if result is None else result
    if matches_contract(data):
        result = await handle_contract_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_appointment(data):
        result = await handle_appointment_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_preference(data):
        result = await handle_preference_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_service(data):
        result = await handle_service_callback(update, context, query, data, user)
        return MAIN if result is None else result
    if matches_listing(data):
        result = await handle_listing_callback(update, context, query, data, user)
        return MAIN if result is None else result
    return MAIN
