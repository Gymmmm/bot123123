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
from .admin_contract_ui import handle_callback as handle_admin_contract_callback, matches as matches_admin_contract


async def handle_ui_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, *, hooks: dict | None = None) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = update.effective_user
    hooks = hooks or {}
    (hooks.get('upsert_user_profile') or upsert_user_profile)(user)

    # 只有明确重新发起“预约看房”时才丢弃旧预约草稿；查看详情、相册或咨询
    # 不应打断用户正在填写的预约。
    if data.startswith("listing:appoint:"):
        context.user_data.pop("appt", None)

    logger.info("[CALLBACK] user_id=%s data=%s", user.id, data)

    if matches_admin_contract(data):
        result = await handle_admin_contract_callback(update, context, query, data, user)
        return MAIN if result is None else result

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

    # 旧版本遗留按钮、已失效的消息或未来漏登记的回调都不能静默失败。
    logger.warning("[CALLBACK] unhandled user_id=%s data=%s", user.id, data)
    text = "这个操作已失效，请返回首页继续。"
    reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 返回首页", callback_data="home")]])
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup)
    return MAIN
