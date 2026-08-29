"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *
from telegram.request import HTTPXRequest

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_common import main_keyboard
    from .session_deeplink import clear_session_for_fresh_entry
    clear_session_for_fresh_entry(context)
    await update.effective_message.reply_text('❌ 已取消当前操作。', reply_markup=main_keyboard())
    return MAIN

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error_text = str(context.error or '')
    lower = error_text.lower()
    if any((token in lower for token in ('datainvaliderror', 'query is too old', 'query id is invalid', 'message is not modified', 'message to edit not found'))):
        logger.warning('忽略已过期或重复的 Telegram callback/message 状态：%s', error_text)
        query = getattr(update, 'callback_query', None)
        if query is not None:
            try:
                await query.answer('这个按钮已失效，请重新进入预约流程。', show_alert=True)
            except Exception:
                pass
        return
    logger.exception('user_bot handler error: %s', context.error)

def build_application(*, token: str | None = None) -> Application:
    from .admin_contract_ui import cmd_contracts
    from .admin_commands import cmd_deal_done, cmd_lead_response, cmd_push_all, cmd_push_local, cmd_repair_update
    from .admin_contract import _all_user_admin_ids
    from .appointment_flow import appoint_flow_cb, handle_appointment_text
    from .callbacks import handle_ui_callback
    from .jobs import lease_reminder_job, rent_day_reminder_job
    from .message_handlers import cmd_about, cmd_admin_add, cmd_admin_list, cmd_admin_remove, cmd_appointments, cmd_contact, cmd_favorites, cmd_find, cmd_help, cmd_search, cmd_service, handle_find_area, handle_find_budget, handle_main_message
    from .start_routes import start
    active_token = USER_BOT_TOKEN if token is None else str(token)
    if not active_token.strip():
        raise RuntimeError('USER_BOT_TOKEN 未配置：请先复制 .env.example 为 .env，并填写 BotFather Token')
    # Do not inherit an unrelated host-wide HTTP(S)/SOCKS proxy by accident.
    # Operators that need a Telegram proxy must configure it explicitly in the
    # application; this also keeps offline bootstrap and handler inspection free
    # from optional socksio dependencies.
    request = HTTPXRequest(httpx_kwargs={'trust_env': False})
    updates_request = HTTPXRequest(httpx_kwargs={'trust_env': False})
    app = (
        Application.builder()
        .token(active_token)
        .request(request)
        .get_updates_request(updates_request)
        .build()
    )
    conv_handler = ConversationHandler(entry_points=[CommandHandler('start', start), CommandHandler('search', cmd_search), CommandHandler('about', cmd_about), CommandHandler('find', cmd_find), CommandHandler('favorites', cmd_favorites), CommandHandler('appointments', cmd_appointments), CommandHandler('help', cmd_help), CommandHandler('service', cmd_service), CommandHandler('contact', cmd_contact), CommandHandler('admin_list', cmd_admin_list), CommandHandler('admin_add', cmd_admin_add), CommandHandler('admin_remove', cmd_admin_remove), CommandHandler('deal_done', cmd_deal_done), CommandHandler('lead_response', cmd_lead_response), CommandHandler('repair_update', cmd_repair_update), CommandHandler('push_local', cmd_push_local), CommandHandler('push_all', cmd_push_all), CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN)], states={MAIN: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_message)], FIND_AREA: [CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_find_area)], FIND_BUDGET: [CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_find_budget)], APPT_MODE: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN)], APPT_FOCUS: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN)], APPT_DATE: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_appointment_text)], APPT_TIME: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_appointment_text)], APPT_CONFIRM: [CallbackQueryHandler(appoint_flow_cb, pattern=_APPT_CB_PATTERN), CallbackQueryHandler(handle_ui_callback, pattern=_MAIN_CB_PATTERN), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_appointment_text)]}, fallbacks=[CommandHandler('cancel', cancel), CommandHandler('start', start), CommandHandler('search', cmd_search), CommandHandler('about', cmd_about), CommandHandler('find', cmd_find), CommandHandler('favorites', cmd_favorites), CommandHandler('appointments', cmd_appointments), CommandHandler('help', cmd_help), CommandHandler('service', cmd_service), CommandHandler('contact', cmd_contact), CommandHandler('admin_list', cmd_admin_list), CommandHandler('admin_add', cmd_admin_add), CommandHandler('admin_remove', cmd_admin_remove), CommandHandler('deal_done', cmd_deal_done), CommandHandler('lead_response', cmd_lead_response), CommandHandler('repair_update', cmd_repair_update), CommandHandler('push_local', cmd_push_local), CommandHandler('push_all', cmd_push_all)], allow_reentry=True)
    app.add_handler(CommandHandler('contracts', cmd_contracts), group=-1)
    app.add_handler(conv_handler)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_message))
    logger.info('全局兜底 MessageHandler 已注册 (group=0)')
    app.add_error_handler(error_handler)

    async def post_init(application: Application) -> None:
        from telegram import BotCommand, BotCommandScopeChat
        user_commands = [BotCommand('start', '回到首页'), BotCommand('find', '智能找房'), BotCommand('appointments', '我的预约'), BotCommand('service', '侨联服务'), BotCommand('contact', '联系顾问'), BotCommand('help', '使用帮助'), BotCommand('about', '关于侨联')]
        await application.bot.set_my_commands(user_commands)
        admin_commands = user_commands + [BotCommand('contracts', '租客与合同'), BotCommand('admin_list', '管理员管理')]
        for admin_id in sorted(_all_user_admin_ids()):
            scope = BotCommandScopeChat(chat_id=admin_id)
            await application.bot.set_my_commands(admin_commands, scope=scope)
            await application.bot.set_my_commands(admin_commands, scope=scope, language_code='zh')
    app.post_init = post_init
    if app.job_queue is not None:
        app.job_queue.run_daily(lease_reminder_job, time=dt_time(hour=9, minute=5), name='lease_reminder_job')
        app.job_queue.run_daily(rent_day_reminder_job, time=dt_time(hour=9, minute=10), name='rent_day_reminder_job')
    else:
        logger.warning('job_queue 不可用：租约到期提醒任务未启动')
    return app

def main() -> None:
    import asyncio
    import sys
    if sys.version_info >= (3, 10):
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
    app = build_application()
    logger.info('用户服务 Bot 启动中')
    app.run_polling(drop_pending_updates=True)
