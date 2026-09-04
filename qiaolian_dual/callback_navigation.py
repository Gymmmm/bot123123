"""Callback handlers for the navigation domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return data in {
        'home', 'home_smart_search', 'home_brand', 'home_appoint', 'home_consult', 'home_living', 'home_nearby',
        'smart_project', 'smart_movein', 'keyword:handoff',
        'hub:area', 'hub:budget', 'hub:layout', 'hub:latest', 'hub:available', 'hub:find', 'hub:appoint',
        'hub:video_tour', 'hub:advisor', 'hub:precise', 'hub:account', 'hub:favorites', 'hub:appointments',
        'hub:contract', 'hub:service', 'hub:promise', 'hub:help',
    } or data.startswith('resume:')


async def handle_navigation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _binding_contract_text, _contract_actions_keyboard
    from .appointments_view import _appointment_card_keyboard, list_favorites_text, list_recent_appointments
    from .flows import contact_management, show_appointment_hub, show_favorites, show_help, show_precise_filter, show_search_entry, show_service_hub, start_appointment
    from .keyboards_common import main_keyboard, no_match_followup_keyboard, room_type_keyboard
    from .keyboards_search import find_area_keyboard, find_budget_keyboard, local_life_keyboard
    from .listing import start_video_tour_flow
    from .results_admin import send_find_results_as_cards
    from .session_deeplink import clear_session_for_fresh_entry
    from .start_routes import route_start_arg
    from .texts import local_life_text, promise_text, render_panel, welcome_text

    if data == 'home':
        clear_session_for_fresh_entry(context)
        context.user_data.pop('resume_start_arg', None)
        context.user_data.pop('contact_listing_id', None)
        context.user_data.pop('contact_touch_payload', None)
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN

    if data in {'home_smart_search', 'hub:find'}:
        return await show_search_entry(update, context)

    # 历史首页入口兼容：不再生成品牌故事/旧找房助手页面。
    if data == 'home_brand':
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN
    if data in {'home_appoint', 'hub:appoint'}:
        return await show_appointment_hub(update, context)
    if data in {'home_consult', 'keyword:handoff', 'hub:advisor'}:
        return await contact_management(update, context, source='hub')
    if data in {'home_living', 'hub:service'}:
        return await show_service_hub(update, context)
    if data == 'home_nearby':
        await render_panel(update, text=local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard(), context=context)
        return MAIN

    if data == 'smart_project':
        context.user_data['awaiting_keyword_find'] = {'source': 'smart_project'}
        await render_panel(update, text='🏢 <b>按楼盘找</b>\n\n直接发楼盘名，也可以带上预算和户型。\n例如：<code>富力城 一房 800以内</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回找房', callback_data='home_smart_search')]]), context=context)
        return MAIN
    if data == 'smart_movein':
        context.user_data['awaiting_keyword_find'] = {'source': 'smart_movein'}
        await render_panel(update, text='📅 <b>按入住时间找</b>\n\n直接发入住时间，也可以带上区域和预算。\n例如：<code>下月初 BKK1 700以内</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回找房', callback_data='home_smart_search')]]), context=context)
        return MAIN

    if data == 'hub:area':
        context.user_data['search_pref'] = {'source': 'home_area', 'goal': 'any', 'touch_payload': {}}
        await render_panel(update, text='📍 <b>想住哪里？</b>\n\n选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard(), context=context)
        return FIND_AREA
    if data == 'hub:budget':
        context.user_data['search_pref'] = {'source': 'home_budget', 'goal': 'any', 'area': '', 'touch_payload': {}}
        await render_panel(update, text='💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'), context=context)
        return FIND_BUDGET
    if data == 'hub:layout':
        await render_panel(update, text='🛏 <b>想要什么户型？</b>', parse_mode=ParseMode.HTML, reply_markup=room_type_keyboard(), context=context)
        return MAIN

    if data in {'hub:available', 'hub:latest'}:
        matches_found = [item for item in db.list_recent_listings(10) if str(item.get('status') or '').strip().lower() in {'active', 'reserved'}]
        if matches_found:
            await send_find_results_as_cards(update, context, matches_found, 'strict')
        else:
            await render_panel(update, text='暂时没有合适、可以安排看房的房源。\n可以调整条件，或联系中文顾问继续留意。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)
        return MAIN

    if data == 'hub:video_tour':
        return await start_video_tour_flow(update, context, source='home_video_button')
    if data == 'hub:precise':
        return await show_precise_filter(update, context)
    if data == 'hub:account':
        await render_panel(update, text=list_favorites_text(user.id) + '\n\n' + list_recent_appointments(user.id), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context)
        return MAIN
    if data == 'hub:favorites':
        return await show_favorites(update, context)
    if data == 'hub:appointments':
        await render_panel(update, text=list_recent_appointments(user.id), parse_mode=ParseMode.HTML, reply_markup=_appointment_card_keyboard(), context=context)
        return MAIN
    if data == 'hub:contract':
        binding = db.get_active_binding(user.id)
        await render_panel(update, text=_binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id), context=context)
        return MAIN
    if data == 'hub:promise':
        await render_panel(update, text=promise_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context)
        return MAIN
    if data == 'hub:help':
        return await show_help(update, context)

    if data.startswith('resume:'):
        action = data.split(':', 1)[1]
        resume_arg = str(context.user_data.get('resume_start_arg') or '').strip()
        if action == 'continue':
            appt = context.user_data.get('appt') or {}
            listing_id = str(appt.get('listing_id') or context.user_data.get('contact_listing_id') or '').strip()
            if appt and listing_id:
                return await start_appointment(update, context, listing_id, source=str(appt.get('source') or 'channel_deeplink'), touch_payload=appt.get('touch_payload') or {}, initial_mode=str(appt.get('mode') or ''))
            await render_panel(update, text='当前没有可恢复的流程，已返回首页。', reply_markup=main_keyboard(), context=context)
            return MAIN
        if action == 'restart' and resume_arg:
            clear_session_for_fresh_entry(context)
            context.user_data.pop('resume_start_arg', None)
            state = await route_start_arg(update, context, resume_arg)
            if state is not None:
                return state
            await render_panel(update, text='这个房源入口已经失效。\n\n可以返回找房，或联系中文顾问确认。', reply_markup=no_match_followup_keyboard(), context=context)
            return MAIN
    return None
