"""Callback handlers for the navigation domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'home') or (data == 'home_smart_search') or (data == 'home_brand') or (data == 'home_appoint') or (data == 'home_consult') or (data == 'home_living') or (data == 'home_nearby') or (data == 'smart_project') or (data == 'smart_movein') or (data == 'keyword:handoff') or (data == 'hub:area') or (data == 'hub:budget') or (data == 'hub:layout') or (data == 'hub:latest') or (data == 'hub:find') or (data == 'hub:appoint') or (data == 'hub:video_tour') or (data == 'hub:advisor') or (data == 'hub:precise') or (data == 'hub:account') or (data == 'hub:favorites') or (data == 'hub:appointments') or (data == 'hub:contract') or (data == 'hub:service') or (data == 'hub:promise') or (data == 'hub:help') or (data.startswith('resume:'))


async def handle_navigation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _binding_contract_text, _binding_days_left, _binding_end_date, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .appointments_view import _appointment_card_keyboard, _appointment_date_compact, _appointment_details_keyboard, _appointment_listing_compact, _appointment_time_compact, _find_user_appointment, appointment_details_text, list_favorites_text, list_recent_appointments, old_tenant_binding_text
    from .flows import contact_management, show_appointment_hub, show_favorites, show_help, show_precise_filter, show_search_entry, show_service_hub, start_appointment
    from .keyboards_common import _advisor_listing_url, _listing_channel_url, contact_handoff_keyboard, keyword_followup_keyboard, latest_listing_keyboard, lead_capture_keyboard, main_keyboard, no_match_followup_keyboard, old_tenant_followup_keyboard, room_type_keyboard
    from .keyboards_search import _decode_budget_choice, find_area_keyboard, find_budget_keyboard, guided_search_keyboard, local_life_keyboard, merchant_join_keyboard, precise_filter_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_detail_keyboard, service_hub_keyboard, service_repair_keyboard
    from .listing import _latest_listing_text, listing_context, listing_unavailable_keyboard, start_video_tour_flow
    from .results_admin import _allow_admin_notify, _format_listing_choice_lines, _format_match_line, _notify_admins, admin_lead_keyboard, search_results_keyboard, send_find_results_as_cards, send_listing_card
    from .search import create_lead, detect_area, detect_property_type, search_listings_with_fallback, upsert_user_profile
    from .session_deeplink import _remember_video_pref, clear_session_for_fresh_entry, now_ts, user_display_name
    from .start_routes import route_start_arg
    from .texts import advisor_handoff_text, advisor_text, brand_story_text, deposit_text, lead_capture_text, listing_detail_text, local_life_text, promise_text, render_panel, rfcity_text, service_hub_text, smart_search_text, want_home_ack_text, welcome_text
    if data == 'home':
            clear_session_for_fresh_entry(context)
            context.user_data.pop('resume_start_arg', None)
            context.user_data.pop('contact_listing_id', None)
            context.user_data.pop('contact_touch_payload', None)
            await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
            return MAIN
    if data == 'home_smart_search':
            await render_panel(update, text=smart_search_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📍 按区域找', callback_data='hub:area'), InlineKeyboardButton('🏘 按户型找', callback_data='hub:layout')], [InlineKeyboardButton('💰 按预算找', callback_data='hub:budget'), InlineKeyboardButton('🏢 按公寓/楼盘找', callback_data='smart_project')], [InlineKeyboardButton('📅 按入住时间', callback_data='smart_movein'), InlineKeyboardButton('💎 让顾问帮我找', callback_data='keyword:handoff')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            return MAIN
    if data == 'home_brand':
            await render_panel(update, text=brand_story_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔍 智能找房', callback_data='home_smart_search'), InlineKeyboardButton('📅 预约看房', callback_data='home_appoint')], [InlineKeyboardButton('💬 联系我们', callback_data='home_consult'), InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            return MAIN
    if data == 'home_appoint':
            create_lead(user, action='appointment_click', source='home_button', payload={'from_home': True})
            return await start_appointment(update, context, '待推荐', source='home_button', touch_payload={'from_home': True, 'listing_unknown': True})
    if data == 'home_consult':
            create_lead(user, action='consult_click', source='home_button', payload={'from_home': True})
            await _notify_admins(context, title='联系我们（首页按钮）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', '来源：home_consult'])
            await render_panel(update, text=advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard())
            return MAIN
    if data == 'home_living':
            await render_panel(update, text=service_hub_text(), parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard())
            return MAIN
    if data == 'home_nearby':
            await render_panel(update, text=local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard())
            return MAIN
    if data == 'smart_project':
            await render_panel(update, text='<b>🏢 按公寓/楼盘找</b>\n\n请输入楼盘/公寓关键词（可带预算和户型），例如：<code>富力 800 一房</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            context.user_data['awaiting_keyword_find'] = {'source': 'smart_project'}
            return MAIN
    if data == 'smart_movein':
            await render_panel(update, text='<b>📅 按入住时间找</b>\n\n请输入你的入住时间（可带区域/预算），例如：<code>下月初 BKK1 700以内</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            context.user_data['awaiting_keyword_find'] = {'source': 'smart_movein'}
            return MAIN
    if data == 'keyword:handoff':
            create_lead(user, action='handoff_find_click', source='smart_search', payload={'from_smart': True})
            await _notify_admins(context, title='找房需求已提交（让顾问帮我找）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', '来源：smart_search 让顾问帮我找'])
            await render_panel(update, text=copy_handoff_find_ok_text(), reply_markup=main_keyboard())
            return MAIN
    if data == 'hub:area':
            context.user_data['search_pref'] = {'source': 'home_area', 'goal': 'any', 'touch_payload': {}}
            await render_panel(update, text='📍 <b>按区域找房</b>\n\n先选区域，我再带你进下一步预算筛选。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
            return FIND_AREA
    if data == 'hub:budget':
            context.user_data['search_pref'] = {'source': 'home_budget', 'goal': 'any', 'area': '', 'touch_payload': {}}
            await render_panel(update, text='💰 <b>预算多少？</b>', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'))
            return FIND_BUDGET
    if data == 'hub:layout':
            await render_panel(update, text='🛏 <b>按户型找房</b>\n\n先选想看的户型，我先给你一轮结果。', parse_mode=ParseMode.HTML, reply_markup=room_type_keyboard())
            return MAIN
    if data == 'hub:latest':
            await render_panel(update, text=_latest_listing_text(), parse_mode=ParseMode.HTML, reply_markup=latest_listing_keyboard())
            return MAIN
    if data == 'hub:find':
            return await show_search_entry(update, context)
    if data == 'hub:appoint':
            return await show_appointment_hub(update, context)
    if data == 'hub:video_tour':
            return await start_video_tour_flow(update, context, source='home_video_button')
    if data == 'hub:advisor':
            return await contact_management(update, context, source='hub')
    if data == 'hub:precise':
            return await show_precise_filter(update, context)
    if data == 'hub:account':
            await render_panel(update, text=list_favorites_text(user.id) + '\n\n' + list_recent_appointments(user.id), reply_markup=main_keyboard())
            return MAIN
    if data == 'hub:favorites':
            return await show_favorites(update, context)
    if data == 'hub:appointments':
            await render_panel(update, text=list_recent_appointments(user.id), reply_markup=main_keyboard())
            return MAIN
    if data == 'hub:contract':
            binding = db.get_active_binding(user.id)
            await render_panel(update, text=_binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
            return MAIN
    if data == 'hub:service':
            return await show_service_hub(update, context)
    if data == 'hub:promise':
            await render_panel(update, text=promise_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
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
                await render_panel(update, text='已恢复上次进度。' if listing_id else '当前没有可恢复的流程，已返回首页。', reply_markup=main_keyboard())
                return MAIN
            if action == 'restart' and resume_arg:
                clear_session_for_fresh_entry(context)
                context.user_data.pop('resume_start_arg', None)
                state = await route_start_arg(update, context, resume_arg)
                if state is not None:
                    return state
                await render_panel(update, text='入口链接已失效，请从频道帖子重新进入。')
                return MAIN
    return None
