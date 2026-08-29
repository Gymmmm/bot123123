"""Callback handlers for the search domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'findmode:play') or (data == 'findmode:guided') or (data.startswith('roompick:')) or (data == 'profile:repeat') or (data.startswith('findtype:')) or (data.startswith('unavail:more:')) or (data == 'findback:area') or (data.startswith('findarea:')) or (data.startswith('findbudget:'))


async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user, *, hooks: dict | None = None) -> int | None:
    from .admin_contract import _binding_contract_text, _binding_days_left, _binding_end_date, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .appointments_view import _appointment_card_keyboard, _appointment_date_compact, _appointment_details_keyboard, _appointment_listing_compact, _appointment_time_compact, _find_user_appointment, appointment_details_text, list_favorites_text, list_recent_appointments, old_tenant_binding_text
    from .flows import contact_management, show_appointment_hub, show_favorites, show_help, show_precise_filter, show_search_entry, show_service_hub, start_appointment
    from .keyboards_common import _advisor_listing_url, _listing_channel_url, contact_handoff_keyboard, keyword_followup_keyboard, latest_listing_keyboard, lead_capture_keyboard, main_keyboard, no_match_followup_keyboard, old_tenant_followup_keyboard, room_type_keyboard
    from .keyboards_search import _decode_budget_choice, find_area_keyboard, find_budget_keyboard, guided_search_keyboard, local_life_keyboard, merchant_join_keyboard, precise_filter_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_detail_keyboard, service_hub_keyboard, service_repair_keyboard
    from .listing import _latest_listing_text, listing_context, listing_unavailable_keyboard, start_video_tour_flow
    from .results_admin import _allow_admin_notify, _format_listing_choice_lines, _format_match_line, _notify_admins as default_notify_admins, admin_lead_keyboard, search_results_keyboard, send_find_results_as_cards as default_send_find_results_as_cards, send_listing_card
    from .search import create_lead as default_create_lead, detect_area, detect_property_type, search_listings_with_fallback as default_search_listings_with_fallback, upsert_user_profile
    from .session_deeplink import _remember_video_pref, clear_session_for_fresh_entry, now_ts, user_display_name
    from .start_routes import route_start_arg
    from .texts import advisor_handoff_text, advisor_text, brand_story_text, deposit_text, lead_capture_text, listing_detail_text, local_life_text, promise_text, render_panel, rfcity_text, service_hub_text, smart_search_text, want_home_ack_text, welcome_text
    hooks = hooks or {}
    create_lead = hooks.get('create_lead') or default_create_lead
    _notify_admins = hooks.get('notify_admins') or default_notify_admins
    search_listings_with_fallback = hooks.get('search_listings') or default_search_listings_with_fallback
    send_find_results_as_cards = hooks.get('send_results') or default_send_find_results_as_cards
    if data == 'findmode:play':
            context.user_data['awaiting_keyword_find'] = {'source': 'smart_find_play'}
            await query.edit_message_text(smart_find_play_prompt_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📍 改走按类型（稳）', callback_data='findmode:guided')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            return MAIN
    if data == 'findmode:guided':
            # 历史消息仍可能带“按类型找房”按钮；新路径统一从区域开始。
            context.user_data.pop('awaiting_keyword_find', None)
            context.user_data['search_pref'] = {'source': 'user_search', 'goal': 'any', 'touch_payload': {}}
            await query.edit_message_text('📍 <b>想住哪里？</b>\n\n选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
            return FIND_AREA
    if data.startswith('roompick:'):
            room_type = data.split(':', 1)[1]
            _remember_video_pref(context, layout=room_type)
            property_type = detect_property_type(room_type)
            matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=None, budget_min=None, budget_max=None, text_fragment=room_type, limit=5)
            create_lead(user, action='keyword_find_play', source='home_layout', property_type=property_type, payload={'message': room_type, 'match_mode': match_mode, 'room_type': room_type})
            if matches:
                await send_find_results_as_cards(update, context, matches, 'strict')
            else:
                await render_panel(update, text=f'暂时没有完全匹配 <b>{he(room_type)}</b> 的房源。可以换个位置或预算再看看。', parse_mode=ParseMode.HTML, reply_markup=keyword_followup_keyboard(room_type=room_type))
            return MAIN
    if data == 'profile:repeat':
            binding_text, binding = old_tenant_binding_text(user.id)
            if not binding:
                context.user_data['awaiting_old_customer'] = True
                await render_panel(update, text='🔗 <b>绑定租客档案</b>\n\n曾通过侨联租过房？请发送<b>姓氏 + 手机尾号 4 位</b>，或签约时使用的 Telegram 账号。\n\n例如：<code>彭 5678</code> 或 <code>@yourname</code>\n\n核实后会把租约和房屋信息绑定到当前账号。\n🔐 信息仅用于核实租客档案。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📞 联系顾问', callback_data='service:contact'), InlineKeyboardButton('⬅️ 返回', callback_data='service:hub')]]), context=context)
                return MAIN
            create_lead(user, action='repeat_tenant_opt_in', source='help_inline', payload={'via': 'profile:repeat', 'binding_found': bool(binding), 'property_name': str((binding or {}).get('property_name') or ''), 'lease_end_date': str((binding or {}).get('lease_end_date') or ''), 'rent_day': (binding or {}).get('rent_day')})
            await _notify_admins(context, title='老客回流登记', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房号：{he(str((binding or {}).get('property_name') or '-'))}", f"交租日：{he(str((binding or {}).get('rent_day') or '-'))}", f"到期：{he(str((binding or {}).get('lease_end_date') or '-'))}", '说明：后台可按 user_id 维护房号/交租日/合同到期，作为老客回流入口'])
            await render_panel(update, text=binding_text, parse_mode=ParseMode.HTML, reply_markup=old_tenant_followup_keyboard())
            return MAIN
    if data.startswith('findtype:'):
            goal = data.split(':', 1)[1]
            context.user_data['search_pref'] = {'source': 'user_search', 'goal': goal, 'touch_payload': {}}
            goal_text = '我也说不清，直接帮我找' if goal == 'any' else goal
            await query.edit_message_text(f'📍 <b>想住哪里？</b>\n\n已选：{he(goal_text)}', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
            return FIND_AREA
    if data.startswith('unavail:more:'):
            area = detect_area(data.split(':', 2)[2])
            create_lead(user, action='unavailable_more_click', source='listing_unavailable', area=area, listing_id=str(context.user_data.get('contact_listing_id') or ''))
            matches = db.search_listings(areas=[area] if area and area != '不限' else None, limit=3)
            if matches:
                await send_find_results_as_cards(update, context, matches, 'strict')
            else:
                await render_panel(update, text=f"当前同区域（{he(area or '金边')}）暂无更多上架房源。\n可以点「继续找房」筛选，或直接联系顾问给你人工推荐。", parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard())
            return MAIN
    if data == 'findback:area':
            pref = context.user_data.get('search_pref') or {}
            goal = str(pref.get('goal') or 'any')
            goal_text = '' if goal in {'any', '住宅'} else f'已选：{he(goal)}\n\n'
            await query.edit_message_text(f'📍 <b>想住哪里？</b>\n\n{goal_text}选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
            return FIND_AREA
    if data.startswith('findarea:'):
            code = data.split(':', 1)[1]
            if code == 'other':
                await query.edit_message_text('🔍 <b>其他区域</b>\n\n请直接输入您想住的区域、楼盘或附近地标。\n\n例如：<code>永旺2附近</code> 或 <code>俄罗斯市场</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回区域选择', callback_data='findback:area')]]))
                return FIND_AREA
            area = FIND_AREA_CODE_MAP.get(code, '')
            if not area:
                return FIND_AREA
            pref = context.user_data.get('search_pref') or {}
            pref['area'] = area
            context.user_data['search_pref'] = pref
            goal = str(pref.get('goal') or 'any')
            goal_text = '我也说不清，直接帮我找' if goal == 'any' else goal
            await query.edit_message_text(f'💰 <b>每月预算大概多少？</b>\n\n{he(goal_text)} · {he(area)}', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard(goal))
            return FIND_BUDGET
    if data.startswith('findbudget:'):
            code = data.split(':', 1)[1]
            pref = context.user_data.pop('search_pref', {})
            goal = str(pref.get('goal') or 'any')
            area = str(pref.get('area') or '')
            budget_label, budget_min, budget_max = _decode_budget_choice(goal, code)
            layout_hint = '' if goal in {'any', '住宅'} else goal
            _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=layout_hint or None)
            type_filter = '' if goal in {'any', '住宅'} else goal
            create_lead(user, action='search_pref_submit', source=str(pref.get('source', 'user_search')), area=area if area != '不限' else '', property_type=type_filter, budget_min=budget_min, budget_max=budget_max, payload={'message': f'goal={goal}; area={area}; budget={budget_label}', 'goal': goal, 'area_hint': area, 'budget_label': budget_label, **(pref.get('touch_payload') or {})})
            if _allow_admin_notify(context, key=f'search_activity:{int(user.id)}', cooldown_seconds=600):
                await _notify_admins(context, title='找房需求（普通线索）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'类型意向：{he(goal)}', f"区域：{he(area or '-')}", f'预算：{he(budget_label)}', '说明：10 分钟内重复筛选仅记录，不重复提醒'])
            matches, match_mode = search_listings_with_fallback(property_type=type_filter or None, area=area, budget_min=budget_min, budget_max=budget_max, text_fragment=f'{goal} {area} {budget_label}', limit=5)
            if matches:
                if match_mode in {'no_type', 'no_area', 'budget_only', 'fuzzy', 'fallback_recent'}:
                    await query.answer('已自动放宽条件匹配', show_alert=False)
                await send_find_results_as_cards(update, context, matches, match_mode)
            else:
                await render_panel(update, text=find_no_match_text(), parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard())
            return MAIN
    return None
