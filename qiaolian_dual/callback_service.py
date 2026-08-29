"""Callback handlers for the service domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'service:hub') or (data == 'service:promise') or (data == 'service:contact') or (data in ('service:renew', 'service:change', 'service:renew_change')) or (data == 'service:terminate') or (data == 'service:move') or (data == 'service:handover') or (data == 'service:deposit') or (data == 'service:staging') or (data == 'service:addons') or (data in ('service:guide', 'service:local_life')) or (data == 'service:checkin_tips') or (data == 'service:repair_hub') or (data.startswith('service_request:')) or (data.startswith('service_slot:')) or (data == 'local:rfcity') or (data.startswith('rfcity:'))


async def handle_service_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
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
    # 服务对所有用户可见可用；绑定租客档案只用于自动带入房屋和租约信息。
    if data == 'service:hub':
            return await show_service_hub(update, context)
    tenant_only_callbacks = {
        'service:terminate', 'service:move',
        'service:handover', 'service:deposit', 'service:staging',
        'service:addons', 'service:checkin_tips',
    }
    if data in tenant_only_callbacks and not db.get_active_binding(user.id):
            return await show_service_hub(update, context)
    if data == 'service:promise':
            await query.edit_message_text(
                '<b>🛡 租期服务保障</b>\n\n'
                '绑定租客档案后：\n'
                '• 租约到期前 <b>7 天</b>提醒你确认是否续租\n'
                '• 报修提交即生成工单，处理进度会通知你\n'
                '• 续租、换房时自动带上当前租约\n\n'
                '每一项都有记录，不只是口头说“会跟进”。',
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔗 绑定租客档案', callback_data='profile:repeat')],
                    [InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')],
                ]),
            )
            return MAIN
    if data == 'service:contact':
            create_lead(user, action='consult_menu_click', source='service_hub')
            await _notify_admins(context, title='咨询顾问请求（入住服务按钮）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', '来源：service_hub'])
            await render_panel(update, text=advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard())
            return MAIN
    if data == 'service:renew':
            binding = db.get_active_binding(user.id)
            if binding:
                from .callback_contract import handle_contract_callback
                return await handle_contract_callback(update, context, query, 'contract:renew', user)
            await query.edit_message_text(
                '<b>🔁 我要续租</b>\n\n如果你是侨联在租客户，可以先绑定租约，顾问核实后会自动带上当前房屋和到期日。\n\n暂时没有绑定也没关系，可以直接联系中文顾问。',
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔗 绑定我的租约', callback_data='profile:repeat')],
                    [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
                    [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
                ]),
            )
            return MAIN
    if data == 'service:change':
            context.user_data['search_pref'] = {'source': 'tenant_change_home', 'goal': 'any', 'touch_payload': {'entry': 'service_change'}}
            return await show_search_entry(update, context)
    if data == 'service:renew_change':
            create_lead(user, action='service_renew_change_click', source='service_hub', listing_id=str(context.user_data.get('contact_listing_id') or ''))
            if not db.get_active_binding(user.id):
                await query.edit_message_text('<b>🔁 续租 / 换房</b>\n\n需要续租当前房源，先绑定租客档案；想换房可以直接开始找。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔗 绑定租客档案', callback_data='profile:repeat'), InlineKeyboardButton('🔍 换房找新房', callback_data='contract:change')], [InlineKeyboardButton('💬 联系顾问', callback_data='service:contact')], [InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')]]))
                return MAIN
            await query.edit_message_text('<b>🔁 续租 / 换房</b>\n\n选择你现在要办的事：', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📝 续租当前房源', callback_data='contract:renew'), InlineKeyboardButton('🔍 换房找新房', callback_data='contract:change')], [InlineKeyboardButton('💬 联系顾问', callback_data='service:contact')], [InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')]]))
            return MAIN
    if data == 'service:terminate':
            binding = db.get_active_binding(user.id)
            create_lead(user, action='lease_terminate_request', source='service_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id')})
            await _notify_admins(context, title='退租协助请求', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"当前房源：{he(str((binding or {}).get('property_name') or '-'))}", f"到期：{he(_binding_end_date(binding) or '-')}"])
            await render_panel(update, text='<b>🚪 已收到你的退租安排</b>\n\n顾问会先和你核对到期日、提前通知期和押金相关事项，再帮你和房东或物业沟通。\n\n有新的时间安排，直接联系顾问告诉我们就行。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系顾问', callback_data='service:contact'), InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')]]), context=context)
            return MAIN
    if data == 'service:move':
            await query.edit_message_text('<b>📦 搬家协助</b>\n\n需要协调搬家时，顾问可帮你对接：\n• 车辆和人手\n• 搬家时间\n• 到达后的入住安排\n\n把你的时间发给顾问，我们一起定下来。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:handover':
            await query.edit_message_text('<b>🧾 入住交接留档</b>\n\n签约或入住当天，建议一起核对：\n• 房屋和家具家电现状\n• 水电表读数\n• 钥匙和门卡数量\n\n照片、视频和确认记录留好，之后有事更好对照。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:deposit':
            create_lead(user, action='deposit_inquiry', source='service_deposit', listing_id=str(context.user_data.get('contact_listing_id') or ''), payload={'intent': 'price_question'})
            await query.edit_message_text(deposit_text() + '\n\n' + lead_capture_text(), parse_mode=ParseMode.HTML, reply_markup=lead_capture_keyboard())
            return MAIN
    if data == 'service:staging':
            await query.edit_message_text('<b>📹 代拍验房</b>\n\n不方便到现场，也可以先拍给你看，或实时视频连线。\n\n我们会重点确认：\n• 空调和家电状态\n• 采光、噪音和周边\n• 水电、网络和押付方式\n\n想安排时，直接联系顾问说一下你的时间。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:addons':
            await query.edit_message_text('<b>🛋 家具家电补配</b>\n\n入住前如果需要补床垫、桌椅、窗帘、小家电，\n请点下方「联系顾问」，我们统一帮你对接和确认。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data in ('service:guide', 'service:local_life'):
            await query.edit_message_text(local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard())
            return MAIN
    if data == 'service:checkin_tips':
            await query.edit_message_text('<b>📋 入住前看这几项</b>\n\n• 门禁卡、钥匙和押金写清\n• 水压、热水、地漏和马桶试一下\n• 窗户、隔音和阳台排水看一遍\n• 空调试机，听一下外机噪音\n• 合同里的维修责任和联系人确认好\n\n想让顾问陪你按这份清单核对，直接联系顾问。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:repair_hub':
            await query.edit_message_text('<b>🔧 报修</b>\n\n选择问题类别：', parse_mode=ParseMode.HTML, reply_markup=service_repair_keyboard())
            return MAIN
    if data.startswith('service_request:'):
            issue_key = data.split(':', 1)[1]
            issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
            context.user_data['awaiting_service_request'] = {'issue_key': issue_key, 'issue_label': issue_label}
            await render_panel(update, text=f'<b>🔧 {he(issue_label)}</b>\n\n发一下位置和问题即可。\n例如：<code>空调开机不制冷</code> 或 <code>B栋3楼走廊灯坏了</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系顾问', callback_data='service:contact'), InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')]]))
            return MAIN
    if data.startswith('service_slot:'):
            _, issue_key, slot = data.split(':', 2)
            issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
            slot_map = {'today': '今天内安排', 'tomorrow_am': '明天上午', 'tomorrow_pm': '明天下午'}
            slot_label = slot_map.get(slot, slot)
            binding = db.get_active_binding(user.id)
            detail_ctx = context.user_data.pop('service_request_detail', {})
            detail = str(detail_ctx.get('detail') or f'按钮提交：{slot_label}')
            binding_id = int((binding or {}).get('id') or 0) or None
            ticket_id = db.create_repair_ticket(user.id, binding_id, issue_label, f'{detail}\n希望时间：{slot_label}', now_ts())
            create_lead(user, action='service_request_submit', source='service_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'issue_key': issue_key, 'issue_label': issue_label, 'time_slot': slot, 'detail': detail, 'binding_id': binding_id})
            from .results_admin import admin_repair_keyboard
            await _notify_admins(
                context,
                title='新报修请求',
                lines=[
                    f'客户：{_user_mention_html(user)}',
                    f'房源：{he(str((binding or {}).get("property_name") or "-"))}',
                    f'问题：{he(issue_label)}',
                    f'说明：{he(detail)}',
                    f'希望时间：{he(slot_label)}',
                ],
                reply_markup=admin_repair_keyboard(ticket_id),
            )
            await render_panel(update, text=f"<b>✅ 报修已提交</b>\n\n工单｜<code>WX{ticket_id:05d}</code>\n问题｜{he(issue_label)}\n时间｜<b>{he(slot_label)}</b>\n\n处理进度会在这里通知你。", parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'local:rfcity':
            create_lead(user, action='local_area_click', source='local_life', area='rfcity', payload={'area': 'rfcity', 'category': 'overview'})
            await query.edit_message_text(rfcity_text(), parse_mode=ParseMode.HTML, reply_markup=rfcity_keyboard())
            return MAIN
    if data.startswith('rfcity:'):
            category = data.split(':', 1)[1]
            create_lead(user, action='local_category_click', source='rfcity', area='rfcity', payload={'area': 'rfcity', 'category': category})
            _rfcity_texts = {'restaurant': copy_rfcity_restaurant_text, 'bbq': copy_rfcity_bbq_text, 'drinks': copy_rfcity_drinks_text, 'supermarket': copy_rfcity_supermarket_text, 'hotel': copy_rfcity_hotel_text, 'recreation': copy_rfcity_recreation_text, 'logistics': copy_rfcity_logistics_text, 'property': copy_rfcity_property_text}
            if category == 'join':
                await query.edit_message_text(copy_merchant_join_text(), parse_mode=ParseMode.HTML, reply_markup=merchant_join_keyboard())
            elif category in _rfcity_texts:
                await query.edit_message_text(_rfcity_texts[category](), parse_mode=ParseMode.HTML, reply_markup=rfcity_back_keyboard())
            else:
                await query.edit_message_text(rfcity_text(), parse_mode=ParseMode.HTML, reply_markup=rfcity_keyboard())
            return MAIN
    return None
