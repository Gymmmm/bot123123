"""Callback handlers for the service domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'service:hub') or (data == 'service:contact') or (data == 'service:renew_change') or (data == 'service:terminate') or (data == 'service:move') or (data == 'service:handover') or (data == 'service:deposit') or (data == 'service:staging') or (data == 'service:addons') or (data in ('service:guide', 'service:local_life')) or (data == 'service:checkin_tips') or (data == 'service:repair_hub') or (data.startswith('service_request:')) or (data.startswith('service_slot:')) or (data == 'local:rfcity') or (data.startswith('rfcity:'))


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
    if data == 'service:hub':
            await query.edit_message_text(service_hub_text(), parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(user.id))
            return MAIN
    if data == 'service:contact':
            create_lead(user, action='consult_menu_click', source='service_hub')
            await _notify_admins(context, title='咨询顾问请求（入住服务按钮）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', '来源：service_hub'])
            await render_panel(update, text=advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard())
            return MAIN
    if data == 'service:renew_change':
            create_lead(user, action='service_renew_change_click', source='service_hub', listing_id=str(context.user_data.get('contact_listing_id') or ''))
            await query.edit_message_text('🔁 <b>续租 / 换房服务</b>\n\n如果你准备续租、换房或退租，侨联可以协助：\n• 先核对当前租约关键条款\n• 评估续租谈判或换房方案\n• 对接下一套看房与衔接时间\n\n请选择你现在更需要哪种协助：', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📝 续租当前房源', callback_data='contract:renew'), InlineKeyboardButton('🔍 换房找新房', callback_data='contract:change')], [InlineKeyboardButton('🚪 退租（到期不续）', callback_data='service:terminate')], [InlineKeyboardButton('💬 联系我们', callback_data='service:contact')], [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')]]))
            return MAIN
    if data == 'service:terminate':
            binding = db.get_active_binding(user.id)
            create_lead(user, action='lease_terminate_request', source='service_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id')})
            await _notify_admins(context, title='退租协助请求', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"当前房源：{he(str((binding or {}).get('property_name') or '-'))}", f"到期：{he(_binding_end_date(binding) or '-')}"])
            await render_panel(update, text='🚪 <b>退租协助已登记</b>\n\n顾问会先核对租约到期日、提前通知期和押金退还条件，再协助您与房东/物业沟通。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📞 联系顾问', callback_data='service:contact'), InlineKeyboardButton('⬅️ 返回', callback_data='service:hub')]]), context=context)
            return MAIN
    if data == 'service:move':
            await query.edit_message_text('<b>📦 搬家协助</b>\n\n可协助你对接搬家车辆、人手与时间安排。\n我们会按你的入住时间给出执行建议。\n\n请点下方「联系顾问」转人工协助。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:handover':
            await query.edit_message_text('<b>🧾 入住交接留档</b>\n\n入住前建议把房屋现状、水电表、家具家电状态拍照留档，\n便于后续退租时对照。\n\n需要时也可以让我们提醒你现场重点看哪些细节。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:deposit':
            create_lead(user, action='deposit_inquiry', source='service_deposit', listing_id=str(context.user_data.get('contact_listing_id') or ''), payload={'intent': 'price_question'})
            await query.edit_message_text(deposit_text() + '\n\n' + lead_capture_text(), parse_mode=ParseMode.HTML, reply_markup=lead_capture_keyboard())
            return MAIN
    if data == 'service:staging':
            await query.edit_message_text('<b>📹 代拍验房</b>\n\n没空到现场也没关系。\n我们可以先过去拍，或和你实时视频连线。\n\n会优先替你确认：\n• 空调型号和老旧程度\n• 冰箱、洗衣机等家电状态\n• 采光、噪音、楼道与周边情况\n• 水电网和押付方式\n\n如果要安排，请点下方「联系顾问」。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:addons':
            await query.edit_message_text('<b>🛋 家具家电补配</b>\n\n入住前如果需要补床垫、桌椅、窗帘、小家电，\n请点下方「联系顾问」，我们统一帮你对接和确认。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data in ('service:guide', 'service:local_life'):
            await query.edit_message_text(local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard())
            return MAIN
    if data == 'service:checkin_tips':
            await query.edit_message_text('<b>📋 入住注意事项</b>\n\n给你一份「少踩坑」清单，都是现场容易忘看的点：\n• 门禁 / 电梯卡几张、押金多少\n• 水压、热水、地漏、马桶冲水\n• 窗户密封与隔音、阳台排水\n• 空调试机 10 分钟、外机噪音\n• 合同里维修责任与联系人写清\n\n需要顾问按这套清单帮你走一遍，点下方「联系顾问」。', parse_mode=ParseMode.HTML, reply_markup=service_detail_keyboard())
            return MAIN
    if data == 'service:repair_hub':
            await query.edit_message_text('<b>🔧 租后管家服务</b>\n\n遇到问题，找侨联，更省心。\n\n请选择你现在需要协助的事项：', parse_mode=ParseMode.HTML, reply_markup=service_repair_keyboard())
            return MAIN
    if data.startswith('service_request:'):
            issue_key = data.split(':', 1)[1]
            issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
            context.user_data['awaiting_service_request'] = {'issue_key': issue_key, 'issue_label': issue_label}
            await render_panel(update, text=f'🔧 <b>{he(issue_label)}</b>\n\n请直接发送问题描述。\n\n例如：\n<code>B栋3楼走廊灯坏了，需要联系物业报修</code>\n<code>空调开机后不制冷</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📞 联系顾问', callback_data='service:contact'), InlineKeyboardButton('⬅️ 返回', callback_data='service:hub')]]))
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
            await _notify_admins(context, title='新入住服务请求（按钮提交）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房号：{he(str((binding or {}).get('property_name') or '-'))}", f'事项：{he(issue_label)}', f'描述：{he(detail)}', f'时间：{he(slot_label)}', f'工单：{he(str(ticket_id))}'])
            await render_panel(update, text=f"✅ <b>已收到您的{he(issue_label)}需求</b>\n\n📅 希望时间：<b>{he(slot_label)}</b>\n📋 工单编号：<b>{he(str(ticket_id))}</b>\n\n侨联顾问会通过 Telegram 跟进处理。\n⏱️ 预计 <b>60 分钟内</b>响应\n📞 紧急情况可直接联系：<b>{he(str(ADVISOR_TG or '@pengqingw'))}</b>", parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
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
