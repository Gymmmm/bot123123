"""Callback handlers for the contract domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'contract:view') or (data == 'contract:toggle_reminder') or (data == 'contract:renew') or (data.startswith('contract:renew_yes:')) or (data == 'contract:change')


async def handle_contract_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
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
    if data == 'contract:view':
            binding = db.get_active_binding(user.id)
            create_lead(user, action='contract_view_click', source='contract_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id')})
            await render_panel(update, text=_binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
            return MAIN
    if data == 'contract:toggle_reminder':
            sub = db.toggle_lease_reminder(user.id, now_ts())
            enabled = int(sub.get('lease_reminder_enabled', 1) or 1) == 1
            binding = db.get_active_binding(user.id)
            create_lead(user, action='lease_reminder_enable_click' if enabled else 'lease_reminder_disable_click', source='contract_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id'), 'lease_reminder_enabled': enabled})
            prefix = '已开启到期提醒，30/7/3 天节点会自动提醒您。' if enabled else '已关闭到期提醒，后续不再自动推送到期消息。'
            await render_panel(update, text=f'{prefix}\n\n{_binding_contract_text(binding, user.id)}', parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
            return MAIN
    if data == 'contract:renew':
            binding = db.get_active_binding(user.id)
            if not binding:
                await render_panel(update, text='当前还没有绑定租约档案。\n请点「💬 联系顾问」，我们先把房号和到期日录入。', reply_markup=contact_handoff_keyboard())
                return MAIN
            days_left = _binding_days_left(binding)
            day_text = f'{days_left} 天' if days_left is not None else '待确认'
            open_tracking = db.get_open_renewal_tracking(binding_id=int(binding.get('id') or 0), user_id=user.id)
            create_lead(user, action='renewal_inquiry_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding.get('id'), 'days_left': days_left, 'open_tracking_id': (open_tracking or {}).get('id')})
            await render_panel(update, text=f"🔄 <b>续租咨询</b>\n\n🏠 当前房号：{he(str(binding.get('property_name') or '-'))}\n📅 到期日：{he(_binding_end_date(binding) or '待确认')}\n⏳ 剩余：<b>{he(day_text)}</b>\n\n" + ('当前已有一张续租工单在跟进，若继续点击确认，我们会沿用原工单继续推进。' if open_tracking else '您打算继续住这套吗？确认后我们会把工单推给管理号跟进。'), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('✅ 确认续租', callback_data=f"contract:renew_yes:{int(binding.get('id') or 0)}"), InlineKeyboardButton('🏠 我想换房', callback_data='contract:change')], [InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')]]))
            return MAIN
    if data.startswith('contract:renew_yes:'):
            raw_bid = data.split(':', 2)[2]
            try:
                binding_id = int(raw_bid)
            except (TypeError, ValueError):
                binding_id = 0
            binding = db.get_binding_by_id(binding_id) if binding_id > 0 else None
            if not binding or int(binding.get('user_id') or 0) != int(user.id):
                await render_panel(update, text='未找到可确认的续租档案。\n请点「🔄 续租咨询」重新发起。', reply_markup=_contract_actions_keyboard(user.id))
                return MAIN
            existing = db.get_open_renewal_tracking(binding_id=binding_id, user_id=user.id)
            tracking_id = int(existing.get('id') or 0) if existing else 0
            if tracking_id <= 0:
                tracking_id = db.create_renewal_tracking(binding_id=binding_id, user_id=user.id, listing_id=str(binding.get('property_name') or ''), renewal_status='pending', user_response='用户确认续租', created_at=now_ts())
            create_lead(user, action='renewal_confirm_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding_id, 'tracking_id': tracking_id, 'deduped': bool(existing)})
            if not existing:
                await _notify_admins(context, title='续租意向确认', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房号：{he(str(binding.get('property_name') or '-'))}", f"到期：{he(_binding_end_date(binding) or '-')}", f'工单：RT-{he(str(tracking_id))}', '请在 24 小时内联系租客确认续租条款。'])
            await render_panel(update, text=('⏳ <b>续租工单已在跟进中</b>\n\n我们沿用之前的工单继续处理，管理号会尽快联系您确认租期与价格。' if existing else '✅ <b>续租意向已提交</b>\n\n管理号已收到工单，会尽快联系您确认租期与价格。') + '\n如有变更，也可以直接点下方联系顾问。', parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
            return MAIN
    if data == 'contract:change':
            binding = db.get_active_binding(user.id)
            create_lead(user, action='change_house_click', source='contract_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id')})
            if binding:
                await _notify_admins(context, title='老客换房意向', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"当前房号：{he(str(binding.get('property_name') or '-'))}", f"到期：{he(_binding_end_date(binding) or '-')}"])
            await render_panel(update, text='🏠 <b>换房服务</b>\n\n我们会按您当前预算/区域重新筛选 1-3 套可决策房源。\n您可以直接浏览频道，也可以让顾问立刻接手。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🔍 立即筛房', callback_data='findmode:guided')], [InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')], [InlineKeyboardButton('📋 返回租约', callback_data='contract:view')]]))
            return MAIN
    return None
