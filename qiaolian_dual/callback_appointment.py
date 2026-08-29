"""Callback handlers for the appointment domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data.startswith('appointment_menu:cancel_confirm:')) or (data.startswith('appointment_menu:cancel:')) or (data == 'appointment_menu:details') or (data == 'appointment_menu:list') or (data.startswith('appointment_menu:contact')) or (data.startswith('appointment_menu:'))


async def handle_appointment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
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
    if data.startswith('appointment_menu:cancel_confirm:'):
            try:
                appointment_id = int(data.rsplit(':', 1)[1])
            except (TypeError, ValueError):
                await answer_callback_once(query, '预约编号无效', show_alert=True)
                return MAIN
            row = _find_user_appointment(user.id, appointment_id)
            current_status = str((row or {}).get('status') or '')
            if not row or current_status in {'done', 'cancelled'}:
                await answer_callback_once(query, '这条预约已无法取消', show_alert=True)
                return MAIN
            if db.update_appointment_status(appointment_id, 'cancelled'):
                from .channel_status_sync import sync_channel_listing_status
                await sync_channel_listing_status(str(row.get('listing_id') or ''))
                await answer_callback_once(query, '预约已取消', show_alert=False)
                await render_panel(update, text=appointment_details_text(user.id), parse_mode=ParseMode.HTML, reply_markup=_appointment_details_keyboard(user.id), context=context)
            else:
                await answer_callback_once(query, '取消失败，请联系顾问', show_alert=True)
            return MAIN
    if data.startswith('appointment_menu:cancel:'):
            try:
                appointment_id = int(data.rsplit(':', 1)[1])
            except (TypeError, ValueError):
                await answer_callback_once(query, '预约编号无效', show_alert=True)
                return MAIN
            row = _find_user_appointment(user.id, appointment_id)
            if not row or str(row.get('status') or '') in {'done', 'cancelled'}:
                await answer_callback_once(query, '这条预约已无法取消', show_alert=True)
                return MAIN
            listing = _appointment_listing_compact(row.get('listing_id'))
            date_text = _appointment_date_compact(row.get('appointment_date'))
            time_text = _appointment_time_compact(row.get('appointment_time'))
            await render_panel(update, text=f'⚠️ <b>确认取消预约？</b>\n\n{he(date_text)} · {he(time_text)}\n房源：<b>{he(listing)}</b>\n\n取消后如需再次看房，需要重新提交预约。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('确认取消', callback_data=f'appointment_menu:cancel_confirm:{appointment_id}')], [InlineKeyboardButton('暂不取消', callback_data='appointment_menu:details')]]), context=context)
            return MAIN
    if data == 'appointment_menu:details':
            await render_panel(update, text=appointment_details_text(user.id), parse_mode=ParseMode.HTML, reply_markup=_appointment_details_keyboard(user.id), context=context)
            return MAIN
    if data == 'appointment_menu:list':
            await render_panel(update, text=list_recent_appointments(user.id), parse_mode=ParseMode.HTML, reply_markup=_appointment_card_keyboard(), context=context)
            return MAIN
    if data.startswith('appointment_menu:contact'):
            parts = data.split(':', 3)
            scope = parts[2] if len(parts) >= 3 else ''
            ref = parts[3] if len(parts) >= 4 else ''
            listing_id = ''
            binding = db.get_active_binding(user.id)
            source_label = 'appointment_hub'
            if scope == 'listing':
                listing_id = ref
                context.user_data['contact_listing_id'] = listing_id
                source_label = 'listing_landing'
            else:
                listing_id = str(context.user_data.get('contact_listing_id') or '')
            create_lead(user, action='consult_menu_click', source=source_label, listing_id=listing_id or str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id'), 'listing_id': listing_id})
            await _notify_admins(context, title='咨询顾问请求（按钮承接）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'来源：{he(source_label)}', f"房源：{he(listing_id or str((binding or {}).get('property_name') or '-'))}"])
            await render_panel(update, text=advisor_handoff_text(listing_id=listing_id, user_id=user.id), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(listing_id=listing_id))
            return MAIN
    if data.startswith('appointment_menu:'):
            mode = data.split(':', 1)[1]
            create_lead(user, action='appointment_click', source='menu_appointment', payload={'from_menu': True, 'preferred_mode': mode})
            return await start_appointment(update, context, '待推荐', source='menu_appointment', touch_payload={'from_menu': True, 'listing_unknown': True}, initial_mode=mode)
    return None
