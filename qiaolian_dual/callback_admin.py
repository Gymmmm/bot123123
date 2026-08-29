"""Callback handlers for the admin domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return data.startswith(('adminlead:', 'adminrepair:'))


async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
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
    if data.startswith('adminrepair:'):
            from .admin_contract import _is_admin_user
            from .messages import repair_progress_text
            if not _is_admin_user(getattr(user, 'id', 0)):
                await query.answer('仅顾问可操作', show_alert=True)
                return MAIN
            parts = data.split(':')
            if len(parts) != 3 or not str(parts[2]).isdigit():
                await query.answer('这条报修已失效', show_alert=True)
                return MAIN
            stage = str(parts[1] or '')
            labels = {
                'accepted': '已接手', 'scheduled': '已安排', 'in_progress': '处理中',
                'done': '已完成', 'need_info': '需要客户补充',
            }
            if stage not in labels:
                return MAIN
            ticket_id = int(parts[2])
            ticket = db.update_repair_ticket_status(ticket_id, stage)
            if not ticket:
                await query.answer('未找到这条报修，可能已处理', show_alert=True)
                return MAIN
            customer_id = int(ticket.get('user_id') or 0)
            if customer_id > 0:
                try:
                    await context.bot.send_message(
                        chat_id=customer_id,
                        text=repair_progress_text(str(ticket.get('issue_type') or '报修事项'), stage),
                        parse_mode=ParseMode.HTML,
                        reply_markup=service_detail_keyboard(),
                    )
                except Exception:
                    logger.exception('报修状态已更新，但客户通知发送失败: ticket_id=%s', ticket_id)
            original = str(getattr(query.message, 'text_html', '') or getattr(query.message, 'text', '') or '')
            await query.edit_message_text(
                original + f'\n\n<b>处理状态：</b>{he(labels[stage])}',
                parse_mode=ParseMode.HTML,
            )
            await query.answer(f'已通知客户：{labels[stage]}')
            return MAIN
    if data.startswith('adminlead:'):
            from .admin_contract import _is_admin_user
            if not _is_admin_user(getattr(user, 'id', 0)):
                await query.answer('仅顾问可操作', show_alert=True)
                return MAIN
            parts = data.split(':')
            if len(parts) != 5 or not all((part.isdigit() for part in parts[2:])):
                await query.answer('线索参数已失效', show_alert=True)
                return MAIN
            action = parts[1]
            lead_id, appointment_id, customer_id = map(int, parts[2:])
            status_map = {'claim': ('claimed', 'assigned', '顾问已接手'), 'contacted': ('contacted', 'contacted', '顾问跟进中'), 'invalid': ('invalid', 'cancelled', '已标记无效')}
            if action not in status_map:
                return MAIN
            lead_status, appointment_status, label = status_map[action]
            advisor_name = user_display_name(user)
            ok = db.update_lead_workflow(lead_id, status=lead_status, advisor_id=str(user.id), advisor_name=advisor_name)
            if appointment_id > 0:
                db.update_appointment_status(appointment_id, appointment_status)
                with db.connect() as conn:
                    appt_row = conn.execute('SELECT listing_id FROM appointments WHERE id=?', (appointment_id,)).fetchone()
                if appt_row:
                    from .channel_status_sync import sync_channel_listing_status
                    await sync_channel_listing_status(str(appt_row[0] or ''))
            if not ok:
                await query.answer('线索不存在或已失效', show_alert=True)
                return MAIN
            original = str(getattr(query.message, 'text_html', '') or getattr(query.message, 'text', '') or '')
            status_line = f'\n\n<b>处理状态：</b>{he(label)} · {he(advisor_name)}'
            await query.edit_message_text(original + status_line, parse_mode=ParseMode.HTML, reply_markup=admin_lead_keyboard(lead_id=lead_id, appointment_id=appointment_id, user_id=customer_id) if action == 'claim' else None)
            if action in {'claim', 'contacted'}:
                from .messages import advisor_response_notice_text
                user_text = advisor_response_notice_text()
                try:
                    await context.bot.send_message(chat_id=customer_id, text=user_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📋 查看我的预约', callback_data='appointment_menu:list')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
                except Exception:
                    logger.exception('预约状态通知用户失败: user_id=%s', customer_id)
            return MAIN
    return None
