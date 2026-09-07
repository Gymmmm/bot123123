"""Callback handlers for the preference domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'lead_capture:phone') or (data == 'pref:clear') or (data.startswith('pref:toggle:')) or (data == 'pref:submit')


async def handle_preference_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
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
    if data == 'lead_capture:phone':
            await render_panel(update, text='无需另外填写手机号或微信。\n\n侨联顾问会直接通过 Telegram 接手当前需求。', reply_markup=lead_capture_keyboard(), context=context)
            return MAIN
    if data == 'pref:clear':
            context.user_data['pref_select'] = {'source': 'menu_precise', 'selected': []}
            await query.edit_message_text('📍 <b>条件筛选</b>\n\n已清空，继续点选后提交即可。', parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()))
            return MAIN
    if data.startswith('pref:toggle:'):
            key = data.split(':', 2)[2]
            if key not in PREF_CONDITION_LABELS:
                return MAIN
            pref_ctx = context.user_data.setdefault('pref_select', {'source': 'menu_precise', 'selected': []})
            selected = [str(x) for x in pref_ctx.get('selected') or [] if str(x) in PREF_CONDITION_LABELS]
            selected_set = set(selected)
            if key in selected_set:
                selected_set.remove(key)
            else:
                selected_set.add(key)
            pref_ctx['selected'] = list(selected_set)
            summary = '、'.join((PREF_CONDITION_LABELS[k] for k in pref_ctx['selected'][:6])) or '未选择'
            await query.edit_message_text(f'📍 <b>条件筛选</b>\n\n当前已选：{he(summary)}\n选完点 <b>提交条件</b>，无需手动打字。', parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set(pref_ctx['selected'])))
            return MAIN
    if data == 'pref:submit':
            pref_ctx = context.user_data.pop('pref_select', {'source': 'menu_precise', 'selected': []})
            selected = [str(x) for x in pref_ctx.get('selected') or [] if str(x) in PREF_CONDITION_LABELS]
            selected_labels = [PREF_CONDITION_LABELS[x] for x in selected]
            summary = '、'.join(selected_labels) if selected_labels else '未勾选具体条件'
            create_lead(user, action='search_pref_submit', source=str(pref_ctx.get('source', 'menu_precise')), payload={'condition_keys': selected, 'condition_labels': selected_labels, 'message': summary})
            await _notify_admins(context, title='新条件筛选（点击提交）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'条件：{he(summary)}', '说明：用户通过按钮提交条件筛选'])
            await render_panel(update, text=want_home_ack_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
            return MAIN
    return None
