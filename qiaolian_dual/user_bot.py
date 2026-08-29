"""侨联用户 Bot 兼容入口。

业务实现已拆分到多个模块；保留原有函数名导出，降低迁移风险。
"""
from __future__ import annotations

# Conversation state constants and shared configuration were public exports of
# the original monolithic user_bot module.  Keep that compatibility contract in
# addition to re-exporting the split implementation functions below.
from .common import *

from .utils_formatting import _fmt_price, _display_listing_id
from .keyboards_common import main_keyboard, no_match_followup_keyboard, quick_start_keyboard, room_type_keyboard, latest_listing_keyboard, keyword_followup_keyboard, _advisor_tg_url, _advisor_listing_url, _listing_channel_url, contact_handoff_keyboard, appointment_success_keyboard, channel_return_keyboard, lead_capture_keyboard, old_tenant_followup_keyboard
from .texts import welcome_text, channel_welcome_text, discussion_entry_welcome_text, lead_capture_text, _channel_index_action, _personal_greeting, render_panel, promise_text, deposit_text, advisor_text, advisor_handoff_text, smart_search_text, about_text, brand_story_text, help_text, service_hub_text, local_life_text, rfcity_text, want_home_prompt_text, want_home_ack_text, listing_detail_text
from .keyboards_search import _search_type_button_rows, search_entry_keyboard, guided_search_keyboard, find_area_keyboard, _budget_options_for_goal, find_budget_keyboard, _decode_budget_choice, appointment_menu_keyboard, precise_filter_keyboard, service_hub_keyboard, service_repair_keyboard, service_detail_keyboard, local_life_keyboard, rfcity_keyboard, rfcity_back_keyboard, merchant_join_keyboard
from .session_deeplink import user_display_name, now_ts, clear_main_flags, clear_session_for_fresh_entry, _remember_video_pref, _base36_decode, parse_start_arg_payload, build_source_label, _deep_link, _build_start_payload, _extract_caption_variant, _normalize_variant, _split_target_meta, _latest_draft_context, _latest_draft_review_status
from .listing import listing_context, listing_cost_text, listing_cost_keyboard, listing_is_available, listing_unavailable_text, listing_unavailable_keyboard, _store_active_entry, _active_entry_resume_keyboard, channel_topic_welcome_text, _resolve_area_from_target, _daily_listing_line, _latest_listing_text, _resolve_video_pref_snapshot, _video_tour_intro_text, _video_tour_match_text, _video_match_keyboard, start_video_tour_flow, _keyword_intro_text, listing_landing_text, listing_landing_keyboard
from .search import parse_budget_range, detect_area, detect_room_type, detect_property_type, search_listings_with_fallback, upsert_user_profile, create_lead
from .admin_contract import _user_mention_html, _user_contact_text, _is_admin_user, _extra_user_admin_ids, _all_user_admin_ids, _save_extra_user_admin_ids, _budget_text, _parse_date_safe, _binding_end_date, _binding_days_left, _contract_status_text, _lease_reminder_label, _binding_contract_text, _contract_actions_keyboard
from .results_admin import send_listing_card, send_find_results_as_cards, _format_match_line, _format_listing_choice_lines, search_results_keyboard, _notify_admins, admin_lead_keyboard, _allow_admin_notify
from .appointments_view import old_tenant_binding_text, _appointment_date_compact, _appointment_time_compact, _appointment_listing_compact, _appointment_card_keyboard, _appointment_sort_key, _appointment_is_upcoming, _appointment_summary_line, list_recent_appointments, _appointment_details_keyboard, _find_user_appointment, appointment_details_text, list_favorites_text
from .start_routes import route_start_arg as _route_start_arg_impl, start as _start_impl
from .appointment_ui import _appointment_date_keyboard, _appointment_mode_keyboard, _appointment_time_keyboard, _title_layout_label, _appointment_confirm_text, _appointment_confirm_keyboard, _normalize_custom_date, _focus_summary_lines, _appointment_focus_keyboard, _appointment_focus_prompt
from .flows import start_appointment as _start_appointment_impl, show_search_entry, show_precise_filter, show_appointment_hub, show_service_hub, show_favorites, show_help, contact_management
from .message_handlers import handle_main_message, handle_find_area, handle_find_budget, cmd_find, cmd_favorites, cmd_appointments, cmd_help, cmd_service, cmd_admin_list, cmd_admin_add, cmd_admin_remove, cmd_contact, cmd_search, cmd_about
from .admin_commands import cmd_deal_done, cmd_lead_response, cmd_push_local, cmd_push_all
from .callbacks import handle_ui_callback as _handle_ui_callback_impl
from .jobs import lease_reminder_job, rent_day_reminder_job
from .appointment_flow import appoint_flow_cb as _appoint_flow_cb_impl, handle_appointment_text
from .app import cancel, error_handler, build_application as _build_application_impl, main


def build_application():
    """Compatibility facade that honors runtime overrides on this module."""
    return _build_application_impl(token=USER_BOT_TOKEN)


async def start_appointment(update, context, listing_id, **kwargs):
    return await _start_appointment_impl(
        update, context, listing_id, render_panel_fn=render_panel, **kwargs
    )


async def appoint_flow_cb(update, context):
    return await _appoint_flow_cb_impl(
        update, context, create_lead_fn=create_lead, notify_admins_fn=_notify_admins
    )


async def route_start_arg(update, context, arg):
    return await _route_start_arg_impl(update, context, arg, create_lead_fn=create_lead)


async def start(update, context):
    return await _start_impl(
        update,
        context,
        upsert_user_profile_fn=upsert_user_profile,
        create_lead_fn=create_lead,
    )


async def handle_ui_callback(update, context):
    return await _handle_ui_callback_impl(
        update,
        context,
        hooks={
            'upsert_user_profile': upsert_user_profile,
            'create_lead': create_lead,
            'notify_admins': _notify_admins,
            'search_listings': search_listings_with_fallback,
            'send_results': send_find_results_as_cards,
        },
    )

if __name__ == '__main__':
    main()
