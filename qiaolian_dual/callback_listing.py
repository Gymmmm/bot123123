"""Callback handlers for the listing domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (data == 'find:show_more') or (data.startswith('findcard:')) or (data.startswith('listing:open:')) or (data.startswith('listing:photos:')) or (data.startswith('listing:appoint:')) or (data.startswith('listing:consult:')) or (data.startswith('listing:detail:')) or (data.startswith('listing:similar:'))


async def handle_listing_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _binding_contract_text, _binding_days_left, _binding_end_date, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .appointments_view import _appointment_card_keyboard, _appointment_date_compact, _appointment_details_keyboard, _appointment_listing_compact, _appointment_time_compact, _find_user_appointment, appointment_details_text, list_favorites_text, list_recent_appointments, old_tenant_binding_text
    from .flows import contact_management, show_appointment_hub, show_favorites, show_help, show_precise_filter, show_search_entry, show_service_hub, start_appointment
    from .keyboards_common import _advisor_listing_url, _listing_channel_url, contact_handoff_keyboard, keyword_followup_keyboard, latest_listing_keyboard, lead_capture_keyboard, main_keyboard, no_match_followup_keyboard, old_tenant_followup_keyboard, room_type_keyboard
    from .keyboards_search import _decode_budget_choice, find_area_keyboard, find_budget_keyboard, guided_search_keyboard, local_life_keyboard, merchant_join_keyboard, precise_filter_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_detail_keyboard, service_hub_keyboard, service_repair_keyboard
    from .listing import _latest_listing_text, listing_action_allowed, listing_context, listing_cost_text, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text, start_video_tour_flow
    from .results_admin import _allow_admin_notify, _format_listing_choice_lines, _format_match_line, _notify_admins, admin_lead_keyboard, search_results_keyboard, send_find_result_card, send_find_results_as_cards, send_listing_card, send_listing_photo_preview
    from .search import create_lead, detect_area, detect_property_type, search_listings_with_fallback, upsert_user_profile
    from .session_deeplink import _remember_video_pref, clear_session_for_fresh_entry, now_ts, user_display_name
    from .start_routes import route_start_arg
    from .texts import advisor_handoff_text, advisor_text, brand_story_text, deposit_text, lead_capture_text, listing_detail_text, local_life_text, promise_text, render_panel, rfcity_text, service_hub_text, smart_search_text, want_home_ack_text, welcome_text
    if data.startswith('findcard:'):
            parts = data.split(':', 2)
            value = parts[1] if len(parts) > 1 else ''
            if value == 'noop':
                return MAIN
            try:
                index = int(value)
            except (TypeError, ValueError):
                return MAIN
            # New callbacks carry both real index and target listing_id; old
            # findcard:{index} callbacks remain compatible.
            if len(parts) == 3 and parts[2] not in {'', 'unknown'}:
                ids = list(context.user_data.get('find_card_listing_ids') or [])
                if 0 <= index < len(ids) and ids[index] != parts[2]:
                    return MAIN
            await send_find_result_card(update, context, index, replace=True)
            return MAIN
    if data.startswith('listing:photos:'):
            lid = data.split(':', 2)[2]
            is_available, availability_reason = listing_is_available(lid)
            if not is_available:
                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)
                return MAIN
            context.user_data['contact_listing_id'] = lid
            try:
                await send_listing_photo_preview(context.bot, update.effective_chat.id, lid)
            except Exception:
                logger.exception('完整相册发送失败: listing_id=%s', lid)
                await render_panel(
                    update,
                    text='📸 <b>完整实拍</b>\n\n这套房的实拍暂时无法加载。你可以稍后再试，或联系中文顾问补充图片。',
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{lid}')],
                        [InlineKeyboardButton('⬅️ 返回这套房', callback_data=f'listing:open:{lid}')],
                    ]),
                    context=context,
                )
            return MAIN
    if data == 'find:show_more':
            # Legacy callback only: fold any old remainder into the same single-card carousel.
            ids = list(context.user_data.get('find_card_listing_ids') or [])
            ids.extend(str(value) for value in (context.user_data.pop('find_more_listing_ids', []) or []) if str(value or '').strip())
            ids = list(dict.fromkeys(value for value in ids if value))
            if not ids:
                ids = [str(item.get('listing_id') or '') for item in db.list_recent_listings(10) if item.get('listing_id')]
            context.user_data['find_card_listing_ids'] = ids
            if ids:
                await send_find_result_card(update, context, 0, replace=True)
            else:
                await render_panel(update, text='暂时没有可以安排看房的房源。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)
            return MAIN
    if data.startswith('listing:open:'):
            lid = data.split(':', 2)[2]
            logger.info(f'[CALLBACK] listing:open triggered, listing_id={lid}')
            is_available, availability_reason = listing_is_available(lid)
            if not is_available:
                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)
                return MAIN
            item = listing_context(lid)
            if not item or not item.get('listing_id'):
                await render_panel(update, text='这套房源可能已下架。我可以继续帮你找相近房源。', reply_markup=no_match_followup_keyboard(), context=context)
                return MAIN
            context.user_data['contact_listing_id'] = lid
            listing_id = item.get('listing_id', '')
            media_file = item.get('media_file_id', '')
            media_files = item.get('media_files', [])
            detail_text = listing_cost_text(listing_id)
            result_ids = list(context.user_data.get('find_card_listing_ids') or [])
            try:
                result_index = result_ids.index(str(listing_id))
                back_callback = f'findcard:{result_index}'
                back_label = '⬅️ 返回推荐'
            except ValueError:
                back_callback = 'home'
                back_label = '🏠 返回首页'
            detail_rows = [
                [InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{listing_id}'), InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}')],
                [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{listing_id}')],
                [InlineKeyboardButton(back_label, callback_data=back_callback)],
            ]
            detail_keyboard = InlineKeyboardMarkup(detail_rows)
            bot = context.bot
            chat_id = update.effective_chat.id
            if getattr(query.message, 'photo', None):
                await query.edit_message_caption(caption=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_keyboard)
                return MAIN
            available_photos = []
            if media_files:
                for mf in media_files:
                    if isinstance(mf, str) and os.path.exists(mf):
                        available_photos.append(mf)
            elif media_file and os.path.exists(media_file):
                available_photos.append(media_file)
            # 房源详情只展示一张封面；完整相册由用户主动点击“更多实拍”查看。
            available_photos = available_photos[:1]
            if len(available_photos) == 1:
                try:
                    with open(available_photos[0], 'rb') as photo:
                        await bot.send_photo(chat_id=chat_id, photo=photo, caption=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_keyboard)
                except Exception as e:
                    logger.error(f'发送单图失败: {e}')
                    await bot.send_message(chat_id=chat_id, text=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_keyboard)
            else:
                await bot.send_message(chat_id=chat_id, text=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_keyboard)
            return MAIN
    if data.startswith('listing:appoint:'):
            logger.info(f'[CALLBACK] listing:appoint triggered, data={data}')
            parts = data.split(':', 3)
            logger.info(f'[CALLBACK] parts={parts}, len={len(parts)}')
            if len(parts) == 4 and parts[2] in {'offline', 'video'}:
                mode, lid = (parts[2], parts[3])
                context.user_data['contact_listing_id'] = lid
                return await start_appointment(update, context, lid, source='listing_card', touch_payload={'listing_id': lid}, initial_mode=mode)
            elif len(parts) == 3:
                lid = parts[2]
                context.user_data['contact_listing_id'] = lid
                return await start_appointment(update, context, lid, source='listing_card', touch_payload={'listing_id': lid})
            return MAIN
    if data.startswith('listing:consult:'):
            lid = data.split(':', 2)[2]
            is_available, availability_reason = listing_action_allowed(lid, 'consult')
            if not is_available:
                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)
                return MAIN
            context.user_data['contact_listing_id'] = lid
            context.user_data['contact_touch_payload'] = {'listing_id': lid, 'entry': 'listing_card'}
            return await contact_management(update, context, source='listing_card', from_listing=lid)
    if data.startswith('listing:detail:'):
            lid = data.split(':', 2)[2]
            is_available, availability_reason = listing_action_allowed(lid, 'detail')
            if not is_available:
                await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(lid), context=context)
                return MAIN
            item = db.get_listing(lid) if lid else None
            if not item:
                await render_panel(update, text='未找到该房源详情，可能已下架。', reply_markup=main_keyboard(), context=context)
                return MAIN
            context.user_data['contact_listing_id'] = lid
            create_lead(user, action='listing_detail_view', source='listing_landing', listing_id=lid)
            detail_rows = []
            if availability_reason in {'active', 'reserved'}:
                detail_rows.append([InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{lid}')])
                detail_rows.append([InlineKeyboardButton('📸 更多实拍', callback_data=f'listing:photos:{lid}')])
            detail_rows.extend([
                [InlineKeyboardButton('💬 联系中文顾问', callback_data=f'listing:consult:{lid}')],
                [InlineKeyboardButton(
                    '⬅️ 返回这套房' if availability_reason in {'active', 'reserved'} else '🏠 返回首页',
                    callback_data=f'listing:open:{lid}' if availability_reason in {'active', 'reserved'} else 'home',
                )],
            ])
            detail_kb = InlineKeyboardMarkup(detail_rows)
            detail_text = listing_cost_text(lid)
            if getattr(query.message, 'photo', None):
                await query.edit_message_caption(caption=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb)
            else:
                await render_panel(update, text=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb, context=context)
            return MAIN
    if data.startswith('listing:similar:'):
            lid = data.split(':', 2)[2]
            item = db.get_listing(lid) if lid else None
            if not item:
                await render_panel(update, text='未找到房源信息。', reply_markup=main_keyboard())
                return MAIN
            area = item.get('area', '')
            context.user_data['search_pref'] = {'source': 'similar_listing', 'area': area, 'touch_payload': {'from_listing': lid}}
            await render_panel(update, text=f'📍 为你查找 {area} 的相似房源\n\n请选择预算区间：', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'))
            return FIND_BUDGET
    return None
