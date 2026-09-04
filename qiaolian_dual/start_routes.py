"""User Bot /start 与频道深链路由。"""
from __future__ import annotations

from .common import *


async def route_start_arg(update: Update, context: ContextTypes.DEFAULT_TYPE, arg: str, *, create_lead_fn=None) -> int | None:
    from .admin_contract import _binding_contract_text, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .flows import contact_management, show_search_entry, show_service_hub, start_appointment
    from .keyboards_common import contact_handoff_keyboard, keyword_followup_keyboard, latest_listing_keyboard, main_keyboard, no_match_followup_keyboard, room_type_keyboard
    from .keyboards_search import find_area_keyboard, find_budget_keyboard, precise_filter_keyboard
    from .listing import (
        _latest_listing_text,
        _resolve_area_from_target,
        _store_active_entry,
        channel_topic_welcome_text,
        listing_action_allowed,
        listing_context,
        listing_cost_keyboard,
        listing_cost_text,
        listing_entry_keyboard,
        listing_entry_text,
        listing_is_available,
        listing_unavailable_keyboard,
        listing_unavailable_text,
        start_video_tour_flow,
    )
    from .results_admin import _allow_admin_notify, _format_listing_choice_lines, _notify_admins, send_listing_photo_preview
    from .search import create_lead as default_create_lead, detect_area
    from .session_deeplink import _normalize_variant, _split_target_meta, build_source_label, now_ts, parse_start_arg_payload, resolve_public_token
    from .texts import about_text, brand_story_text, want_home_prompt_text
    from .utils_formatting import _display_listing_id, _internal_listing_id

    user = update.effective_user
    message = update.effective_message
    create_lead = create_lead_fn or default_create_lead
    payload = parse_start_arg_payload(arg)
    if payload is None:
        return None

    action = str(payload.get('action') or '')
    raw_target = str(payload.get('target') or '')
    if payload.get('opaque_token'):
        target = resolve_public_token(payload.get('opaque_token'))
        target_meta = {}
        if not target:
            return None
    else:
        target, target_meta = _split_target_meta(raw_target)
    target = target or raw_target
    if action in {'appoint', 'consult', 'photos', 'details', 'book', 'similar', 'video', 'fav', 'discussion_entry'}:
        target = _internal_listing_id(target)

    post_token = str(payload.get('post_token') or '')
    channel_message_id = payload.get('channel_message_id')
    source = build_source_label(post_token)
    touch_payload = {
        'start_arg': arg,
        'post_token': post_token,
        'channel_message_id': channel_message_id,
        'first_touch_action': action,
    }
    if target_meta:
        touch_payload['target_meta'] = target_meta

    if action in {'appoint', 'book'}:
        listing_id = target
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = touch_payload
        _store_active_entry(context, arg=arg, action='appoint', listing_id=listing_id, touch_payload=touch_payload)
        available, reason = listing_is_available(listing_id)
        if not available:
            if reason == 'missing':
                create_lead(user, action='broken_link', source=source, listing_id=listing_id, payload={**touch_payload, 'reason': reason})
            await message.reply_text(listing_unavailable_text(reason, listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        listing_info = listing_context(listing_id)
        caption_variant = _normalize_variant(target_meta.get('cv')) or _normalize_variant(listing_info.get('caption_variant')) or 'a'
        initial_mode = str(target_meta.get('mode') or '').strip().lower()
        create_lead(user, action='appointment_click', source=source, listing_id=listing_id, payload={**touch_payload, 'preferred_mode': initial_mode, 'caption_variant': caption_variant})
        entry_source = str(target_meta.get('entry') or target_meta.get('src') or '').strip().lower()
        if entry_source == 'discussion' and _allow_admin_notify(context, key=f'discussion_appoint:{listing_id}:{post_token}:{int(user.id)}', cooldown_seconds=180):
            await _notify_admins(context, title='讨论区预约点击', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'房源：{he(_display_listing_id(listing_id))}'])
        return await start_appointment(update, context, listing_id, source=source, touch_payload={**touch_payload, 'entry': entry_source}, initial_mode=initial_mode)

    if action == 'consult':
        listing_id = target
        allowed, reason = listing_action_allowed(listing_id, 'consult')
        if not allowed:
            await message.reply_text(listing_unavailable_text(reason, listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = touch_payload
        _store_active_entry(context, arg=arg, action=action, listing_id=listing_id, touch_payload=touch_payload)
        create_lead(user, action='consult_click', source=source, listing_id=listing_id, payload=touch_payload)
        return await contact_management(update, context, source=source, from_listing=listing_id)

    if action == 'photos':
        listing_id = target
        allowed, reason = listing_action_allowed(listing_id, 'photos')
        if not allowed:
            await message.reply_text(listing_unavailable_text(reason, listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = {**touch_payload, 'entry': 'photos'}
        _store_active_entry(context, arg=arg, action=action, listing_id=listing_id, touch_payload=touch_payload)
        create_lead(user, action='photos_click', source=source, listing_id=listing_id, payload=touch_payload)
        await send_listing_photo_preview(context.bot, message.chat_id, listing_id)
        return MAIN

    if action == 'details':
        listing_id = target
        allowed, reason = listing_action_allowed(listing_id, 'detail')
        if not allowed:
            await message.reply_text(listing_unavailable_text(reason, listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        context.user_data['contact_listing_id'] = listing_id
        _store_active_entry(context, arg=arg, action=action, listing_id=listing_id, touch_payload=touch_payload)
        create_lead(user, action='listing_detail_view', source=source, listing_id=listing_id, payload=touch_payload)
        await message.reply_text(listing_cost_text(listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_cost_keyboard(listing_id))
        return MAIN

    if action in {'index_area', 'area_index'}:
        context.user_data['search_pref'] = {'source': 'channel_index', 'goal': 'any', 'touch_payload': touch_payload}
        await message.reply_text('📍 <b>想住哪里？</b>\n\n选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA
    if action == 'index_budget':
        context.user_data['search_pref'] = {'source': 'channel_index', 'goal': 'any', 'area': '', 'touch_payload': touch_payload}
        await message.reply_text('💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'))
        return FIND_BUDGET
    if action == 'index_layout':
        await message.reply_text('🛏 <b>想要什么户型？</b>', parse_mode=ParseMode.HTML, reply_markup=room_type_keyboard())
        return MAIN
    if action in {'index_latest', 'latest'}:
        await message.reply_text(_latest_listing_text(), parse_mode=ParseMode.HTML, reply_markup=latest_listing_keyboard())
        return MAIN
    if action == 'index_video':
        return await start_video_tour_flow(update, context, source='channel_index')
    if action in {'index_advisor', 'consult_general', 'cooperate'}:
        create_lead(user, action=action, source=source, payload=touch_payload)
        return await contact_management(update, context, source='channel_index')
    if action == 'index_service':
        return await show_service_hub(update, context)

    if action == 'tenant_bind':
        # 历史深链仍可由顾问发给客户完成后台核实绑定；首页不再暴露自助绑定按钮。
        binding = db.bind_by_code(user.id, target)
        if not binding:
            await message.reply_text('这个绑定链接已失效或已使用过。\n请联系中文顾问核对。', reply_markup=contact_handoff_keyboard())
            return MAIN
        create_lead(user, action='tenant_bind_success', source='tenant_bind', listing_id=str(binding.get('property_name') or ''), payload={'binding_code': target, 'binding_id': binding.get('id')})
        await message.reply_text('✅ <b>租约档案已核对</b>\n\n' + _binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
        return MAIN

    if action == 'channel_topic':
        topic = target
        context.user_data.pop('contact_listing_id', None)
        create_lead(user, action='channel_topic_click', source='channel_topic', area=detect_area(topic) if topic else '', payload={'topic': topic})
        if topic == 'district_guide':
            context.user_data['search_pref'] = {'source': 'channel_topic', 'goal': 'any', 'touch_payload': {'topic': topic}}
            await message.reply_text(channel_topic_welcome_text(topic), reply_markup=find_area_keyboard())
            return FIND_AREA
        if topic == 'service':
            return await show_service_hub(update, context)
        if topic == 'video_tour':
            return await start_video_tour_flow(update, context, source='channel_topic')
        await message.reply_text(channel_topic_welcome_text(topic), reply_markup=main_keyboard())
        return MAIN

    if action == 'discussion_entry':
        listing_id = target
        context.user_data['contact_listing_id'] = listing_id
        create_lead(user, action='discussion_entry_click', source='discussion_entry', listing_id=listing_id, payload=touch_payload)
        await _notify_admins(context, title='讨论区入口点击', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'房源：{he(_display_listing_id(listing_id))}'])
        # 讨论区进入仍展示当前房源上下文；预约是否可用由按钮层按房态决定。
        if not db.get_listing(listing_id):
            await message.reply_text('这套房源目前无法找到。', reply_markup=no_match_followup_keyboard())
            return MAIN
        await message.reply_text(listing_entry_text(listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_entry_keyboard(listing_id))
        return MAIN

    if action == 'fav':
        listing_id = target
        if not db.get_listing(listing_id):
            return None
        db.favorite_listing(user.id, listing_id, now_ts())
        create_lead(user, action='favorite_click', source=source, listing_id=listing_id, payload=touch_payload)
        await message.reply_text('❤️ 这套先帮你记下了。', reply_markup=listing_entry_keyboard(listing_id))
        return MAIN

    if action in {'more', 'similar'}:
        area, listing_id = _resolve_area_from_target(target)
        if action == 'similar' and listing_id:
            area = str(listing_context(listing_id).get('area') or area)
        create_lead(user, action=f'{action}_click', source=source, area=area, listing_id=listing_id, payload=touch_payload)
        matches_found = [item for item in db.search_listings(areas=[area] if area and area != '不限' else None, limit=5) if str(item.get('status') or '').lower() in {'active', 'reserved'}]
        if matches_found:
            from .results_admin import send_find_results_as_cards
            await send_find_results_as_cards(update, context, matches_found, 'strict')
        else:
            await message.reply_text('当前同区域暂时没有可预约房源。\n可以调整条件，或联系中文顾问继续留意。', reply_markup=no_match_followup_keyboard())
        return MAIN

    if action == 'video':
        listing_id = target
        info = listing_context(listing_id)
        area = str(info.get('area') or '')
        create_lead(user, action='video_click', source=source, listing_id=listing_id, area=area, payload=touch_payload)
        # 视频不是独立产品，最终仍回到具体房源预约链路。
        if listing_id and listing_is_available(listing_id)[0]:
            return await start_appointment(update, context, listing_id, source=source, touch_payload=touch_payload, initial_mode='video')
        return await start_video_tour_flow(update, context, source=source, area=area)

    if action == 'find_home':
        return await show_search_entry(update, context)

    # 旧品牌/关于深链只保留兼容，不影响首页；明确请求时仍可查看内容。
    if action in {'brand', 'about', 'want_home', 'ask'}:
        create_lead(user, action=f'{action}_click', source=source, payload=touch_payload)
        if action == 'brand':
            await message.reply_text(brand_story_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        elif action == 'about':
            await message.reply_text(about_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        elif action == 'want_home':
            context.user_data['pref_select'] = {'source': 'channel_want_home', 'selected': []}
            await message.reply_text(want_home_prompt_text(), parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()))
        else:
            return await contact_management(update, context, source=source)
        return MAIN

    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, *, upsert_user_profile_fn=None, create_lead_fn=None) -> int:
    from .keyboards_common import main_keyboard, no_match_followup_keyboard
    from .search import upsert_user_profile as default_upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    from .texts import channel_welcome_text

    user = update.effective_user
    upsert_user_profile = upsert_user_profile_fn or default_upsert_user_profile
    upsert_user_profile(user)

    if context.args:
        arg = context.args[0]
        clear_session_for_fresh_entry(context)
        context.user_data.pop('resume_start_arg', None)
        state = await route_start_arg(update, context, arg, create_lead_fn=create_lead_fn)
        if state is not None:
            return state
        clear_session_for_fresh_entry(context)
        await update.effective_message.reply_text('这个房源入口已经失效。\n\n可以返回找房，或联系中文顾问确认。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard())
        return MAIN

    # 无论是否已绑定租约，直接打开 Bot 都先进入统一五键首页。
    clear_session_for_fresh_entry(context)
    context.user_data.clear()
    await update.effective_message.reply_text(
        channel_welcome_text(first_name=getattr(user, 'first_name', '') or getattr(user, 'full_name', '') or ''),
        reply_markup=main_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return MAIN
