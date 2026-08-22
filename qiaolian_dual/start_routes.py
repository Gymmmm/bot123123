"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def route_start_arg(update: Update, context: ContextTypes.DEFAULT_TYPE, arg: str, *, create_lead_fn=None) -> int | None:
    from .admin_contract import _binding_contract_text, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .flows import contact_management, show_search_entry, start_appointment
    from .keyboards_common import contact_handoff_keyboard, keyword_followup_keyboard, latest_listing_keyboard, main_keyboard, no_match_followup_keyboard, room_type_keyboard
    from .keyboards_search import find_area_keyboard, find_budget_keyboard, precise_filter_keyboard, service_hub_keyboard
    from .listing import _latest_listing_text, _resolve_area_from_target, _store_active_entry, channel_topic_welcome_text, listing_context, listing_cost_keyboard, listing_cost_text, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text, start_video_tour_flow
    from .results_admin import _allow_admin_notify, _format_listing_choice_lines, _notify_admins
    from .search import create_lead as default_create_lead, detect_area
    from .session_deeplink import _normalize_variant, _split_target_meta, build_source_label, now_ts, parse_start_arg_payload, resolve_public_token
    from .texts import about_text, advisor_text, brand_story_text, discussion_entry_welcome_text, service_hub_text, want_home_prompt_text
    from .utils_formatting import _display_listing_id
    user = update.effective_user
    create_lead = create_lead_fn or default_create_lead
    message = update.effective_message
    payload = parse_start_arg_payload(arg)
    if payload is None:
        return None
    action = payload['action']
    raw_target = payload['target']
    if payload.get('opaque_token'):
        target = resolve_public_token(payload.get('opaque_token'))
        target_meta = {}
        if not target:
            return None
    else:
        target, target_meta = _split_target_meta(raw_target)
    if not target:
        target = raw_target
    post_token = payload.get('post_token', '')
    channel_message_id = payload.get('channel_message_id')
    source = build_source_label(post_token)
    touch_payload = {'start_arg': arg, 'post_token': post_token, 'channel_message_id': channel_message_id, 'first_touch_action': action}
    if target_meta:
        touch_payload['target_meta'] = target_meta
    if action == 'appoint':
        listing_id = target
        listing_info = listing_context(listing_id)
        caption_variant = _normalize_variant(target_meta.get('cv')) or _normalize_variant(listing_info.get('caption_variant')) or 'a'
        touch_payload['caption_variant'] = caption_variant
        entry_source = str(target_meta.get('entry') or target_meta.get('src') or '').strip().lower()
        if entry_source:
            touch_payload['entry'] = entry_source
            touch_payload['entry_step'] = str(target_meta.get('step') or '').strip()
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = {**touch_payload, 'caption_variant': caption_variant}
        _store_active_entry(context, arg=arg, action=action, listing_id=listing_id, touch_payload={**touch_payload, 'caption_variant': caption_variant})
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            if availability_reason == 'missing':
                create_lead(user, action='broken_link', source=source, listing_id=listing_id, payload={**touch_payload, 'reason': availability_reason})
            await message.reply_text(listing_unavailable_text(), reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        initial_mode = str(target_meta.get('mode') or '').strip().lower()
        create_lead(user, action='appointment_click', source=source, listing_id=listing_id, payload={**touch_payload, 'preferred_mode': initial_mode, 'caption_variant': caption_variant})
        if entry_source == 'discussion' and _allow_admin_notify(context, key=f'discussion_appoint:{listing_id}:{post_token}:{int(user.id)}', cooldown_seconds=180):
            await _notify_admins(context, title='讨论区预约点击（首段）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房源：{he(_display_listing_id(listing_id or '-'))}", f'来源：{he(source)}', f"post_token：{he(post_token or '-')}", f"预约方式：{he(initial_mode or '未选择')}"])
        return await start_appointment(update, context, listing_id, source=source, touch_payload={**touch_payload, 'entry': entry_source or ''}, initial_mode=initial_mode)
    if action == 'consult':
        listing_id = target
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            if availability_reason == 'missing':
                create_lead(user, action='broken_link', source=source, listing_id=listing_id, payload={**touch_payload, 'reason': availability_reason})
            await message.reply_text(listing_unavailable_text(), reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        listing_info = listing_context(listing_id)
        caption_variant = _normalize_variant(target_meta.get('cv')) or str(listing_info.get('caption_variant') or 'a').lower()
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = {**touch_payload, 'caption_variant': caption_variant}
        _store_active_entry(context, arg=arg, action=action, listing_id=listing_id, touch_payload={**touch_payload, 'caption_variant': caption_variant})
        create_lead(user, action='consult_click', source=source, listing_id=listing_id, payload={**touch_payload, 'caption_variant': caption_variant})
        if arg.startswith('q_') or arg.startswith('q__'):
            await message.reply_text(listing_cost_text(listing_id), parse_mode=ParseMode.HTML, reply_markup=listing_cost_keyboard(listing_id))
            return MAIN
        await _notify_admins(context, title='咨询点击（频道深链）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房源：{he(_display_listing_id(listing_id or '-'))}", f'来源：{he(source)}'])
        return await contact_management(update, context, source=source, from_listing=listing_id)
    if action == 'index_area':
        context.user_data['search_pref'] = {'source': 'channel_index', 'goal': 'any', 'touch_payload': touch_payload}
        await message.reply_text('📍 <b>按区域找房</b>\n\n请选择区域：', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA
    if action == 'index_budget':
        context.user_data['search_pref'] = {'source': 'channel_index', 'goal': 'any', 'touch_payload': touch_payload}
        await message.reply_text('💰 <b>预算多少？</b>', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'))
        return FIND_BUDGET
    if action == 'index_layout':
        await message.reply_text('🛏 <b>按户型找房</b>\n\n请选择户型：', parse_mode=ParseMode.HTML, reply_markup=room_type_keyboard())
        return MAIN
    if action == 'index_latest':
        await message.reply_text(_latest_listing_text(), parse_mode=ParseMode.HTML, reply_markup=latest_listing_keyboard())
        return MAIN
    if action == 'index_video':
        return await start_video_tour_flow(update, context, source='channel_index')
    if action == 'index_advisor':
        return await contact_management(update, context, source='channel_index')
    if action == 'index_service':
        await message.reply_text(service_hub_text(), parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard())
        return MAIN
    if action == 'tenant_bind':
        binding_code = target
        binding = db.bind_by_code(user.id, binding_code)
        if not binding:
            create_lead(user, action='tenant_bind_invalid', source='tenant_bind', payload={'binding_code': binding_code})
            await message.reply_text('这个绑定链接已失效或已使用过。\n请联系我们重新获取绑定码，或直接发消息给我们 ↓', reply_markup=contact_handoff_keyboard())
            return MAIN
        create_lead(user, action='tenant_bind_success', source='tenant_bind', listing_id=str(binding.get('property_name') or ''), payload={'binding_code': binding_code, 'binding_id': binding.get('id')})
        _store_active_entry(context, arg=arg, action=action, listing_id=str(binding.get('property_name') or ''), touch_payload={'binding_code': binding_code, 'binding_id': binding.get('id')})
        await message.reply_text(f"✅ 已识别你的在租档案 {he(getattr(user, 'first_name', '') or '')} 🏠\n\n" + _binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
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
            await message.reply_text(channel_topic_welcome_text(topic), reply_markup=service_hub_keyboard())
            return MAIN
        if topic == 'video_tour':
            return await start_video_tour_flow(update, context, source='channel_topic')
        await message.reply_text(channel_topic_welcome_text(topic), reply_markup=main_keyboard())
        return MAIN
    if action == 'fav':
        listing_id = target
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            await message.reply_text(listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        db.favorite_listing(user.id, listing_id, now_ts())
        create_lead(user, action='favorite_click', source=source, listing_id=listing_id, payload=touch_payload)
        await message.reply_text('❤️ 这套先帮你记下了。', parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return MAIN
    if action == 'more':
        area, listing_id = _resolve_area_from_target(target)
        create_lead(user, action='more_area_click', source=source, area=area, listing_id=listing_id, payload={**touch_payload, 'resolved_area': area, 'listing_id': listing_id})
        matches = db.search_listings(areas=[area] if area and area != '不限' else None, limit=3)
        if matches:
            intro = ''
            if listing_id:
                intro = '🏠 这套房同区域还有这些在架房源，你可以直接继续看：\n\n'
            await message.reply_text(intro + _format_listing_choice_lines(matches), parse_mode=ParseMode.HTML, reply_markup=keyword_followup_keyboard(area=area))
        else:
            await message.reply_text(f"当前同区域（{he(area or '金边')}）暂无更多上架房源。\n已同步管理号继续盯新房；你也可以继续按预算或户型缩小一轮。", parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard())
        return MAIN
    if action in {'brand', 'about', 'want_home', 'ask'}:
        action_map = {'brand': 'brand_click', 'about': 'about_click', 'want_home': 'want_home_click', 'ask': 'ask_click'}
        create_lead(user, action=action_map[action], source=source, payload={'start_arg': arg, **touch_payload})
        if action == 'brand':
            await message.reply_text(brand_story_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        elif action == 'about':
            await message.reply_text(about_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        elif action == 'want_home':
            context.user_data['pref_select'] = {'source': 'channel_want_home', 'selected': []}
            await message.reply_text(want_home_prompt_text(), parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()))
        else:
            await _notify_admins(context, title='咨询入口点击（频道深链）', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'来源：{he(source)} ask'])
            await message.reply_text(advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard())
        return MAIN
    if action == 'discussion_entry':
        listing_id = target
        entry_source = str(target_meta.get('entry') or 'discussion').strip().lower() or 'discussion'
        context.user_data['contact_listing_id'] = listing_id or None
        create_lead(user, action='discussion_entry_click', source='discussion_entry', listing_id=listing_id, payload={'post_token': post_token, 'listing_id': listing_id, 'source': 'discussion_entry', 'entry': entry_source, **touch_payload})
        await _notify_admins(context, title='讨论区入口点击', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"房源：{he(_display_listing_id(listing_id or '-'))}", f"post_token：{he(post_token or '-')}"])
        first_name = str(getattr(user, 'first_name', '') or '')
        await message.reply_text(discussion_entry_welcome_text(first_name=first_name, listing_id=listing_id), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return MAIN
    if action == 'book':
        touch_payload['first_touch_action'] = 'book'
        initial_mode = str(target_meta.get('mode') or '').strip().lower()
        listing_id = target
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            if availability_reason == 'missing':
                create_lead(user, action='broken_link', source=source, listing_id=listing_id, payload={**touch_payload, 'reason': availability_reason})
            await message.reply_text(listing_unavailable_text(), reply_markup=listing_unavailable_keyboard(listing_id))
            return MAIN
        listing_info = listing_context(listing_id)
        caption_variant = _normalize_variant(target_meta.get('cv')) or _normalize_variant(listing_info.get('caption_variant')) or 'a'
        touch_payload['caption_variant'] = caption_variant
        context.user_data['contact_listing_id'] = listing_id
        context.user_data['contact_touch_payload'] = {**touch_payload}
        _store_active_entry(context, arg=arg, action='appoint', listing_id=listing_id, touch_payload=touch_payload)
        create_lead(user, action='appointment_click', source=source, listing_id=listing_id, payload={**touch_payload, 'preferred_mode': initial_mode})
        return await start_appointment(update, context, listing_id, source=source, touch_payload=touch_payload, initial_mode=initial_mode)
    if action == 'similar':
        listing_id = target
        area = ''
        if listing_id:
            area = str(listing_context(listing_id).get('area') or '')
        create_lead(user, action='similar_click', source=source, listing_id=listing_id, area=area, payload=touch_payload)
        matches = db.search_listings(areas=[area] if area and area != '不限' else None, limit=3)
        if matches:
            await message.reply_text(_format_listing_choice_lines(matches), parse_mode=ParseMode.HTML, reply_markup=keyword_followup_keyboard(area=area))
        else:
            await message.reply_text(f"当前同区域（{he(area or '金边')}）暂无更多在架房源，已同步顾问人工推荐。", parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard())
        return MAIN
    if action == 'video':
        listing_id = target
        info = listing_context(listing_id)
        area = str(info.get('area') or '')
        create_lead(user, action='video_click', source=source, listing_id=listing_id, area=area, payload=touch_payload)
        return await start_video_tour_flow(update, context, source=source, area=area)
    if action == 'find_home':
        return await show_search_entry(update, context)
    if action in ('area_index', 'index_area'):
        context.user_data['search_pref'] = {'source': 'deeplink', 'goal': 'any', 'touch_payload': touch_payload}
        await message.reply_text('📍 <b>按区域找房</b>\n\n请选择区域：', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA
    if action in ('latest', 'index_latest'):
        await message.reply_text(_latest_listing_text(), parse_mode=ParseMode.HTML, reply_markup=latest_listing_keyboard())
        return MAIN
    if action in ('cooperate', 'consult_general'):
        create_lead(user, action=action, source=source, payload=touch_payload)
        await _notify_admins(context, title=f'深链入口点击：{action}', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}'])
        await message.reply_text(advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard())
        return MAIN
    return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE, *, upsert_user_profile_fn=None, create_lead_fn=None) -> int:
    from .admin_contract import _binding_contract_text, _contract_actions_keyboard
    from .keyboards_common import main_keyboard
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
        await update.effective_message.reply_text('链接已失效。\n\n' + copy_home_text(), parse_mode=ParseMode.HTML, reply_markup=main_keyboard())
        return MAIN
    clear_session_for_fresh_entry(context)
    context.user_data.clear()
    binding = db.get_active_binding(user.id)
    if binding:
        await update.effective_message.reply_text('✅ <b>已识别你的在租档案</b>\n\n' + _binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id))
        return MAIN
    await update.effective_message.reply_text(channel_welcome_text(first_name=getattr(user, 'first_name', '') or getattr(user, 'full_name', '') or ''), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    return MAIN
