"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def handle_main_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .keyboards_common import contact_handoff_keyboard, keyword_followup_keyboard, main_keyboard
    from .keyboards_search import search_entry_keyboard, service_hub_keyboard
    from .listing import _keyword_intro_text, listing_context, start_video_tour_flow
    from .results_admin import _allow_admin_notify, _notify_admins, send_find_results_as_cards
    from .search import create_lead, detect_area, detect_property_type, detect_room_type, parse_budget_range, search_listings_with_fallback, upsert_user_profile
    from .session_deeplink import _remember_video_pref, clear_session_for_fresh_entry
    from .texts import render_panel, welcome_text
    from .utils_formatting import _display_listing_id
    user = update.effective_user
    # Channel/service updates may not have an effective user or user_data.
    # They must not enter the user conversation state machine.
    if user is None or context.user_data is None:
        logger.info('route=handle_main_message skipped_non_user_update update_id=%s', getattr(update, 'update_id', None))
        return MAIN
    upsert_user_profile(user)
    text = (update.effective_message.text or '').strip()
    logger.info('route=handle_main_message update_id=%s user_id=%s', getattr(update, 'update_id', None), getattr(user, 'id', None))
    from .admin_contract_ui import handle_message as handle_admin_contract_message
    admin_contract_result = await handle_admin_contract_message(update, context)
    if admin_contract_result is not None:
        return admin_contract_result
    old_customer = context.user_data.pop('awaiting_old_customer', None)
    if old_customer is not None:
        if len(text) < 4:
            context.user_data['awaiting_old_customer'] = True
            await update.effective_message.reply_text('请发姓氏 + 手机尾号 4 位，或签约时使用的 Telegram 账号。')
            return MAIN
        create_lead(user, action='repeat_tenant_details', source='service_hub', payload={'details': text[:500]})
        await _notify_admins(context, title='老客档案查询', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'查询信息：<code>{he(text[:500])}</code>'])
        await render_panel(update, text='✅ <b>租客档案已提交</b>\n\n顾问核实后，会把租约和房屋信息绑定到当前账号。\n之后报修或联系物业时，不用再重复填写；租约到期前 7 天也会提醒你。\n🔐 信息仅用于档案核实。', parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(user.id), context=context)
        return MAIN
    service_request = context.user_data.pop('awaiting_service_request', None)
    if isinstance(service_request, dict):
        if len(text) < 4:
            context.user_data['awaiting_service_request'] = service_request
            await update.effective_message.reply_text('请简单描述发生了什么，例如：B栋3楼走廊灯坏了。')
            return MAIN
        service_request['detail'] = text[:800]
        context.user_data['service_request_detail'] = service_request
        issue_key = str(service_request.get('issue_key') or 'repair_other')
        await render_panel(update, text=f'✅ 已记录：{he(text[:500])}\n\n选希望处理的时间：', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🚨 今天内', callback_data=f'service_slot:{issue_key}:today'), InlineKeyboardButton('🕘 明天上午', callback_data=f'service_slot:{issue_key}:tomorrow_am')], [InlineKeyboardButton('🕒 明天下午', callback_data=f'service_slot:{issue_key}:tomorrow_pm')], [InlineKeyboardButton('⬅️ 返回入住后服务', callback_data='service:hub')]]), context=context)
        return MAIN
    general_kind = None
    if context.user_data.pop('awaiting_service_general', None):
        general_kind = '通用服务咨询'
    elif context.user_data.pop('awaiting_nearby_request', None):
        general_kind = '周边需求'
    if general_kind:
        if len(text) < 2:
            context.user_data['awaiting_service_general' if general_kind == '通用服务咨询' else 'awaiting_nearby_request'] = True
            await update.effective_message.reply_text('请简单说一下需要什么帮助。')
            return MAIN
        create_lead(user, action='service_general', source='service_hub', payload={'kind': general_kind, 'details': text[:800]})
        await _notify_admins(context, title=general_kind, lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'需求：<code>{he(text[:700])}</code>'])
        await render_panel(update, text='✅ 已收到你的需求\n\n顾问会根据这条内容联系你。', parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(user.id), context=context)
        return MAIN
    kctx = context.user_data.pop('awaiting_keyword_find', None)
    if kctx is not None:
        if not text:
            await render_panel(update, text='发一句需求就可以，例如：<code>BKK1 预算800内 一房 安静</code>。', parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context, prefer_edit_anchor=True)
            return MAIN
        budget_min, budget_max = parse_budget_range(text)
        area_raw = detect_area(text)
        area_use = area_raw if area_raw != text[:40] else ''
        property_type = detect_property_type(text)
        matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=area_use, budget_min=budget_min, budget_max=budget_max, text_fragment=text, limit=5)
        logger.info('route=awaiting_keyword update_id=%s user_id=%s area=%s budget=%s-%s mode=%s matched=%d ids=%s', getattr(update, 'update_id', None), getattr(user, 'id', None), area_use or '-', budget_min, budget_max, match_mode, len(matches), ','.join((str(item.get('listing_id') or '-') for item in matches[:5])))
        source = str(kctx.get('source', 'smart_find_play'))
        create_lead(user, action='keyword_find_play', source=source, area=area_use, property_type=property_type, budget_min=budget_min, budget_max=budget_max, payload={'message': text, 'match_mode': match_mode})
        if _allow_admin_notify(context, key=f'search_activity:{int(user.id)}', cooldown_seconds=600):
            notify_title = '找房需求（顾问协助）' if source == 'advisor_handoff' else '找房需求（普通线索）'
            await _notify_admins(context, title=notify_title, lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'模式：{he(match_mode)}', f'需求：<code>{he(text[:700])}</code>', '说明：10 分钟内重复搜索仅记录，不重复提醒'])
        if source == 'advisor_handoff':
            await update.effective_message.reply_text('✅ 已收到你的需求。我先为你匹配当前可预约房源；顾问也会继续按这些条件帮你筛选。', reply_markup=main_keyboard())
        await send_find_results_as_cards(update, context, matches, match_mode)
        return MAIN
    normalized_text = text.lower()
    # Guided state wins over natural-language detection so typing an area does
    # not silently discard the saved area/budget session.
    pref = context.user_data.get('search_pref') or {}
    if pref.get('goal') or pref.get('area'):
        await render_panel(update, text='已保留你的筛选进度。请点上方的区域或预算按钮继续；想换一种找法，可点「🏠 返回首页」重新开始。', reply_markup=main_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    natural_area = detect_area(text)
    area_use = natural_area if natural_area != text[:40] else ''
    room_type = detect_room_type(text)
    budget_min, budget_max = parse_budget_range(text)
    property_type = detect_property_type(text)
    wants_video = any((token in normalized_text for token in ('视频看房', '视频代看', '实拍', '视频')))
    if area_use or room_type or budget_min is not None or (budget_max is not None):
        _remember_video_pref(context, area=area_use or None, budget_min=budget_min, budget_max=budget_max, layout=room_type or property_type or None)
    if wants_video:
        return await start_video_tour_flow(update, context, source='natural_keyword', area=area_use, budget_min=budget_min, budget_max=budget_max, layout=room_type or property_type)
    if area_use or room_type or budget_min is not None or (budget_max is not None):
        matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=area_use, budget_min=budget_min, budget_max=budget_max, text_fragment=f'{text} {room_type}'.strip(), limit=5)
        logger.info('route=natural_keyword update_id=%s user_id=%s area=%s budget=%s-%s mode=%s matched=%d ids=%s', getattr(update, 'update_id', None), getattr(user, 'id', None), area_use or '-', budget_min, budget_max, match_mode, len(matches), ','.join((str(item.get('listing_id') or '-') for item in matches[:5])))
        create_lead(user, action='keyword_find_play', source='natural_keyword', area=area_use, property_type=property_type, budget_min=budget_min, budget_max=budget_max, payload={'message': text[:700], 'match_mode': match_mode, 'room_type': room_type})
        if matches:
            await send_find_results_as_cards(update, context, matches, match_mode)
        else:
            await render_panel(update, text=_keyword_intro_text(area=area_use, room_type=room_type, budget_min=budget_min, budget_max=budget_max) + '\n\n暂时没有完全符合条件、可以预约看房的房源。你可以换一个预算或位置，也可以让中文顾问继续帮你找。', parse_mode=ParseMode.HTML, reply_markup=keyword_followup_keyboard(area=area_use, room_type=room_type), context=context, prefer_edit_anchor=True)
        return MAIN
    if text in {'🏠 返回首页', '🏠 返回首页'}:
        clear_session_for_fresh_entry(context)
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context, prefer_edit_anchor=True)
        return MAIN
    if any((token in normalized_text for token in ('找房', '租房', '房子', '公寓', '别墅'))):
        await render_panel(update, text='🔍 <b>我来帮你找</b>\n\n直接发一句需求就可以，例如：\n<code>BKK1 800以内 一房</code>\n<code>钻石岛 两房 下月入住</code>\n\n不想打字，也可以点按钮选择。', parse_mode=ParseMode.HTML, reply_markup=search_entry_keyboard(), context=context, prefer_edit_anchor=True)
        return MAIN
    contact_listing_id = str(context.user_data.get('contact_listing_id') or '').strip()
    if contact_listing_id:
        item = listing_context(contact_listing_id)
        contact_touch_payload = context.user_data.get('contact_touch_payload') or {}
        create_lead(user, action='consult_message', source='listing_chat', listing_id=contact_listing_id, area=str(item.get('area') or ''), property_type=str(item.get('property_type') or ''), payload={'message': text[:700], **contact_touch_payload})
        await _notify_admins(context, title='房源咨询留言', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'房源：{he(_display_listing_id(contact_listing_id))}', f'留言：<code>{he(text[:700])}</code>'])
        await render_panel(update, text='已收到你对这套房的留言，顾问会按这条内容继续跟进。\n\n如果方便，也可以直接点下方按钮预约看房或直连顾问。', reply_markup=contact_handoff_keyboard(listing_id=contact_listing_id), parse_mode=ParseMode.HTML, context=context, prefer_edit_anchor=True)
        return MAIN
    await render_panel(update, text='我可以帮你找房。直接发「BKK1、500以内、一房、视频看房」这类关键词，或从下方选择一个入口开始。', reply_markup=main_keyboard(), context=context, prefer_edit_anchor=True)
    return MAIN

async def handle_find_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import find_budget_keyboard
    from .search import detect_area
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel
    area = detect_area((update.effective_message.text or '').strip())
    current = context.user_data.get('search_pref') or {}
    goal = current.get('goal') or 'any'
    context.user_data['search_pref'] = {'area': area, 'source': current.get('source', 'user_search'), 'goal': goal, 'touch_payload': current.get('touch_payload') or {}}
    _remember_video_pref(context, area=area)
    await render_panel(update, text=find_area_budget_hint_text(), parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard(goal), context=context, prefer_edit_anchor=True)
    return FIND_BUDGET

async def handle_find_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _budget_text, _user_contact_text, _user_mention_html
    from .keyboards_common import no_match_followup_keyboard
    from .results_admin import _notify_admins, send_find_results_as_cards
    from .search import create_lead, detect_property_type, detect_room_type, parse_budget_range, search_listings_with_fallback
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel
    user = update.effective_user
    text = (update.effective_message.text or '').strip()
    pref = context.user_data.pop('search_pref', {})
    budget_min, budget_max = parse_budget_range(text)
    property_type = detect_property_type(text)
    area = pref.get('area', '')
    goal = str(pref.get('goal') or '')
    room_hint = detect_room_type(text) or ('' if goal in {'', 'any', '住宅'} else goal)
    _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=room_hint or None)
    create_lead(user, action='search_pref_submit', source=pref.get('source', 'user_search'), area=area, property_type=property_type, budget_min=budget_min, budget_max=budget_max, payload={'message': text, 'area_hint': area, 'goal': goal, **(pref.get('touch_payload') or {})})
    await _notify_admins(context, title='新找房条件', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f"来源：{he(str(pref.get('source', 'user_search')))}", f"类型意向：{he(goal or '-')}", f"区域：{he(area or '-')}", f'预算：{he(_budget_text(budget_min, budget_max))}', f"户型：{he(property_type or '-')}", f'条件：<code>{he(text[:700])}</code>'])
    matches, match_mode = search_listings_with_fallback(property_type=property_type or None, area=area, budget_min=budget_min, budget_max=budget_max, text_fragment=text, limit=3)
    if matches:
        await send_find_results_as_cards(update, context, matches, match_mode)
    else:
        await render_panel(update, text=find_no_match_text(), parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context, prefer_edit_anchor=True)
    return MAIN

async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .flows import show_search_entry
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_search_entry(update, context)

async def cmd_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .flows import show_favorites
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_favorites(update, context)

async def cmd_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .appointments_view import list_recent_appointments
    from .keyboards_common import main_keyboard
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    from .texts import render_panel
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    await render_panel(update, text=list_recent_appointments(update.effective_user.id), reply_markup=main_keyboard(), context=context, prefer_edit_anchor=True)
    return MAIN

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .flows import show_help
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_help(update, context)

async def cmd_service(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .flows import show_service_hub
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await show_service_hub(update, context)

async def cmd_admin_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _all_user_admin_ids, _is_admin_user
    uid = getattr(update.effective_user, 'id', 0)
    if not _is_admin_user(uid):
        await update.effective_message.reply_text('❌ 无权限。')
        return MAIN
    owners = set(ADMIN_IDS or [])
    lines = ['👥 <b>用户服务 Bot 管理员</b>']
    for admin_id in sorted(_all_user_admin_ids()):
        lines.append(f"• <code>{admin_id}</code>  {('主管理员' if admin_id in owners else '管理员')}")
    lines.append('\n新增：<code>/admin_add Telegram用户ID</code>\n移除：<code>/admin_remove Telegram用户ID</code>')
    await update.effective_message.reply_text('\n'.join(lines), parse_mode=ParseMode.HTML)
    return MAIN

async def cmd_admin_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _extra_user_admin_ids, _save_extra_user_admin_ids
    uid = getattr(update.effective_user, 'id', 0)
    if uid not in set(ADMIN_IDS or []):
        await update.effective_message.reply_text('⛔ 只有主管理员可以增加管理员。')
        return MAIN
    if not context.args or not str(context.args[0]).isdigit():
        await update.effective_message.reply_text('用法：<code>/admin_add Telegram用户ID</code>\n请填数字 ID，不要填 @用户名。', parse_mode=ParseMode.HTML)
        return MAIN
    target = int(context.args[0])
    ids = _extra_user_admin_ids()
    ids.add(target)
    _save_extra_user_admin_ids(ids)
    await update.effective_message.reply_text(f'✅ 已增加管理员：<code>{target}</code>\n立即生效，重启不丢。', parse_mode=ParseMode.HTML)
    return MAIN

async def cmd_admin_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _extra_user_admin_ids, _save_extra_user_admin_ids
    uid = getattr(update.effective_user, 'id', 0)
    if uid not in set(ADMIN_IDS or []):
        await update.effective_message.reply_text('⛔ 只有主管理员可以移除管理员。')
        return MAIN
    if not context.args or not str(context.args[0]).isdigit():
        await update.effective_message.reply_text('用法：<code>/admin_remove Telegram用户ID</code>', parse_mode=ParseMode.HTML)
        return MAIN
    target = int(context.args[0])
    if target in set(ADMIN_IDS or []):
        await update.effective_message.reply_text('⛔ 主管理员由服务器安全配置保护，不能在 Bot 内删除。')
        return MAIN
    ids = _extra_user_admin_ids()
    ids.discard(target)
    _save_extra_user_admin_ids(ids)
    await update.effective_message.reply_text(f'✅ 已移除管理员：<code>{target}</code>', parse_mode=ParseMode.HTML)
    return MAIN

async def cmd_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .flows import contact_management
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    return await contact_management(update, context, source='command')

async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """搜索房源命令"""
    from .keyboards_search import find_area_keyboard
    from .search import upsert_user_profile
    from .session_deeplink import clear_session_for_fresh_entry
    upsert_user_profile(update.effective_user)
    clear_session_for_fresh_entry(context)
    await update.effective_message.reply_text('📍 <b>按区域找房</b>\n\n请选择区域：', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
    return FIND_AREA

async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """关于侨联命令"""
    from .keyboards_common import main_keyboard
    from .search import upsert_user_profile
    upsert_user_profile(update.effective_user)
    await update.effective_message.reply_text(copy_brand_text(), parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=main_keyboard())
    return MAIN
