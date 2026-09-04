"""Callback handlers for the search domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data in {'findmode:play', 'findmode:guided', 'profile:repeat', 'findback:area', 'findback:budget', 'find:similar'}
        or data.startswith('roompick:')
        or data.startswith('findtype:')
        or data.startswith('unavail:more:')
        or data.startswith('findarea:')
        or data.startswith('findbudget:')
    )


async def handle_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user, *, hooks: dict | None = None) -> int | None:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .appointments_view import old_tenant_binding_text
    from .keyboards_common import no_match_followup_keyboard, old_tenant_followup_keyboard
    from .keyboards_search import _decode_budget_choice, find_area_keyboard, find_budget_keyboard
    from .listing import listing_unavailable_keyboard
    from .results_admin import _allow_admin_notify, _notify_admins as default_notify_admins, send_find_results_as_cards as default_send_find_results_as_cards
    from .search import create_lead as default_create_lead, detect_area, detect_property_type, search_listings_with_fallback as default_search_listings_with_fallback, search_similar_listings
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel

    hooks = hooks or {}
    create_lead = hooks.get('create_lead') or default_create_lead
    notify_admins = hooks.get('notify_admins') or default_notify_admins
    search_listings = hooks.get('search_listings') or default_search_listings_with_fallback
    send_results = hooks.get('send_results') or default_send_find_results_as_cards

    if data == 'findmode:play':
        context.user_data['awaiting_keyword_find'] = {'source': 'smart_find_play'}
        await query.edit_message_text('🔍 <b>帮我找房</b>\n\n直接发一句需求就可以。\n例如：<code>BKK1 两房 800以内</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回找房', callback_data='home_smart_search')]]))
        return MAIN

    if data == 'findmode:guided':
        context.user_data.pop('awaiting_keyword_find', None)
        context.user_data['search_pref'] = {'source': 'user_search', 'goal': 'any', 'touch_payload': {}}
        await query.edit_message_text('📍 <b>想住哪里？</b>\n\n选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA

    if data.startswith('roompick:'):
        room_type = data.split(':', 1)[1]
        property_type = '' if room_type == 'any' else detect_property_type(room_type)
        _remember_video_pref(context, layout=None if room_type == 'any' else room_type)
        matches_found, match_mode = search_listings(property_type=property_type or None, area=None, budget_min=None, budget_max=None, text_fragment='' if room_type == 'any' else room_type, limit=5)
        create_lead(user, action='keyword_find_play', source='home_layout', property_type=property_type, payload={'message': room_type, 'match_mode': match_mode, 'room_type': room_type})
        if matches_found:
            await send_results(update, context, matches_found, 'strict')
        else:
            context.user_data['last_search_pref'] = {'property_type': property_type or None, 'area': None, 'budget_min': None, 'budget_max': None}
            label = '不限户型' if room_type == 'any' else room_type
            await render_panel(update, text=f'🔎 <b>暂时没有完全符合条件的房源</b>\n\n{he(label)}\n\n你可以调整一个条件继续找，\n也可以让中文顾问按这个需求继续留意。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)
        return MAIN

    if data == 'profile:repeat':
        binding_text, binding = old_tenant_binding_text(user.id)
        if not binding:
            await render_panel(update, text='📋 <b>我的租约</b>\n\n目前还没有绑定租约档案。\n\n如果是通过侨联入住的房源，联系中文顾问核对房号后即可补上。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')], [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')]]), context=context)
            return MAIN
        await render_panel(update, text=binding_text, parse_mode=ParseMode.HTML, reply_markup=old_tenant_followup_keyboard(), context=context)
        return MAIN

    if data.startswith('findtype:'):
        goal = data.split(':', 1)[1]
        context.user_data['search_pref'] = {'source': 'user_search', 'goal': goal, 'touch_payload': {}}
        goal_text = '不限类型' if goal == 'any' else goal
        await query.edit_message_text(f'📍 <b>想住哪里？</b>\n\n已选：{he(goal_text)}', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA

    if data.startswith('unavail:more:'):
        area = detect_area(data.split(':', 2)[2])
        create_lead(user, action='unavailable_more_click', source='listing_unavailable', area=area, listing_id=str(context.user_data.get('contact_listing_id') or ''))
        matches_found = db.search_listings(areas=[area] if area and area not in {'不限', 'any'} else None, limit=5)
        if matches_found:
            await send_results(update, context, matches_found, 'strict')
        else:
            await render_panel(update, text=f"当前同区域（{he(area or '金边')}）暂时没有可预约房源。\n可以调整条件，或直接联系中文顾问。", parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(), context=context)
        return MAIN

    if data == 'findback:area':
        pref = context.user_data.get('search_pref') or {}
        goal = str(pref.get('goal') or 'any')
        goal_text = '' if goal in {'any', '住宅'} else f'已选：{he(goal)}\n\n'
        await query.edit_message_text(f'📍 <b>想住哪里？</b>\n\n{goal_text}选一个大概位置就行。', parse_mode=ParseMode.HTML, reply_markup=find_area_keyboard())
        return FIND_AREA

    if data == 'findback:budget':
        context.user_data.pop('awaiting_custom_budget', None)
        pref = context.user_data.get('search_pref') or {'goal': 'any'}
        area = str(pref.get('area') or '')
        await query.edit_message_text(f'💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月' + (f'\n已选：{he(area)}' if area else ''), parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard(str(pref.get('goal') or 'any')))
        return FIND_BUDGET

    if data.startswith('findarea:'):
        code = data.split(':', 1)[1]
        if code == 'other':
            await query.edit_message_text('📍 <b>其他位置</b>\n\n直接输入区域、楼盘或附近地标。\n例如：<code>永旺2附近</code>', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回区域选择', callback_data='findback:area')]]))
            return FIND_AREA
        area = FIND_AREA_CODE_MAP.get(code, '')
        if not area:
            await answer_callback_once(query, '这个位置暂时无法识别，请选择其他位置。', show_alert=True)
            return FIND_AREA
        pref = context.user_data.get('search_pref') or {'source': 'user_search', 'goal': 'any', 'touch_payload': {}}
        pref['area'] = area
        context.user_data['search_pref'] = pref
        await query.edit_message_text(f'💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月\n已选：{he(area)}', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard(str(pref.get('goal') or 'any')))
        return FIND_BUDGET

    if data.startswith('findbudget:'):
        code = data.split(':', 1)[1]
        if code == 'other':
            context.user_data['awaiting_custom_budget'] = True
            await query.edit_message_text('💰 <b>自己输入预算</b>\n\n直接发每月预算，例如：<code>800以内</code> 或 <code>600-900</code>。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回预算选择', callback_data='findback:budget')]]))
            return FIND_BUDGET
        pref = context.user_data.pop('search_pref', {})
        goal = str(pref.get('goal') or 'any')
        area = str(pref.get('area') or '')
        budget_label, budget_min, budget_max = _decode_budget_choice(goal, code)
        type_filter = '' if goal in {'any', '住宅'} else detect_property_type(goal) or goal
        _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=None if goal in {'any', '住宅'} else goal)
        context.user_data['last_search_pref'] = {'property_type': type_filter or None, 'area': area or None, 'budget_min': budget_min, 'budget_max': budget_max}
        create_lead(user, action='search_pref_submit', source=str(pref.get('source', 'user_search')), area=area if area != '不限' else '', property_type=type_filter, budget_min=budget_min, budget_max=budget_max, payload={'goal': goal, 'area_hint': area, 'budget_label': budget_label, **(pref.get('touch_payload') or {})})
        if _allow_admin_notify(context, key=f'search_activity:{int(user.id)}', cooldown_seconds=600):
            await notify_admins(context, title='找房需求', lines=[f'用户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'类型：{he(goal)}', f"区域：{he(area or '-')}", f'预算：{he(budget_label)}'])
        matches_found, _ = search_listings(property_type=type_filter or None, area=area, budget_min=budget_min, budget_max=budget_max, text_fragment='', limit=5)
        if matches_found:
            await send_results(update, context, matches_found, 'strict')
        else:
            summary = '｜'.join(value for value in (area, '' if goal in {'any', '住宅'} else goal, budget_label) if value)
            await render_panel(update, text=f'🔎 <b>暂时没有完全符合条件的房源</b>\n\n{he(summary)}\n\n你可以调整一个条件继续找，\n也可以让中文顾问按这个需求继续留意。', parse_mode=ParseMode.HTML, reply_markup=no_match_followup_keyboard(), context=context)
        return MAIN

    if data == 'find:similar':
        pref = context.user_data.get('last_search_pref') or {}
        matches_found, mode = search_similar_listings(property_type=pref.get('property_type'), area=pref.get('area'), budget_min=pref.get('budget_min'), budget_max=pref.get('budget_max'), limit=5)
        if matches_found:
            await send_results(update, context, matches_found, mode)
        else:
            await render_panel(update, text='暂时也没有合适的类似房源。\n可以调整条件，或联系中文顾问继续留意。', reply_markup=no_match_followup_keyboard(), context=context)
        return MAIN

    return None
