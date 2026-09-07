"""Callback handlers for customer listing actions."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data == 'find:show_more'
        or data.startswith('findcard:')
        or data.startswith('listing:open:')
        or data.startswith('listing:photos:')
        or data.startswith('listing:appoint:')
        or data.startswith('listing:consult:')
        or data.startswith('listing:detail:')
        or data.startswith('listing:similar:')
    )


def _with_return_nav(markup: InlineKeyboardMarkup, *, include_search: bool=True) -> InlineKeyboardMarkup:
    """给房源二/三级页统一补退出链路，避免用户被困在当前页。"""
    rows = [list(row) for row in (getattr(markup, 'inline_keyboard', None) or [])]
    callbacks = {
        str(getattr(button, 'callback_data', '') or '')
        for row in rows
        for button in row
    }
    nav: list[InlineKeyboardButton] = []
    if include_search and 'home_smart_search' not in callbacks:
        nav.append(InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'))
    if 'home' not in callbacks:
        nav.append(InlineKeyboardButton('🏠 返回首页', callback_data='home'))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


async def handle_listing_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .flows import contact_management, start_appointment
    from .keyboards_common import main_keyboard, no_match_followup_keyboard
    from .keyboards_search import find_budget_keyboard
    from .listing import (
        listing_action_allowed,
        listing_context,
        listing_cost_keyboard,
        listing_cost_text,
        listing_is_available,
        listing_unavailable_keyboard,
        listing_unavailable_text,
    )
    from .results_admin import send_find_result_card, send_find_results_as_cards, send_listing_photo_preview
    from .search import create_lead
    from .texts import render_panel

    if data.startswith('findcard:'):
        parts = data.split(':', 2)
        value = parts[1] if len(parts) > 1 else ''
        if value == 'noop':
            return MAIN
        try:
            index = int(value)
        except (TypeError, ValueError):
            return MAIN
        if len(parts) == 3 and parts[2] not in {'', 'unknown'}:
            ids = list(context.user_data.get('find_card_listing_ids') or [])
            if 0 <= index < len(ids) and ids[index] != parts[2]:
                return MAIN
        await send_find_result_card(update, context, index, replace=True)
        return MAIN

    if data == 'find:show_more':
        ids = list(context.user_data.get('find_card_listing_ids') or [])
        ids.extend(str(value) for value in (context.user_data.pop('find_more_listing_ids', []) or []) if str(value or '').strip())
        ids = list(dict.fromkeys(value for value in ids if value))
        context.user_data['find_card_listing_ids'] = ids
        if ids:
            await send_find_result_card(update, context, 0, replace=True)
        else:
            await render_panel(update, text='暂时没有可以安排看房的房源。', reply_markup=no_match_followup_keyboard(), context=context)
        return MAIN

    # 历史 listing:open 只作为兼容 alias；新 UI 不再展示“查看这套”。
    if data.startswith('listing:open:'):
        lid = data.split(':', 2)[2]
        data = f'listing:detail:{lid}'

    if data.startswith('listing:photos:'):
        lid = data.split(':', 2)[2]
        allowed, reason = listing_action_allowed(lid, 'photos')
        if not allowed:
            await render_panel(update, text=listing_unavailable_text(reason, lid), parse_mode=ParseMode.HTML, reply_markup=_with_return_nav(listing_unavailable_keyboard(lid)), context=context)
            return MAIN
        context.user_data['contact_listing_id'] = lid
        await send_listing_photo_preview(context.bot, update.effective_chat.id, lid)
        return MAIN

    if data.startswith('listing:appoint:'):
        parts = data.split(':', 3)
        if len(parts) == 4 and parts[2] in {'offline', 'video'}:
            mode, lid = parts[2], parts[3]
        elif len(parts) == 3:
            mode, lid = '', parts[2]
        else:
            return MAIN
        allowed, reason = listing_is_available(lid)
        if not allowed:
            await render_panel(update, text=listing_unavailable_text(reason, lid), parse_mode=ParseMode.HTML, reply_markup=_with_return_nav(listing_unavailable_keyboard(lid)), context=context)
            return MAIN
        context.user_data['contact_listing_id'] = lid
        return await start_appointment(update, context, lid, source='listing_card', touch_payload={'listing_id': lid}, initial_mode=mode)

    if data.startswith('listing:consult:'):
        lid = data.split(':', 2)[2]
        allowed, reason = listing_action_allowed(lid, 'consult')
        if not allowed:
            await render_panel(update, text=listing_unavailable_text(reason, lid), parse_mode=ParseMode.HTML, reply_markup=_with_return_nav(listing_unavailable_keyboard(lid)), context=context)
            return MAIN
        context.user_data['contact_listing_id'] = lid
        context.user_data['contact_touch_payload'] = {'listing_id': lid, 'entry': 'listing_card'}
        return await contact_management(update, context, source='listing_card', from_listing=lid)

    if data.startswith('listing:detail:'):
        lid = data.split(':', 2)[2]
        allowed, reason = listing_action_allowed(lid, 'detail')
        if not allowed:
            await render_panel(update, text=listing_unavailable_text(reason, lid), parse_mode=ParseMode.HTML, reply_markup=_with_return_nav(listing_unavailable_keyboard(lid)), context=context)
            return MAIN
        item = listing_context(lid)
        if not item or not db.get_listing(lid):
            await render_panel(update, text='未找到该房源详情，可能已下架。', reply_markup=main_keyboard(), context=context)
            return MAIN
        context.user_data['contact_listing_id'] = lid
        create_lead(user, action='listing_detail_view', source='listing_landing', listing_id=lid)
        detail_text = listing_cost_text(lid)
        detail_kb = _with_return_nav(listing_cost_keyboard(lid))
        if getattr(query.message, 'photo', None):
            await query.edit_message_caption(caption=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb)
        else:
            await render_panel(update, text=detail_text, parse_mode=ParseMode.HTML, reply_markup=detail_kb, context=context)
        return MAIN

    if data.startswith('listing:similar:'):
        lid = data.split(':', 2)[2]
        item = db.get_listing(lid) if lid else None
        if not item:
            await render_panel(update, text='未找到房源信息。', reply_markup=main_keyboard(), context=context)
            return MAIN
        area = str(item.get('area') or '')
        context.user_data['search_pref'] = {'source': 'similar_listing', 'area': area, 'goal': 'any', 'touch_payload': {'from_listing': lid}}
        await render_panel(update, text=f'💰 <b>每月预算大概多少？</b>\n\n已选：{he(area)}', parse_mode=ParseMode.HTML, reply_markup=find_budget_keyboard('any'), context=context)
        return FIND_BUDGET

    return None
