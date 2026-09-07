"""Guided find-home text input handlers for area and custom budget."""
from __future__ import annotations

from .common import *


async def handle_find_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import find_budget_keyboard
    from .search import detect_area
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel

    raw = str(update.effective_message.text or '').strip()
    area = detect_area(raw)
    current = context.user_data.get('search_pref') or {}
    goal = str(current.get('goal') or 'any')
    context.user_data['search_pref'] = {
        'area': area,
        'source': current.get('source', 'user_search'),
        'goal': goal,
        'touch_payload': current.get('touch_payload') or {},
    }
    _remember_video_pref(context, area=area)
    await render_panel(
        update,
        text=f'💰 <b>每月预算大概多少？</b>\n\n单位：USD / 月\n已选：{he(area)}',
        parse_mode=ParseMode.HTML,
        reply_markup=find_budget_keyboard(goal),
        context=context,
        prefer_edit_anchor=True,
    )
    return FIND_BUDGET


async def handle_find_budget(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _budget_text, _user_contact_text, _user_mention_html
    from .keyboards_common import no_match_followup_keyboard
    from .results_admin import _notify_admins, send_find_results_as_cards
    from .search import create_lead, detect_property_type, parse_budget_range, search_listings_with_fallback
    from .session_deeplink import _remember_video_pref
    from .texts import render_panel

    user = update.effective_user
    text = str(update.effective_message.text or '').strip()
    pref = context.user_data.pop('search_pref', {})
    context.user_data.pop('awaiting_custom_budget', None)
    budget_min, budget_max = parse_budget_range(text)
    if budget_min is None and budget_max is None:
        context.user_data['search_pref'] = pref
        context.user_data['awaiting_custom_budget'] = True
        await render_panel(
            update,
            text='预算没有识别出来。请试试：<code>800以内</code>、<code>600-900</code> 或 <code>1500以上</code>。',
            parse_mode=ParseMode.HTML,
            context=context,
            prefer_edit_anchor=True,
        )
        return FIND_BUDGET

    area = str(pref.get('area') or '')
    goal = str(pref.get('goal') or 'any')
    type_filter = '' if goal in {'', 'any', '住宅'} else (detect_property_type(goal) or goal)
    _remember_video_pref(context, area=area or None, budget_min=budget_min, budget_max=budget_max, layout=None if not type_filter else goal)
    context.user_data['last_search_pref'] = {
        'property_type': type_filter or None,
        'area': area or None,
        'budget_min': budget_min,
        'budget_max': budget_max,
    }
    create_lead(
        user,
        action='search_pref_submit',
        source=str(pref.get('source') or 'user_search'),
        area=area,
        property_type=type_filter,
        budget_min=budget_min,
        budget_max=budget_max,
        payload={'message': text, 'area_hint': area, 'goal': goal, **(pref.get('touch_payload') or {})},
    )
    await _notify_admins(
        context,
        title='新找房条件',
        lines=[
            f'用户：{_user_mention_html(user)}',
            f'联系方式：{he(_user_contact_text(user))}',
            f'类型：{he(goal if goal != "any" else "不限类型")}',
            f'区域：{he(area or "-")}',
            f'预算：{he(_budget_text(budget_min, budget_max))}',
        ],
    )
    matches, _ = search_listings_with_fallback(
        property_type=type_filter or None,
        area=area or None,
        budget_min=budget_min,
        budget_max=budget_max,
        text_fragment='',
        limit=5,
    )
    if matches:
        await send_find_results_as_cards(update, context, matches, 'strict')
    else:
        summary = '｜'.join(value for value in (area, '' if goal in {'any', '住宅'} else goal, _budget_text(budget_min, budget_max)) if value)
        await render_panel(
            update,
            text=f'🔎 <b>暂时没有完全符合条件的房源</b>\n\n{he(summary)}\n\n您可以调整一个条件继续找，\n也可以让中文顾问按这个需求继续留意。',
            parse_mode=ParseMode.HTML,
            reply_markup=no_match_followup_keyboard(),
            context=context,
            prefer_edit_anchor=True,
        )
    return MAIN
