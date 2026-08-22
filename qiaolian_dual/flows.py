"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def start_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, listing_id: str, *, source: str='user_bot', touch_payload: dict | None=None, initial_mode: str='', render_panel_fn=None) -> int:
    from .appointment_ui import _appointment_date_keyboard, _appointment_mode_keyboard, _title_layout_label
    from .listing import listing_context, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text
    from .texts import render_panel as default_render_panel
    from .utils_formatting import _display_listing_id, _fmt_price
    touch_payload = touch_payload or {}
    render_panel = render_panel_fn or default_render_panel
    is_general_request = bool(touch_payload.get('listing_unknown')) or str(listing_id or '') == '待推荐'
    if not is_general_request:
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            await render_panel(update, text=listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(listing_id), context=context)
            return MAIN
    info = listing_context(listing_id)
    title = str(info.get('title') or info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
    area = str(info.get('area') or '').strip()
    layout = str(info.get('layout') or info.get('property_type') or '').strip()
    size = str(info.get('size_sqm') or info.get('size') or '').strip()
    facts = [value for value in (area, layout, f'{size}㎡' if size and '㎡' not in size else size) if value]
    floor = str(info.get('floor') or '').strip()
    highlights_raw = info.get('highlights') or ''
    if isinstance(highlights_raw, str):
        highlights = [part.strip() for part in re.split('[\\n｜,，]', highlights_raw) if part.strip()]
    elif isinstance(highlights_raw, (list, tuple)):
        highlights = [str(part).strip() for part in highlights_raw if str(part).strip()]
    else:
        highlights = []
    home_facts = [value for value in (layout, f'{size}㎡' if size and '㎡' not in size else size, floor) if value]
    summary_lines = [f'<b>🏠 {he(title)}</b>', f"💰 <b>{he(_fmt_price(info.get('price')))}</b>"]
    if area:
        summary_lines.append(f'📍 位置：{he(area)}')
    if home_facts:
        summary_lines.append(f"📐 户型：{he(' · '.join(home_facts))}")
    if highlights:
        summary_lines.append(f"✨ 亮点：{he(' · '.join(highlights[:2]))}")
    summary_lines.append(f"📌 编号：<code>{he(_display_listing_id(listing_id or '待推荐'))}</code>")
    listing_summary = '\n'.join(summary_lines)
    context.user_data['appt'] = {'listing_id': listing_id, 'source': source, 'touch_payload': touch_payload, 'focus_keys': list(APPOINTMENT_FOCUS_ORDER)}
    mode = str(initial_mode or '').strip().lower()
    if mode in APPOINTMENT_MODE_LABELS:
        context.user_data['appt']['mode'] = mode
        await render_panel(
            update,
            text=(
                f"📅 <b>{he(APPOINTMENT_MODE_LABELS[mode])}</b>\n\n"
                "第二步：选择方便的日期。\n"
                "验房关注点默认帮你全项核对，提交前仍可确认。"
            ),
            reply_markup=_appointment_date_keyboard(),
            parse_mode=ParseMode.HTML,
            context=context,
        )
        return APPT_DATE
    context.user_data['appt'].pop('mode', None)
    await render_panel(update, text=f"📅 <b>预约看房 · {he(_title_layout_label(title, layout, '｜'))}</b>\n\n请选择看房方式：", reply_markup=_appointment_mode_keyboard(listing_id), parse_mode=ParseMode.HTML, context=context)
    return APPT_MODE

async def show_search_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """智能找房入口 - 直接进入位置选择"""
    from .keyboards_search import find_area_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data['search_pref'] = {'source': 'user_search', 'touch_payload': {}}
    await render_panel(update, text='📍 <b>想住哪一片？</b>', reply_markup=find_area_keyboard(), parse_mode=ParseMode.HTML, context=context)
    return FIND_AREA

async def show_precise_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import precise_filter_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data.pop('awaiting_want_home', None)
    context.user_data['pref_select'] = {'source': 'menu_precise', 'selected': []}
    await render_panel(update, text='<b>📍 条件筛选</b>\n\n你最在意哪类条件？直接点选即可。\n选完点 <b>提交条件</b>，我会同步推送管理号人工收窄到 1-3 套。', parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()), context=context)
    return MAIN

async def show_appointment_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_common import contact_handoff_keyboard
    from .search import create_lead, upsert_user_profile
    from .texts import advisor_text, render_panel
    user = update.effective_user
    upsert_user_profile(user)
    create_lead(user, action='appointment_hub_view', source='main_menu', payload={'from_menu': True})
    await render_panel(update, text=advisor_text(), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(), context=context)
    return MAIN

async def show_service_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import service_hub_keyboard
    from .texts import render_panel, service_hub_text
    user_id = update.effective_user.id if update.effective_user else None
    binding = db.get_active_binding(user_id) if user_id else None
    intro = f"\n\n✅ <b>已识别你的在租档案</b>\n当前房源：{he(str(binding.get('property_name') or '待完善'))}" if binding else '\n\n以前通过侨联租过房？点「我以前在侨联租过」，顾问会帮你找回档案。'
    await render_panel(update, text=service_hub_text() + intro, parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(user_id), context=context)
    return MAIN

async def show_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .appointments_view import list_favorites_text
    from .keyboards_common import main_keyboard
    from .texts import render_panel
    await render_panel(update, text=list_favorites_text(update.effective_user.id), reply_markup=main_keyboard(), context=context)
    return MAIN

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .texts import help_text, render_panel
    await render_panel(update, text=help_text(), parse_mode=ParseMode.HTML, reply_markup=help_repeat_keyboard(), context=context)
    return MAIN

async def contact_management(update: Update, context: ContextTypes.DEFAULT_TYPE, *, source: str='menu', from_listing: str | None = None) -> int:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .keyboards_common import contact_handoff_keyboard
    from .results_admin import _notify_admins
    from .search import create_lead
    from .texts import advisor_handoff_text, render_panel
    listing_id = str(from_listing or '').strip()
    if listing_id:
        context.user_data['contact_listing_id'] = listing_id
    else:
        listing_id = str(context.user_data.get('contact_listing_id') or '')
    binding = db.get_active_binding(update.effective_user.id)
    create_lead(update.effective_user, action='consult_menu_click', source=source, listing_id=listing_id or str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id'), 'listing_id': listing_id})
    await _notify_admins(context, title='咨询顾问请求（按钮）', lines=[f'用户：{_user_mention_html(update.effective_user)}', f'联系方式：{he(_user_contact_text(update.effective_user))}', f'来源：{he(source)}', f"房源：{he(listing_id or str((binding or {}).get('property_name') or '-'))}"])
    await render_panel(update, text=advisor_handoff_text(listing_id=listing_id, user_id=update.effective_user.id), parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(), context=context)
    return MAIN
