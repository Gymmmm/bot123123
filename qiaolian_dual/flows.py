"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def start_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, listing_id: str, *, source: str='user_bot', touch_payload: dict | None=None, initial_mode: str='', render_panel_fn=None) -> int:
    from .appointment_ui import _appointment_date_keyboard, _title_layout_label
    from .listing import listing_context, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text
    from .texts import render_panel as default_render_panel
    from .utils_formatting import _display_floor, _display_layout, _display_listing_id, _fmt_price
    touch_payload = touch_payload or {}
    render_panel = render_panel_fn or default_render_panel
    is_general_request = bool(touch_payload.get('listing_unknown')) or str(listing_id or '') == '待推荐'
    if not is_general_request:
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id), context=context)
            return MAIN
    info = listing_context(listing_id)
    title = str(info.get('title') or info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
    area = str(info.get('area') or '').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
    size = str(info.get('size_sqm') or info.get('size') or '').strip()
    floor = _display_floor(info.get('floor'))
    highlights_raw = info.get('highlights') or ''
    if isinstance(highlights_raw, str):
        highlights = [part.strip() for part in re.split('[\\n｜,，]', highlights_raw) if part.strip()]
    elif isinstance(highlights_raw, (list, tuple)):
        highlights = [str(part).strip() for part in highlights_raw if str(part).strip()]
    else:
        highlights = []
    home_facts = [value for value in (layout, f'{size}㎡' if size and '㎡' not in size else size, floor) if value]
    summary_lines = [f'🏠 <b>{he(title)}</b>', f"💰 <b>租金：{he(_fmt_price(info.get('price')))}</b>"]
    if area:
        summary_lines.append(f'📍 位置：{he(area)}')
    if home_facts:
        summary_lines.append(f"📐 户型：{he(' · '.join(home_facts))}")
    if highlights:
        summary_lines.append(f"✨ 亮点：{he(' · '.join(highlights[:2]))}")
    if not is_general_request:
        summary_lines.append(f"📌 编号：<code>{he(_display_listing_id(listing_id))}</code>")
    listing_summary = '\n'.join(summary_lines)
    context.user_data['appt'] = {'listing_id': listing_id, 'source': source, 'touch_payload': touch_payload, 'focus_keys': list(APPOINTMENT_FOCUS_ORDER), 'listing_summary': listing_summary}
    mode = str(initial_mode or 'offline').strip().lower()
    if mode not in APPOINTMENT_MODE_LABELS:
        mode = 'offline'
    context.user_data['appt']['mode'] = mode
    subject = '尚未确定房源' if is_general_request else _title_layout_label(title, layout, '｜')
    price_line = '' if is_general_request or info.get('price') in (None, '', 0, '0') else f"\n💰 <b>租金：{he(_fmt_price(info.get('price')))}</b>"
    video_note = '' if mode == 'video' else '\n\n没时间到现场？\n也可以安排实时视频看房。'
    await render_panel(
        update,
        text=f"📅 <b>{'实时视频看房' if mode == 'video' else '预约看房'}</b>\n\n🏠 <b>{he(subject)}</b>{price_line}\n\n请选择方便看房的日期。{video_note}",
        reply_markup=_appointment_date_keyboard(show_video=(mode != 'video')),
        parse_mode=ParseMode.HTML,
        context=context,
    )
    return APPT_DATE

async def show_search_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import find_area_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data['search_pref'] = {'source': 'user_search', 'touch_payload': {}}
    await render_panel(update, text='📍 <b>想住哪里？</b>\n\n选一个大概位置就行。', reply_markup=find_area_keyboard(), parse_mode=ParseMode.HTML, context=context)
    return FIND_AREA

async def show_precise_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import precise_filter_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data.pop('awaiting_want_home', None)
    context.user_data['pref_select'] = {'source': 'menu_precise', 'selected': []}
    await render_panel(update, text='<b>📍 提交找房需求</b>\n\n选出你最在意的条件即可。提交后，顾问会据此帮你筛出 1–3 套更值得看的房源。', parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()), context=context)
    return MAIN

async def show_appointment_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_common import contact_handoff_keyboard
    from .search import create_lead, upsert_user_profile
    from .texts import render_panel
    user = update.effective_user
    upsert_user_profile(user)
    create_lead(user, action='appointment_hub_view', source='main_menu', payload={'from_menu': True})
    await render_panel(update, text='📅 <b>预约看房</b>\n\n看中具体房源时，点房源卡片里的「预约看房」即可自动带入信息。\n\n如果还没确定房源，先点「联系中文顾问」，告诉我们你的区域、预算或户型，顾问会帮你推荐。', parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(), context=context)
    return MAIN

async def show_service_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import service_hub_keyboard
    from .texts import render_panel
    user_id = update.effective_user.id if update.effective_user else None
    binding = db.get_active_binding(user_id) if user_id else None
    if binding:
        property_name = he(str(binding.get('property_name') or '当前租住房源'))
        text = f"<b>🛠 入住服务</b>\n\n🏠 {property_name}\n需要处理什么？"
    else:
        text = (
            "<b>🛠 入住服务</b>\n\n"
            "入住后遇到什么问题？\n"
            "报修、物业沟通或周边生活，直接选一项。\n\n"
            "已在租的客户可先绑定租客档案，之后会自动带上租约和房屋信息；租约到期前 7 天会提醒你确认是否续租。"
        )
    await render_panel(update, text=text, parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(user_id), context=context)
    return MAIN

async def show_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .appointments_view import list_favorites_text
    from .keyboards_common import main_keyboard
    from .texts import render_panel
    await render_panel(update, text=list_favorites_text(update.effective_user.id), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context)
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
    source_labels = {'hub': '首页咨询', 'menu': '菜单咨询', 'command': '用户主动联系', 'listing_card': '房源详情页', 'channel_index': '频道入口'}
    source_text = source_labels.get(source, source or '用户咨询')
    listing_text = listing_id or str((binding or {}).get('property_name') or '')
    admin_lines = [f'用户：{_user_mention_html(update.effective_user)}', f'联系方式：{he(_user_contact_text(update.effective_user))}', f'入口：{he(source_text)}']
    if listing_text:
        admin_lines.append(f'咨询房源：{he(listing_text)}')
    else:
        admin_lines.append('咨询内容：未指定房源，请询问位置、预算和户型。')
    await _notify_admins(context, title='用户联系顾问', lines=admin_lines)
    if listing_id or binding:
        response_text = advisor_handoff_text(listing_id=listing_id, user_id=update.effective_user.id)
    else:
        response_text = '✅ <b>中文顾问已收到你的咨询</b>\n\n把最在意的区域、预算或户型发给我即可；如果暂时说不清，也可以直接说「想找两房」「下月入住」这类需求。\n\n中文顾问会通过 Telegram 继续跟进，无需重复填写联系方式。'
    await render_panel(update, text=response_text, parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(listing_id=listing_id), context=context)
    return MAIN
