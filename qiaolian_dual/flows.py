"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


async def start_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE, listing_id: str, *, source: str='user_bot', touch_payload: dict | None=None, initial_mode: str='', render_panel_fn=None) -> int:
    from .appointment_ui import _appointment_date_keyboard, _title_layout_label
    from .listing import listing_context, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text
    from .texts import render_panel as default_render_panel
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price

    touch_payload = touch_payload or {}
    render_panel = render_panel_fn or default_render_panel
    is_general_request = bool(touch_payload.get('listing_unknown')) or str(listing_id or '') == '待推荐'
    if not is_general_request:
        is_available, availability_reason = listing_is_available(listing_id)
        if not is_available:
            await render_panel(update, text=listing_unavailable_text(availability_reason), parse_mode=ParseMode.HTML, reply_markup=listing_unavailable_keyboard(listing_id), context=context)
            return MAIN

    info = listing_context(listing_id)
    title = str(info.get('project') or info.get('community') or info.get('area') or info.get('title') or '这套房').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
    context.user_data['appt'] = {'listing_id': listing_id, 'source': source, 'touch_payload': touch_payload, 'focus_keys': []}
    mode = str(initial_mode or 'offline').strip().lower()
    if mode not in APPOINTMENT_MODE_LABELS:
        mode = 'offline'
    context.user_data['appt']['mode'] = mode

    if is_general_request:
        await render_panel(
            update,
            text='📅 <b>预约看房</b>\n\n请先从具体房源的「预约看房」进入，房源和编号会自动带上。',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')],
                [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
                [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
            ]),
            parse_mode=ParseMode.HTML,
            context=context,
        )
        context.user_data.pop('appt', None)
        return MAIN

    subject = _title_layout_label(title, layout, '｜')
    qc = _display_listing_id(listing_id)
    price_line = '' if info.get('price') in (None, '', 0, '0') else f"\n💰 <b>{he(_fmt_price(info.get('price')))}</b>"
    heading = f"🎥 <b>视频看房｜{he(qc)}</b>" if mode == 'video' else f"📅 <b>预约看房｜{he(qc)}</b>"
    question = '哪天方便视频看房？' if mode == 'video' else '哪天方便看房？'
    await render_panel(
        update,
        text=f'{heading}\n\n🏠 <b>{he(subject)}</b>{price_line}\n\n{question}',
        reply_markup=_appointment_date_keyboard(show_video=(mode != 'video')),
        parse_mode=ParseMode.HTML,
        context=context,
    )
    return APPT_DATE


async def show_search_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import guided_search_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data['search_pref'] = {'source': 'user_search', 'goal': 'any', 'touch_payload': {}}
    context.user_data['awaiting_keyword_find'] = {'source': 'user_search'}
    await render_panel(
        update,
        text=(
            '🔍 <b>帮我找房</b>\n\n'
            '直接告诉我需求就可以，例如：\n\n'
            'BKK1 两房 800以内\n'
            '富力城 一房\n'
            '钻石岛 两房\n'
            '500以内 单间\n\n'
            '也可以点下面按钮筛选。'
        ),
        reply_markup=guided_search_keyboard(),
        parse_mode=ParseMode.HTML,
        context=context,
    )
    return MAIN


async def show_precise_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import precise_filter_keyboard
    from .texts import render_panel
    context.user_data.pop('awaiting_keyword_find', None)
    context.user_data.pop('awaiting_want_home', None)
    context.user_data['pref_select'] = {'source': 'menu_precise', 'selected': []}
    await render_panel(update, text='📍 <b>提交找房需求</b>\n\n选出你最在意的条件即可。', parse_mode=ParseMode.HTML, reply_markup=precise_filter_keyboard(set()), context=context)
    return MAIN


async def show_appointment_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_common import main_keyboard
    from .appointments_view import list_recent_appointments
    from .texts import render_panel
    await render_panel(update, text=list_recent_appointments(update.effective_user.id), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(), context=context)
    return MAIN


async def show_service_hub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .keyboards_search import service_hub_keyboard
    from .texts import render_panel
    text = (
        '🛠 <b>入住服务</b>\n\n'
        '已经入住了？\n'
        '房屋、物业和租约相关的事情，\n'
        '可以继续从这里处理。'
    )
    await render_panel(update, text=text, parse_mode=ParseMode.HTML, reply_markup=service_hub_keyboard(update.effective_user.id if update.effective_user else None), context=context)
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
    from .listing import listing_context
    from .results_admin import _notify_admins
    from .search import create_lead
    from .texts import render_panel
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price

    listing_id = str(from_listing or '').strip()
    if listing_id:
        context.user_data['contact_listing_id'] = listing_id
    else:
        listing_id = str(context.user_data.get('contact_listing_id') or '')
    binding = db.get_active_binding(update.effective_user.id)
    create_lead(update.effective_user, action='consult_menu_click', source=source, listing_id=listing_id or str((binding or {}).get('property_name') or ''), payload={'binding_id': (binding or {}).get('id'), 'listing_id': listing_id})
    admin_lines = [f'用户：{_user_mention_html(update.effective_user)}', f'联系方式：{he(_user_contact_text(update.effective_user))}', f'入口：{he(source or "用户咨询")}']
    if listing_id:
        admin_lines.append(f'咨询房源：{he(_display_listing_id(listing_id))}')
    await _notify_admins(context, title='用户联系中文顾问', lines=admin_lines)

    if listing_id:
        item = listing_context(listing_id)
        project = str(item.get('project') or item.get('community') or item.get('area') or '这套房').strip()
        layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
        subject = '｜'.join(v for v in (project, layout) if v)
        response_text = (
            '✅ <b>咨询已记录</b>\n\n'
            f'🏠 <b>{he(subject)}</b>\n'
            f'💰 <b>{he(_fmt_price(item.get("price")))}</b>\n'
            f'🆔 {he(_display_listing_id(listing_id))}\n\n'
            '房源信息已经带上，\n直接和中文顾问继续聊就可以。'
        )
    else:
        response_text = (
            '💬 <b>联系中文顾问</b>\n\n'
            '直接说你现在需要什么就可以。\n\n'
            '比如：\n\n'
            'BKK1 两房 800以内\n'
            '下个月入住\n'
            '想找可以做饭的一房\n'
            '想问一下押金和费用'
        )
    await render_panel(update, text=response_text, parse_mode=ParseMode.HTML, reply_markup=contact_handoff_keyboard(listing_id=listing_id), context=context)
    return MAIN
