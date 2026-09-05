"""预约日期 → 时间 → 直接提交流程。"""
from __future__ import annotations

from .common import *


def _date_display(value: object) -> str:
    raw = str(value or '').strip()
    bits = raw.replace('/', '-').split('-')
    if len(bits) >= 2 and all(part.isdigit() for part in bits[-2:]):
        return f'{int(bits[-2])}月{int(bits[-1])}日'
    return raw or '待安排'


async def _render_date_page(query, appt: dict) -> int:
    from .appointment_ui import _appointment_date_keyboard, _title_layout_label
    from .listing import listing_context
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price

    lid = str(appt.get('listing_id') or '').strip()
    mode = str(appt.get('mode') or 'offline')
    info = listing_context(lid)
    title = str(info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
    subject = _title_layout_label(title, layout, '｜')
    qc = _display_listing_id(lid)
    price_line = '' if info.get('price') in (None, '', 0, '0') else f"\n💰 <b>{he(_fmt_price(info.get('price')))}</b>"
    heading = f"🎥 <b>视频看房｜{he(qc)}</b>" if mode == 'video' else f"📅 <b>预约看房｜{he(qc)}</b>"
    question = '哪天方便视频看房？' if mode == 'video' else '哪天方便看房？'
    await query.edit_message_text(
        f'{heading}\n\n🏠 <b>{he(subject)}</b>{price_line}\n\n{question}',
        reply_markup=_appointment_date_keyboard(show_video=(mode != 'video')),
        parse_mode=ParseMode.HTML,
    )
    return APPT_DATE


async def _render_time_page(query, appt: dict) -> int:
    from .appointment_ui import _appointment_time_keyboard, _title_layout_label
    from .listing import listing_context
    from .utils_formatting import _display_layout

    info = listing_context(str(appt.get('listing_id') or ''))
    title = str(info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
    await query.edit_message_text(
        f'🕐 <b>选择时间</b>\n\n📅 {he(_date_display(appt.get("date")))}\n🏠 {he(_title_layout_label(title, layout, "｜"))}',
        reply_markup=_appointment_time_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return APPT_TIME


async def _submit_appointment(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    appt: dict,
    *,
    create_lead_fn=None,
    notify_admins_fn=None,
) -> int:
    """选完时间即提交；旧确认按钮仍可兼容调用本函数。"""
    from .admin_consult import format_consult_notify
    from .admin_contract import _user_contact_text, _user_mention_html
    from .appointment_ui import _title_layout_label
    from .appointments_view import _appointment_date_compact, _appointment_time_compact
    from .keyboards_common import appointment_success_keyboard
    from .listing import listing_context, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text
    from .results_admin import _notify_admins as default_notify_admins, admin_lead_keyboard
    from .search import create_lead as default_create_lead
    from .session_deeplink import now_ts, user_display_name
    from .utils_formatting import _display_layout, _display_listing_id

    create_lead = create_lead_fn or default_create_lead
    notify_admins = notify_admins_fn or default_notify_admins
    query = getattr(update, 'callback_query', None)
    message = getattr(update, 'effective_message', None)

    async def respond(text: str, reply_markup=None) -> None:
        kwargs = {'parse_mode': ParseMode.HTML, 'reply_markup': reply_markup}
        if query is not None:
            await query.edit_message_text(text, **kwargs)
        elif message is not None:
            await message.reply_text(text, **kwargs)

    lid = str(appt.get('listing_id') or '').strip()
    if not lid or lid in {'待推荐', '未知'}:
        context.user_data.pop('appt', None)
        await respond('请先从具体房源的「预约看房」进入。', InlineKeyboardMarkup([[InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')]]))
        return MAIN

    available, reason = listing_is_available(lid)
    if not available:
        context.user_data.pop('appt', None)
        await respond(listing_unavailable_text(reason, lid), listing_unavailable_keyboard(lid))
        return MAIN

    if not appt.get('date') or not appt.get('time'):
        await respond('预约信息不完整，请重新选择日期和时间。', InlineKeyboardMarkup([[InlineKeyboardButton('📅 重新预约', callback_data=f'listing:appoint:{lid}')]]))
        return MAIN

    user = update.effective_user
    mode = str(appt.get('mode') or 'offline')
    time_value = str(appt.get('time') or '')
    touch_payload = dict(appt.get('touch_payload') or {})
    edit_id = int(touch_payload.get('edit_appointment_id') or 0)
    lead_id = None

    if edit_id:
        with db.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM appointments WHERE id=? AND user_id=? AND status NOT IN ('done','cancelled')",
                (edit_id, int(user.id)),
            ).fetchone()
            if not existing:
                context.user_data.pop('appt', None)
                await respond('这条预约已经无法修改。', InlineKeyboardMarkup([[InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list')]]))
                return MAIN
            conn.execute(
                "UPDATE appointments SET viewing_mode=?, appointment_date=?, appointment_time=?, status='pending' WHERE id=? AND user_id=?",
                (mode, appt.get('date'), time_value, edit_id, int(user.id)),
            )
            conn.commit()
        appointment_id = edit_id
        lead_id = create_lead(
            user,
            action='appointment_time_update',
            source='appointment_edit',
            listing_id=lid,
            payload={'appointment_id': edit_id, 'viewing_mode': mode, 'appointment_date': appt.get('date'), 'appointment_time': time_value},
        )
    else:
        with db.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM appointments WHERE user_id=? AND listing_id=? AND viewing_mode=? AND appointment_date=? AND appointment_time=? AND status NOT IN ('cancelled','done') ORDER BY id DESC LIMIT 1",
                (int(user.id), lid, mode, appt.get('date'), time_value),
            ).fetchone()
        if existing:
            appointment_id = int(existing[0])
        else:
            appointment_id = db.create_appointment({
                'user_id': user.id,
                'username': getattr(user, 'username', '') or '',
                'display_name': user_display_name(user),
                'listing_id': lid,
                'viewing_mode': mode,
                'appointment_date': appt.get('date'),
                'appointment_time': time_value,
                'contact_value': f"@{getattr(user, 'username', '')}" if getattr(user, 'username', '') else str(user.id),
                'note': '',
                'status': 'pending',
                'created_at': now_ts(),
            })
            lead_id = create_lead(
                user,
                action='appointment_submit',
                source=str(appt.get('source') or 'user_bot'),
                listing_id=lid,
                payload={'viewing_mode': mode, 'appointment_date': appt.get('date'), 'appointment_time': time_value, **touch_payload},
            )

    from .channel_status_sync import sync_channel_listing_status
    await sync_channel_listing_status(lid)

    item = listing_context(lid)
    title = str(item.get('project') or item.get('community') or item.get('area') or '这套房').strip()
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
    subject = _title_layout_label(title, layout, '｜')
    date_text = _appointment_date_compact(appt.get('date'))
    time_text = _appointment_time_compact(time_value)
    mode_label = APPOINTMENT_MODE_LABELS.get(mode, '实地看房')

    notify_title, notify_lines = format_consult_notify(
        user_id=int(user.id),
        title=f"📅 {'预约时间已修改' if edit_id else '新预约'} #{appointment_id}",
        lines=[
            f'🏠 <b>{he(subject)}</b>',
            f'🕐 {he(date_text)} · {he(time_text)}',
            f'📍 {he(mode_label)}',
            '',
            f'👤 客户｜{_user_mention_html(user)}',
            f'💬 Telegram｜{he(_user_contact_text(user))}',
            '',
            '<b>当前状态｜🟡 待确认</b>',
        ],
        current_action='appointment_submit' if not edit_id else 'appointment_time_update',
    )
    await notify_admins(
        context,
        title=notify_title,
        lines=notify_lines,
        reply_markup=admin_lead_keyboard(lead_id=lead_id, appointment_id=appointment_id, user_id=int(user.id)) if lead_id else None,
        show_bell=False,
    )

    heading = '✅ <b>预约时间已修改</b>' if edit_id else ('✅ <b>视频看房申请已提交</b>' if mode == 'video' else '✅ <b>预约申请已提交</b>')
    lines = [
        heading,
        '',
        f'🏠 <b>{he(subject)}</b>',
        f'📅 {he(date_text)} · {he(time_text)}',
        f"{'🎥' if mode == 'video' else '👀'} {he(mode_label)}",
        f'🆔 {he(_display_listing_id(lid))}',
        '',
    ]
    if mode == 'video':
        lines.extend(['顾问确认时间后，', '会在预约前通过 Telegram 发送视频通话入口。'])
    else:
        lines.extend(['顾问会先确认最新房态和具体时间，', '确认后通过 Telegram 联系你。'])
    context.user_data.pop('appt', None)
    await respond('\n'.join(lines), appointment_success_keyboard())
    return MAIN


async def appoint_flow_cb(update: Update, context: ContextTypes.DEFAULT_TYPE, *, create_lead_fn=None, notify_admins_fn=None) -> int:
    from .appointment_ui import _appointment_confirm_keyboard, _appointment_confirm_text, _appointment_time_keyboard
    from .keyboards_common import main_keyboard
    from .listing import listing_cost_keyboard, listing_cost_text
    from .session_deeplink import clear_session_for_fresh_entry
    from .texts import render_panel, welcome_text

    query = update.callback_query
    await answer_callback_once(query)
    data = str(query.data or '')
    appt = context.user_data.setdefault('appt', {})

    if data != 'home' and not appt:
        await query.edit_message_text('这次预约已失效。\n\n请重新从房源页点击「预约看房」。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
        return MAIN

    if data.startswith('apmode:'):
        appt['mode'] = data.split(':', 1)[1]
        return await _render_date_page(query, appt)

    # 旧关注点 callback 只做兼容，不再生成额外页面。
    if data.startswith('apfocus:'):
        return await _render_date_page(query, appt)

    if data in {'appoint_back_mode', 'apfocus:back_mode'}:
        lid = str(appt.get('listing_id') or '').strip()
        context.user_data.pop('appt', None)
        if lid and lid not in {'待推荐', '未知'}:
            await query.edit_message_text(listing_cost_text(lid), parse_mode=ParseMode.HTML, reply_markup=listing_cost_keyboard(lid))
            return MAIN
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN

    if data.startswith('apdate:'):
        chosen = data.split(':', 1)[1]
        if chosen == 'other':
            appt['awaiting_custom_date'] = True
            await query.edit_message_text('📅 <b>请输入日期</b>\n\n例如：<code>0905</code>、<code>9月5日</code> 或 <code>下周三</code>', parse_mode=ParseMode.HTML)
            return APPT_DATE
        appt['date'] = chosen
        return await _render_time_page(query, appt)

    if data in {'appoint_back_date', 'apedit:date'}:
        return await _render_date_page(query, appt)

    if data in {'appoint_back_time', 'apedit:time'}:
        return await _render_time_page(query, appt)

    if data == 'apedit:contact':
        # 历史确认页兼容；新流程不会生成这个按钮。
        await query.edit_message_text(_appointment_confirm_text(appt), parse_mode=ParseMode.HTML, reply_markup=_appointment_confirm_keyboard())
        return APPT_CONFIRM

    if data.startswith('aptime:'):
        chosen = data.split(':', 1)[1]
        if chosen == 'other':
            appt['awaiting_custom_time'] = True
            await query.edit_message_text('🕐 <b>其他时间</b>\n\n直接输入，例如：<code>20:00</code> 或 <code>晚上8点</code>', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = chosen
        return await _submit_appointment(update, context, appt, create_lead_fn=create_lead_fn, notify_admins_fn=notify_admins_fn)

    if data == 'apconfirm:yes':
        return await _submit_appointment(update, context, appt, create_lead_fn=create_lead_fn, notify_admins_fn=notify_admins_fn)

    if data == 'home':
        clear_session_for_fresh_entry(context)
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN
    return MAIN


async def handle_appointment_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """承接其他日期 / 其他时间输入。"""
    from .appointment_ui import _normalize_custom_date, _appointment_time_keyboard, _title_layout_label
    from .listing import listing_context
    from .utils_formatting import _display_layout

    appt = context.user_data.get('appt') or {}
    value = str(update.effective_message.text or '').strip()[:40]

    if appt.pop('awaiting_custom_date', False):
        normalized = _normalize_custom_date(value)
        if not normalized:
            appt['awaiting_custom_date'] = True
            await update.effective_message.reply_text('日期格式没有识别出来。请试试：<code>0905</code>、<code>9月5日</code> 或 <code>下周三</code>。', parse_mode=ParseMode.HTML)
            return APPT_DATE
        appt['date'] = normalized
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('community') or info.get('area') or '这套房')
        layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
        await update.effective_message.reply_text(
            f'🕐 <b>选择时间</b>\n\n📅 {he(normalized)}\n🏠 {he(_title_layout_label(title, layout, "｜"))}',
            parse_mode=ParseMode.HTML,
            reply_markup=_appointment_time_keyboard(),
        )
        return APPT_TIME

    if appt.pop('awaiting_custom_time', False):
        valid = bool(re.fullmatch(r'(?:[01]?\d|2[0-3]):[0-5]\d|(?:上午|下午|傍晚|晚上)\s*\d{1,2}(?::[0-5]\d|点)?', value))
        if not valid:
            appt['awaiting_custom_time'] = True
            await update.effective_message.reply_text('时间格式没有识别出来。请试试：<code>20:00</code> 或 <code>晚上8点</code>。', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = value
        return await _submit_appointment(update, context, appt)

    await update.effective_message.reply_text('请点击页面上的日期或时间按钮。')
    return APPT_DATE