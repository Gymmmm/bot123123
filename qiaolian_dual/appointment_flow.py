"""预约日期 → 时间 → 直接提交流程。"""
from __future__ import annotations

from .common import *


async def _submit_appointment(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    appt: dict,
    *,
    create_lead_fn=None,
    notify_admins_fn=None,
) -> int:
    """提交预约；历史确认按钮仍复用本函数，新页面不再生成确认页。"""
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
    _notify_admins = notify_admins_fn or default_notify_admins
    query = getattr(update, 'callback_query', None)
    message = getattr(update, 'effective_message', None)

    async def respond(text: str, *, reply_markup=None) -> None:
        kwargs = {'parse_mode': ParseMode.HTML, 'reply_markup': reply_markup}
        if query is not None:
            await query.edit_message_text(text, **kwargs)
        elif message is not None:
            await message.reply_text(text, **kwargs)
        else:
            raise RuntimeError('appointment_response_target_missing')

    lid_submit = str(appt.get('listing_id') or '').strip()
    is_general_request = lid_submit in {'', '待推荐'} or bool((appt.get('touch_payload') or {}).get('listing_unknown'))
    if not is_general_request:
        is_available, availability_reason = listing_is_available(lid_submit)
        if not is_available:
            context.user_data.pop('appt', None)
            await respond(listing_unavailable_text(availability_reason), reply_markup=listing_unavailable_keyboard(lid_submit))
            return MAIN

    if not appt.get('date') or not appt.get('time'):
        context.user_data.pop('appt', None)
        await respond(
            '预约信息不完整或已过期。\n请从房源详情页的「预约看房」重新发起。',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]),
        )
        return MAIN

    user = update.effective_user
    time_value = str(appt.get('time') or '')
    mode = str(appt.get('mode') or 'offline')
    mode_label = APPOINTMENT_MODE_LABELS.get(mode, '实地看房')
    listing_value = appt.get('listing_id', '') if appt.get('listing_id') else '待推荐'
    touch_payload = dict(appt.get('touch_payload') or {})
    edit_appointment_id = int(touch_payload.get('edit_appointment_id') or 0)
    lead_id = None

    if edit_appointment_id:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT id FROM appointments WHERE id=? AND user_id=? AND status NOT IN ('done','cancelled')",
                (edit_appointment_id, int(user.id)),
            ).fetchone()
            if not row:
                context.user_data.pop('appt', None)
                await respond('这条预约已经无法修改。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list')]]))
                return MAIN
            conn.execute(
                "UPDATE appointments SET viewing_mode=?, appointment_date=?, appointment_time=?, status='pending' WHERE id=? AND user_id=?",
                (mode, appt.get('date', ''), time_value, edit_appointment_id, int(user.id)),
            )
            conn.commit()
        appointment_id = edit_appointment_id
        lead_id = create_lead(
            user,
            action='appointment_time_update',
            source='appointment_edit',
            listing_id=listing_value,
            payload={'appointment_id': appointment_id, 'viewing_mode': mode, 'appointment_date': appt.get('date', ''), 'appointment_time': time_value},
        )
    else:
        with db.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM appointments WHERE user_id=? AND listing_id=? AND viewing_mode=? AND appointment_date=? AND appointment_time=? AND status NOT IN ('cancelled','done') ORDER BY id DESC LIMIT 1",
                (int(user.id), listing_value, mode, appt.get('date', ''), time_value),
            ).fetchone()
        if existing:
            appointment_id = int(existing[0])
        else:
            appointment_id = db.create_appointment({
                'user_id': user.id,
                'username': getattr(user, 'username', '') or '',
                'display_name': user_display_name(user),
                'listing_id': listing_value,
                'viewing_mode': mode,
                'appointment_date': appt.get('date', ''),
                'appointment_time': time_value,
                'contact_value': str(appt.get('contact_value') or (f'@{user.username}' if getattr(user, 'username', '') else str(user.id))),
                'note': '',
                'status': 'pending',
                'created_at': now_ts(),
            })
            lead_id = create_lead(
                user,
                action='appointment_submit',
                source=appt.get('source', 'user_bot'),
                listing_id=listing_value,
                payload={
                    'viewing_mode': mode,
                    'appointment_date': appt.get('date', ''),
                    'appointment_time': time_value,
                    **touch_payload,
                },
            )

    if listing_value not in {'', '待推荐'}:
        from .channel_status_sync import sync_channel_listing_status
        await sync_channel_listing_status(str(listing_value))

    item = listing_context(lid_submit)
    title = str(item.get('project') or item.get('title') or item.get('area') or '这套房').strip()
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
    advisor_listing = _title_layout_label(title, layout, '｜') or '房源待确认'
    date_compact = _appointment_date_compact(appt.get('date') or '-')
    time_compact = _appointment_time_compact(time_value)

    await _notify_admins(
        context,
        title=f"📅 {'预约时间已修改' if edit_appointment_id else '新预约'} #{appointment_id}",
        lines=[
            f'🏠 <b>{he(advisor_listing)}</b>',
            f'🕐 {he(date_compact)} · {he(time_compact)}',
            f'📍 {he(mode_label)}',
            '',
            f'👤 客户｜{_user_mention_html(user)}',
            f'💬 Telegram｜{he(_user_contact_text(user))}',
            '',
            '<b>当前状态｜🟡 待确认</b>',
        ],
        reply_markup=admin_lead_keyboard(lead_id=lead_id, appointment_id=appointment_id, user_id=int(user.id)) if lead_id is not None else None,
        show_bell=False,
    )

    qc_id = _display_listing_id(lid_submit) if not is_general_request else ''
    subject_text = '🏠 <b>房源尚未确定</b>' if is_general_request else f"🏠 <b>{he(advisor_listing)}</b>"
    context.user_data.pop('appt', None)

    if edit_appointment_id:
        success_heading = '✅ <b>预约时间已修改</b>'
    elif mode == 'video':
        success_heading = '✅ <b>视频看房申请已提交</b>'
    else:
        success_heading = '✅ <b>预约申请已提交</b>'

    lines = [
        success_heading,
        '',
        subject_text,
        f'📅 {he(date_compact)} · {he(time_compact)}',
        f"{'🎥' if mode == 'video' else '👀'} {he(mode_label)}",
    ]
    if qc_id:
        lines.append(f'🆔 {he(qc_id)}')
    lines.append('')
    if mode == 'video':
        lines.extend(['顾问确认时间后，', '会在预约前通过 Telegram 发送视频通话入口。'])
    else:
        lines.extend(['顾问会先确认最新房态和具体时间，', '确认后通过 Telegram 联系你。'])
    await respond('\n'.join(lines), reply_markup=appointment_success_keyboard())
    return MAIN


async def appoint_flow_cb(update: Update, context: ContextTypes.DEFAULT_TYPE, *, create_lead_fn=None, notify_admins_fn=None) -> int:
    from .appointment_ui import _appointment_confirm_keyboard, _appointment_confirm_text, _appointment_date_keyboard, _appointment_time_keyboard
    from .flows import start_appointment
    from .keyboards_common import main_keyboard
    from .listing import listing_context, listing_cost_keyboard, listing_cost_text
    from .session_deeplink import clear_session_for_fresh_entry
    from .texts import render_panel, welcome_text
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price

    query = update.callback_query
    create_lead = create_lead_fn
    _notify_admins = notify_admins_fn
    await answer_callback_once(query)
    data = query.data
    appt = context.user_data.setdefault('appt', {})

    if data != 'home' and not appt:
        await query.edit_message_text(
            '这次预约已失效。\n\n请重新从房源页点击「预约看房」。',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]),
        )
        context.user_data.pop('appt', None)
        return MAIN

    if data.startswith('apmode:'):
        lid = str(appt.get('listing_id') or '').strip()
        if not lid or lid == '未知':
            context.user_data.pop('appt', None)
            await query.edit_message_text('无法识别这套房源。\n请从房源详情页的「预约看房」重新进入。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
            return MAIN
        mode = data.split(':', 1)[1]
        appt['mode'] = mode
        info = listing_context(lid)
        title = str(info.get('project') or info.get('community') or info.get('area') or '这套房').strip()
        layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
        subject = _title_layout_label(title, layout, '｜')
        price_line = f"\n💰 <b>{he(_fmt_price(info.get('price')))}</b>" if info.get('price') not in (None, '', 0, '0') else ''
        qc = _display_listing_id(lid)
        heading = f"🎥 <b>视频看房｜{he(qc)}</b>" if mode == 'video' else f"📅 <b>预约看房｜{he(qc)}</b>"
        question = '哪天方便视频看房？' if mode == 'video' else '哪天方便看房？'
        await query.edit_message_text(
            f'{heading}\n\n🏠 <b>{he(subject)}</b>{price_line}\n\n{question}',
            reply_markup=_appointment_date_keyboard(show_video=(mode != 'video')),
            parse_mode=ParseMode.HTML,
        )
        return APPT_DATE

    if data.startswith('apfocus:toggle:') or data == 'apfocus:next':
        appt['focus_keys'] = []
        return await start_appointment(update, context, appt.get('listing_id', '未知'), source=appt.get('source', 'user_bot'), touch_payload=appt.get('touch_payload'))

    if data == 'apfocus:back_mode' or data == 'appoint_back_mode':
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
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('community') or info.get('area') or '这套房')
        layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
        await query.edit_message_text(
            f'🕐 <b>选择时间</b>\n\n📅 {he(_appointment_date_compact_local(chosen))}\n🏠 {he(_title_layout_label(title, layout, "｜"))}',
            reply_markup=_appointment_time_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return APPT_TIME

    if data == 'appoint_back_date':
        mode = str(appt.get('mode') or 'offline')
        lid = str(appt.get('listing_id') or '')
        info = listing_context(lid)
        title = str(info.get('project') or info.get('community') or info.get('area') or '这套房')
        layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
        qc = _display_listing_id(lid)
        price_line = f"\n💰 <b>{he(_fmt_price(info.get('price')))}</b>" if info.get('price') not in (None, '', 0, '0') else ''
        heading = f"🎥 <b>视频看房｜{he(qc)}</b>" if mode == 'video' else f"📅 <b>预约看房｜{he(qc)}</b>"
        question = '哪天方便视频看房？' if mode == 'video' else '哪天方便看房？'
        await query.edit_message_text(
            f'{heading}\n\n🏠 <b>{he(_title_layout_label(title, layout, "｜"))}</b>{price_line}\n\n{question}',
            reply_markup=_appointment_date_keyboard(show_video=(mode != 'video')),
            parse_mode=ParseMode.HTML,
        )
        return APPT_DATE

    if data == 'apedit:date':
        return await appoint_flow_cb(update, context, create_lead_fn=create_lead, notify_admins_fn=_notify_admins)
    if data == 'apedit:time':
        await query.edit_message_text(f"🕐 <b>选择时间</b>\n\n📅 {he(str(appt.get('date') or '-'))}", parse_mode=ParseMode.HTML, reply_markup=_appointment_time_keyboard())
        return APPT_TIME
    if data == 'apedit:contact':
        await query.edit_message_text(_appointment_confirm_text(appt), reply_markup=_appointment_confirm_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_CONFIRM

    if data.startswith('aptime:'):
        chosen_time = data.split(':', 1)[1]
        if chosen_time == 'other':
            appt['awaiting_custom_time'] = True
            await query.edit_message_text('🕐 <b>其他时间</b>\n\n直接输入，例如：<code>20:00</code> 或 <code>晚上8点</code>', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = chosen_time
        return await _submit_appointment(update, context, appt, create_lead_fn=create_lead, notify_admins_fn=_notify_admins)

    if data == 'appoint_back_time':
        return APPT_TIME

    if data == 'apconfirm:yes':
        return await _submit_appointment(update, context, appt, create_lead_fn=create_lead, notify_admins_fn=_notify_admins)

    if data == 'home':
        clear_session_for_fresh_entry(context)
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN
    return MAIN


def _appointment_date_compact_local(value: object) -> str:
    raw = str(value or '').strip()
    bits = raw.replace('/', '-').split('-')
    if len(bits) >= 2 and all(part.isdigit() for part in bits[-2:]):
        return f'{int(bits[-2])}月{int(bits[-1])}日'
    return raw


async def handle_appointment_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """承接“其他日期 / 其他时间”的手动输入。"""
    from .appointment_ui import _appointment_confirm_keyboard, _appointment_confirm_text, _appointment_time_keyboard, _normalize_custom_date
    from .listing import listing_context
    from .utils_formatting import _display_layout
    from .appointment_ui import _title_layout_label

    appt = context.user_data.get('appt') or {}
    value = str(update.effective_message.text or '').strip()[:40]

    if appt.pop('awaiting_custom_date', False):
        normalized_date = _normalize_custom_date(value)
        if not normalized_date:
            appt['awaiting_custom_date'] = True
            await update.effective_message.reply_text('日期格式没有识别出来。请试试：<code>0905</code>、<code>9月5日</code> 或 <code>下周三</code>。', parse_mode=ParseMode.HTML)
            return APPT_DATE
        appt['date'] = normalized_date
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('community') or info.get('area') or '这套房')
        layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type'))
        await update.effective_message.reply_text(
            f'🕐 <b>选择时间</b>\n\n📅 {he(normalized_date)}\n🏠 {he(_title_layout_label(title, layout, "｜"))}',
            parse_mode=ParseMode.HTML,
            reply_markup=_appointment_time_keyboard(),
        )
        return APPT_TIME

    if appt.pop('awaiting_custom_time', False):
        valid_time = bool(re.fullmatch('(?:[01]?\\d|2[0-3]):[0-5]\\d|(?:上午|下午|傍晚|晚上)\\s*\\d{1,2}(?::[0-5]\\d|点)?', value))
        if not valid_time:
            appt['awaiting_custom_time'] = True
            await update.effective_message.reply_text('时间格式没有识别出来。请试试：<code>20:00</code> 或 <code>晚上8点</code>。', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = value
        return await _submit_appointment(update, context, appt)

    if appt.pop('awaiting_contact', False):
        await update.effective_message.reply_text(_appointment_confirm_text(appt), parse_mode=ParseMode.HTML, reply_markup=_appointment_confirm_keyboard())
        return APPT_CONFIRM

    await update.effective_message.reply_text('请点击页面上的日期或时间按钮。')
    return APPT_DATE
