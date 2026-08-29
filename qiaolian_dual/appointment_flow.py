"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

async def appoint_flow_cb(update: Update, context: ContextTypes.DEFAULT_TYPE, *, create_lead_fn=None, notify_admins_fn=None) -> int:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .appointment_ui import _appointment_confirm_keyboard, _appointment_confirm_text, _appointment_date_keyboard, _appointment_focus_keyboard, _appointment_focus_prompt, _appointment_time_keyboard, _title_layout_label
    from .appointments_view import _appointment_date_compact, _appointment_listing_compact, _appointment_time_compact
    from .flows import start_appointment
    from .keyboards_common import appointment_success_keyboard, main_keyboard
    from .listing import listing_context, listing_is_available, listing_unavailable_keyboard, listing_unavailable_text
    from .results_admin import _notify_admins as default_notify_admins, admin_lead_keyboard
    from .search import create_lead as default_create_lead
    from .session_deeplink import clear_session_for_fresh_entry, now_ts, user_display_name
    from .texts import _personal_greeting, render_panel, welcome_text
    from .utils_formatting import _display_layout, _display_listing_id
    query = update.callback_query
    create_lead = create_lead_fn or default_create_lead
    _notify_admins = notify_admins_fn or default_notify_admins
    await query.answer()
    data = query.data
    appt = context.user_data.setdefault('appt', {})
    if data != 'home' and (not appt):
        await query.edit_message_text('这次预约已失效。\n\n请重新从房源页点击「预约看房」；我会自动带上对应房源。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]))
        context.user_data.pop('appt', None)
        return MAIN
    if data.startswith('apmode:'):
        lid = str(appt.get('listing_id') or '').strip()
        if not lid or lid == '未知':
            context.user_data.pop('appt', None)
            await query.edit_message_text('无法识别这套房源。\n\n请从房源详情页的「预约看房」重新进入。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]), parse_mode=ParseMode.HTML)
            return MAIN
        appt['mode'] = data.split(':', 1)[1]
        appt['focus_keys'] = list(APPOINTMENT_FOCUS_ORDER)
        mode_label = APPOINTMENT_MODE_LABELS.get(appt['mode'], '预约看房')
        text = f'📅 <b>{he(mode_label)} · 选择日期</b>\n\n选择你方便的日期：'
        await query.edit_message_text(text, reply_markup=_appointment_date_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_DATE
    if data.startswith('apfocus:toggle:'):
        key = data.split(':', 2)[2]
        if key not in APPOINTMENT_FOCUS_LABELS:
            return APPT_FOCUS
        selected = set((str(k) for k in appt.get('focus_keys') or APPOINTMENT_FOCUS_ORDER))
        if key in selected:
            selected.remove(key)
        else:
            selected.add(key)
        appt['focus_keys'] = [k for k in APPOINTMENT_FOCUS_ORDER if k in selected]
        text = _appointment_focus_prompt(appt.get('mode', 'offline'), appt.get('listing_id', ''), selected)
        await query.edit_message_text(text, reply_markup=_appointment_focus_keyboard(selected), parse_mode=ParseMode.HTML)
        return APPT_FOCUS
    if data == 'apfocus:back_mode':
        return await start_appointment(update, context, appt.get('listing_id', '未知'), source=appt.get('source', 'user_bot'), touch_payload=appt.get('touch_payload'))
    if data == 'apfocus:next':
        selected = set((str(k) for k in appt.get('focus_keys') or []))
        if not selected:
            await query.answer('至少保留 1 个关注点', show_alert=True)
            return APPT_FOCUS
        text = '第三步：请选择预约日期。'
        await query.edit_message_text(text, reply_markup=_appointment_date_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_DATE
    if data == 'appoint_back_mode':
        return await start_appointment(update, context, appt.get('listing_id', '未知'), source=appt.get('source', 'user_bot'), touch_payload=appt.get('touch_payload'))
    if data.startswith('apdate:'):
        chosen = data.split(':', 1)[1]
        if chosen == 'other':
            appt['awaiting_custom_date'] = True
            await query.edit_message_text('📅 <b>请输入日期</b>\n\n例如：<code>0820</code>、<code>8月20日</code> 或 <code>下周三</code>', parse_mode=ParseMode.HTML)
            return APPT_DATE
        appt['date'] = chosen
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('title') or info.get('area') or '这套房')
        text = f'📅 <b>{he(chosen)} · {he(title)}</b>\n\n请选择时间段：'
        await query.edit_message_text(text, reply_markup=_appointment_time_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_TIME
    if data == 'appoint_back_date':
        query.data = 'apmode:' + appt.get('mode', 'offline')
        return await appoint_flow_cb(update, context, create_lead_fn=create_lead, notify_admins_fn=_notify_admins)
    if data == 'apedit:date':
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('title') or info.get('area') or '这套房')
        await query.edit_message_text(f'📅 <b>改预约日期 · {he(title)}</b>\n\n请重新选择：', parse_mode=ParseMode.HTML, reply_markup=_appointment_date_keyboard())
        return APPT_DATE
    if data == 'apedit:time':
        await query.edit_message_text(f"⏰ <b>改预约时间</b>\n\n已选日期：{he(str(appt.get('date') or '-'))}", parse_mode=ParseMode.HTML, reply_markup=_appointment_time_keyboard())
        return APPT_TIME
    if data == 'apedit:contact':
        appt['awaiting_contact'] = True
        await query.edit_message_text('📞 <b>留一个方便联系的号码</b>\n\n请直接发送手机号或微信号。', parse_mode=ParseMode.HTML)
        return APPT_CONFIRM
    if data.startswith('aptime:'):
        chosen_time = data.split(':', 1)[1]
        if chosen_time == 'other':
            appt['awaiting_custom_time'] = True
            await query.edit_message_text('⏰ <b>请输入方便的时间</b>\n\n例如：<code>20:00</code> 或 <code>晚上8点</code>', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = chosen_time
        await query.edit_message_text(_appointment_confirm_text(appt), reply_markup=_appointment_confirm_keyboard(), parse_mode=ParseMode.HTML)
        return APPT_CONFIRM
    if data == 'appoint_back_time':
        query.data = 'apdate:' + appt.get('date', '')
        return await appoint_flow_cb(update, context, create_lead_fn=create_lead, notify_admins_fn=_notify_admins)
    if data == 'apconfirm:yes':
        lid_submit = str(appt.get('listing_id') or '').strip()
        is_general_request = lid_submit in {'', '待推荐'} or bool((appt.get('touch_payload') or {}).get('listing_unknown'))
        if not is_general_request:
            is_available, availability_reason = listing_is_available(lid_submit)
            if not is_available:
                context.user_data.pop('appt', None)
                await query.edit_message_text(
                    listing_unavailable_text(availability_reason),
                    reply_markup=listing_unavailable_keyboard(lid_submit),
                    parse_mode=ParseMode.HTML,
                )
                return MAIN
        if not appt.get('date') or not appt.get('time'):
            context.user_data.pop('appt', None)
            await query.edit_message_text('预约信息不完整或已过期。\n请从房源详情页的「预约看房」重新发起。', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 返回首页', callback_data='home')]]), parse_mode=ParseMode.HTML)
            return MAIN
        user = update.effective_user
        time_value = str(appt.get('time') or '')
        time_label = APPOINTMENT_TIME_LABELS.get(time_value, time_value)
        mode_label = APPOINTMENT_MODE_LABELS.get(appt.get('mode'), str(appt.get('mode') or '-'))
        focus_keys = [k for k in APPOINTMENT_FOCUS_ORDER if k in set(appt.get('focus_keys') or APPOINTMENT_FOCUS_ORDER)]
        focus_labels = [APPOINTMENT_FOCUS_LABELS[k] for k in focus_keys]
        focus_text = '；'.join(focus_labels)
        listing_value = appt.get('listing_id', '') if appt.get('listing_id') else '待推荐'
        with db.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM appointments WHERE user_id=? AND listing_id=? AND viewing_mode=? AND appointment_date=? AND appointment_time=? AND status NOT IN ('cancelled','completed') ORDER BY id DESC LIMIT 1",
                (int(user.id), listing_value, appt.get('mode', ''), appt.get('date', ''), time_value),
            ).fetchone()
        if existing:
            appointment_id = int(existing[0])
            lead_id = None
        else:
            appointment_id = db.create_appointment({'user_id': user.id, 'username': getattr(user, 'username', '') or '', 'display_name': user_display_name(user), 'listing_id': listing_value, 'viewing_mode': appt.get('mode', ''), 'appointment_date': appt.get('date', ''), 'appointment_time': time_value, 'contact_value': str(appt.get('contact_value') or (f'@{user.username}' if getattr(user, 'username', '') else str(user.id))), 'note': f'关注点：{focus_text}' if focus_text else '', 'status': 'pending', 'created_at': now_ts()})
            lead_id = create_lead(user, action='appointment_submit', source=appt.get('source', 'user_bot'), listing_id=listing_value, payload={'viewing_mode': appt.get('mode', ''), 'appointment_date': appt.get('date', ''), 'appointment_time': time_value, 'focus_keys': focus_keys, 'focus_labels': focus_labels, **(appt.get('touch_payload') or {})})
        if listing_value not in {'', '待推荐'}:
            from .channel_status_sync import sync_channel_listing_status
            await sync_channel_listing_status(str(listing_value))
        focus_short = '、'.join(focus_labels[:3]) if focus_labels else '默认全项'
        if len(focus_labels) > 3:
            focus_short += '等'
        await _notify_admins(context, title=f'新预约 · 待处理 #{appointment_id}', lines=[f'用户：{_user_mention_html(user)}', f"房源：{he(str(appt.get('listing_id') or '待推荐'))}", f"时间：{he(_appointment_date_compact(appt.get('date') or '-'))} · {he(_appointment_time_compact(time_value))}", f'方式：{he(mode_label)}', f'联系：{he(_user_contact_text(user))}', f'关注：{he(focus_short)}', f"来源：{he(str(appt.get('source', 'user_bot')))} · 线索 #{lead_id or '-'}"], reply_markup=admin_lead_keyboard(lead_id=lead_id, appointment_id=appointment_id, user_id=int(user.id)) if lead_id is not None else None)
        item = listing_context(lid_submit)
        title = str(item.get('project') or item.get('title') or item.get('area') or '这套房').strip()
        layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
        contact = str(appt.get('contact_value') or _user_contact_text(user))
        subject_text = '🏠 <b>房源尚未确定</b>' if is_general_request else f"🏠 <b>{he(_title_layout_label(title, layout))}</b>"
        context.user_data.pop('appt', None)
        await query.edit_message_text(f"✅ <b>已收到你的预约申请</b>\n\n{subject_text}\n📅 {he(str(appt.get('date', '') or '-'))} · {he(time_label)}\n📍 {he(mode_label)}\n\n顾问会先确认房态和可看时间，再通过 Telegram 联系你。无需重复提交。", parse_mode=ParseMode.HTML, reply_markup=appointment_success_keyboard())
        return MAIN
    if data == 'home':
        clear_session_for_fresh_entry(context)
        await render_panel(update, text=welcome_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML, context=context)
        return MAIN
    return MAIN

async def handle_appointment_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """承接“其他日期 / 其他时间”的手动输入。"""
    from .appointment_ui import _appointment_confirm_keyboard, _appointment_confirm_text, _appointment_time_keyboard, _normalize_custom_date
    from .listing import listing_context
    appt = context.user_data.get('appt') or {}
    value = str(update.effective_message.text or '').strip()[:40]
    if appt.pop('awaiting_custom_date', False):
        normalized_date = _normalize_custom_date(value)
        if not normalized_date:
            appt['awaiting_custom_date'] = True
            await update.effective_message.reply_text('日期格式没有识别出来。请试试：<code>0820</code>、<code>8月20日</code> 或 <code>下周三</code>。', parse_mode=ParseMode.HTML)
            return APPT_DATE
        appt['date'] = normalized_date
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('title') or info.get('area') or '这套房')
        await update.effective_message.reply_text(f'📅 <b>{he(normalized_date)} · {he(title)}</b>\n\n请选择时间段：', parse_mode=ParseMode.HTML, reply_markup=_appointment_time_keyboard())
        return APPT_TIME
    if appt.pop('awaiting_custom_time', False):
        valid_time = bool(re.fullmatch('(?:[01]?\\d|2[0-3]):[0-5]\\d|(?:上午|下午|傍晚|晚上)\\s*\\d{1,2}(?::[0-5]\\d|点)?', value))
        if not valid_time:
            appt['awaiting_custom_time'] = True
            await update.effective_message.reply_text('时间格式没有识别出来。请试试：<code>20:00</code> 或 <code>晚上8点</code>。', parse_mode=ParseMode.HTML)
            return APPT_TIME
        appt['time'] = value
        info = listing_context(str(appt.get('listing_id') or ''))
        title = str(info.get('project') or info.get('title') or info.get('area') or '这套房')
        await update.effective_message.reply_text(_appointment_confirm_text(appt), parse_mode=ParseMode.HTML, reply_markup=_appointment_confirm_keyboard())
        return APPT_CONFIRM
    if appt.pop('awaiting_contact', False):
        if len(value) < 4:
            appt['awaiting_contact'] = True
            await update.effective_message.reply_text('这个联系方式太短了，请重新发送手机号或微信号。')
            return APPT_CONFIRM
        appt['contact_value'] = value
        await update.effective_message.reply_text(_appointment_confirm_text(appt), parse_mode=ParseMode.HTML, reply_markup=_appointment_confirm_keyboard())
        return APPT_CONFIRM
    await update.effective_message.reply_text('请点击页面上的日期或时间按钮。')
    return APPT_DATE
