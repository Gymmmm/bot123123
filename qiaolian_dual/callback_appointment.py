"""Callback handlers for the appointment domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data.startswith('appointment_menu:cancel_confirm:')
        or data.startswith('appointment_menu:cancel:')
        or data.startswith('appointment_menu:edit:')
        or data == 'appointment_menu:details'
        or data == 'appointment_menu:list'
        or data.startswith('appointment_menu:contact')
        or data.startswith('appointment_menu:')
    )


async def handle_appointment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .appointments_view import (
        _appointment_card_keyboard,
        _appointment_date_compact,
        _appointment_details_keyboard,
        _appointment_listing_compact,
        _appointment_time_compact,
        _find_user_appointment,
        appointment_details_text,
        list_recent_appointments,
    )
    from .flows import start_appointment
    from .keyboards_common import contact_handoff_keyboard
    from .results_admin import _notify_admins
    from .search import create_lead
    from .texts import advisor_handoff_text, render_panel

    if data.startswith('appointment_menu:cancel_confirm:'):
        try:
            appointment_id = int(data.rsplit(':', 1)[1])
        except (TypeError, ValueError):
            await answer_callback_once(query, '预约编号无效', show_alert=True)
            return MAIN
        row = _find_user_appointment(user.id, appointment_id)
        current_status = str((row or {}).get('status') or '')
        if not row or current_status in {'done', 'cancelled'}:
            await answer_callback_once(query, '这条预约已无法取消', show_alert=True)
            return MAIN
        if db.update_appointment_status(appointment_id, 'cancelled'):
            from .channel_status_sync import sync_channel_listing_status
            await sync_channel_listing_status(str(row.get('listing_id') or ''))
            await answer_callback_once(query, '预约已取消', show_alert=False)
            await render_panel(
                update,
                text=appointment_details_text(user.id),
                parse_mode=ParseMode.HTML,
                reply_markup=_appointment_details_keyboard(user.id),
                context=context,
            )
        else:
            await answer_callback_once(query, '取消失败，请联系中文顾问', show_alert=True)
        return MAIN

    if data.startswith('appointment_menu:cancel:'):
        try:
            appointment_id = int(data.rsplit(':', 1)[1])
        except (TypeError, ValueError):
            await answer_callback_once(query, '预约编号无效', show_alert=True)
            return MAIN
        row = _find_user_appointment(user.id, appointment_id)
        if not row or str(row.get('status') or '') in {'done', 'cancelled'}:
            await answer_callback_once(query, '这条预约已无法取消', show_alert=True)
            return MAIN
        listing = _appointment_listing_compact(row.get('listing_id'))
        date_text = _appointment_date_compact(row.get('appointment_date'))
        time_text = _appointment_time_compact(row.get('appointment_time'))
        await render_panel(
            update,
            text=(
                '⚠️ <b>确认取消预约？</b>\n\n'
                f'{he(date_text)} · {he(time_text)}\n'
                f'房源：<b>{he(listing)}</b>\n\n'
                '取消后如需再次看房，需要重新提交预约。'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('确认取消', callback_data=f'appointment_menu:cancel_confirm:{appointment_id}')],
                [InlineKeyboardButton('暂不取消', callback_data='appointment_menu:details')],
            ]),
            context=context,
        )
        return MAIN

    if data.startswith('appointment_menu:edit:'):
        from .appointment_ui import _appointment_time_keyboard
        try:
            appointment_id = int(data.rsplit(':', 1)[1])
        except (TypeError, ValueError):
            await answer_callback_once(query, '预约编号无效', show_alert=True)
            return MAIN
        row = _find_user_appointment(user.id, appointment_id)
        if not row or str(row.get('status') or '') in {'done', 'cancelled'}:
            await answer_callback_once(query, '这条预约已无法修改', show_alert=True)
            return MAIN
        context.user_data['appt'] = {
            'listing_id': str(row.get('listing_id') or ''),
            'mode': str(row.get('viewing_mode') or 'offline'),
            'date': str(row.get('appointment_date') or ''),
            'source': 'appointment_edit',
            'focus_keys': [],
            'touch_payload': {'edit_appointment_id': appointment_id},
        }
        await query.edit_message_text(
            f"🕐 <b>选择时间</b>\n\n📅 {he(_appointment_date_compact(row.get('appointment_date')))}\n🏠 {he(_appointment_listing_compact(row.get('listing_id')))}",
            parse_mode=ParseMode.HTML,
            reply_markup=_appointment_time_keyboard(),
        )
        return APPT_TIME

    if data == 'appointment_menu:details':
        await render_panel(
            update,
            text=appointment_details_text(user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_appointment_details_keyboard(user.id),
            context=context,
        )
        return MAIN

    if data == 'appointment_menu:list':
        await render_panel(
            update,
            text=list_recent_appointments(user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=_appointment_card_keyboard(),
            context=context,
        )
        return MAIN

    if data.startswith('appointment_menu:contact'):
        parts = data.split(':', 3)
        scope = parts[2] if len(parts) >= 3 else ''
        ref = parts[3] if len(parts) >= 4 else ''
        listing_id = ''
        binding = db.get_active_binding(user.id)
        source_label = 'appointment_hub'
        if scope == 'listing':
            listing_id = ref
            context.user_data['contact_listing_id'] = listing_id
            source_label = 'listing_landing'
        else:
            listing_id = str(context.user_data.get('contact_listing_id') or '')
        create_lead(
            user,
            action='consult_menu_click',
            source=source_label,
            listing_id=listing_id or str((binding or {}).get('property_name') or ''),
            payload={'binding_id': (binding or {}).get('id'), 'listing_id': listing_id},
        )
        await _notify_admins(
            context,
            title='中文顾问咨询请求',
            lines=[
                f'用户：{_user_mention_html(user)}',
                f'联系方式：{he(_user_contact_text(user))}',
                f'来源：{he(source_label)}',
                f"房源：{he(listing_id or str((binding or {}).get('property_name') or '-'))}",
            ],
        )
        await render_panel(
            update,
            text=advisor_handoff_text(listing_id=listing_id, user_id=user.id),
            parse_mode=ParseMode.HTML,
            reply_markup=contact_handoff_keyboard(listing_id=listing_id),
            context=context,
        )
        return MAIN

    if data.startswith('appointment_menu:'):
        mode = data.split(':', 1)[1]
        create_lead(user, action='appointment_click', source='menu_appointment', payload={'from_menu': True, 'preferred_mode': mode})
        return await start_appointment(
            update,
            context,
            '待推荐',
            source='menu_appointment',
            touch_payload={'from_menu': True, 'listing_unknown': True},
            initial_mode=mode,
        )
    return None
