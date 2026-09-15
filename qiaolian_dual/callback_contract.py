"""Callback handlers for the contract domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data == 'contract:view'
        or data == 'contract:toggle_reminder'
        or data == 'contract:renew'
        or data.startswith('contract:renew_yes:')
        or data == 'contract:terminate'
        or data.startswith('contract:terminate_yes:')
        or data == 'contract:change'  # legacy callback only; no current UI generates it.
    )


async def handle_contract_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _binding_contract_text, _binding_days_left, _binding_end_date, _contract_actions_keyboard, _user_contact_text, _user_mention_html
    from .flows import show_search_entry, show_service_hub
    from .results_admin import _notify_admins
    from .search import create_lead
    from .session_deeplink import now_ts
    from .texts import render_panel

    if data == 'contract:view':
        binding = db.get_active_binding(user.id)
        if not binding:
            return await show_service_hub(update, context)
        create_lead(user, action='contract_view_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding.get('id')})
        await render_panel(update, text=_binding_contract_text(binding, user.id), parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id), context=context)
        return MAIN

    if data == 'contract:toggle_reminder':
        binding = db.get_active_binding(user.id)
        if not binding:
            return await show_service_hub(update, context)
        sub = db.toggle_lease_reminder(user.id, now_ts())
        enabled = int(sub.get('lease_reminder_enabled', 1) or 1) == 1
        create_lead(user, action='lease_reminder_enable_click' if enabled else 'lease_reminder_disable_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding.get('id'), 'lease_reminder_enabled': enabled})
        prefix = '已开启到期提醒，租约到期前 7 天会提醒你。' if enabled else '已关闭到期提醒。'
        await render_panel(update, text=f'{prefix}\n\n{_binding_contract_text(binding, user.id)}', parse_mode=ParseMode.HTML, reply_markup=_contract_actions_keyboard(user.id), context=context)
        return MAIN

    if data == 'contract:renew':
        binding = db.get_active_binding(user.id)
        if not binding:
            return await show_service_hub(update, context)
        days_left = _binding_days_left(binding)
        day_text = f'{days_left} 天' if days_left is not None else '待确认'
        open_tracking = db.get_open_renewal_tracking(binding_id=int(binding.get('id') or 0), user_id=user.id)
        create_lead(user, action='renewal_inquiry_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding.get('id'), 'days_left': days_left, 'open_tracking_id': (open_tracking or {}).get('id')})
        followup = '续租正在跟进中。时间或想法有变化，直接联系中文顾问即可。' if open_tracking else '想继续住这套吗？确认后，中文顾问会和你核对新的租期和价格。'
        await render_panel(
            update,
            text=(
                '🔄 <b>续租</b>\n\n'
                f'🏠 当前房源：{he(str(binding.get("property_name") or "-"))}\n'
                f'📅 到期日：{he(_binding_end_date(binding) or "待确认")}\n'
                f'⏳ 还有：<b>{he(day_text)}</b>\n\n'
                f'{followup}'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ 我想续租', callback_data=f'contract:renew_yes:{int(binding.get("id") or 0)}')],
                [InlineKeyboardButton('💬 中文顾问', callback_data='appointment_menu:contact')],
                [InlineKeyboardButton('⬅️ 返回我的租约', callback_data='contract:view')],
            ]),
            context=context,
        )
        return MAIN

    if data.startswith('contract:renew_yes:'):
        raw_bid = data.split(':', 2)[2]
        try:
            binding_id = int(raw_bid)
        except (TypeError, ValueError):
            binding_id = 0
        binding = db.get_binding_by_id(binding_id) if binding_id > 0 else None
        if not binding or int(binding.get('user_id') or 0) != int(user.id):
            await render_panel(update, text='未找到可确认的续租档案。', reply_markup=_contract_actions_keyboard(user.id), context=context)
            return MAIN
        existing = db.get_open_renewal_tracking(binding_id=binding_id, user_id=user.id)
        tracking_id = int(existing.get('id') or 0) if existing else 0
        if tracking_id <= 0:
            tracking_id = db.create_renewal_tracking(binding_id=binding_id, user_id=user.id, listing_id=str(binding.get('property_name') or ''), renewal_status='pending', user_response='用户确认续租', created_at=now_ts())
        create_lead(user, action='renewal_confirm_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding_id, 'tracking_id': tracking_id, 'deduped': bool(existing)})
        if not existing:
            await _notify_admins(context, title='客户申请续租', lines=[f'客户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'绑定档案：#{binding_id}', f'当前房源：{he(str(binding.get("property_name") or "-"))}', f'合同到期：{he(_binding_end_date(binding) or "-")}', '下一步：确认新的租期和价格。'])
        await render_panel(
            update,
            text=(
                '💬 <b>续租正在跟进</b>\n\n中文顾问会继续和你确认新的租期和价格。'
                if existing else
                '✅ <b>已收到你的续租申请</b>\n\n中文顾问会和你确认新的租期和价格，再把结果告诉你。'
            ) + '\n\n时间或想法有变化，直接联系中文顾问即可。',
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('💬 中文顾问', callback_data='appointment_menu:contact')],
                [InlineKeyboardButton('📋 返回我的租约', callback_data='contract:view')],
            ]),
            context=context,
        )
        return MAIN

    if data == 'contract:terminate':
        binding = db.get_active_binding(user.id)
        if not binding:
            return await show_service_hub(update, context)
        create_lead(user, action='termination_inquiry_click', source='contract_hub', listing_id=str(binding.get('property_name') or ''), payload={'binding_id': binding.get('id')})
        await render_panel(
            update,
            text=(
                '🚪 <b>退租</b>\n\n'
                f'🏠 当前房源：{he(str(binding.get("property_name") or "-"))}\n'
                f'📅 合同到期：{he(_binding_end_date(binding) or "待确认")}\n\n'
                '准备退租前，建议先确认合同中的通知期、押金退还条件和费用结算方式。\n\n'
                '提交退租后，中文顾问会和你确认预计退租时间、房屋交接和押金核对安排。'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ 我要退租', callback_data=f'contract:terminate_yes:{int(binding.get("id") or 0)}')],
                [InlineKeyboardButton('🔐 退租与押金说明', callback_data='hub:rental:deposit')],
                [InlineKeyboardButton('💬 中文顾问', callback_data='appointment_menu:contact')],
                [InlineKeyboardButton('⬅️ 返回我的租约', callback_data='contract:view')],
            ]),
            context=context,
        )
        return MAIN

    if data.startswith('contract:terminate_yes:'):
        raw_bid = data.split(':', 2)[2]
        try:
            binding_id = int(raw_bid)
        except (TypeError, ValueError):
            binding_id = 0
        binding = db.get_binding_by_id(binding_id) if binding_id > 0 else None
        if not binding or int(binding.get('user_id') or 0) != int(user.id):
            await render_panel(update, text='未找到可提交的退租档案。', reply_markup=_contract_actions_keyboard(user.id), context=context)
            return MAIN
        create_lead(
            user,
            action='lease_termination_request',
            source='contract_hub',
            listing_id=str(binding.get('property_name') or ''),
            payload={'binding_id': binding_id, 'lease_end_date': _binding_end_date(binding), 'request_status': 'pending'},
        )
        await _notify_admins(
            context,
            title='客户提交退租计划',
            lines=[
                f'客户：{_user_mention_html(user)}',
                f'联系方式：{he(_user_contact_text(user))}',
                f'绑定档案：#{binding_id}',
                f'当前房源：{he(str(binding.get("property_name") or "-"))}',
                f'合同到期：{he(_binding_end_date(binding) or "-")}',
                '下一步：确认预计退租日期、合同通知期、房屋交接和押金核对。',
            ],
        )
        await render_panel(
            update,
            text=(
                '✅ <b>已收到你的退租计划</b>\n\n'
                '中文顾问会与你确认：\n'
                '• 预计退租日期\n'
                '• 合同通知期\n'
                '• 房屋交接时间\n'
                '• 水电及其他费用\n'
                '• 押金核对事项\n\n'
                '确认完成后，再安排正式退租交接。'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('🔐 退租与押金说明', callback_data='hub:rental:deposit')],
                [InlineKeyboardButton('💬 中文顾问', callback_data='appointment_menu:contact')],
                [InlineKeyboardButton('📋 返回我的租约', callback_data='contract:view')],
            ]),
            context=context,
        )
        return MAIN

    if data == 'contract:change':
        # 历史按钮兼容：不再存在“换房”租约动作，直接回到正常找房流程。
        return await show_search_entry(update, context)

    return None
