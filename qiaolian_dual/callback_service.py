"""Callback handlers for the tenant-service domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data == 'service:hub'
        or data == 'service:promise'
        or data == 'service:contact'
        or data in {'service:renew', 'service:change', 'service:renew_change', 'service:terminate', 'service:move', 'service:staging', 'service:addons', 'service:checkin_tips'}
        or data in {'service:guide', 'service:local_life'}
        or data == 'service:repair_hub'
        or data.startswith('service_request:')
        or data.startswith('service_slot:')
        or data == 'local:rfcity'
        or data.startswith('rfcity:')
    )


def _service_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])


async def handle_service_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .flows import contact_management, show_search_entry, show_service_hub
    from .keyboards_search import local_life_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_repair_keyboard
    from .results_admin import _notify_admins, admin_repair_keyboard
    from .search import create_lead
    from .session_deeplink import now_ts
    from .texts import local_life_text, render_panel, rfcity_text

    if data == 'service:hub':
        return await show_service_hub(update, context)

    if data == 'service:contact':
        return await contact_management(update, context, source='service_hub')

    if data == 'service:repair_hub':
        await query.edit_message_text('🔧 <b>报修</b>\n\n哪里需要处理？', parse_mode=ParseMode.HTML, reply_markup=service_repair_keyboard())
        return MAIN

    if data == 'service_request:property':
        await query.edit_message_text(
            '🏢 <b>物业沟通</b>\n\n'
            '门禁、电梯卡、停车、公共区域，\n'
            '或者其他需要和物业沟通的事情，\n'
            '可以直接告诉中文顾问。',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(),
        )
        return MAIN

    if data.startswith('service_request:'):
        issue_key = data.split(':', 1)[1]
        issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
        context.user_data['awaiting_service_request'] = {'issue_key': issue_key, 'issue_label': issue_label}
        urgent = issue_key in {'repair_water', 'repair_power', 'repair_door'}
        note = '\n\n如果情况紧急，请直接联系中文顾问。' if urgent else ''
        await render_panel(
            update,
            text=f'🔧 <b>{he(issue_label)}</b>\n\n发一下位置和问题即可。\n例如：<code>空调开机不制冷</code> 或 <code>B栋3楼走廊灯坏了</code>{note}',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(),
            context=context,
        )
        return MAIN

    if data.startswith('service_slot:'):
        try:
            _, issue_key, slot = data.split(':', 2)
        except ValueError:
            return MAIN
        issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
        slot_map = {'today': '今天内安排', 'tomorrow_am': '明天上午', 'tomorrow_pm': '明天下午'}
        slot_label = slot_map.get(slot, slot)
        binding = db.get_active_binding(user.id)
        detail_ctx = context.user_data.pop('service_request_detail', {})
        detail = str(detail_ctx.get('detail') or f'按钮提交：{slot_label}')
        binding_id = int((binding or {}).get('id') or 0) or None
        ticket_id = db.create_repair_ticket(user.id, binding_id, issue_label, f'{detail}\n希望时间：{slot_label}', now_ts())
        create_lead(
            user,
            action='service_request_submit',
            source='service_hub',
            listing_id=str((binding or {}).get('property_name') or ''),
            payload={'issue_key': issue_key, 'issue_label': issue_label, 'time_slot': slot, 'detail': detail, 'binding_id': binding_id},
        )
        await _notify_admins(
            context,
            title='新报修请求',
            lines=[
                f'客户：{_user_mention_html(user)}',
                f'联系方式：{he(_user_contact_text(user))}',
                f'房源：{he(str((binding or {}).get("property_name") or "-"))}',
                f'问题：{he(issue_label)}',
                f'说明：{he(detail)}',
                f'希望时间：{he(slot_label)}',
            ],
            reply_markup=admin_repair_keyboard(ticket_id),
        )
        urgent = issue_key in {'repair_water', 'repair_power', 'repair_door'}
        urgent_note = '\n\n如果情况紧急，请直接联系中文顾问。' if urgent else ''
        await render_panel(
            update,
            text=(
                '✅ <b>报修需求已提交</b>\n\n'
                '房屋信息已经带上，\n'
                '顾问会继续跟进处理。'
                f'{urgent_note}'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(),
            context=context,
        )
        return MAIN

    if data in {'service:guide', 'service:local_life'}:
        await query.edit_message_text(local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard())
        return MAIN

    if data == 'local:rfcity':
        create_lead(user, action='local_area_click', source='local_life', area='rfcity', payload={'area': 'rfcity', 'category': 'overview'})
        await query.edit_message_text(rfcity_text(), parse_mode=ParseMode.HTML, reply_markup=rfcity_keyboard())
        return MAIN

    if data.startswith('rfcity:'):
        from .messages import (
            rfcity_bbq_text as bbq_text,
            rfcity_drinks_text as drinks_text,
            rfcity_hotel_text as hotel_text,
            rfcity_logistics_text as logistics_text,
            rfcity_property_text as property_text,
            rfcity_recreation_text as recreation_text,
            rfcity_restaurant_text as restaurant_text,
            rfcity_supermarket_text as supermarket_text,
        )
        category = data.split(':', 1)[1]
        text_map = {
            'restaurant': restaurant_text,
            'bbq': bbq_text,
            'drinks': drinks_text,
            'supermarket': supermarket_text,
            'hotel': hotel_text,
            'recreation': recreation_text,
            'logistics': logistics_text,
            'property': property_text,
        }
        renderer = text_map.get(category)
        if renderer:
            create_lead(user, action='local_category_click', source='local_life', area='rfcity', payload={'category': category})
            await query.edit_message_text(renderer(), parse_mode=ParseMode.HTML, reply_markup=rfcity_back_keyboard())
        return MAIN

    # 以下仅保留历史 callback 兼容，新首页/入住服务不再生成这些按钮。
    if data == 'service:change':
        return await show_search_entry(update, context)
    if data in {'service:renew', 'service:renew_change'}:
        await query.edit_message_text('📋 <b>租约事项</b>\n\n续租、换房或租约变更，可以直接联系中文顾问处理。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN
    if data == 'service:terminate':
        await query.edit_message_text('🚪 <b>退租事项</b>\n\n退租时间、合同通知期和押金核对，可以直接联系中文顾问。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN
    if data == 'service:move':
        await query.edit_message_text('📦 <b>搬家协助</b>\n\n需要协调搬家时间、车辆或入住安排，可以直接联系中文顾问。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN
    if data == 'service:staging':
        await query.edit_message_text('🎥 <b>实地 / 视频看房</b>\n\n不方便到现场，也可以安排实时视频看房。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN
    if data == 'service:addons':
        await query.edit_message_text('🛋 <b>家具家电</b>\n\n入住前需要补配或协调家具家电，可以直接联系中文顾问。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN
    if data in {'service:checkin_tips', 'service:promise'}:
        await query.edit_message_text('🛠 <b>入住服务</b>\n\n入住后的房屋、物业和租约事项，可以继续由中文顾问跟进。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard())
        return MAIN

    return None
