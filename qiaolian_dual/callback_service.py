"""Callback handlers for the tenant-service domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data == 'service:hub'
        or data == 'service:promise'
        or data == 'service:contact'
        or data in {'service:renew', 'service:terminate', 'service:move', 'service:staging', 'service:addons', 'service:checkin_tips'}
        or data in {'service:guide', 'service:local_life'}
        or data == 'service:repair_hub'
        or data in {'service:general', 'service:nearby', 'local:other'}
        or data.startswith('service_request:')
        or data.startswith('service_slot:')
        or data == 'local:rfcity'
        or data.startswith('rfcity:')
    )


def _service_back_keyboard(*, parent_callback: str='service:hub', parent_label: str='返回入住服务') -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 中文顾问', callback_data='service:contact')],
        [InlineKeyboardButton(f'⬅️ {parent_label}', callback_data=parent_callback)],
    ])


async def handle_service_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .flows import contact_management, show_service_hub
    from .keyboards_search import local_life_keyboard, nearby_area_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_repair_keyboard
    from .results_admin import _notify_admins, admin_repair_keyboard
    from .search import create_lead
    from .session_deeplink import now_ts
    from .texts import local_life_text, render_panel, rfcity_text

    if data == 'service:hub':
        return await show_service_hub(update, context)

    if data == 'service:contact':
        context.user_data.pop('contact_listing_id', None)
        return await contact_management(update, context, source='service_hub')

    binding = db.get_active_binding(user.id)

    if data in {'service:repair_hub', 'service_request:property'} or data.startswith('service_request:') or data.startswith('service_slot:'):
        if not binding:
            return await show_service_hub(update, context)

    if data == 'service:general':
        for key in ('contact_listing_id', 'contact_touch_payload', 'appt', 'active_listing_id', 'listing_id'):
            context.user_data.pop(key, None)
        context.user_data['awaiting_service_general'] = True
        await render_panel(
            update,
            text='💬 <b>通用服务咨询</b>\n\n请直接说需要什么帮助，中文顾问会继续跟进。',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(),
            context=context,
        )
        return MAIN

    if data == 'service:nearby':
        await render_panel(update, text='🗺 <b>周边服务</b>\n\n可以查看已核实的周边信息；其他区域也可以提交需求。', parse_mode=ParseMode.HTML, reply_markup=nearby_area_keyboard(), context=context)
        return MAIN

    if data == 'local:other':
        for key in ('contact_listing_id', 'contact_touch_payload', 'appt', 'active_listing_id', 'listing_id'):
            context.user_data.pop(key, None)
        context.user_data['awaiting_nearby_request'] = True
        await render_panel(
            update,
            text='📍 <b>其他区域需求</b>\n\n请发送区域或地标，以及需要的餐饮、超市、交通或生活服务。',
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回周边推荐', callback_data='service:nearby')]]),
            context=context,
        )
        return MAIN

    if data == 'service:repair_hub':
        await render_panel(update, text='🔧 <b>报修</b>\n\n请选择需要处理的问题。\n\n房屋信息已经带上，不用重复说明。', parse_mode=ParseMode.HTML, reply_markup=service_repair_keyboard(), context=context)
        return MAIN

    if data == 'service_request:property':
        await render_panel(
            update,
            text=(
                '🏢 <b>物业沟通</b>\n\n'
                '门禁、电梯卡、停车、公共区域或其他物业事项，可以直接交给中文顾问协助沟通。\n\n'
                f'🏠 当前房源：{he(str(binding.get("property_name") or "-"))}'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(),
            context=context,
        )
        return MAIN

    if data.startswith('service_request:'):
        issue_key = data.split(':', 1)[1]
        issue_label = SERVICE_REQUEST_LABELS.get(issue_key, issue_key)
        context.user_data['awaiting_service_request'] = {'issue_key': issue_key, 'issue_label': issue_label}
        urgent = issue_key in {'repair_water', 'repair_power', 'repair_door'}
        note = '\n\n如情况紧急或可能造成进一步损失，请立即联系中文顾问。' if urgent else ''
        await render_panel(
            update,
            text=f'🔧 <b>{he(issue_label)}</b>\n\n请发送问题照片或短视频，并简单说明异常情况。\n例如：<code>空调可以启动，但一直不制冷。</code>{note}',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(parent_callback='service:repair_hub', parent_label='返回报修'),
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
        detail_ctx = context.user_data.pop('service_request_detail', {})
        detail = str(detail_ctx.get('detail') or f'按钮提交：{slot_label}')
        binding_id = int(binding.get('id') or 0) or None
        ticket_id = db.create_repair_ticket(user.id, binding_id, issue_label, f'{detail}\n希望时间：{slot_label}', now_ts())
        create_lead(
            user,
            action='service_request_submit',
            source='service_hub',
            listing_id=str(binding.get('property_name') or ''),
            payload={'issue_key': issue_key, 'issue_label': issue_label, 'time_slot': slot, 'detail': detail, 'binding_id': binding_id, 'ticket_id': ticket_id},
        )
        await _notify_admins(
            context,
            title='新报修请求',
            lines=[
                f'客户：{_user_mention_html(user)}',
                f'联系方式：{he(_user_contact_text(user))}',
                f'绑定档案：#{binding_id}',
                f'房源：{he(str(binding.get("property_name") or "-"))}',
                f'问题：{he(issue_label)}',
                f'说明：{he(detail)}',
                f'希望时间：{he(slot_label)}',
            ],
            reply_markup=admin_repair_keyboard(ticket_id),
        )
        urgent = issue_key in {'repair_water', 'repair_power', 'repair_door'}
        urgent_note = '\n\n如情况紧急或可能造成进一步损失，请立即联系中文顾问。' if urgent else ''
        await render_panel(
            update,
            text=f'✅ <b>报修已提交</b>\n\n房屋信息已经带上，中文顾问会继续跟进。{urgent_note}',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(parent_callback='service:hub'),
            context=context,
        )
        return MAIN

    if data in {'service:guide', 'service:local_life'}:
        if not binding:
            return await show_service_hub(update, context)
        await render_panel(update, text=local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard(), context=context)
        return MAIN

    if data == 'local:rfcity':
        create_lead(user, action='local_area_click', source='local_life', area='rfcity', payload={'area': 'rfcity', 'category': 'overview', 'binding_id': (binding or {}).get('id')})
        await render_panel(update, text=rfcity_text(), parse_mode=ParseMode.HTML, reply_markup=rfcity_keyboard(), context=context)
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
            'restaurant': restaurant_text, 'bbq': bbq_text, 'drinks': drinks_text, 'supermarket': supermarket_text,
            'hotel': hotel_text, 'recreation': recreation_text, 'logistics': logistics_text, 'property': property_text,
        }
        renderer = text_map.get(category)
        if renderer:
            create_lead(user, action='local_category_click', source='local_life', area='rfcity', payload={'category': category, 'binding_id': (binding or {}).get('id')})
            await render_panel(update, text=renderer(), parse_mode=ParseMode.HTML, reply_markup=rfcity_back_keyboard(), context=context)
        return MAIN

    if data == 'service:renew':
        from .callback_contract import handle_contract_callback
        return await handle_contract_callback(update, context, query, 'contract:renew', user)
    if data == 'service:terminate':
        from .callback_contract import handle_contract_callback
        return await handle_contract_callback(update, context, query, 'contract:terminate', user)
    if data == 'service:move':
        await render_panel(update, text='📦 <b>搬家协助</b>\n\n需要协调搬家时间、车辆或入住安排，可以联系中文顾问。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data == 'service:staging':
        await render_panel(update, text='🎥 <b>实地 / 视频看房</b>\n\n不方便到现场，也可以安排实时视频看房。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(parent_callback='home', parent_label='返回首页'), context=context)
        return MAIN
    if data == 'service:addons':
        await render_panel(update, text='🛋 <b>家具家电</b>\n\n入住前需要补配或协调家具家电，可以联系中文顾问。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data in {'service:checkin_tips', 'service:promise'}:
        return await show_service_hub(update, context)

    return None
