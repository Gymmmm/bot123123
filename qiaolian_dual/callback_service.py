"""Callback handlers for the tenant-service domain."""
from __future__ import annotations

from .common import *


def matches(data: str) -> bool:
    return (
        data == 'service:hub'
        or data == 'service:resident'
        or data.startswith('repair_')
        or data.startswith('coordination_')
        or data == 'service:promise'
        or data == 'service:contact'
        or data in {'service:renew', 'service:change', 'service:renew_change', 'service:terminate', 'service:move', 'service:staging', 'service:addons', 'service:checkin_tips'}
        or data in {'service:guide', 'service:local_life'}
        or data == 'service:repair_hub'
        or data in {'service:general', 'service:nearby', 'local:other'}
        or data.startswith('service_request:')
        or data.startswith('service_slot:')
        or data == 'local:rfcity'
        or data.startswith('rfcity:')
    )


def _service_back_keyboard(*, parent_callback: str='service:hub', parent_label: str='返回上一页') -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系我们', callback_data='service:contact')],
        [InlineKeyboardButton(f'⬅️ {parent_label}', callback_data=parent_callback)],
    ])


async def handle_service_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _user_contact_text, _user_mention_html
    from .flows import contact_management, show_search_entry, show_service_hub
    from .keyboards_search import local_life_keyboard, nearby_area_keyboard, rfcity_back_keyboard, rfcity_keyboard, service_repair_keyboard, resident_service_keyboard, repair_attachment_keyboard, repair_time_keyboard, repair_confirm_keyboard
    from .results_admin import _notify_admins, admin_repair_keyboard
    from .search import create_lead
    from .session_deeplink import now_ts
    from .texts import local_life_text, render_panel, rfcity_text

    if data == 'service:hub':
        return await show_service_hub(update, context)

    if data == 'service:contact':
        # Generic service contact must not inherit a listing opened earlier in the chat.
        context.user_data.pop('contact_listing_id', None)
        return await contact_management(update, context, source='service_hub')

    if data == 'service:resident':
        context.user_data.pop('repair_flow', None)
        context.user_data.pop('coordination_flow', None)
        await render_panel(
            update,
            text='🛡️ <b>入住服务</b>\n签约不是服务的结束。',
            parse_mode=ParseMode.HTML,
            reply_markup=resident_service_keyboard(),
            context=context,
        )
        return MAIN

    if data == 'service:repair_hub':
        context.user_data.pop('repair_flow', None)
        await render_panel(update, text='🔧 <b>房屋报修</b>', parse_mode=ParseMode.HTML, reply_markup=service_repair_keyboard(), context=context)
        return MAIN

    if data.startswith('repair_type_'):
        issue_key = data[len('repair_type_'):]
        issue_labels = {
            'ac': '空调',
            'water': '热水 / 漏水',
            'power': '灯具 / 电路',
            'door': '门锁 / 门禁',
            'washer': '洗衣机',
            'fridge': '冰箱',
            'network': '网络',
            'furniture': '家具损坏',
            'other': '其他问题',
        }
        issue_label = issue_labels.get(issue_key)
        if not issue_label:
            return MAIN
        context.user_data['repair_flow'] = {'issue_key': issue_key, 'issue_label': issue_label, 'step': 'description'}
        await render_panel(update, text=f'🔧 <b>{he(issue_label)}</b>\n\n请描述具体问题。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 退出报修', callback_data='service:resident')]]), context=context)
        return MAIN

    if data == 'repair_attachment_skip':
        flow = context.user_data.get('repair_flow') or {}
        if flow.get('step') != 'attachment':
            return MAIN
        flow['step'] = 'time'
        context.user_data['repair_flow'] = flow
        await render_panel(update, text='请选择方便处理的时间。', reply_markup=repair_time_keyboard(), context=context)
        return MAIN

    if data.startswith('repair_time_'):
        flow = context.user_data.get('repair_flow') or {}
        if flow.get('step') != 'time':
            return MAIN
        slot = data[len('repair_time_'):]
        slot_map = {'today': '今天', 'tomorrow_am': '明天上午', 'tomorrow_pm': '明天下午'}
        if slot not in slot_map:
            return MAIN
        flow['preferred_time'] = slot_map[slot]
        flow['step'] = 'confirm'
        context.user_data['repair_flow'] = flow
        await render_panel(update, text=f"问题｜{he(str(flow.get('issue_label') or '-'))}\n时间｜{he(flow['preferred_time'])}", reply_markup=repair_confirm_keyboard(), context=context)
        return MAIN

    if data == 'repair_confirm':
        flow = context.user_data.get('repair_flow') or {}
        if flow.get('step') != 'confirm':
            return MAIN
        binding = db.get_active_binding(user.id)
        binding_id = int((binding or {}).get('id') or 0) or None
        detail = str(flow.get('description') or '')
        attachment = str(flow.get('attachment_file_id') or '')
        if attachment:
            detail = f'{detail}\n附件：{attachment}'
        ticket_id = db.create_repair_ticket(user.id, binding_id, str(flow.get('issue_label') or '其他问题'), detail, now_ts())
        create_lead(user, action='service_request_submit', source='service_hub', listing_id=str((binding or {}).get('property_name') or ''), payload={'ticket_id': ticket_id, 'issue_type': flow.get('issue_label'), 'preferred_time': flow.get('preferred_time'), 'binding_id': binding_id})
        await _notify_admins(context, title='新报修请求', lines=[f'客户：{_user_mention_html(user)}', f'联系方式：{he(_user_contact_text(user))}', f'工单：{ticket_id}', f"问题：{he(str(flow.get('issue_label') or '-'))}", f"时间：{he(str(flow.get('preferred_time') or '-'))}", f'说明：{he(detail)}'], reply_markup=admin_repair_keyboard(ticket_id))
        context.user_data.pop('repair_flow', None)
        await render_panel(
            update,
            text=(
                '✅ <b>报修已提交</b>\n'
                f'工单｜{ticket_id}\n'
                f"问题｜{he(str(flow.get('issue_label') or '-'))}\n"
                f"时间｜{he(str(flow.get('preferred_time') or '-'))}\n"
                '处理进度会在这里通知你。'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('💬 中文顾问', callback_data=f'repair_advisor_{ticket_id}'), InlineKeyboardButton('🛡️ 返回入住服务', callback_data='service:resident')]]),
            context=context,
        )
        return MAIN

    if data.startswith('repair_advisor_'):
        raw_ticket = data[len('repair_advisor_'):]
        create_lead(user, action='repair_advisor_click', source='service_hub', payload={'ticket_id': raw_ticket})
        context.user_data['service_ticket_id'] = raw_ticket
        return await contact_management(update, context, source=f'repair_ticket:{raw_ticket}')

    if data == 'coordination_start':
        context.user_data['coordination_flow'] = {'step': 'issue'}
        await render_panel(update, text='🏢 <b>物业协调</b>\n\n请描述需要协调的问题。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:resident')]]), context=context)
        return MAIN

    if data == 'service:general':
        for key in ('contact_listing_id', 'contact_touch_payload', 'appt', 'active_listing_id', 'listing_id'):
            context.user_data.pop(key, None)
        context.user_data['awaiting_service_general'] = True
        await render_panel(
            update,
            text='💬 <b>通用服务咨询</b>\n\n请直接说需要什么帮助，我们会按服务需求跟进。',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(parent_callback='service:hub', parent_label='返回入住服务'),
            context=context,
        )
        return MAIN

    if data == 'service:nearby':
        await render_panel(update, text='🗺 <b>周边服务</b>\n\n可查看富力城已核实的周边商家；其他区域可提交需求，我们会在1个工作日内反馈。', parse_mode=ParseMode.HTML, reply_markup=nearby_area_keyboard(), context=context)
        return MAIN

    if data == 'local:other':
        for key in ('contact_listing_id', 'contact_touch_payload', 'appt', 'active_listing_id', 'listing_id'):
            context.user_data.pop(key, None)
        context.user_data['awaiting_nearby_request'] = True
        await render_panel(
            update,
            text='📍 <b>其他区域需求提交</b>\n\n请发送区域或地标，以及需要的餐饮、超市、交通或生活服务。提交后，我们会在1个工作日内回复。',
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回周边推荐', callback_data='service:nearby')]]),
            context=context,
        )
        return MAIN


    if data == 'service_request:property':
        await render_panel(
            update,
            text=(
                '🏢 <b>物业协调</b>\n\n'
                '如遇噪音、停车、门禁、公共区域或垃圾处理等问题，可以直接说明具体情况。\n\n'
                '建议包括：\n'
                '• 发生了什么\n'
                '• 大概从什么时候开始\n'
                '• 是否已经与物业沟通过\n\n'
                '我们会协助整理沟通重点，并根据实际情况对接物业。'
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
        note = '\n\n如果情况紧急，请直接联系我们。' if urgent else ''
        await render_panel(
            update,
            text=f'🔧 <b>{he(issue_label)}</b>\n\n请发送问题照片或短视频，并简单说明异常情况。\n例如：<code>空调可以启动，但一直不制冷。</code>{note}',
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(parent_callback='service:repair_hub'),
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
        urgent_note = '\n\n如果情况紧急，请直接联系我们。' if urgent else ''
        await render_panel(
            update,
            text=(
                '✅ <b>报修信息已收到</b>\n\n'
                '我们会根据您提供的情况进行整理，并协助对接后续处理。'
                f'{urgent_note}'
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_service_back_keyboard(parent_callback='service:repair_hub'),
            context=context,
        )
        return MAIN

    if data in {'service:guide', 'service:local_life'}:
        await render_panel(update, text=local_life_text(), parse_mode=ParseMode.HTML, reply_markup=local_life_keyboard(), context=context)
        return MAIN

    # 富力城周边内容仅保留历史兼容入口。正式“周边推荐”不再硬编码进入该页面。
    if data == 'local:rfcity':
        create_lead(user, action='local_area_click', source='local_life', area='rfcity', payload={'area': 'rfcity', 'category': 'overview'})
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
            await render_panel(update, text=renderer(), parse_mode=ParseMode.HTML, reply_markup=rfcity_back_keyboard(), context=context)
        return MAIN

    # 以下仅保留历史 callback 兼容，新首页/入住服务不再生成这些按钮。
    if data == 'service:change':
        return await show_search_entry(update, context)
    if data in {'service:renew', 'service:renew_change'}:
        await render_panel(update, text='📋 <b>租约事项</b>\n\n续租、换房或租约变更，可以直接联系我们处理。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data == 'service:terminate':
        await render_panel(update, text='🚪 <b>退租事项</b>\n\n退租时间、合同通知期和押金核对，可以直接联系我们。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data == 'service:move':
        await render_panel(update, text='📦 <b>搬家协助</b>\n\n需要协调搬家时间、车辆或入住安排，可以直接联系我们。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data == 'service:staging':
        await render_panel(update, text='🎥 <b>实地 / 视频看房</b>\n\n不方便到现场，也可以安排实时视频看房。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data == 'service:addons':
        await render_panel(update, text='🛋 <b>家具家电</b>\n\n入住前需要补配或协调家具家电，可以直接联系我们。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN
    if data in {'service:checkin_tips', 'service:promise'}:
        await render_panel(update, text='🛠 <b>入住服务</b>\n\n入住后的房屋、物业和租约事项，可以继续联系侨联。', parse_mode=ParseMode.HTML, reply_markup=_service_back_keyboard(), context=context)
        return MAIN

    return None
