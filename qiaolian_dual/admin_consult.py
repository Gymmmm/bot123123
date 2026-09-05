"""中文咨询后台与管理员通知格式。"""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .common import answer_callback_once, db, he
from .attribution import admin_source_group_zh, entry_action_zh, lead_status_zh, source_type_zh
from .attribution_store import (
    list_leads_by_status,
    list_listing_leads,
    list_service_tickets,
    list_today_appointments,
    source_stats,
    update_lead_status,
)


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🆕 新咨询', callback_data='adminq:new'), InlineKeyboardButton('📅 今日预约', callback_data='adminq:appointments')],
        [InlineKeyboardButton('🏠 房源线索', callback_data='adminq:listings'), InlineKeyboardButton('🛠 服务工单', callback_data='adminq:services')],
        [InlineKeyboardButton('📊 来源统计', callback_data='adminq:sources'), InlineKeyboardButton('📚 跟进记录', callback_data='adminq:history')],
    ])


def consult_action_keyboard(
    *,
    lead_id: int,
    appointment_id: int = 0,
    user_id: int = 0,
    listing_id: str = '',
    include_back: bool = True,
) -> InlineKeyboardMarkup:
    suffix = f'{int(lead_id or 0)}:{int(appointment_id or 0)}:{int(user_id or 0)}'
    rows = [
        [
            InlineKeyboardButton('✅ 我来跟进', callback_data=f'adminlead:claim:{suffix}'),
            InlineKeyboardButton('📞 已联系', callback_data=f'adminlead:contacted:{suffix}'),
        ],
        [
            InlineKeyboardButton('✅ 完成', callback_data=f'adminlead:done:{suffix}'),
            InlineKeyboardButton('🚫 结束跟进', callback_data=f'adminlead:invalid:{suffix}'),
        ],
    ]
    if listing_id:
        rows.append([InlineKeyboardButton('🏠 查看房源', callback_data=f'adminlead:view:{suffix}')])
    if include_back:
        rows.append([InlineKeyboardButton('⬅️ 返回后台', callback_data='adminq:home')])
    for row in rows:
        for button in row:
            raw = str(button.callback_data or '')
            if len(raw.encode('utf-8')) > 64:
                raise ValueError(f'callback_data too long: {raw}')
    return InlineKeyboardMarkup(rows)


def _customer_line(user=None, *, username: str = '', display_name: str = '', user_id: int = 0) -> str:
    if user is not None:
        display_name = getattr(user, 'full_name', '') or getattr(user, 'first_name', '') or display_name or '客户'
        username = getattr(user, 'username', '') or username
        user_id = int(getattr(user, 'id', 0) or user_id or 0)
    name = (display_name or '客户').strip()
    handle = f' @{username}' if username else ''
    if not handle and user_id:
        handle = f' {user_id}'
    return f'{name}{handle}'.strip()


def _listing_facts(listing_id: str) -> dict[str, str]:
    from .listing import listing_context
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price

    info = listing_context(listing_id) if listing_id else {}
    project = str(info.get('project') or info.get('community') or info.get('area') or '').strip()
    layout = _display_layout(info.get('layout') or info.get('property_type'), info.get('property_type')) if info else ''
    return {
        'qc': _display_listing_id(listing_id) if listing_id else '',
        'project': project,
        'layout': layout or '',
        'price': _fmt_price(info.get('price')) if info else '',
    }


def format_consult_notify(
    *,
    user=None,
    touch: dict | None = None,
    listing_id: str = '',
    title: str = '房源咨询',
    current_action: str = '',
    username: str = '',
    display_name: str = '',
    user_id: int = 0,
) -> tuple[str, list[str]]:
    touch = dict(touch or {})
    listing_id = str(listing_id or touch.get('listing_id') or touch.get('latest_listing_id') or '').strip()
    facts = _listing_facts(listing_id)
    first_type = touch.get('first_source_type') or touch.get('source_type') or 'other'
    latest_type = touch.get('latest_source_type') or touch.get('source_type') or first_type
    first_label = source_type_zh(first_type)
    if touch.get('first_legacy') or touch.get('legacy'):
        first_label = f'{first_label}（历史入口）'
    latest_label = admin_source_group_zh(latest_type)
    action_label = entry_action_zh(current_action or touch.get('entry_action'))
    entry = str(touch.get('deep_link_payload') or touch.get('latest_deep_link') or touch.get('first_deep_link') or '').strip()
    if entry.startswith('discussion_entry'):
        entry = '历史讨论区入口'
    lines = [
        f'客户：{he(_customer_line(user, username=username, display_name=display_name, user_id=user_id))}',
        f'来源：{he(latest_label)}',
    ]
    if facts['qc']:
        lines.append(f"房源：{he(facts['qc'])}")
    if facts['project']:
        lines.append(f"楼盘：{he(facts['project'])}")
    if facts['layout']:
        lines.append(f"户型：{he(facts['layout'])}")
    if facts['price']:
        lines.append(f"租金：{he(facts['price'])}")
    lines.extend(['', f'首次进入：{he(first_label)}', f'本次动作：{he(action_label)}'])
    if entry:
        lines.append(f'入口：{he(entry)}')
    return title, lines


def _lead_card(lead: dict) -> str:
    first_type = str(lead.get('first_source_type') or lead.get('source_type') or 'other')
    return '\n'.join([
        f"🔔 <b>房源咨询 #{int(lead.get('id') or 0)}</b>",
        '',
        f"客户：{he(str(lead.get('display_name') or lead.get('username') or lead.get('user_id') or '客户'))}",
        f"来源：{he(admin_source_group_zh(first_type))}",
        f"房源：{he(str(lead.get('listing_id') or '-'))}",
        f"状态：{he(lead_status_zh(lead.get('lead_status')))}",
        '',
        f"首次进入：{he(source_type_zh(first_type))}",
        f"本次动作：{he(entry_action_zh(lead.get('entry_action') or lead.get('action')))}",
        f"入口：<code>{he(str(lead.get('deep_link_payload') or '-'))}</code>",
    ])


def _lead_list_keyboard(rows: list[dict]) -> InlineKeyboardMarkup:
    kb = []
    for lead in rows[:12]:
        lead_id = int(lead.get('id') or 0)
        source = admin_source_group_zh(lead.get('first_source_type') or lead.get('source_type'))
        name = str(lead.get('display_name') or lead.get('username') or '客户')[:10]
        kb.append([InlineKeyboardButton(f'{lead_status_zh(lead.get("lead_status"))}｜{name}｜{source}'[:60], callback_data=f'adminq:lead:{lead_id}')])
    kb.append([InlineKeyboardButton('⬅️ 返回咨询后台', callback_data='adminq:home')])
    return InlineKeyboardMarkup(kb)


async def cmd_admin_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from .admin_contract import _is_admin_user
    user = update.effective_user
    if not user or not _is_admin_user(user.id):
        return
    await update.effective_message.reply_text('🧭 <b>侨联咨询后台</b>\n\n请选择要查看的内容。', parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard())


async def handle_admin_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    from .admin_contract import _is_admin_user
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or not _is_admin_user(user.id):
        return None
    await answer_callback_once(query)
    data = str(query.data or '')
    action = data.split(':', 1)[1] if ':' in data else 'home'
    if action == 'home':
        await query.edit_message_text('🧭 <b>侨联咨询后台</b>\n\n请选择要查看的内容。', parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard())
        return 0
    if action == 'new':
        rows = list_leads_by_status('new', 20)
        await query.edit_message_text(f'🆕 <b>新咨询</b>\n\n当前 {len(rows)} 条。', parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return 0
    if action == 'history':
        rows = list_leads_by_status('all', 20)
        await query.edit_message_text('📚 <b>最近跟进记录</b>\n\n最近 20 条。', parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return 0
    if action == 'listings':
        rows = list_listing_leads(20)
        await query.edit_message_text('🏠 <b>房源线索</b>', parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return 0
    if action == 'appointments':
        today = datetime.now().strftime('%Y-%m-%d')
        rows = list_today_appointments(today, 20)
        lines = ['📅 <b>今日预约</b>', ''] + ([f"• #{int(r.get('id') or 0)}｜{he(str(r.get('listing_id') or '-'))}｜{he(str(r.get('appointment_time') or '待定'))}" for r in rows[:12]] or ['今天暂时没有预约。'])
        await query.edit_message_text('\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回咨询后台', callback_data='adminq:home')]]))
        return 0
    if action == 'services':
        rows = list_service_tickets(20)
        lines = ['🛠 <b>服务工单</b>', ''] + ([f"• #{int(r.get('id') or 0)}｜{he(str(r.get('issue_type') or '服务'))}｜{he(str(r.get('status') or 'new'))}" for r in rows[:12]] or ['暂时没有服务工单。'])
        await query.edit_message_text('\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回咨询后台', callback_data='adminq:home')]]))
        return 0
    if action == 'sources':
        rows = source_stats(20)
        lines = ['📊 <b>来源统计</b>', ''] + ([f"• {he(admin_source_group_zh(r.get('src')))}：<b>{int(r.get('total') or 0)}</b>" for r in rows] or ['暂无来源数据。'])
        await query.edit_message_text('\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ 返回咨询后台', callback_data='adminq:home')]]))
        return 0
    if action.startswith('lead:'):
        raw = action.split(':', 1)[1]
        if raw.isdigit():
            lead = db.get_lead(int(raw))
            if lead:
                uid = int(lead.get('user_id') or 0)
                await query.edit_message_text(
                    _lead_card(lead),
                    parse_mode=ParseMode.HTML,
                    reply_markup=consult_action_keyboard(
                        lead_id=int(raw),
                        appointment_id=0,
                        user_id=uid,
                        listing_id=str(lead.get('listing_id') or ''),
                    ),
                )
        return 0
    return 0


async def handle_lead_view(query, lead_id: int) -> None:
    lead = db.get_lead(lead_id) or {}
    listing_id = str(lead.get('listing_id') or '').strip()
    user_obj = SimpleNamespace(
        id=int(lead.get('user_id') or 0),
        username=str(lead.get('username') or ''),
        first_name=str(lead.get('display_name') or '客户'),
        full_name=str(lead.get('display_name') or '客户'),
    )
    title, lines = format_consult_notify(
        user=user_obj,
        touch={
            'first_source_type': lead.get('first_source_type') or lead.get('source_type'),
            'latest_source_type': lead.get('source_type'),
            'first_legacy': 'legacy_discussion_entry' in str(lead.get('source_detail') or lead.get('deep_link_payload') or ''),
            'deep_link_payload': lead.get('deep_link_payload') or '',
            'entry_action': lead.get('entry_action') or lead.get('action') or '',
            'listing_id': listing_id,
        },
        listing_id=listing_id,
        title='查看房源',
        current_action=str(lead.get('entry_action') or lead.get('action') or ''),
    )
    text = f"🏠 <b>{he(title)}</b>\n\n" + '\n'.join(lines) + f"\n\n当前状态：{he(lead_status_zh(lead.get('lead_status')))}"
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=consult_action_keyboard(
            lead_id=lead_id,
            appointment_id=0,
            user_id=int(lead.get('user_id') or 0),
            listing_id=listing_id,
        ),
    )


def apply_admin_lead_action(action: str, lead_id: int, *, advisor_id: str, advisor_name: str) -> tuple[bool, str]:
    mapping = {
        'claim': ('claimed', '已接手'),
        'contacted': ('contacted', '已联系'),
        'done': ('done', '已完成'),
        'invalid': ('invalid', '无效'),
    }
    if action not in mapping:
        return False, ''
    status, label = mapping[action]
    return update_lead_status(lead_id, status, advisor_id=advisor_id, advisor_name=advisor_name), label
