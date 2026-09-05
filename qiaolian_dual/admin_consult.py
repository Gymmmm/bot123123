"""中文咨询后台与管理员通知格式。"""
from __future__ import annotations

from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .common import answer_callback_once, db, he
from .attribution import admin_source_group_zh, entry_action_zh, lead_status_zh, source_type_zh
from .attribution_store import (
    get_user_attribution,
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


def consult_action_keyboard(*, lead_id: int | None, appointment_id: int, user_id: int) -> InlineKeyboardMarkup:
    suffix = f'{int(lead_id or 0)}:{int(appointment_id or 0)}:{int(user_id or 0)}'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ 我来跟进', callback_data=f'adminlead:claim:{suffix}')],
        [InlineKeyboardButton('📞 已联系', callback_data=f'adminlead:contacted:{suffix}')],
        [InlineKeyboardButton('✅ 完成', callback_data=f'adminlead:done:{suffix}')],
        [InlineKeyboardButton('🏠 查看房源', callback_data=f'adminlead:view:{suffix}')],
    ])


def format_consult_notify(*, user_id: int, title: str, lines: list[str], current_action: str = '') -> tuple[str, list[str]]:
    attr = get_user_attribution(int(user_id or 0)) or {}
    first_type = str(attr.get('first_source_type') or 'other')
    latest_type = str(attr.get('latest_source_type') or first_type or 'other')
    action = str(current_action or attr.get('latest_entry_action') or '')
    deep = str(attr.get('latest_deep_link') or attr.get('first_deep_link') or '')
    cleaned = [str(line) for line in lines if not str(line or '').startswith('入口：')]
    cleaned.extend([
        '',
        f'来源：{he(admin_source_group_zh(first_type))}',
        f'首次进入：{he(source_type_zh(first_type))}',
        f'本次动作：{he(entry_action_zh(action))}',
    ])
    if deep:
        cleaned.append(f'入口：<code>{he(deep)}</code>')
    if latest_type != first_type:
        cleaned.append(f'最近来源：{he(source_type_zh(latest_type))}')
    return title, cleaned


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
                await query.edit_message_text(_lead_card(lead), parse_mode=ParseMode.HTML, reply_markup=consult_action_keyboard(lead_id=int(raw), appointment_id=0, user_id=uid))
        return 0
    return 0


def apply_admin_lead_action(*, lead_id: int, action: str, advisor_id: str = '', advisor_name: str = '') -> bool:
    mapping = {'claim': 'claimed', 'contacted': 'contacted', 'done': 'done', 'invalid': 'invalid'}
    status = mapping.get(str(action or ''))
    if not status:
        return False
    return update_lead_status(lead_id, status, advisor_id=advisor_id, advisor_name=advisor_name)
