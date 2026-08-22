"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def _user_mention_html(user) -> str:
    from .session_deeplink import user_display_name
    name = he(user_display_name(user) or str(getattr(user, 'id', '')))
    uid = int(getattr(user, 'id', 0) or 0)
    if uid > 0:
        return f'<a href="tg://user?id={uid}">{name}</a>'
    return name

def _user_contact_text(user) -> str:
    username = (getattr(user, 'username', '') or '').strip()
    if username:
        return f'@{username}'
    uid = int(getattr(user, 'id', 0) or 0)
    return f'tg://user?id={uid}' if uid > 0 else '-'

def _is_admin_user(user_id: int | None) -> bool:
    try:
        uid = int(user_id or 0)
        return uid in _all_user_admin_ids()
    except (TypeError, ValueError):
        return False

def _extra_user_admin_ids() -> set[int]:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT setting_value FROM bot_settings WHERE setting_key='user_extra_admin_ids'").fetchone()
        return {int(x) for x in str(row[0] if row else '').replace(' ', '').split(',') if x.isdigit()}
    except Exception:
        return set()

def _all_user_admin_ids() -> set[int]:
    return set(ADMIN_IDS or []) | _extra_user_admin_ids()

def _save_extra_user_admin_ids(ids: set[int]) -> None:
    value = ','.join((str(x) for x in sorted(ids)))
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT INTO bot_settings(setting_key, setting_value, updated_at)\n               VALUES ('user_extra_admin_ids', ?, CURRENT_TIMESTAMP)\n               ON CONFLICT(setting_key) DO UPDATE SET\n                 setting_value=excluded.setting_value, updated_at=CURRENT_TIMESTAMP", (value,))
        conn.commit()

def _budget_text(budget_min: int | None, budget_max: int | None) -> str:
    if budget_min is not None and budget_max is not None:
        return f'{budget_min}-{budget_max} USD/月'
    if budget_min is not None:
        return f'>= {budget_min} USD/月'
    if budget_max is not None:
        return f'<= {budget_max} USD/月'
    return '-'

def _parse_date_safe(raw: str) -> datetime | None:
    text = str(raw or '').strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None

def _binding_end_date(binding: dict | None) -> str:
    if not binding:
        return ''
    return str(binding.get('contract_end_date') or binding.get('lease_end_date') or '').strip()

def _binding_days_left(binding: dict | None) -> int | None:
    dt = _parse_date_safe(_binding_end_date(binding))
    if dt is None:
        return None
    return max((dt.date() - datetime.now().date()).days, 0)

def _contract_status_text(days_left: int | None) -> str:
    if days_left is None:
        return '资料待补全'
    if days_left <= 3:
        return '临近到期，请优先跟进'
    if days_left <= 7:
        return '本周内建议确认续租/换房'
    if days_left <= 30:
        return '本月内可提前安排'
    return '租约状态稳定'

def _lease_reminder_label(user_id: int | None) -> str:
    enabled = True if user_id is None else db.is_lease_reminder_enabled(user_id)
    return '🔔 到期提醒：已开启' if enabled else '🔕 到期提醒：已关闭'

def _binding_contract_text(binding: dict | None, user_id: int | None=None) -> str:
    if not binding:
        return '📋 <b>我的租约</b>\n\n当前还没有绑定租约档案。\n请点「💬 联系我们」，我们会后台录入房号/交租日/到期日。'
    property_name = str(binding.get('property_name') or '-')
    rent_day = binding.get('rent_day')
    rent_text = f'每月 {int(rent_day)} 号' if isinstance(rent_day, int) else '待确认'
    end_date = _binding_end_date(binding) or '待确认'
    days_left = _binding_days_left(binding)
    day_line = f'{days_left} 天' if days_left is not None else '待确认'
    monthly_rent = binding.get('monthly_rent')
    try:
        rent_value = float(monthly_rent or 0)
    except (TypeError, ValueError):
        rent_value = 0
    rent_line = f'${int(rent_value)}/月' if rent_value > 0 else '待确认'
    deposit_months = binding.get('deposit_months')
    try:
        deposit_line = f'{int(deposit_months)} 个月' if int(deposit_months) > 0 else '待确认'
    except (TypeError, ValueError):
        deposit_line = '待确认'
    reminder_line = _lease_reminder_label(user_id)
    status_line = _contract_status_text(days_left)
    return f'📋 <b>我的租约</b>\n\n🏠 房号/项目：{he(property_name)}\n💰 月租：{he(rent_line)}\n🔐 押金：{he(deposit_line)}\n📅 交租日：{he(rent_text)}\n⏳ 合同到期：{he(end_date)}\n🕒 剩余：<b>{he(day_line)}</b>\n🧭 状态：<b>{he(status_line)}</b>\n{he(reminder_line)}'

def _contract_actions_keyboard(user_id: int | None=None) -> InlineKeyboardMarkup:
    reminder_label = _lease_reminder_label(user_id)
    rows: list[list[InlineKeyboardButton]] = [[InlineKeyboardButton('🔄 续租咨询', callback_data='contract:renew'), InlineKeyboardButton('🏠 我要换房', callback_data='contract:change')], [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('⚡ 入住服务', callback_data='service:hub')], [InlineKeyboardButton(reminder_label, callback_data='contract:toggle_reminder'), InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]]
    return InlineKeyboardMarkup(rows)
