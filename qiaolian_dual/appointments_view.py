"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


def old_tenant_binding_text(user_id: int) -> tuple[str, dict | None]:
    from .admin_contract import _binding_contract_text
    binding = db.get_active_binding(user_id)
    if not binding:
        return ('目前还没有绑定租约档案。\n\n如果是通过侨联入住的房源，联系我们核对房号后即可补上。', None)
    return ('✅ 已识别侨联租约档案\n\n' + _binding_contract_text(binding), binding)


def _appointment_date_compact(value: object) -> str:
    raw = str(value or '').strip()
    if not raw:
        return '待安排'
    parts = raw.replace('/', '-').split('-')
    if len(parts) >= 2 and all((part.isdigit() for part in parts[-2:])):
        return f'{int(parts[-2])}月{int(parts[-1])}日'
    return raw


def _appointment_time_compact(value: object) -> str:
    raw = str(value or '').strip()
    label = APPOINTMENT_TIME_LABELS.get(raw, raw or '待安排')
    return label.replace('-', '–').replace('至', '–').strip() or '待安排'


def _appointment_listing_compact(value: object) -> str:
    from .utils_formatting import _display_listing_id
    raw = str(value or '待推荐').strip()
    if not raw or raw in {'-', '未填写'}:
        return '待顾问匹配'
    if re.fullmatch('(?i)l[_-]?\\d+', raw):
        return _display_listing_id(raw)
    return raw


def _appointment_card_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔎 查看详情', callback_data='appointment_menu:details'), InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'), InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def _appointment_sort_key(row: dict):
    raw = str(row.get('appointment_date') or '')
    bits = raw.replace('/', '-').split('-')
    try:
        nums = [int(x) for x in bits if x.isdigit()]
        return (nums[-2], nums[-1], str(row.get('appointment_time') or ''))
    except (ValueError, IndexError):
        return (0, 0, '')


def _appointment_is_upcoming(row: dict) -> bool:
    status = str(row.get('status') or 'pending')
    if status in {'done', 'cancelled'}:
        return False
    raw = str(row.get('appointment_date') or '')
    bits = raw.replace('/', '-').split('-')
    try:
        nums = [int(x) for x in bits if x.isdigit()]
        month, day = (nums[-2], nums[-1])
        now = datetime.now(ZoneInfo('Asia/Phnom_Penh'))
        return (month, day) >= (now.month, now.day)
    except (ValueError, IndexError):
        return True


def _appointment_summary_line(row: dict) -> list[str]:
    from .listing import listing_context
    from .utils_formatting import _display_layout
    mode = APPOINTMENT_MODE_LABELS.get(str(row.get('viewing_mode') or ''), str(row.get('viewing_mode') or '待确认'))
    time_label = _appointment_time_compact(row.get('appointment_time'))
    raw_status = str(row.get('status') or 'pending')
    status_map = {
        'pending': ('🟡', '等待确认'),
        'assigned': ('🟡', '等待确认'),
        'contacted': ('🟡', '等待确认'),
        'confirmed': ('🟢', '预约已确认'),
        'done': ('🔵', '看房已完成'),
        'cancelled': ('⚪', '已取消'),
    }
    status_icon, status = status_map.get(raw_status, ('🟡', APPOINTMENT_STATUS_LABELS.get(raw_status, '等待确认')))
    listing_id = str(row.get('listing_id') or '')
    item = listing_context(listing_id) if listing_id else {}
    project = str(item.get('project') or item.get('community') or item.get('area') or '').strip()
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type')) if item else ''
    subject = '｜'.join(value for value in (project, layout) if value) or _appointment_listing_compact(listing_id)
    qc = _appointment_listing_compact(listing_id)
    mode_icon = '🎥' if str(row.get('viewing_mode') or '') == 'video' else '🚶'
    return [
        f'{status_icon} <b>{he(status)}</b>',
        f'🏠 <b>{he(subject)}</b>',
        f'📅 {he(_appointment_date_compact(row.get("appointment_date")))} · {he(time_label)}',
        f'{mode_icon} {he(mode)}',
        f'🆔 {he(qc)}',
    ]


def list_recent_appointments(user_id: int) -> str:
    rows = db.list_appointments(user_id, limit=20)
    if not rows:
        return (
            '📅 <b>目前还没有看房预约</b>\n\n'
            '看到喜欢的房源，点击「预约看房」就可以提交。'
        )
    rows = sorted(rows, key=_appointment_sort_key, reverse=True)
    upcoming = [row for row in rows if _appointment_is_upcoming(row)]
    history = [row for row in rows if row not in upcoming]
    parts = ['📅 <b>我的预约</b>', '']
    if upcoming:
        for index, row in enumerate(upcoming[:2]):
            parts.extend(_appointment_summary_line(row))
            if index < min(len(upcoming), 2) - 1:
                parts.extend(['', '──────────', ''])
    if history:
        if upcoming:
            parts.append('')
        parts.append(f'📁 历史预约 · {len(history)} 条')
    return '\n'.join(parts)


def _appointment_details_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = db.list_appointments(user_id, limit=10)
    buttons = []
    for row in rows:
        status = str(row.get('status') or 'pending')
        if status in {'pending', 'assigned', 'contacted', 'confirmed'}:
            appt_id = int(row.get('id') or 0)
            if appt_id:
                buttons.append([
                    InlineKeyboardButton('✏️ 修改时间', callback_data=f'appointment_menu:edit:{appt_id}'),
                    InlineKeyboardButton('❌ 取消预约', callback_data=f'appointment_menu:cancel:{appt_id}'),
                ])
                listing_id = str(row.get('listing_id') or '')
                if listing_id:
                    buttons.append([
                        InlineKeyboardButton('📋 查看房源', callback_data=f'listing:detail:{listing_id}'),
                        InlineKeyboardButton('💬 联系我们', callback_data=f'listing:consult:{listing_id}'),
                    ])
                break
    buttons.append([InlineKeyboardButton('⬅️ 返回预约列表', callback_data='appointment_menu:list')])
    buttons.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(buttons)


def _find_user_appointment(user_id: int, appointment_id: int) -> dict | None:
    for row in db.list_appointments(user_id, limit=30):
        if int(row.get('id') or 0) == int(appointment_id):
            return row
    return None


def appointment_details_text(user_id: int) -> str:
    rows = db.list_appointments(user_id, limit=5)
    if not rows:
        return list_recent_appointments(user_id)
    rows = sorted(rows, key=_appointment_sort_key, reverse=True)
    active_rows = [row for row in rows if _appointment_is_upcoming(row)]
    row = (active_rows or rows)[0]
    return '📅 <b>我的预约</b>\n\n' + '\n'.join(_appointment_summary_line(row))


def list_favorites_text(user_id: int) -> str:
    from .utils_formatting import _fmt_price
    rows = db.list_favorites(user_id)
    if not rows:
        return '⭐ 暂无收藏房源。'
    parts = ['⭐ 您收藏过的房源：']
    for item in rows[:8]:
        detail = []
        if item.get('layout'):
            detail.append(str(item.get('layout')))
        if item.get('size_sqm'):
            detail.append(f"{item.get('size_sqm')}㎡")
        detail_text = f" | {' · '.join(detail)}" if detail else ''
        parts.append(f"• {item.get('listing_id', '-')} | {item.get('area', '金边')} | {_fmt_price(item.get('price'))}{detail_text}")
    parts.append('\n需要继续咨询时，点「💬 联系我们」。')
    return '\n'.join(parts)
