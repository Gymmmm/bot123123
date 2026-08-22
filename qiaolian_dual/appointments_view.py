"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def old_tenant_binding_text(user_id: int) -> tuple[str, dict | None]:
    from .admin_contract import _binding_contract_text
    binding = db.get_active_binding(user_id)
    if not binding:
        return ('✅ 已登记老客回流。\n\n当前还没有绑定到您的租住档案。\n请点下方「联系我们」，我们会用后台资料完成绑定。', None)
    return ('✅ 已识别侨联老用户档案\n\n' + _binding_contract_text(binding), binding)

def _appointment_date_compact(value: object) -> str:
    raw = str(value or '').strip()
    if not raw:
        return '待安排'
    parts = raw.replace('/', '-').split('-')
    if len(parts) >= 2 and all((part.isdigit() for part in parts[-2:])):
        return f'{int(parts[-2]):02d}/{int(parts[-1]):02d}'
    return raw

def _appointment_time_compact(value: object) -> str:
    raw = str(value or '').strip()
    label = APPOINTMENT_TIME_LABELS.get(raw, raw or '待安排')
    label = label.replace('上午', '').replace('下午', '').replace('晚上', '')
    label = label.replace('-', '–').replace('至', '–')
    return label.strip() or '待安排'

def _appointment_listing_compact(value: object) -> str:
    from .utils_formatting import _display_listing_id
    raw = str(value or '待推荐').strip()
    if not raw or raw in {'-', '未填写'}:
        return '待顾问匹配'
    if re.fullmatch('(?i)l[_-]?\\d+', raw):
        return _display_listing_id(raw)
    return raw

def _appointment_card_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🔎 查看预约', callback_data='appointment_menu:details'), InlineKeyboardButton('💬 联系顾问', callback_data='appointment_menu:contact')]])

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
    mode = APPOINTMENT_MODE_LABELS.get(str(row.get('viewing_mode') or ''), str(row.get('viewing_mode') or '待确认'))
    time_label = _appointment_time_compact(row.get('appointment_time'))
    status = APPOINTMENT_STATUS_LABELS.get(str(row.get('status') or ''), str(row.get('status') or '待确认'))
    status_icon = {'待确认': '🟡', '已确认': '🟢', '顾问联系中': '🔵', '已完成': '✅', '已取消': '⚪', '未到场': '🔴'}.get(status, '🟡')
    listing = _appointment_listing_compact(row.get('listing_id'))
    return [f"<b>{_appointment_date_compact(row.get('appointment_date'))} · {he(time_label)}</b>", f'{he(mode)}｜<b>{he(listing)}</b>', f'{status_icon} {he(status)}']

def list_recent_appointments(user_id: int) -> str:
    rows = db.list_appointments(user_id, limit=20)
    if not rows:
        return '📅 <b>我的预约</b>\n\n暂时还没有预约记录。\n\n看中频道房源后，点击帖子里的「预约看房」即可提交时间。'
    rows = sorted(rows, key=_appointment_sort_key, reverse=True)
    upcoming = [row for row in rows if _appointment_is_upcoming(row)]
    history = [row for row in rows if row not in upcoming]
    parts = ['📅 <b>我的预约</b>', '']
    if upcoming:
        parts.append('<b>即将进行</b>')
        for index, row in enumerate(upcoming[:2]):
            parts.extend(_appointment_summary_line(row))
            if index < min(len(upcoming), 2) - 1:
                parts.append('')
    if history:
        if upcoming:
            parts.append('')
        parts.append(f'📁 历史预约 · {len(history)} 条')
        parts.append('已完成、已取消或已过期的记录不在首页展开。')
    if not upcoming and (not history):
        parts.append('暂无可显示的预约记录。')
    parts.extend(['', '需要改时间或补充要求，直接联系顾问即可。'])
    return '\n'.join(parts)

def _appointment_details_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = db.list_appointments(user_id, limit=10)
    buttons = []
    for row in rows:
        status = str(row.get('status') or 'pending')
        if status in {'pending', 'assigned', 'contacted', 'confirmed'}:
            appt_id = int(row.get('id') or 0)
            if appt_id:
                label = f"取消 {_appointment_date_compact(row.get('appointment_date'))} · {_appointment_listing_compact(row.get('listing_id'))}"
                buttons.append([InlineKeyboardButton(label[:60], callback_data=f'appointment_menu:cancel:{appt_id}')])
    buttons.append([InlineKeyboardButton('💬 联系顾问', callback_data='appointment_menu:contact')])
    buttons.append([InlineKeyboardButton('⬅️ 返回预约列表', callback_data='appointment_menu:list')])
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

    def _details_sort_key(row: dict):
        raw = str(row.get('appointment_date') or '')
        bits = raw.replace('/', '-').split('-')
        try:
            nums = [int(x) for x in bits if x.isdigit()]
            return (nums[-2], nums[-1], str(row.get('appointment_time') or ''))
        except (ValueError, IndexError):
            return (0, 0, '')
    rows = sorted(rows, key=_details_sort_key, reverse=True)
    active_rows = [row for row in rows if _appointment_is_upcoming(row)]
    row = (active_rows or rows)[0]
    rows = [row]
    parts = ['📋 <b>当前预约</b>', '']
    for index, row in enumerate(rows):
        mode = APPOINTMENT_MODE_LABELS.get(str(row.get('viewing_mode') or ''), str(row.get('viewing_mode') or '待确认'))
        time_label = _appointment_time_compact(row.get('appointment_time'))
        status = APPOINTMENT_STATUS_LABELS.get(str(row.get('status') or ''), str(row.get('status') or '待确认'))
        status_icon = {'待确认': '🟡', '已确认': '🟢', '顾问联系中': '🔵', '已完成': '✅', '已取消': '⚪', '未到场': '🔴'}.get(status, '🟡')
        listing = _appointment_listing_compact(row.get('listing_id'))
        parts.extend([f"<b>{_appointment_date_compact(row.get('appointment_date'))} · {he(time_label)}</b>", f'房源：<b>{he(listing)}</b>', f'方式：{he(mode)}', f'状态：{status_icon} {he(status)}'])
        note = str(row.get('note') or '').strip()
        if note:
            parts.append('看房要求：空调、家电、采光、用水和费用')
        if index < len(rows) - 1:
            parts.extend(['', '──────────', ''])
    parts.extend(['', '如需改时间、取消或补充要求，请联系顾问处理。'])
    return '\n'.join(parts)

def list_favorites_text(user_id: int) -> str:
    from .utils_formatting import _fmt_price
    rows = db.list_favorites(user_id)
    if not rows:
        return '⭐ 暂无收藏房源。\n\n在频道里点“收藏房源”后，这里会保留清单，方便你回头对比。'
    parts = ['⭐ 你收藏过的房源：']
    for item in rows[:8]:
        detail = []
        if item.get('layout'):
            detail.append(str(item.get('layout')))
        if item.get('size_sqm'):
            detail.append(f"{item.get('size_sqm')}㎡")
        detail_text = f" | {' · '.join(detail)}" if detail else ''
        parts.append(f"• {item.get('listing_id', '-')} | {item.get('area', '金边')} | {_fmt_price(item.get('price'))}{detail_text}")
    parts.append('\n需要从收藏里优先挑选，点「💬 联系我们」即可。')
    return '\n'.join(parts)
