"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def _appointment_date_keyboard() -> InlineKeyboardMarkup:
    today = datetime.now(ZoneInfo('Asia/Phnom_Penh'))
    btns = [InlineKeyboardButton('今天', callback_data='apdate:' + today.strftime('%m-%d')), InlineKeyboardButton('明天', callback_data='apdate:' + (today + timedelta(days=1)).strftime('%m-%d')), InlineKeyboardButton('后天', callback_data='apdate:' + (today + timedelta(days=2)).strftime('%m-%d'))]
    return InlineKeyboardMarkup([btns, [InlineKeyboardButton('其他日期', callback_data='apdate:other')], [InlineKeyboardButton('⬅️ 返回选择看房方式', callback_data='appoint_back_mode')]])

def _appointment_mode_keyboard(listing_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📹 视频看房', callback_data='apmode:video')], [InlineKeyboardButton('📅 预约线下看房', callback_data='apmode:offline')], [InlineKeyboardButton('⬅️ 返回', callback_data=f'listing:detail:{listing_id}')]])

def _appointment_time_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('上午 9:00-12:00', callback_data='aptime:am'), InlineKeyboardButton('下午 14:00-17:00', callback_data='aptime:pm')], [InlineKeyboardButton('傍晚 17:00-19:00', callback_data='aptime:evening'), InlineKeyboardButton('其他时间', callback_data='aptime:other')], [InlineKeyboardButton('⬅️ 返回上一步', callback_data='appoint_back_date'), InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def _title_layout_label(title: str, layout: str, separator: str=' · ') -> str:
    """房源标题已含户型时不再重复追加。"""
    title_text = str(title or '').strip()
    layout_text = str(layout or '').strip()
    if not layout_text or layout_text.lower() in title_text.lower():
        return title_text
    return f'{title_text}{separator}{layout_text}'

def _appointment_confirm_text(appt: dict) -> str:
    from .listing import listing_context
    from .utils_formatting import _display_listing_id
    item = listing_context(str(appt.get('listing_id') or ''))
    title = str(item.get('project') or item.get('title') or item.get('area') or '这套房').strip()
    layout = str(item.get('layout') or item.get('property_type') or '').strip()
    mode = APPOINTMENT_MODE_LABELS.get(str(appt.get('mode') or 'offline'), '实地看房')
    time_label = APPOINTMENT_TIME_LABELS.get(str(appt.get('time') or ''), str(appt.get('time') or '-'))
    return '\n'.join(['📅 <b>确认看房</b>', '', f"🏠 <b>{he(_title_layout_label(title, layout, '｜'))}</b>", f"<code>{he(_display_listing_id(appt.get('listing_id') or ''))}</code>", '', f"📅 <b>{he(str(appt.get('date') or '-'))} · {he(time_label)}</b>", f'{he(mode)}', '', '顾问确认后会通过 Telegram 联系你。'])

def _appointment_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ 提交预约', callback_data='apconfirm:yes')], [InlineKeyboardButton('改日期', callback_data='apedit:date'), InlineKeyboardButton('改时间', callback_data='apedit:time')], [InlineKeyboardButton('取消', callback_data='home')]])

def _normalize_custom_date(value: str) -> str:
    """支持 0820 / 820 / 08-20 / 8月20日，统一展示为 8月20日。"""
    raw = str(value or '').strip()
    compact = re.sub('\\s+', '', raw)
    month = day = 0
    if re.fullmatch('\\d{3,4}', compact):
        month = int(compact[:-2])
        day = int(compact[-2:])
    else:
        match = re.fullmatch('(?:\\d{4}[-/.])?(\\d{1,2})[-/.](\\d{1,2})', compact)
        if not match:
            match = re.fullmatch('(\\d{1,2})月(\\d{1,2})日', compact)
        if match:
            month, day = (int(match.group(1)), int(match.group(2)))
    if month and day:
        try:
            datetime(2024, month, day)
        except ValueError:
            return ''
        return f'{month}月{day}日'
    if re.fullmatch('(?:下周|本周)?[一二三四五六日天]', compact):
        return compact
    return ''

def _focus_summary_lines(keys: list[str] | set[str] | tuple[str, ...]) -> str:
    picked = [k for k in APPOINTMENT_FOCUS_ORDER if k in set(keys)]
    if not picked:
        return '（未选择）'
    return '\n'.join((f'• {he(APPOINTMENT_FOCUS_LABELS[k])}' for k in picked))

def _appointment_focus_keyboard(selected: set[str]) -> InlineKeyboardMarkup:
    btn_rows: list[list[InlineKeyboardButton]] = []
    labels = APPOINTMENT_FOCUS_LABELS
    order = APPOINTMENT_FOCUS_ORDER
    row: list[InlineKeyboardButton] = []
    for idx, key in enumerate(order, start=1):
        checked = '✅' if key in selected else '▫️'
        row.append(InlineKeyboardButton(f'{checked} {labels[key]}', callback_data=f'apfocus:toggle:{key}'))
        if idx % 2 == 0:
            btn_rows.append(row)
            row = []
    if row:
        btn_rows.append(row)
    btn_rows.append([InlineKeyboardButton('✅ 下一步（选日期）', callback_data='apfocus:next')])
    btn_rows.append([InlineKeyboardButton('⬅️ 返回方式', callback_data='apfocus:back_mode'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(btn_rows)

def _appointment_focus_prompt(mode: str, listing_id: str, selected: set[str]) -> str:
    from .utils_formatting import _display_listing_id
    mode_label = APPOINTMENT_MODE_LABELS.get(mode, '预约看房')
    safe_lid = listing_id if listing_id and listing_id != '待推荐' else '暂未指定'
    return f'<b>📅 {mode_label}</b>\n房源：<code>{he(_display_listing_id(safe_lid))}</code>\n\n<b>第二步：请选择你最关注的验房点</b>\n默认 5 项全选，你只需点 <b>下一步</b> 就能继续。\n\n{_focus_summary_lines(selected)}'
