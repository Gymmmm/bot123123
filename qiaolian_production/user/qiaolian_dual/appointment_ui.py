"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


def _appointment_date_keyboard(*, show_video: bool = True) -> InlineKeyboardMarkup:
    today = datetime.now(ZoneInfo('Asia/Phnom_Penh'))
    btns = [
        InlineKeyboardButton('今天', callback_data='apdate:' + today.strftime('%m-%d')),
        InlineKeyboardButton('明天', callback_data='apdate:' + (today + timedelta(days=1)).strftime('%m-%d')),
        InlineKeyboardButton('后天', callback_data='apdate:' + (today + timedelta(days=2)).strftime('%m-%d')),
    ]
    rows = [
        btns[:2],
        [btns[2], InlineKeyboardButton('📅 其他日期', callback_data='apdate:other')],
    ]
    if show_video:
        rows.append([InlineKeyboardButton('🎥 改为视频看房', callback_data='apmode:video')])
    else:
        rows.append([InlineKeyboardButton('🚶 改为实地看房', callback_data='apmode:offline')])
    rows.append([InlineKeyboardButton('⬅️ 返回房源', callback_data='appoint_back_mode'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)


def _appointment_mode_keyboard(listing_id: str) -> InlineKeyboardMarkup:
    """历史回调兼容；正常预约不再展示独立方式选择页。"""
    back_target = 'home' if str(listing_id or '').strip() in {'', '待推荐'} else f'listing:detail:{listing_id}'
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🚶 实地看房', callback_data='apmode:offline'), InlineKeyboardButton('🎥 视频看房', callback_data='apmode:video')],
        [InlineKeyboardButton('⬅️ 返回房源', callback_data=back_target), InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def _appointment_time_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('上午 09:00–12:00', callback_data='aptime:am')],
        [InlineKeyboardButton('下午 14:00–17:00', callback_data='aptime:pm')],
        [InlineKeyboardButton('晚上 17:00–19:00', callback_data='aptime:evening')],
        [InlineKeyboardButton('✍️ 其他时间', callback_data='aptime:other')],
        [InlineKeyboardButton('⬅️ 修改日期', callback_data='appoint_back_date'), InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def _title_layout_label(title: str, layout: str, separator: str=' · ') -> str:
    """房源标题已含户型时不再重复追加。"""
    title_text = str(title or '').strip()
    from .utils_formatting import _display_layout
    layout_text = _display_layout(layout)
    if not layout_text or layout_text.lower() in title_text.lower():
        return title_text
    return f'{title_text}{separator}{layout_text}'


def _appointment_confirm_text(appt: dict) -> str:
    """历史确认页兼容文案；新流程选完时间直接提交。"""
    from .listing import listing_context
    from .utils_formatting import _display_layout, _fmt_price
    listing_id = str(appt.get('listing_id') or '').strip()
    is_general_request = listing_id in {'', '待推荐'} or bool((appt.get('touch_payload') or {}).get('listing_unknown'))
    item = listing_context(listing_id)
    title = str(item.get('project') or item.get('title') or item.get('area') or '这套房').strip()
    layout = _display_layout(item.get('layout') or item.get('property_type'), item.get('property_type'))
    mode = APPOINTMENT_MODE_LABELS.get(str(appt.get('mode') or 'offline'), '实地看房')
    time_label = APPOINTMENT_TIME_LABELS.get(str(appt.get('time') or ''), str(appt.get('time') or '-'))
    subject_lines = ['🏠 <b>尚未确定房源</b>'] if is_general_request else [f"🏠 <b>{he(_title_layout_label(title, layout, '｜'))}</b>"]
    if not is_general_request and item.get('price') not in (None, '', 0, '0'):
        subject_lines.append(f"💰 <b>{he(_fmt_price(item.get('price')))}</b>")
    heading = '🎥 <b>确认视频看房</b>' if str(appt.get('mode') or '') == 'video' else '📅 <b>确认看房预约</b>'
    return '\n'.join([
        heading,
        '',
        *subject_lines,
        '',
        f"📅 {he(str(appt.get('date') or '-'))}",
        f'🕐 {he(time_label)}',
        f'👀 {he(mode)}',
        '',
        '提交后，顾问会再次确认最新房态和具体时间。',
    ])


def _appointment_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ 提交预约', callback_data='apconfirm:yes')],
        [InlineKeyboardButton('⬅️ 修改时间', callback_data='apedit:time'), InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


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
    """历史关注点回调的兼容落点；新 UI 只继续到日期。"""
    return _appointment_date_keyboard()


def _appointment_focus_prompt(mode: str, listing_id: str, selected: set[str]) -> str:
    """历史关注点回调的兼容文案；不再生成关注点页面。"""
    mode_label = APPOINTMENT_MODE_LABELS.get(mode, '预约看房')
    return f'📅 <b>{he(mode_label)}</b>\n\n哪天方便看房？'
