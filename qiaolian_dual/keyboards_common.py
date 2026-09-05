"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *


def main_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search'), InlineKeyboardButton('📅 我的预约', callback_data='hub:appointments')],
        [InlineKeyboardButton('🛡 侨联保障', callback_data='hub:rental'), InlineKeyboardButton('🛠 入住服务', callback_data='hub:service')],
    ]
    channel_url = str(CHANNEL_URL or '').strip()
    if channel_url:
        rows.append([InlineKeyboardButton('房源频道', url=channel_url), InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')])
    else:
        rows.append([InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')])
    return InlineKeyboardMarkup(rows)

def rental_service_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💰 费用说明', callback_data='hub:rental:fees'), InlineKeyboardButton('📋 入住留档', callback_data='hub:rental:handover')],
        [InlineKeyboardButton('🔐 押金说明', callback_data='hub:rental:deposit'), InlineKeyboardButton('🎥 实地 / 视频看房', callback_data='hub:rental:viewing')],
        [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def no_match_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✏️ 调整条件', callback_data='findmode:guided'), InlineKeyboardButton('🏘 看相近房源', callback_data='find:similar')],
        [InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def quick_start_keyboard() -> InlineKeyboardMarkup:
    return main_keyboard()


def room_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('单间', callback_data='roompick:studio'), InlineKeyboardButton('一房', callback_data='roompick:1房')],
        [InlineKeyboardButton('两房', callback_data='roompick:2房'), InlineKeyboardButton('三房', callback_data='roompick:3房')],
        [InlineKeyboardButton('四房+', callback_data='roompick:4房'), InlineKeyboardButton('不限', callback_data='roompick:any')],
        [InlineKeyboardButton('⬅️ 返回', callback_data='home_smart_search')],
    ])


def latest_listing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search'), InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def keyword_followup_keyboard(*, area: str='', room_type: str='') -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if area:
        rows.append([InlineKeyboardButton('💰 按预算', callback_data='hub:budget'), InlineKeyboardButton('🏠 按户型', callback_data='hub:layout')])
    elif room_type:
        rows.append([InlineKeyboardButton('📍 按区域', callback_data='hub:area'), InlineKeyboardButton('💰 按预算', callback_data='hub:budget')])
    else:
        rows.append([InlineKeyboardButton('📍 按区域', callback_data='hub:area'), InlineKeyboardButton('💰 按预算', callback_data='hub:budget')])
    rows.append([InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')])
    rows.append([InlineKeyboardButton('⬅️ 返回', callback_data='home_smart_search')])
    return InlineKeyboardMarkup(rows)


def _advisor_tg_url() -> str:
    handle = str(ADVISOR_TG or '').strip().lstrip('@')
    return f'https://t.me/{handle}' if handle else ''


def _advisor_listing_url(listing_id: str) -> str:
    """直达顾问，并预填当前房源的 QC、项目户型和租金。"""
    from .listing import listing_context
    from .utils_formatting import _display_listing_id, _display_layout, _fmt_price
    base = _advisor_tg_url()
    if not base:
        return ''
    info = listing_context(listing_id)
    project = str(info.get('project') or info.get('community') or info.get('area') or '').strip()
    layout = _display_layout(info.get('layout'), info.get('property_type'))
    lines = [f'你好，我想咨询房源 {_display_listing_id(listing_id)}']
    subject = '｜'.join(value for value in (project, layout) if value)
    if subject:
        lines.extend(['', subject])
    price = _fmt_price(info.get('price'))
    if price and price != '价格待确认':
        lines.append(price)
    return f"{base}?text={quote(chr(10).join(lines), safe='')}"


def _listing_channel_url(listing_id: str) -> str:
    from .listing import listing_context
    info = listing_context(listing_id)
    message_id = info.get('channel_message_id')
    try:
        mid = int(message_id or 0)
    except (TypeError, ValueError):
        mid = 0
    base = str(CHANNEL_URL or '').rstrip('/')
    return f'{base}/{mid}' if base and mid > 0 else ''


def contact_handoff_keyboard(*, listing_id: str='') -> InlineKeyboardMarkup:
    """所有客户联系入口统一使用“联系我们”。"""
    listing_id = str(listing_id or '').strip()
    advisor_url = _advisor_listing_url(listing_id) if listing_id else _advisor_tg_url()
    if advisor_url:
        chat_btn = InlineKeyboardButton('💬 联系我们', url=advisor_url)
    else:
        chat_btn = InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')
    if listing_id:
        return InlineKeyboardMarkup([
            [chat_btn],
            [InlineKeyboardButton('📅 预约看房', callback_data=f'listing:appoint:{listing_id}'), InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')],
        ])
    return InlineKeyboardMarkup([
        [chat_btn],
        [InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def appointment_success_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📅 查看我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')],
    ])


def channel_return_keyboard(channel_url: str='') -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if channel_url and channel_url.strip():
        rows.append([InlineKeyboardButton('📺 返回频道继续看', url=channel_url)])
    rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)


def lead_capture_keyboard() -> InlineKeyboardMarkup:
    advisor_url = _advisor_tg_url()
    chat_btn = InlineKeyboardButton('💬 联系我们', url=advisor_url) if advisor_url else InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')
    return InlineKeyboardMarkup([
        [chat_btn],
        [InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])


def old_tenant_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 我的租约', callback_data='contract:view')],
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('🛠 入住服务', callback_data='service:hub')],
        [InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])
