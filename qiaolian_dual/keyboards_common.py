"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def main_keyboard() -> InlineKeyboardMarkup:
    """手机首页的客户任务入口。"""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search'),
                InlineKeyboardButton('🏠 可预约房源', callback_data='hub:available'),
            ],
            [
                InlineKeyboardButton('📅 我的预约', callback_data='hub:appointments'),
                InlineKeyboardButton('🛠 入住服务', callback_data='hub:service'),
            ],
            [InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')],
        ]
    )

def no_match_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('✏️ 修改条件', callback_data='findmode:guided'), InlineKeyboardButton('💬 让顾问帮我找', callback_data='appointment_menu:contact')], [InlineKeyboardButton('🏠 看其他可预约房源', callback_data='hub:latest')]])

def quick_start_keyboard() -> InlineKeyboardMarkup:
    return main_keyboard()

def room_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('开间', callback_data='roompick:studio'), InlineKeyboardButton('1房', callback_data='roompick:1房')], [InlineKeyboardButton('2房', callback_data='roompick:2房'), InlineKeyboardButton('3房+', callback_data='roompick:3房')], [InlineKeyboardButton('别墅/排屋', callback_data='roompick:别墅'), InlineKeyboardButton('商铺/办公', callback_data='roompick:商铺')], [InlineKeyboardButton('⬅️ 返回', callback_data='home')]])

def latest_listing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🔍 帮我找房', callback_data='home_smart_search'), InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')], [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')]])

def keyword_followup_keyboard(*, area: str='', room_type: str='') -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if area:
        rows.append([InlineKeyboardButton('💰 再按预算缩小', callback_data='hub:budget'), InlineKeyboardButton('🛏 再按户型缩小', callback_data='hub:layout')])
    elif room_type:
        rows.append([InlineKeyboardButton('📍 补一个区域', callback_data='hub:area'), InlineKeyboardButton('💰 补一个预算', callback_data='hub:budget')])
    else:
        rows.append([InlineKeyboardButton('📍 按区域继续找', callback_data='hub:area'), InlineKeyboardButton('💰 按预算继续找', callback_data='hub:budget')])
    # 视频看房只作为“预约看房”日期页里的分支，不在找房结果页另开入口。
    rows.append([InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def _advisor_tg_url() -> str:
    handle = str(ADVISOR_TG or '').strip().lstrip('@')
    return f'https://t.me/{handle}' if handle else ''

def _advisor_listing_url(listing_id: str) -> str:
    """房源页直达真人顾问，并预填当前房源摘要。"""
    from .listing import listing_context
    from .utils_formatting import _display_listing_id, _display_layout, _fmt_price
    base = _advisor_tg_url()
    if not base:
        return ''
    info = listing_context(listing_id)
    display_layout = _display_layout(info.get('layout'), info.get('property_type'))
    facts = [str(info.get('project') or info.get('community') or info.get('area') or '').strip(), display_layout, _fmt_price(info.get('price'))]
    summary = '｜'.join((value for value in facts if value and value != '价格待确认'))
    message = f'你好，我想咨询房源 {_display_listing_id(listing_id)}'
    if summary:
        message += f'（{summary}）'
    return f"{base}?text={quote(message, safe='')}"

def _listing_channel_url(listing_id: str) -> str:
    """房源详情页的频道主帖入口；主帖下方即留言区。"""
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
    """咨询交接按钮；视频看房统一从预约日期页切换，不重复暴露入口。"""
    listing_id = str(listing_id or '').strip()
    advisor_url = _advisor_listing_url(listing_id) if listing_id else _advisor_tg_url()
    chat_btn = InlineKeyboardButton('💬 联系中文顾问', url=advisor_url) if advisor_url else InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')
    return InlineKeyboardMarkup([
        [chat_btn],
        [InlineKeyboardButton('📅 预约看房', callback_data='appointment_menu:offline')],
        [InlineKeyboardButton('🏠 继续看房', callback_data='hub:latest')],
    ])

def appointment_success_keyboard() -> InlineKeyboardMarkup:
    """提交后给出查看、顾问与继续看房三个明确去向。"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 继续看房', callback_data='hub:latest')],
    ])

def channel_return_keyboard(channel_url: str='') -> InlineKeyboardMarkup:
    """完成私聊转化后，给用户返回频道或继续筛选的选择。"""
    rows: list[list[InlineKeyboardButton]] = []
    if channel_url and channel_url.strip():
        rows.append([InlineKeyboardButton('📺 返回频道继续看', url=channel_url)])
    rows.append([InlineKeyboardButton('🔍 继续筛选其他房源', callback_data='hub:find'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def lead_capture_keyboard() -> InlineKeyboardMarkup:
    """关键行为完成后的下一步：联系中文顾问或继续看房。"""
    rows: list[list[InlineKeyboardButton]] = []
    advisor_url = _advisor_tg_url()
    if advisor_url:
        rows.append([InlineKeyboardButton('💬 联系中文顾问', url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton('💬 联系中文顾问', callback_data='hub:advisor')])
    rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def old_tenant_followup_keyboard() -> InlineKeyboardMarkup:
    """老客新页面不再生成续租/换房按钮；旧回调仍由处理器兼容。"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 我的租约', callback_data='contract:view')],
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('🛠 入住服务', callback_data='service:hub')],
        [InlineKeyboardButton('💬 联系中文顾问', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])
