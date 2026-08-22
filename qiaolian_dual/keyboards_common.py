"""从 user_bot.py 拆分出的职责模块。"""
from __future__ import annotations

from .common import *

def main_keyboard() -> InlineKeyboardMarkup:
    """手机首页的客户任务入口。

    首页只展示客户真正会使用的动作；收藏、帮助和品牌介绍放到二级
    页面，避免首次打开时让用户理解内部功能。
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton('🔍 找房', callback_data='home_smart_search'),
                InlineKeyboardButton('🆕 最新实拍', callback_data='hub:latest'),
            ],
            [
                InlineKeyboardButton('📅 预约看房', callback_data='hub:appoint'),
                InlineKeyboardButton('📋 我的预约', callback_data='hub:appointments'),
            ],
            [
                InlineKeyboardButton('🏠 我已租住', callback_data='hub:contract'),
                InlineKeyboardButton('🧰 生活服务', callback_data='hub:service'),
            ],
            [InlineKeyboardButton('💬 联系顾问', callback_data='hub:advisor')],
        ]
    )

def no_match_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')], [InlineKeyboardButton('🎯 继续筛选', callback_data='findmode:guided'), InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def quick_start_keyboard() -> InlineKeyboardMarkup:
    return main_keyboard()

def room_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('🛏 单间 / Studio', callback_data='roompick:studio'), InlineKeyboardButton('🛏 1房', callback_data='roompick:1房')], [InlineKeyboardButton('🛏 2房', callback_data='roompick:2房'), InlineKeyboardButton('🛏 3房+', callback_data='roompick:3房')], [InlineKeyboardButton('🏡 别墅 / 排屋', callback_data='roompick:别墅'), InlineKeyboardButton('🏬 商铺 / 办公', callback_data='roompick:商铺')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def latest_listing_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📍 按区域继续找', callback_data='hub:area'), InlineKeyboardButton('💰 按预算继续找', callback_data='hub:budget')], [InlineKeyboardButton('🛏 按户型继续找', callback_data='hub:layout'), InlineKeyboardButton('🎥 视频代看', callback_data='hub:video_tour')], [InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor'), InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def keyword_followup_keyboard(*, area: str='', room_type: str='') -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if area:
        rows.append([InlineKeyboardButton('💰 再按预算缩小', callback_data='hub:budget'), InlineKeyboardButton('🛏 再按户型缩小', callback_data='hub:layout')])
    elif room_type:
        rows.append([InlineKeyboardButton('📍 补一个区域', callback_data='hub:area'), InlineKeyboardButton('💰 补一个预算', callback_data='hub:budget')])
    else:
        rows.append([InlineKeyboardButton('📍 按区域继续找', callback_data='hub:area'), InlineKeyboardButton('💰 按预算继续找', callback_data='hub:budget')])
    rows.append([InlineKeyboardButton('🎥 视频代看', callback_data='appointment_menu:video'), InlineKeyboardButton('💬 联系顾问', callback_data='appointment_menu:contact')])
    rows.append([InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def _advisor_tg_url() -> str:
    handle = str(ADVISOR_TG or '').strip().lstrip('@')
    return f'https://t.me/{handle}' if handle else ''

def _advisor_listing_url(listing_id: str) -> str:
    """房源页直达真人顾问，并预填当前房源摘要。"""
    from .listing import listing_context
    from .utils_formatting import _display_listing_id, _fmt_price
    base = _advisor_tg_url()
    if not base:
        return ''
    info = listing_context(listing_id)
    facts = [str(info.get('project') or info.get('community') or info.get('area') or '').strip(), str(info.get('layout') or '').strip(), _fmt_price(info.get('price'))]
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

def contact_handoff_keyboard() -> InlineKeyboardMarkup:
    advisor_url = _advisor_tg_url()
    chat_btn = InlineKeyboardButton('💬 直接联系顾问', url=advisor_url) if advisor_url else InlineKeyboardButton('💬 直接联系顾问', callback_data='appointment_menu:contact')
    return InlineKeyboardMarkup([[InlineKeyboardButton('📅 预约实地看房', callback_data='appointment_menu:offline'), InlineKeyboardButton('🎥 改视频代看', callback_data='appointment_menu:video')], [InlineKeyboardButton('🏠 继续看房', callback_data='home'), chat_btn]])

def appointment_success_keyboard() -> InlineKeyboardMarkup:
    """提交后只保留两个最清楚的下一步。"""
    return InlineKeyboardMarkup([[InlineKeyboardButton('📋 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('🏠 返回首页', callback_data='home')]])

def channel_return_keyboard(channel_url: str='') -> InlineKeyboardMarkup:
    """完成私聊转化后，给用户返回频道或继续筛选的选择。"""
    rows: list[list[InlineKeyboardButton]] = []
    if channel_url and channel_url.strip():
        rows.append([InlineKeyboardButton('📺 返回频道继续看', url=channel_url)])
    rows.append([InlineKeyboardButton('🔍 继续筛选其他房源', callback_data='hub:find'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def lead_capture_keyboard() -> InlineKeyboardMarkup:
    """关键行为完成后的下一步：直接联系我们或继续看房。"""
    rows: list[list[InlineKeyboardButton]] = []
    advisor_url = _advisor_tg_url()
    if advisor_url:
        rows.append([InlineKeyboardButton('💬 打开顾问对话', url=advisor_url)])
    else:
        rows.append([InlineKeyboardButton('💬 联系我们', callback_data='hub:advisor')])
    rows.append([InlineKeyboardButton('🔍 继续找房', callback_data='home_smart_search'), InlineKeyboardButton('🏠 返回首页', callback_data='home')])
    return InlineKeyboardMarkup(rows)

def old_tenant_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('📋 我的租约', callback_data='contract:view'), InlineKeyboardButton('🔄 续租咨询', callback_data='contract:renew')], [InlineKeyboardButton('🏠 我要换房', callback_data='contract:change'), InlineKeyboardButton('💬 联系我们', callback_data='appointment_menu:contact')], [InlineKeyboardButton('📅 预约看房', callback_data='appointment_menu:offline')], [InlineKeyboardButton('🏠 返回首页', callback_data='home')]])
