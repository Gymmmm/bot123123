"""Integrated tenant-service keyboard overrides."""
from __future__ import annotations

from .keyboards_search_base import *  # noqa: F401,F403


def service_hub_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔧 报修与维护', callback_data='service:repair_hub'), InlineKeyboardButton('🏢 物业沟通', callback_data='service_request:property')],
        [InlineKeyboardButton('🔄 续租 / 换房', callback_data='service:renew_change'), InlineKeyboardButton('🚪 退租与押金', callback_data='service:terminate')],
        [InlineKeyboardButton('📋 我的租约', callback_data='contract:view'), InlineKeyboardButton('🧭 周边服务', callback_data='service:local_life')],
        [InlineKeyboardButton('💬 联系顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回首页', callback_data='home')],
    ])


def service_detail_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('💬 联系顾问', callback_data='service:contact')],
        [InlineKeyboardButton('⬅️ 返回入住服务', callback_data='service:hub')],
    ])
