"""Integrated keyboards for the modular User Bot.

Re-exports the UI-clean keyboard set and only overrides the customer-facing
entry points that changed after the cleanup branch.
"""
from __future__ import annotations

from .keyboards_common_base import *  # noqa: F401,F403


def main_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton('🔍 智能找房', callback_data='home_smart_search'), InlineKeyboardButton('📖 关于侨联', callback_data='hub:about')],
        [InlineKeyboardButton('📅 预约看房', callback_data='hub:appointments'), InlineKeyboardButton('💬 联系顾问', callback_data='hub:advisor')],
        [InlineKeyboardButton('🛡 入住服务', callback_data='hub:service'), InlineKeyboardButton('🧭 周边服务', callback_data='service:local_life')],
    ]
    channel_url = str(CHANNEL_URL or '').strip()
    if channel_url:
        rows.append([InlineKeyboardButton('📢 房源频道', url=channel_url)])
    return InlineKeyboardMarkup(rows)


def quick_start_keyboard() -> InlineKeyboardMarkup:
    return main_keyboard()


def old_tenant_followup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📋 我的租约', callback_data='contract:view'), InlineKeyboardButton('🛡 入住服务', callback_data='service:hub')],
        [InlineKeyboardButton('📅 我的预约', callback_data='appointment_menu:list'), InlineKeyboardButton('💬 联系顾问', callback_data='appointment_menu:contact')],
        [InlineKeyboardButton('🏠 返回首页', callback_data='home')],
    ])
