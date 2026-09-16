"""Action-first room-status dashboard for the Publisher bot.

The existing inventory operator keeps all status writes, search, detail and
batch-sync behavior. This layer only changes the landing page so the owner sees
what needs attention first instead of a duplicate status-count menu.
"""
from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .inventory_operator_ui import PublisherInventoryAdminController


class PublisherInventoryDashboardController(PublisherInventoryAdminController):
    """Daily workbench over the existing inventory management actions."""

    async def show_listing_categories(self, message: Any) -> None:
        c = self._inventory_counts()
        lines = [
            "<b>🔵 房态工作台</b>",
            "",
            "<b>今天先处理</b>",
            f"🟠 待确认 {c['pending']}　　⏰ 超3天 {c['overdue']}",
            f"🟡 已有预约 {c['reserved']}　　🆕 今日发布 {c['today']}",
            "",
            "<b>当前库存</b>",
            f"🟢 可预约 {c['active']}　　🔴 已租出 {c['rented']}　　⚫ 已下架 {c['offline']}",
            "",
            "点房源可直接改房态、看完整资料和打开频道原帖。",
        ]
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(f"🟠 待确认 {c['pending']}", callback_data="v3smp|inv_rows|pending"),
                        InlineKeyboardButton(f"⏰ 超时 {c['overdue']}", callback_data="v3smp|inv_rows|overdue"),
                    ],
                    [
                        InlineKeyboardButton(f"🟡 已有预约 {c['reserved']}", callback_data="v3smp|inv_rows|reserved"),
                        InlineKeyboardButton(f"🆕 今日发布 {c['today']}", callback_data="v3smp|inv_rows|today"),
                    ],
                    [
                        InlineKeyboardButton("🕘 最近发布", callback_data="v3smp|inv_rows|recent"),
                        InlineKeyboardButton("🔍 搜索房源", callback_data="v3smp|inv_search"),
                    ],
                    [InlineKeyboardButton("☑️ 批量改房态", callback_data="v3smp|inv_batch")],
                    [
                        InlineKeyboardButton(f"🟢 可预约 {c['active']}", callback_data="v3smp|inv_rows|active"),
                        InlineKeyboardButton(f"🔴 已租出 {c['rented']}", callback_data="v3smp|inv_rows|rented"),
                    ],
                    [InlineKeyboardButton(f"⚫ 已下架 {c['offline']}", callback_data="v3smp|inv_rows|offline")],
                    self.home_row(),
                ]
            ),
        )


__all__ = ["PublisherInventoryDashboardController"]
