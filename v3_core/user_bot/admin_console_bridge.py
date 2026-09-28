"""Unified V3 admin home while legacy admin workflows remain adapter-owned."""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🆕 新咨询", callback_data="adminq:new"),
                InlineKeyboardButton("📅 今日预约", callback_data="adminq:appointments"),
            ],
            [
                InlineKeyboardButton("🏠 房源线索", callback_data="adminq:listings"),
                InlineKeyboardButton("🛠 服务工单", callback_data="adminq:services"),
            ],
            [
                InlineKeyboardButton("📄 租客与合同", callback_data="admincontract:home"),
                InlineKeyboardButton("🔄 续租跟进", callback_data="admincontract:renewals"),
            ],
            [
                InlineKeyboardButton("📊 来源统计", callback_data="adminq:sources"),
                InlineKeyboardButton("📚 跟进记录", callback_data="adminq:history"),
            ],
        ]
    )


async def show_unified_admin_home(update, context) -> None:
    message = getattr(update, "effective_message", None)
    if message is None:
        return
    await message.reply_text(
        "🧭 <b>侨联管理后台</b>\n\n"
        "咨询、预约、租约、续租和服务工单统一从这里处理。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_home_keyboard(),
    )


__all__ = ["admin_home_keyboard", "show_unified_admin_home"]
