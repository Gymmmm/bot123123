"""中文管理员咨询台：通知卡片 + /admin 后台。不改 callback_admin.py。"""
from __future__ import annotations

from html import escape as he

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .attribution import (
    admin_source_group_zh,
    entry_action_zh,
    lead_status_zh,
    source_type_zh,
)
from .attribution_store import (
    list_leads_by_status,
    list_listing_leads,
    list_service_tickets,
    list_today_appointments,
    source_stats,
    update_lead_status,
)
from .common import MAIN, db
from .session_deeplink import now_ts


def _cb(*parts: object) -> str:
    data = ":".join(str(part) for part in parts)
    if len(data.encode("utf-8")) > 64:
        raise ValueError(f"callback_data too long: {data}")
    return data


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🆕 新咨询", callback_data=_cb("adminq", "new")),
                InlineKeyboardButton("📅 今日预约", callback_data=_cb("adminq", "today")),
            ],
            [
                InlineKeyboardButton("🏠 房源线索", callback_data=_cb("adminq", "listings")),
                InlineKeyboardButton("🛠 服务工单", callback_data=_cb("adminq", "tickets")),
            ],
            [
                InlineKeyboardButton("📊 来源统计", callback_data=_cb("adminq", "stats")),
                InlineKeyboardButton("📚 跟进记录", callback_data=_cb("adminq", "journal")),
            ],
        ]
    )


def consult_action_keyboard(
    *,
    lead_id: int,
    appointment_id: int = 0,
    user_id: int = 0,
    listing_id: str = "",
    include_back: bool = True,
) -> InlineKeyboardMarkup:
    suffix = f"{int(lead_id or 0)}:{int(appointment_id or 0)}:{int(user_id or 0)}"
    rows = [
        [InlineKeyboardButton("✅ 我来跟进", callback_data=f"adminlead:claim:{suffix}")],
        [InlineKeyboardButton("📞 已联系", callback_data=f"adminlead:contacted:{suffix}")],
        [InlineKeyboardButton("✅ 完成", callback_data=f"adminlead:done:{suffix}")],
    ]
    if listing_id:
        rows.append([InlineKeyboardButton("🏠 查看房源", callback_data=f"adminlead:view:{suffix}")])
    if include_back:
        rows.append([InlineKeyboardButton("⬅️ 返回后台", callback_data=_cb("adminq", "home"))])
    for row in rows:
        for button in row:
            raw = str(button.callback_data or "")
            if len(raw.encode("utf-8")) > 64:
                raise ValueError(f"callback_data too long: {raw}")
    return InlineKeyboardMarkup(rows)
