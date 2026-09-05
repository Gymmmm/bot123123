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
from .admin_consult_pages import apply_admin_lead_action, handle_admin_console, handle_lead_view


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


def _customer_line(user=None, *, username: str = "", display_name: str = "", user_id: int = 0) -> str:
    if user is not None:
        display_name = getattr(user, "full_name", "") or getattr(user, "first_name", "") or display_name or "客户"
        username = getattr(user, "username", "") or username
        user_id = int(getattr(user, "id", 0) or user_id or 0)
    name = (display_name or "客户").strip()
    handle = f" @{username}" if username else ""
    if not handle and user_id:
        handle = f" {user_id}"
    return f"{name}{handle}".strip()


def _listing_facts(listing_id: str) -> dict[str, str]:
    from .listing import listing_context
    from .utils_formatting import _display_layout, _display_listing_id, _fmt_price
    info = listing_context(listing_id) if listing_id else {}
    project = str(info.get("project") or info.get("community") or info.get("area") or "").strip()
    layout = _display_layout(info.get("layout") or info.get("property_type"), info.get("property_type")) if info else ""
    return {
        "qc": _display_listing_id(listing_id) if listing_id else "",
        "project": project,
        "layout": layout or "",
        "price": _fmt_price(info.get("price")) if info else "",
    }


def format_consult_notify(
    *,
    user=None,
    touch: dict | None = None,
    listing_id: str = "",
    title: str = "房源咨询",
    current_action: str = "",
    username: str = "",
    display_name: str = "",
    user_id: int = 0,
) -> tuple[str, list[str]]:
    touch = dict(touch or {})
    listing_id = str(listing_id or touch.get("listing_id") or touch.get("latest_listing_id") or "").strip()
    facts = _listing_facts(listing_id)
    first_type = touch.get("first_source_type") or touch.get("source_type") or "other"
    latest_type = touch.get("latest_source_type") or touch.get("source_type") or first_type
    first_label = source_type_zh(first_type)
    if touch.get("first_legacy") or touch.get("legacy"):
        first_label = f"{first_label}（历史入口）"
    latest_label = admin_source_group_zh(latest_type)
    action_label = entry_action_zh(current_action or touch.get("entry_action"))
    entry = str(touch.get("deep_link_payload") or touch.get("latest_deep_link") or touch.get("first_deep_link") or "").strip()
    if entry.startswith("discussion_entry"):
        entry = "历史讨论区入口"
    lines = [
        f"客户：{he(_customer_line(user, username=username, display_name=display_name, user_id=user_id))}",
        f"来源：{he(latest_label)}",
    ]
    if facts["qc"]:
        lines.append(f"房源：{he(facts['qc'])}")
    if facts["project"]:
        lines.append(f"楼盘：{he(facts['project'])}")
    if facts["layout"]:
        lines.append(f"户型：{he(facts['layout'])}")
    if facts["price"]:
        lines.append(f"租金：{he(facts['price'])}")
    lines.extend(["", f"首次进入：{he(first_label)}", f"本次动作：{he(action_label)}"])
    if entry:
        lines.append(f"入口：{he(entry)}")
    return title, lines


def admin_home_text() -> str:
    return (
        "📋 <b>侨联咨询后台</b>\n\n"
        "客户从频道或 Bot 进来的咨询、预约和服务，都在这里跟进。\n"
        "状态只用中文：新咨询 / 待跟进 / 已接手 / 已联系 / 已预约 / 已完成 / 无效。"
    )


async def cmd_admin_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user

    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("这个入口只给中文顾问使用。")
        return MAIN
    await update.effective_message.reply_text(
        admin_home_text(), parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard()
    )
    return MAIN
