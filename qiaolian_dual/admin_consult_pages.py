"""咨询后台列表页。admin_consult 再导出，不改 callback_admin.py。"""
from __future__ import annotations

from html import escape as he

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .attribution import admin_source_group_zh, lead_status_zh
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
    return ":".join(str(part) for part in parts)


def _lead_brief(lead: dict) -> str:
    from .utils_formatting import _display_listing_id

    status = lead_status_zh(lead.get("lead_status"))
    source = admin_source_group_zh(lead.get("first_source_type") or lead.get("source_type"))
    name = str(lead.get("display_name") or "客户").strip()
    username = str(lead.get("username") or "").strip()
    handle = f" @{username}" if username else ""
    listing = str(lead.get("listing_id") or "").strip()
    qc = _display_listing_id(listing) if listing else "未带房源"
    return f"• {he(status)}｜{he(name)}{he(handle)}｜{he(qc)}｜{he(source)}"


async def _edit_or_send(query, text: str, reply_markup=None) -> None:
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Exception:
        await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


async def handle_admin_console(update, context, query, data: str, user) -> int:
    from .admin_consult import admin_home_keyboard, admin_home_text
    from .admin_contract import _is_admin_user

    if not _is_admin_user(getattr(user, "id", 0)):
        await query.answer("仅顾问可操作", show_alert=True)
        return MAIN
    action = data.split(":", 1)[1] if ":" in data else "home"
    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回后台", callback_data=_cb("adminq", "home"))]])
    if action == "home":
        await _edit_or_send(query, admin_home_text(), admin_home_keyboard())
        return MAIN
    if action == "new":
        leads = list_leads_by_status("new", limit=15)
        body = "🆕 <b>新咨询</b>\n\n" + ("\n".join(_lead_brief(i) for i in leads) if leads else "暂时没有未跟进的咨询。")
        await _edit_or_send(query, body, back)
        return MAIN
    if action == "today":
        today = now_ts()[:10]
        compact = f"{int(today[5:7])}月{int(today[8:10])}日" if len(today) >= 10 else today
        rows = list_today_appointments(today, limit=15) or list_today_appointments(compact, limit=15)
        if not rows:
            body = "📅 <b>今日预约</b>\n\n今天还没有新的看房预约。"
        else:
            from .utils_formatting import _display_listing_id
            lines = []
            for item in rows:
                name = str(item.get("display_name") or "客户")
                qc = _display_listing_id(str(item.get("listing_id") or ""))
                when = f"{item.get('appointment_date') or ''} {item.get('appointment_time') or ''}".strip()
                lines.append(f"• {he(name)}｜{he(qc)}｜{he(when)}")
            body = "📅 <b>今日预约</b>\n\n" + "\n".join(lines)
        await _edit_or_send(query, body, back)
        return MAIN
    if action == "listings":
        leads = list_listing_leads(limit=15)
        body = "🏠 <b>房源线索</b>\n\n" + ("\n".join(_lead_brief(i) for i in leads) if leads else "还没有带房号的线索。")
        await _edit_or_send(query, body, back)
        return MAIN
    if action == "tickets":
        tickets = list_service_tickets(limit=15)
        if not tickets:
            body = "🛠 <b>服务工单</b>\n\n暂时没有报修或物业工单。"
        else:
            lines = [f"• #{item.get('id')}｜{he(str(item.get('issue_type') or '服务'))}｜{he(str(item.get('status') or 'new'))}" for item in tickets]
            body = "🛠 <b>服务工单</b>\n\n" + "\n".join(lines)
        await _edit_or_send(query, body, back)
        return MAIN
    if action == "stats":
        rows = source_stats(limit=12)
        if not rows:
            body = "📊 <b>来源统计</b>\n\n还没有可统计的线索。"
        else:
            lines = [f"• {he(admin_source_group_zh(item.get('src')))}：{int(item.get('total') or 0)} 条" for item in rows]
            body = "📊 <b>来源统计</b>\n\n" + "\n".join(lines)
        await _edit_or_send(query, body, back)
        return MAIN
    if action == "journal":
        leads = list_leads_by_status(None, limit=12)
        body = "📚 <b>跟进记录</b>\n\n" + ("\n".join(_lead_brief(i) for i in leads) if leads else "还没有跟进记录。")
        await _edit_or_send(query, body, back)
        return MAIN
    return MAIN


async def handle_lead_view(query, lead_id: int) -> None:
    from .admin_consult import consult_action_keyboard, format_consult_notify
    from .attribution import lead_status_zh as status_zh

    lead = db.get_lead(lead_id) or {}
    listing_id = str(lead.get("listing_id") or "").strip()
    title, lines = format_consult_notify(
        touch={
            "first_source_type": lead.get("first_source_type") or lead.get("source_type"),
            "latest_source_type": lead.get("source_type"),
            "first_legacy": "legacy_discussion_entry" in str(lead.get("source_detail") or lead.get("deep_link_payload") or ""),
            "deep_link_payload": lead.get("deep_link_payload") or "",
            "entry_action": lead.get("entry_action") or lead.get("action") or "",
            "listing_id": listing_id,
        },
        listing_id=listing_id,
        title="查看房源",
        current_action=str(lead.get("entry_action") or lead.get("action") or ""),
        username=str(lead.get("username") or ""),
        display_name=str(lead.get("display_name") or ""),
        user_id=int(lead.get("user_id") or 0),
    )
    text = f"🏠 <b>{he(title)}</b>\n\n" + "\n".join(lines) + f"\n\n当前状态：{he(status_zh(lead.get('lead_status')))}"
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=consult_action_keyboard(
            lead_id=lead_id, appointment_id=0, user_id=int(lead.get("user_id") or 0), listing_id=listing_id
        ),
    )


def apply_admin_lead_action(action: str, lead_id: int, *, advisor_id: str, advisor_name: str) -> tuple[bool, str]:
    mapping = {
        "claim": ("claimed", "已接手"),
        "contacted": ("contacted", "已联系"),
        "done": ("done", "已完成"),
        "invalid": ("invalid", "无效"),
    }
    if action not in mapping:
        return False, ""
    status, label = mapping[action]
    return update_lead_status(lead_id, status, advisor_id=advisor_id, advisor_name=advisor_name), label
