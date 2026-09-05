"""中文咨询后台：复用现有 leads / appointments / repair_tickets。"""
from __future__ import annotations

from datetime import datetime
import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .common import answer_callback_once, db, he
from .attribution import admin_source_group_zh, entry_action_zh, lead_status_zh, source_type_zh
from .attribution_store import (
    get_user_attribution,
    list_leads_by_status,
    list_listing_leads,
    list_service_tickets,
    list_today_appointments,
    source_stats,
    update_lead_status,
)


def _is_admin(user_id: int) -> bool:
    from .admin_contract import _is_admin_user
    return bool(_is_admin_user(int(user_id or 0)))


def _display_listing_id(value: str) -> str:
    raw = str(value or "").strip()
    if raw.lower().startswith("l_") and raw[2:].isdigit():
        return f"QC{int(raw[2:]):04d}"
    return raw


def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 新咨询", callback_data="adminq:new"), InlineKeyboardButton("📅 今日预约", callback_data="adminq:appointments")],
        [InlineKeyboardButton("🏠 房源线索", callback_data="adminq:listings"), InlineKeyboardButton("🛠 服务工单", callback_data="adminq:services")],
        [InlineKeyboardButton("📊 来源统计", callback_data="adminq:sources"), InlineKeyboardButton("📚 跟进记录", callback_data="adminq:history")],
    ])


def consult_action_keyboard(*, lead_id: int | None, appointment_id: int = 0, user_id: int = 0, listing_id: str = "") -> InlineKeyboardMarkup:
    lid = int(lead_id or 0)
    aid = int(appointment_id or 0)
    uid = int(user_id or 0)
    suffix = f"{lid}:{aid}:{uid}"
    rows = [
        [InlineKeyboardButton("✅ 我来跟进", callback_data=f"adminlead:claim:{suffix}")],
        [InlineKeyboardButton("📞 已联系", callback_data=f"adminlead:contacted:{suffix}")],
        [InlineKeyboardButton("✅ 完成", callback_data=f"adminlead:done:{suffix}")],
    ]
    if lid:
        rows.append([InlineKeyboardButton("🏠 查看房源", callback_data=f"adminq:view:{lid}")])
    return InlineKeyboardMarkup(rows)


def _lead_source_type(lead: dict) -> str:
    return str(lead.get("first_source_type") or lead.get("source_type") or "other")


def format_lead_card(lead: dict) -> str:
    user_id = int(lead.get("user_id") or 0)
    attr = get_user_attribution(user_id) or {}
    first_type = str(lead.get("first_source_type") or attr.get("first_source_type") or lead.get("source_type") or "other")
    latest_type = str(lead.get("source_type") or attr.get("latest_source_type") or first_type or "other")
    action = str(lead.get("entry_action") or attr.get("latest_entry_action") or lead.get("action") or "")
    deep = str(lead.get("deep_link_payload") or attr.get("latest_deep_link") or "")
    listing_id = str(lead.get("listing_id") or attr.get("latest_listing_id") or "")
    username = str(lead.get("username") or "").strip()
    display = str(lead.get("display_name") or "客户").strip()
    who = f"{he(display)}" + (f" @{he(username)}" if username else "")
    lines = [
        f"🔔 <b>房源咨询 #{int(lead.get('id') or 0)}</b>",
        "",
        f"客户：{who}",
        f"来源：{he(admin_source_group_zh(first_type))}",
    ]
    if listing_id:
        lines.append(f"房源：{he(_display_listing_id(listing_id))}")
    lines.extend([
        f"状态：{he(lead_status_zh(lead.get('lead_status')))}",
        "",
        f"首次进入：{he(source_type_zh(first_type))}",
        f"本次动作：{he(entry_action_zh(action))}",
    ])
    if deep:
        lines.append(f"入口：<code>{he(deep)}</code>")
    if latest_type and latest_type != first_type:
        lines.append(f"最近来源：{he(source_type_zh(latest_type))}")
    return "\n".join(lines)


def format_consult_notify(*, user_id: int, title: str, lines: list[str], current_action: str = "") -> tuple[str, list[str]]:
    """给现有 _notify_admins 使用的中文归因行；不改变发送机制。"""
    attr = get_user_attribution(int(user_id or 0)) or {}
    first_type = str(attr.get("first_source_type") or "other")
    latest_type = str(attr.get("latest_source_type") or first_type or "other")
    action = str(current_action or attr.get("latest_entry_action") or "")
    deep = str(attr.get("latest_deep_link") or attr.get("first_deep_link") or "")
    cleaned = [line for line in lines if not re.match(r"^\s*入口[：｜]", str(line or ""))]
    cleaned.extend([
        "",
        f"来源｜{he(admin_source_group_zh(first_type))}",
        f"首次进入｜{he(source_type_zh(first_type))}",
        f"本次动作｜{he(entry_action_zh(action))}",
    ])
    if deep:
        cleaned.append(f"入口｜<code>{he(deep)}</code>")
    if latest_type != first_type:
        cleaned.append(f"最近来源｜{he(source_type_zh(latest_type))}")
    return title, cleaned


def _extract_user_id(lines: list[str]) -> int:
    text = "\n".join(str(x or "") for x in lines)
    for pattern in (r"tg://user\?id=(\d+)", r"用户ID[：｜]\s*(\d+)", r"客户ID[：｜]\s*(\d+)"):
        m = re.search(pattern, text)
        if m:
            return int(m.group(1))
    return 0


def enrich_admin_notification(title: str, lines: list[str]) -> tuple[str, list[str]]:
    uid = _extract_user_id(lines)
    if uid:
        return format_consult_notify(user_id=uid, title=title, lines=lines)
    return title, lines


def _lead_list_keyboard(rows: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for lead in rows[:12]:
        lid = int(lead.get("id") or 0)
        name = str(lead.get("display_name") or lead.get("username") or "客户")[:10]
        source = admin_source_group_zh(_lead_source_type(lead))
        buttons.append([InlineKeyboardButton(f"{lead_status_zh(lead.get('lead_status'))}｜{name}｜{source}"[:58], callback_data=f"adminq:lead:{lid}")])
    buttons.append([InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")])
    return InlineKeyboardMarkup(buttons)


async def cmd_admin_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return
    await update.effective_message.reply_text(
        "🧭 <b>侨联咨询后台</b>\n\n"
        "来源、房源和跟进状态都用中文显示。\n"
        "首次来源保留不覆盖，后续动作单独记录。",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_home_keyboard(),
    )


async def handle_admin_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or not _is_admin(user.id):
        return
    await answer_callback_once(query)
    data = str(query.data or "")
    action = data.split(":", 1)[1] if ":" in data else "home"

    if action == "home":
        await query.edit_message_text("🧭 <b>侨联咨询后台</b>\n\n请选择要查看的内容。", parse_mode=ParseMode.HTML, reply_markup=admin_home_keyboard())
        return
    if action == "new":
        rows = list_leads_by_status("new", 20)
        await query.edit_message_text(f"🆕 <b>新咨询</b>\n\n当前 {len(rows)} 条。", parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return
    if action == "history":
        rows = list_leads_by_status("all", 20)
        await query.edit_message_text("📚 <b>最近跟进记录</b>\n\n最近 20 条线索。", parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return
    if action == "listings":
        rows = list_listing_leads(20)
        await query.edit_message_text("🏠 <b>房源线索</b>\n\n最近带具体房源的咨询。", parse_mode=ParseMode.HTML, reply_markup=_lead_list_keyboard(rows))
        return
    if action == "appointments":
        today = datetime.now().strftime("%Y-%m-%d")
        rows = list_today_appointments(today, 20)
        text = ["📅 <b>今日预约</b>", ""]
        if not rows:
            text.append("今天暂时没有预约。")
        for row in rows[:12]:
            text.append(f"• #{int(row.get('id') or 0)}｜{he(_display_listing_id(str(row.get('listing_id') or '-')))}｜{he(str(row.get('appointment_time') or '待定'))}｜{he(str(row.get('status') or 'pending'))}")
        await query.edit_message_text("\n".join(text), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]]))
        return
    if action == "services":
        rows = list_service_tickets(20)
        text = ["🛠 <b>服务工单</b>", ""]
        if not rows:
            text.append("暂时没有服务工单。")
        for row in rows[:12]:
            text.append(f"• #{int(row.get('id') or 0)}｜{he(str(row.get('issue_type') or '服务'))}｜{he(str(row.get('status') or 'new'))}")
        await query.edit_message_text("\n".join(text), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]]))
        return
    if action == "sources":
        rows = source_stats(20)
        text = ["📊 <b>来源统计</b>", ""]
        if not rows:
            text.append("暂无来源数据。")
        for row in rows:
            text.append(f"• {he(admin_source_group_zh(row.get('src')))}：<b>{int(row.get('total') or 0)}</b>")
        await query.edit_message_text("\n".join(text), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回咨询后台", callback_data="adminq:home")]]))
        return
    if action.startswith("lead:") or action.startswith("view:"):
        raw = action.split(":", 1)[1]
        if not raw.isdigit():
            return
        lead = db.get_lead(int(raw))
        if not lead:
            await answer_callback_once(query, "这条线索不存在", show_alert=True)
            return
        appointment_id = 0
        uid = int(lead.get("user_id") or 0)
        listing_id = str(lead.get("listing_id") or "")
        with db.connect() as conn:
            row = conn.execute("SELECT id FROM appointments WHERE user_id=? AND (?='' OR listing_id=?) ORDER BY id DESC LIMIT 1", (uid, listing_id, listing_id)).fetchone()
            if row:
                appointment_id = int(row[0])
        await query.edit_message_text(format_lead_card(lead), parse_mode=ParseMode.HTML, reply_markup=consult_action_keyboard(lead_id=int(raw), appointment_id=appointment_id, user_id=uid, listing_id=listing_id))
        return


async def handle_admin_done(update: Update, context: ContextTypes.DEFAULT_TYPE, query, *, lead_id: int, appointment_id: int, customer_id: int) -> bool:
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return False
    from .session_deeplink import user_display_name
    ok = update_lead_status(lead_id, "done", advisor_id=str(user.id), advisor_name=user_display_name(user))
    if appointment_id > 0:
        db.update_appointment_status(appointment_id, "done")
    if not ok:
        await answer_callback_once(query, "线索不存在或已失效", show_alert=True)
        return True
    lead = db.get_lead(lead_id) or {}
    await query.edit_message_text(format_lead_card(lead) + "\n\n<b>当前状态｜✅ 已完成</b>", parse_mode=ParseMode.HTML)
    await answer_callback_once(query, "已完成")
    return True
