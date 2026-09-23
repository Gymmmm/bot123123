"""Adapter for existing admin lead/repair workflow callbacks used by V3 User Bot."""
from __future__ import annotations

import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .admin_consult import format_lead_card, handle_admin_done
from .admin_contract import _is_admin_user
from .common import DB_PATH, answer_callback_once, db, he
from .messages import advisor_response_notice_text, repair_progress_text
from .results_admin import admin_lead_keyboard
from .session_deeplink import user_display_name
from .keyboards_search import service_detail_keyboard


def v3_admin_repair_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    ticket = int(ticket_id or 0)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ 已接手", callback_data=f"adminrepairv3:accepted:{ticket}"),
            InlineKeyboardButton("📅 已安排", callback_data=f"adminrepairv3:scheduled:{ticket}"),
        ],
        [
            InlineKeyboardButton("🔧 处理中", callback_data=f"adminrepairv3:in_progress:{ticket}"),
            InlineKeyboardButton("✅ 已完成", callback_data=f"adminrepairv3:done:{ticket}"),
        ],
        [InlineKeyboardButton("💬 需要客户补充", callback_data=f"adminrepairv3:need_info:{ticket}")],
    ])


def _update_v3_repair_ticket(ticket_id: int, stage: str):
    import sqlite3
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM repair_tickets_v3 WHERE id=? LIMIT 1",
                (int(ticket_id),),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE repair_tickets_v3 SET status=? WHERE id=?",
                (str(stage or ""), int(ticket_id)),
            )
            conn.commit()
            updated = conn.execute(
                "SELECT * FROM repair_tickets_v3 WHERE id=? LIMIT 1",
                (int(ticket_id),),
            ).fetchone()
        return dict(updated) if updated is not None else None
    except sqlite3.OperationalError:
        return None


async def cmd_v3_repair_update(update, context):
    """Takeover-compatible repair status command: V3 tickets first, legacy fallback."""
    user = update.effective_user
    if not _is_admin_user(getattr(user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。该命令仅限管理员使用。")
        return
    args = [str(value).strip() for value in (context.args or []) if str(value).strip()]
    if len(args) < 2:
        await update.effective_message.reply_text(
            "用法：\n/repair_update <工单号> <已接手|已安排|处理中|已完成|需补充> [补充说明]"
        )
        return
    try:
        ticket_id = int(args[0])
    except (TypeError, ValueError):
        await update.effective_message.reply_text("❌ 工单号需要是数字。")
        return
    stage_map = {
        "已接手": "accepted", "接手": "accepted", "accepted": "accepted",
        "已安排": "scheduled", "安排": "scheduled", "scheduled": "scheduled",
        "处理中": "in_progress", "处理": "in_progress", "in_progress": "in_progress",
        "已完成": "done", "完成": "done", "done": "done",
        "需补充": "need_info", "补充": "need_info", "need_info": "need_info",
    }
    stage = stage_map.get(args[1].lower())
    if not stage:
        await update.effective_message.reply_text("❌ 进度请使用：已接手、已安排、处理中、已完成或需补充。")
        return
    ticket = _update_v3_repair_ticket(ticket_id, stage)
    if not ticket:
        ticket = db.update_repair_ticket_status(ticket_id, stage)
    if not ticket:
        await update.effective_message.reply_text("⚠️ 未找到工单，或进度无效。")
        return
    note = " ".join(args[2:]).strip()
    try:
        await context.bot.send_message(
            chat_id=int(ticket.get("user_id") or 0),
            text=repair_progress_text(str(ticket.get("issue_type") or "报修事项"), stage, note),
            parse_mode=ParseMode.HTML,
            reply_markup=service_detail_keyboard(),
        )
    except Exception:
        await update.effective_message.reply_text("⚠️ 进度已保存，但通知发送失败。")
        return
    await update.effective_message.reply_text(f"✅ 已通知客户：工单 #{ticket_id} 已更新。")


async def handle_v3_admin_workflow(update, context, query, data: str, user):
    if not _is_admin_user(getattr(user, "id", 0)):
        await answer_callback_once(query, "仅顾问可操作", show_alert=True)
        return

    if data.startswith(("adminrepair:", "adminrepairv3:")):
        is_v3 = data.startswith("adminrepairv3:")
        parts = data.split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            await answer_callback_once(query, "这条报修已失效", show_alert=True)
            return
        stage = parts[1]
        labels = {"accepted": "已接手", "scheduled": "已安排", "in_progress": "处理中", "done": "已完成", "need_info": "需要客户补充"}
        if stage not in labels:
            return
        ticket_id = int(parts[2])
        ticket = (
            _update_v3_repair_ticket(ticket_id, stage)
            if is_v3
            else db.update_repair_ticket_status(ticket_id, stage)
        )
        if not ticket:
            await answer_callback_once(query, "未找到这条报修，可能已处理", show_alert=True)
            return
        customer_id = int(ticket.get("user_id") or 0)
        if customer_id > 0:
            try:
                await context.bot.send_message(
                    chat_id=customer_id,
                    text=repair_progress_text(str(ticket.get("issue_type") or "报修事项"), stage),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛡 入住服务", callback_data="v3u:home:service")]]),
                )
            except Exception:
                pass
        original = str(getattr(query.message, "text_html", "") or getattr(query.message, "text", "") or "")
        await query.edit_message_text(original + f"\n\n<b>处理状态：</b>{he(labels[stage])}", parse_mode=ParseMode.HTML)
        await answer_callback_once(query, f"已通知客户：{labels[stage]}")
        return

    if not data.startswith("adminlead:"):
        return
    parts = data.split(":")
    if len(parts) != 5 or not all(part.isdigit() for part in parts[2:]):
        await answer_callback_once(query, "线索参数已失效", show_alert=True)
        return
    action = parts[1]
    lead_id, appointment_id, customer_id = map(int, parts[2:])
    if action == "done":
        await handle_admin_done(update, context, query, lead_id=lead_id, appointment_id=appointment_id, customer_id=customer_id)
        return

    status_map = {"claim": ("claimed", "assigned", "🟢 顾问跟进中"), "contacted": ("contacted", "contacted", "🟢 顾问跟进中"), "invalid": ("invalid", "cancelled", "⚪ 已结束跟进")}
    if action not in status_map:
        return
    lead_status, appointment_status, label = status_map[action]
    advisor_name = user_display_name(user)
    ok = db.update_lead_workflow(lead_id, status=lead_status, advisor_id=str(user.id), advisor_name=advisor_name)
    if appointment_id > 0:
        db.update_appointment_status(appointment_id, appointment_status)
    if not ok:
        await answer_callback_once(query, "线索不存在或已失效", show_alert=True)
        return

    lead = db.get_lead(lead_id) or {}
    card = format_lead_card(lead)
    status_line = f"<b>当前状态｜{he(label)}</b>\n跟进顾问｜{he(advisor_name)}"
    card = re.sub(r"(?:<b>)?当前状态｜[^\n<]*(?:</b>)?(?:\n跟进顾问｜[^\n<]*)?", status_line, card, count=1) if "当前状态｜" in card else card.rstrip() + "\n\n" + status_line
    await query.edit_message_text(card, parse_mode=ParseMode.HTML, reply_markup=admin_lead_keyboard(lead_id=lead_id, appointment_id=appointment_id, user_id=customer_id) if action == "claim" else None)

    if action in {"claim", "contacted"} and customer_id > 0:
        try:
            await context.bot.send_message(
                chat_id=customer_id,
                text=advisor_response_notice_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📅 我的预约", callback_data="v3u:home:appointments")],
                    [InlineKeyboardButton("🔍 继续找房", callback_data="v3u:home:search")],
                ]),
            )
        except Exception:
            pass
    await answer_callback_once(query, "已更新")


__all__ = [
    "cmd_v3_repair_update",
    "handle_v3_admin_workflow",
    "v3_admin_repair_keyboard",
]
