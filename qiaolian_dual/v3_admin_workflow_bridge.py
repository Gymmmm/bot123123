"""Adapter for existing admin lead/repair workflow callbacks used by V3 User Bot."""
from __future__ import annotations

import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .admin_consult import format_lead_card, handle_admin_done
from .admin_contract import _is_admin_user
from .common import answer_callback_once, db, he
from .messages import advisor_response_notice_text, repair_progress_text
from .results_admin import admin_lead_keyboard
from .session_deeplink import user_display_name


async def handle_v3_admin_workflow(update, context, query, data: str, user):
    if not _is_admin_user(getattr(user, "id", 0)):
        await answer_callback_once(query, "仅顾问可操作", show_alert=True)
        return

    if data.startswith("adminrepair:"):
        parts = data.split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            await answer_callback_once(query, "这条报修已失效", show_alert=True)
            return
        stage = parts[1]
        labels = {"accepted": "已接手", "scheduled": "已安排", "in_progress": "处理中", "done": "已完成", "need_info": "需要客户补充"}
        if stage not in labels:
            return
        ticket_id = int(parts[2])
        ticket = db.update_repair_ticket_status(ticket_id, stage)
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


__all__ = ["handle_v3_admin_workflow"]
