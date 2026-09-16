"""Button-driven contract administration for User Bot advisors."""
from __future__ import annotations

import secrets
from datetime import datetime

from .common import *


def matches(data: str) -> bool:
    return data.startswith("admincontract:")


def admin_contract_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ 录入租约", callback_data="admincontract:new"), InlineKeyboardButton("⏰ 即将到期", callback_data="admincontract:upcoming")],
        [InlineKeyboardButton("🔄 续租跟进", callback_data="admincontract:renewals"), InlineKeyboardButton("🔧 报修工单", callback_data="admincontract:repairs")],
        [InlineKeyboardButton("🏠 返回客户首页", callback_data="home")],
    ])


def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ 取消录入", callback_data="admincontract:cancel")]])


async def cmd_contracts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    from .admin_contract import _is_admin_user
    if not _is_admin_user(getattr(update.effective_user, "id", 0)):
        await update.effective_message.reply_text("❌ 无权限。")
        return MAIN
    context.user_data.pop("admin_contract_draft", None)
    await update.effective_message.reply_text("📄 <b>租客与合同</b>\n\n录入合同后，客户绑定档案即会自动开启到期提醒。", parse_mode=ParseMode.HTML, reply_markup=admin_contract_keyboard())
    return MAIN


async def _ask(message, text: str) -> None:
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=cancel_keyboard())


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, query, data: str, user) -> int | None:
    from .admin_contract import _is_admin_user
    from .session_deeplink import _deep_link, now_ts
    if not _is_admin_user(getattr(user, "id", 0)):
        await answer_callback_once(query, "仅管理员可操作", show_alert=True)
        return MAIN
    action = data.split(":", 1)[1]
    if action in {"home", "cancel"}:
        context.user_data.pop("admin_contract_draft", None)
        await query.edit_message_text("📄 <b>租客与合同</b>\n\n选择要处理的事：", parse_mode=ParseMode.HTML, reply_markup=admin_contract_keyboard())
        return MAIN
    if action == "new":
        context.user_data["admin_contract_draft"] = {"step": "customer"}
        await query.edit_message_text("➕ <b>录入租约 · 1/7</b>\n\n请发送客户的 <b>@Telegram用户名</b> 或 <b>数字ID</b>。\n客户还没有启动机器人时，可以输入 <code>待绑定</code>。", parse_mode=ParseMode.HTML, reply_markup=cancel_keyboard())
        return MAIN
    if action == "upcoming":
        rows = db.list_bindings_expiring_within(45)
        lines = ["⏰ <b>45天内到期</b>"]
        if not rows:
            lines.append("\n\n暂无即将到期的已绑定租约。")
        for row in rows[:20]:
            end = str(row.get("contract_end_date") or row.get("lease_end_date") or "-")
            lines.append(f'\n\n• {he(str(row.get("property_name") or "-"))}\n  到期｜{he(end)} · 客户｜<code>{int(row.get("user_id") or 0)}</code>')
        await query.edit_message_text("".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_contract_keyboard())
        return MAIN
    if action == "renewals":
        rows = db.list_open_renewal_tracking(limit=20)
        lines = ["🔄 <b>续租跟进</b>"]
        if not rows:
            lines.append("\n\n当前没有待跟进的续租意向。")
        for row in rows:
            lines.append(f'\n\n• {he(str(row.get("property_name") or row.get("listing_id") or "-"))}\n  客户｜<code>{int(row.get("user_id") or 0)}</code> · 状态｜{he(str(row.get("renewal_status") or "pending"))}')
        await query.edit_message_text("".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_contract_keyboard())
        return MAIN
    if action == "repairs":
        rows = db.list_open_repair_tickets(limit=20)
        lines = ["🔧 <b>报修工单</b>"]
        if not rows:
            lines.append("\n\n当前没有待处理工单。")
        status_map = {"new": "待接手", "accepted": "已接手", "scheduled": "已安排", "in_progress": "处理中", "need_info": "待补充"}
        for row in rows:
            status = str(row.get("status") or "new")
            lines.append(f'\n\n• <code>WX{int(row.get("id") or 0):05d}</code> · {he(str(row.get("issue_type") or "报修"))}\n  {he(status_map.get(status, status))} · 客户 <code>{int(row.get("user_id") or 0)}</code>')
        await query.edit_message_text("".join(lines), parse_mode=ParseMode.HTML, reply_markup=admin_contract_keyboard())
        return MAIN
    if action == "confirm":
        draft = context.user_data.get("admin_contract_draft") or {}
        if draft.get("step") != "confirm":
            await answer_callback_once(query, "这份录入已失效，请重新开始", show_alert=True)
            return MAIN
        code = f'QL{datetime.now().strftime("%y%m%d")}{secrets.token_hex(3).upper()}'
        target_user_id = int(draft.get("user_id") or 0)
        binding_id = db.create_binding(user_id=target_user_id, binding_code=code, property_name=str(draft["property_name"]), lease_end_date=str(draft["end_date"]), rent_day=int(draft["rent_day"]), created_at=now_ts(), status="pending", monthly_rent=float(draft["monthly_rent"]), contract_start_date=str(draft["start_date"]), contract_end_date=str(draft["end_date"]), deposit_months=int(draft["deposit_months"]), contract_notes=f'客户参考：{draft.get("customer_ref", "待绑定")}')
        deep_link = _deep_link(f"t_bind_{code}")
        pushed = False
        if target_user_id > 0:
            try:
                await context.bot.send_message(chat_id=target_user_id, text=f'🏠 <b>您的租客档案已准备好</b>\n\n房源｜{he(str(draft["property_name"]))}\n到期｜{he(str(draft["end_date"]))}\n\n点下方完成绑定，即可查看租约、接收到期提醒和提交报修。', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 绑定租客档案", url=deep_link)]]))
                pushed = True
            except Exception:
                logger.exception("租约已录入，但绑定链接推送失败: binding_id=%s", binding_id)
        context.user_data.pop("admin_contract_draft", None)
        await query.edit_message_text(f'✅ <b>租约已录入</b>\n\n房源｜{he(str(draft["property_name"]))}\n月租｜${int(float(draft["monthly_rent"]))}\n交租日｜每月 {int(draft["rent_day"])} 号\n到期｜{he(str(draft["end_date"]))}\n档案｜<code>CT{binding_id:05d}</code>\n\n客户通知｜{("已发送" if pushed else "请把下方绑定入口发给客户")}', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 客户绑定入口", url=deep_link)], [InlineKeyboardButton("📄 返回租客与合同", callback_data="admincontract:home")]]))
        return MAIN
    return MAIN


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    from .admin_contract import _is_admin_user
    if not _is_admin_user(getattr(update.effective_user, "id", 0)):
        return None
    draft = context.user_data.get("admin_contract_draft")
    if not isinstance(draft, dict):
        return None
    text = str(update.effective_message.text or "").strip()
    step = draft.get("step")
    if step == "customer":
        if len(text) < 2:
            await _ask(update.effective_message, "请输入 @用户名、数字ID，或“待绑定”。")
            return MAIN
        user_row = db.find_user_by_reference(text)
        draft.update(customer_ref=text[:100], user_id=int((user_row or {}).get("user_id") or (int(text) if text.isdigit() else 0)), step="property")
        await _ask(update.effective_message, "➕ <b>录入租约 · 2/7</b>\n\n请发送<b>项目名 + 房号</b>。\n例如：<code>富力城 B2-2904</code>")
        return MAIN
    if step == "property":
        if len(text) < 2:
            await _ask(update.effective_message, "请填写项目名或房号。")
            return MAIN
        draft.update(property_name=text[:160], step="rent")
        await _ask(update.effective_message, "➕ <b>录入租约 · 3/7</b>\n\n请发送<b>月租数字</b>。\n例如：<code>850</code>")
        return MAIN
    if step == "rent":
        try:
            value = float(text.replace("$", "").replace(",", ""))
            assert value > 0
        except (ValueError, AssertionError):
            await _ask(update.effective_message, "月租请只填数字，例如 <code>850</code>。")
            return MAIN
        draft.update(monthly_rent=value, step="deposit")
        await _ask(update.effective_message, "➕ <b>录入租约 · 4/7</b>\n\n押金是几个月？\n例如：<code>2</code>")
        return MAIN
    if step == "deposit":
        if not text.isdigit() or not 0 <= int(text) <= 12:
            await _ask(update.effective_message, "请填 0–12 之间的数字。")
            return MAIN
        draft.update(deposit_months=int(text), step="rent_day")
        await _ask(update.effective_message, "➕ <b>录入租约 · 5/7</b>\n\n每月几号交租？\n例如：<code>5</code>")
        return MAIN
    if step == "rent_day":
        if not text.isdigit() or not 1 <= int(text) <= 31:
            await _ask(update.effective_message, "交租日请填 1–31。")
            return MAIN
        draft.update(rent_day=int(text), step="start_date")
        await _ask(update.effective_message, "➕ <b>录入租约 · 6/7</b>\n\n合同从哪天开始？\n格式：<code>2026-09-01</code>")
        return MAIN
    if step in {"start_date", "end_date"}:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            await _ask(update.effective_message, "日期格式不对，请按 <code>2026-09-01</code> 填写。")
            return MAIN
        if step == "start_date":
            draft.update(start_date=text, step="end_date")
            await _ask(update.effective_message, "➕ <b>录入租约 · 7/7</b>\n\n合同哪天到期？\n格式：<code>2027-08-31</code>")
            return MAIN
        start = datetime.strptime(str(draft["start_date"]), "%Y-%m-%d").date()
        if parsed <= start:
            await _ask(update.effective_message, "到期日必须晚于开始日，请重新填写。")
            return MAIN
        draft.update(end_date=text, step="confirm")
        await update.effective_message.reply_text(f'📋 <b>请确认租约</b>\n\n客户｜{he(str(draft.get("customer_ref") or "待绑定"))}\n房源｜{he(str(draft["property_name"]))}\n月租｜${int(float(draft["monthly_rent"]))}\n押金｜{int(draft["deposit_months"])} 个月\n交租日｜每月 {int(draft["rent_day"])} 号\n租期｜{he(str(draft["start_date"]))} 至 {he(text)}', parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ 确认录入", callback_data="admincontract:confirm")], [InlineKeyboardButton("❌ 取消", callback_data="admincontract:cancel")]]))
        return MAIN
    return MAIN
