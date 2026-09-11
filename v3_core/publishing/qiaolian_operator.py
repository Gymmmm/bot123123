"""Publisher operator UI additions for the controlled ``侨联说`` feature."""
from __future__ import annotations

import asyncio
from html import escape
import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.inventory.qiaolian_say import qiaolian_notes_from_facts

from .autopilot import ERROR_LABELS
from .operator_flow import (
    BLOCKER_LABELS,
    DISPLAY_FIELDS,
    FIELD_CODE_BY_NAME,
    OPTIONAL_FIELDS,
    OperatorPublisherAdminController,
)
from .simple_admin import NEW_LISTING_STATE_KEY, SIMPLE_EDIT_STATE_KEY


class QiaolianOperatorPublisherAdminController(OperatorPublisherAdminController):
    """Keep parser, operator supplement and public ``侨联说`` on one fact set."""

    @staticmethod
    def _qiaolian_notes(detail: Any) -> list[str]:
        facts = dict(detail.canonical.get("facts") or {})
        seed = str(detail.listing.get("public_listing_id") or facts.get("canonical_facts_hash") or "")
        return qiaolian_notes_from_facts(facts, seed=seed, limit=2, allow_fallback=True)

    async def show_manual_confirmation(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        try:
            blockers, _media = await self._manual_blockers(detail)
        except Exception:
            blockers = ["unreadable_media"]
        optional_missing = [name for name in OPTIONAL_FIELDS if values.get(name) in (None, "")]
        notes = self._qiaolian_notes(detail)

        lines = ["<b>🏠 房源资料已整理</b>", ""]
        for name, label in DISPLAY_FIELDS:
            lines.append(f"{label}：{escape(self._display_value(name, values.get(name)))}")
        lines.extend(["", "<b>💬 侨联说</b>"])
        if notes:
            lines.extend(escape(note) for note in notes)
        else:
            lines.append("暂不显示")
        lines.extend(
            [
                "",
                f"📷 图片：{len(state.get('images') or [])} 张",
                f"🎬 视频：{len(state.get('videos') or [])} 条",
            ]
        )
        if blockers:
            required = "、".join(BLOCKER_LABELS.get(code, ERROR_LABELS.get(code, code)) for code in blockers)
            lines.extend(["", f"🔴 发布前需要补充：{escape(required)}"])
        elif optional_missing:
            labels = {name: label.split(" ", 1)[-1] for name, label in DISPLAY_FIELDS}
            lines.extend(
                [
                    "",
                    "✅ 当前资料已经可以发布",
                    "💡 可选补充：" + "、".join(labels.get(name, name) for name in optional_missing),
                ]
            )
        else:
            lines.extend(["", "✅ 房源资料已满足发布条件。"])

        buttons: list[list[InlineKeyboardButton]] = []
        if blockers or optional_missing:
            buttons.append(
                [
                    InlineKeyboardButton("➕ 补充资料", callback_data="v3smp|manual_supplement"),
                    InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit"),
                ]
            )
        else:
            buttons.append([InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit")])
        buttons.append([InlineKeyboardButton("💬 调整侨联说", callback_data="v3smp|manual_qiaolian")])
        if not blockers:
            buttons.append([InlineKeyboardButton("👀 生成预览", callback_data="v3smp|manual_preview")])
        buttons.append([InlineKeyboardButton("❌ 取消", callback_data="v3smp|manual_cancel")])
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def show_supplement(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        missing = [name for name in OPTIONAL_FIELDS if values.get(name) in (None, "")]
        try:
            blockers, _media = await self._manual_blockers(detail)
        except Exception:
            blockers = ["unreadable_media"]
        lines = ["<b>➕ 补充资料</b>", ""]
        if blockers:
            lines.append(
                "发布前还需要："
                + "、".join(BLOCKER_LABELS.get(code, ERROR_LABELS.get(code, code)) for code in blockers)
            )
        if missing:
            label_map = {name: label.split(" ", 1)[-1] for name, label in DISPLAY_FIELDS}
            lines.append("可选补充：" + "、".join(label_map.get(name, name) for name in missing))
        lines.extend(
            [
                "",
                "可以直接补一句事实，例如：",
                "<code>面积120㎡，18楼，押2付1，物业费含，宽带已装，每周保洁2次</code>",
                "",
                "系统会重新扫描整段资料，并同步更新「侨联说」。",
                "只写确认过的事实；侨联说最多展示2句。",
                "",
                "也可以继续发送文字、单图、多图相册或单条视频。",
            ]
        )
        state["mode"] = "supplement"
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("💬 手动写侨联说", callback_data="v3smp|manual_qiaolian")],
                    [InlineKeyboardButton("✅ 不再补充", callback_data="v3smp|manual_back_confirm")],
                    [InlineKeyboardButton("❌ 取消发布", callback_data="v3smp|manual_cancel")],
                ]
            ),
        )

    async def show_manual_edit_menu(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        rows: list[list[InlineKeyboardButton]] = []
        current: list[InlineKeyboardButton] = []
        for name, label in DISPLAY_FIELDS:
            code = FIELD_CODE_BY_NAME.get(name)
            if not code:
                continue
            short_label = label.split(" ", 1)[-1]
            value = self._display_value(name, values.get(name))
            current.append(
                InlineKeyboardButton(
                    f"{short_label}：{value}"[:28],
                    callback_data=f"v3smp|manual_field|{code}",
                )
            )
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([InlineKeyboardButton("💬 侨联说", callback_data="v3smp|manual_qiaolian")])
        rows.append([InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")])
        await message.reply_text(
            "<b>✏️ 修改资料</b>\n\n选择要修改的项目，然后直接发送新值。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _set_qiaolian_copy(self, update: Any, context: Any, raw: str) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            return

        normalized = raw.strip()
        lowered = normalized.lower().replace(" ", "")
        block = ["侨联说重置"]
        if lowered in {"自动", "恢复自动", "恢复自动生成", "清除", "reset", "auto"}:
            message = "✅ 已恢复自动生成侨联说，正在重新整理。"
        elif lowered in {"关闭", "不显示", "不要", "无", "off"}:
            block.append("侨联说关闭")
            message = "✅ 这套房将不显示侨联说，正在重新整理。"
        else:
            candidates = [
                re.sub(r"\s+", " ", item).strip(" ｜|；;")
                for item in re.split(r"\n+|\s*\|\s*|\s*｜\s*", normalized)
            ]
            notes: list[str] = []
            for item in candidates:
                if item and item not in notes:
                    notes.append(item[:120])
                if len(notes) == 2:
                    break
            if not notes:
                await update.effective_message.reply_text("请发送1-2句侨联说，或发送“恢复自动”。")
                return
            block.extend(f"侨联说：{item}" for item in notes)
            message = "✅ 侨联说已更新，正在重新整理。"

        existing = str(state.get("text") or "").rstrip()
        addition = "\n".join(block)
        state["text"] = addition if not existing else existing + "\n" + addition
        context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
        await update.effective_message.reply_text(message)
        await self._materialize_manual(
            update.effective_message,
            context,
            user_id=int(update.effective_user.id),
        )

    async def handle_text(self, update: Any, context: Any) -> bool:
        edit = context.user_data.get(SIMPLE_EDIT_STATE_KEY)
        if isinstance(edit, dict) and str(edit.get("kind") or "") == "qiaolian_say":
            raw = str(update.effective_message.text or "").strip()
            if not raw:
                await update.effective_message.reply_text("请发送1-2句侨联说，或发送“恢复自动”。")
                return True
            await self._set_qiaolian_copy(update, context, raw)
            return True
        return await super().handle_text(update, context)

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = update.callback_query
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        if raw == "v3smp|manual_qiaolian":
            context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "qiaolian_say"}
            await query.message.reply_text(
                "<b>💬 调整侨联说</b>\n\n"
                "发送1-2句，换行分开。\n"
                "这里会覆盖自动文案，但不会修改房源事实。\n\n"
                "例如：\n"
                "<code>一周两次保洁，日常维护挺省心。\n物业费和网络费都已经包含。</code>\n\n"
                "发送“恢复自动”可重新按扫描事实生成；发送“不显示”可关闭这套房的侨联说。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")]]
                ),
            )
            return True
        return await super().handle_callback(update, context)


__all__ = ["QiaolianOperatorPublisherAdminController"]
