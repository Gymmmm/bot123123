"""Telegram-facing V3 Publisher broadcast controller.

The controller owns only admin UI, Telegram sends, and JobQueue execution.
Configuration, live-card generation and schedule claiming live in broadcast.py.
"""
from __future__ import annotations

import asyncio
from html import escape
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .broadcast import BUTTON_LABELS, TEMPLATES, BroadcastService


BROADCAST_EDIT_STATE_KEY = "v3_publisher_broadcast_edit"


class BroadcastAdminController:
    def __init__(
        self,
        *,
        service: BroadcastService,
        channel_chat_id: str,
        timezone_name: str,
    ):
        self.service = service
        self.channel_chat_id = str(channel_chat_id or "").strip()
        self.timezone_name = str(timezone_name or "Asia/Phnom_Penh").strip()
        if not self.channel_chat_id:
            raise ValueError("broadcast_channel_chat_id_required")
        self.service.ensure_defaults()

    @staticmethod
    def dashboard_button() -> InlineKeyboardButton:
        return InlineKeyboardButton("📢 广播中心", callback_data="v3bc")

    def _footer_markup(self) -> InlineKeyboardMarkup | None:
        rows = self.service.footer_rows()
        if not rows:
            return None
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(button.label, url=button.url) for button in row]
                for row in rows
            ]
        )

    @staticmethod
    def _center_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🌤 每日天气汇率", callback_data="v3bc|tpl|live")],
                [
                    InlineKeyboardButton("📅 每周找房", callback_data="v3bc|tpl|weekly"),
                    InlineKeyboardButton("🏠 周末看房", callback_data="v3bc|tpl|weekend"),
                ],
                [
                    InlineKeyboardButton("📋 看房准备", callback_data="v3bc|tpl|viewing"),
                    InlineKeyboardButton("📝 签约提醒", callback_data="v3bc|tpl|contract"),
                ],
                [InlineKeyboardButton("✏️ 自定义文案", callback_data="v3bc|custom")],
                [InlineKeyboardButton("⬅️ 返回发布后台", callback_data="v3h")],
            ]
        )

    @staticmethod
    def _daily_keyboard(enabled: bool) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📋 选择模板", callback_data="v3bc"),
                    InlineKeyboardButton("✏️ 编辑文案", callback_data="v3bc|custom"),
                ],
                [
                    InlineKeyboardButton("💱 调整汇率", callback_data="v3bc|fx"),
                    InlineKeyboardButton("🔘 底部按钮", callback_data="v3bc|buttons"),
                ],
                [
                    InlineKeyboardButton("👀 预览", callback_data="v3bc|preview"),
                    InlineKeyboardButton("📤 立即发送", callback_data="v3bc|send"),
                ],
                [
                    InlineKeyboardButton("⏰ 09:30", callback_data="v3bc|time|0930"),
                    InlineKeyboardButton("⏰ 12:30", callback_data="v3bc|time|1230"),
                    InlineKeyboardButton("⏰ 18:30", callback_data="v3bc|time|1830"),
                ],
                [InlineKeyboardButton("✏️ 其他时间", callback_data="v3bc|time|custom")],
                [
                    InlineKeyboardButton(
                        "⏸ 暂停定时" if enabled else "▶️ 开启定时",
                        callback_data="v3bc|off" if enabled else "v3bc|on",
                    )
                ],
                [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
            ]
        )

    @staticmethod
    def _fx_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("-0.30", callback_data="v3bc|fxset|-30"),
                    InlineKeyboardButton("-0.20", callback_data="v3bc|fxset|-20"),
                    InlineKeyboardButton("-0.10", callback_data="v3bc|fxset|-10"),
                ],
                [
                    InlineKeyboardButton("0.00", callback_data="v3bc|fxset|0"),
                    InlineKeyboardButton("+0.10", callback_data="v3bc|fxset|10"),
                    InlineKeyboardButton("+0.20", callback_data="v3bc|fxset|20"),
                ],
                [InlineKeyboardButton("⬅️ 返回广播页面", callback_data="v3bc|open")],
            ]
        )

    @staticmethod
    def _button_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("不带按钮", callback_data="v3bc|btn|none")],
                [
                    InlineKeyboardButton("🔍 租房找房", callback_data="v3bc|btn|find"),
                    InlineKeyboardButton("🏠 最新房源", callback_data="v3bc|btn|latest"),
                ],
                [
                    InlineKeyboardButton("💬 租房置业咨询", callback_data="v3bc|btn|contact"),
                    InlineKeyboardButton("组合按钮", callback_data="v3bc|btn|combo"),
                ],
                [InlineKeyboardButton("⬅️ 返回广播页面", callback_data="v3bc|open")],
            ]
        )

    async def show_center(self, message: Any) -> None:
        await message.reply_text(
            "📢 <b>广播中心</b>\n\n"
            "每日天气汇率使用实时数据；其他模板可直接预览、手动发送，或设为每日定时内容。\n"
            "定时默认关闭，只有管理员明确开启后才会自动发送。",
            parse_mode=ParseMode.HTML,
            reply_markup=self._center_keyboard(),
        )

    async def show_daily(self, message: Any, *, notice: str = "") -> None:
        config = self.service.config()
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        await message.reply_text(
            prefix
            + "📢 <b>广播页面</b>\n\n"
            f"当前模板：<b>{escape(self.service.template_title(config.template_key))}</b>\n"
            f"汇率偏移：<b>{config.fx_offset:+.2f}</b>\n"
            f"定时：<b>{'已开启' if config.enabled else '未开启'}</b> · "
            f"{escape(config.send_time)}（{escape(self.timezone_name)}）\n"
            f"底部按钮：<b>{escape(BUTTON_LABELS.get(config.button_key, '不带按钮'))}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=self._daily_keyboard(config.enabled),
        )

    async def _send_channel(self, context: Any, body: str, *, trigger_type: str) -> Any:
        config = self.service.config()
        local_date = self.service.local_now().date().isoformat()
        try:
            result = await context.bot.send_message(
                chat_id=self.channel_chat_id,
                text=body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._footer_markup(),
            )
        except Exception as exc:
            self.service.repository.log_delivery(
                trigger_type=trigger_type,
                template_key=config.template_key,
                channel_chat_id=self.channel_chat_id,
                local_date=local_date,
                status="failed_or_unknown",
                error_text=f"{type(exc).__name__}: {exc}",
            )
            raise
        self.service.repository.log_delivery(
            trigger_type=trigger_type,
            template_key=config.template_key,
            channel_chat_id=self.channel_chat_id,
            local_date=local_date,
            status="sent",
            channel_message_id=getattr(result, "message_id", None),
        )
        return result

    async def scheduled_tick(self, context: Any) -> None:
        claimed, local_date = self.service.claim_scheduled_due()
        if not claimed:
            return
        try:
            body = await asyncio.to_thread(self.service.body)
            await self._send_channel(context, body, trigger_type="scheduled")
        except Exception:
            # The claim intentionally remains consumed for today.  An ambiguous
            # Telegram failure must never trigger an automatic duplicate loop.
            return
        self.service.mark_scheduled_sent(local_date)

    async def handle_text(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(BROADCAST_EDIT_STATE_KEY)
        if not isinstance(state, dict):
            return False
        kind = str(state.get("kind") or "")
        text = str(getattr(update.effective_message, "text", "") or "").strip()
        if kind == "custom":
            self.service.set_custom_html(text)
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text("✅ 广播文案已保存，不会立即发送。")
            await self.show_daily(update.effective_message)
            return True
        if kind == "time":
            try:
                normalized = self.service.set_time(text)
            except ValueError:
                await update.effective_message.reply_text("时间格式不正确，请发送例如：09:00。")
                return True
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text(f"✅ 定时时间已保存：{escape(normalized)}")
            await self.show_daily(update.effective_message)
            return True
        context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
        return False

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        if query is None:
            return False
        raw = str(query.data or "")
        if not (raw == "v3bc" or raw.startswith("v3bc|")):
            return False
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else "center"

        if action == "center":
            await self.show_center(query.message)
            return True
        if action == "open":
            await self.show_daily(query.message)
            return True
        if action == "tpl" and len(parts) == 3:
            self.service.set_template(parts[2])
            await self.show_daily(query.message, notice=f"已选择 {self.service.template_title(parts[2])}")
            return True
        if action == "custom":
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "custom"}
            await query.message.reply_text("请直接发送新的广播文案。保存后不会立即发送；发送 /cancel 可取消。")
            return True
        if action == "fx":
            config = self.service.config()
            await query.message.reply_text(
                "💱 <b>汇率偏移</b>\n\n"
                f"当前：<b>{config.fx_offset:+.2f}</b>\n\n"
                "例如 -0.20 = 实时市场 USD/CNY 减 0.20。",
                parse_mode=ParseMode.HTML,
                reply_markup=self._fx_keyboard(),
            )
            return True
        if action == "fxset" and len(parts) == 3:
            try:
                value = int(parts[2]) / 100.0
            except ValueError:
                raise ValueError("broadcast_invalid_fx_callback")
            self.service.set_fx_offset(value)
            await self.show_daily(query.message)
            return True
        if action == "buttons":
            await query.message.reply_text(
                "🔘 <b>选择帖子底部按钮</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=self._button_keyboard(),
            )
            return True
        if action == "btn" and len(parts) == 3:
            self.service.set_button(parts[2])
            await self.show_daily(query.message, notice=BUTTON_LABELS[parts[2]])
            return True
        if action == "preview":
            body = await asyncio.to_thread(self.service.body)
            await query.message.reply_text(
                "<b>👀 频道发送预览</b>\n\n" + body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._footer_markup(),
            )
            return True
        if action == "send":
            body = await asyncio.to_thread(self.service.body)
            sent = await self._send_channel(context, body, trigger_type="manual")
            await query.message.reply_text(
                "✅ <b>广播已发送</b>\n"
                f"message: <code>{escape(str(getattr(sent, 'message_id', '')))}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=self._daily_keyboard(self.service.config().enabled),
            )
            return True
        if action == "time" and len(parts) == 3:
            choice = parts[2]
            if choice == "custom":
                context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "time"}
                await query.message.reply_text("请直接发送时间，例如：09:00。仅保存时间，不会立即发送。")
                return True
            if len(choice) == 4 and choice.isdigit():
                self.service.set_time(f"{choice[:2]}:{choice[2:]}")
                await self.show_daily(query.message)
                return True
            raise ValueError("broadcast_invalid_time_callback")
        if action == "on":
            self.service.set_enabled(True)
            await self.show_daily(query.message, notice="定时广播已开启")
            return True
        if action == "off":
            self.service.set_enabled(False)
            await self.show_daily(query.message, notice="定时广播已暂停")
            return True
        return True


__all__ = ["BROADCAST_EDIT_STATE_KEY", "BroadcastAdminController"]
