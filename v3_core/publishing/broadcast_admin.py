"""Telegram-facing V3 Publisher broadcast controller.

The normal administrator view intentionally exposes only today's broadcast,
preview/send, automatic enable/disable, schedule selection, and home.  Existing
broadcast templates/settings remain available underneath for compatibility.
"""
from __future__ import annotations

import asyncio
from html import escape
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .broadcast import BUTTON_LABELS, BroadcastService


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
        return InlineKeyboardButton("📣 每日广播", callback_data="v3bc")

    def _footer_markup(self) -> InlineKeyboardMarkup | None:
        rows = self.service.footer_rows()
        if not rows:
            return None
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton(button.label, url=button.url) for button in row] for row in rows]
        )

    def _center_keyboard(self) -> InlineKeyboardMarkup:
        enabled = self.service.config().enabled
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("查看今日广播", callback_data="v3bc|today")],
                [InlineKeyboardButton("立即预览", callback_data="v3bc|preview")],
                [InlineKeyboardButton("发布今日广播", callback_data="v3bc|send")],
                [
                    InlineKeyboardButton(
                        "暂停自动广播" if enabled else "开启自动广播",
                        callback_data="v3bc|off" if enabled else "v3bc|on",
                    )
                ],
                [InlineKeyboardButton("选择广播时间", callback_data="v3bc|time_menu")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")],
            ]
        )

    @staticmethod
    def _time_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("09:30", callback_data="v3bc|time|0930"),
                    InlineKeyboardButton("12:30", callback_data="v3bc|time|1230"),
                    InlineKeyboardButton("18:30", callback_data="v3bc|time|1830"),
                ],
                [InlineKeyboardButton("其他时间", callback_data="v3bc|time|custom")],
                [InlineKeyboardButton("⬅️ 返回每日广播", callback_data="v3bc")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")],
            ]
        )

    # Hidden compatibility keyboards.  Existing settings stay usable from old
    # callbacks, but are intentionally not linked from the normal admin UI.
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
                [InlineKeyboardButton("⬅️ 返回每日广播", callback_data="v3bc")],
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
                [InlineKeyboardButton("⬅️ 返回每日广播", callback_data="v3bc")],
            ]
        )

    async def show_center(self, message: Any, *, notice: str = "") -> None:
        config = self.service.config()
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        await message.reply_text(
            prefix
            + "<b>📣 每日广播</b>\n\n"
            f"自动广播：<b>{'已开启' if config.enabled else '已暂停'}</b>\n"
            f"广播时间：<b>{escape(config.send_time)}</b>（{escape(self.timezone_name)}）\n\n"
            "每日广播和房源自动发布相互独立。",
            parse_mode=ParseMode.HTML,
            reply_markup=self._center_keyboard(),
        )

    async def show_daily(self, message: Any, *, notice: str = "") -> None:
        await self.show_center(message, notice=notice)

    async def _render_today(self, message: Any, *, title: str) -> None:
        body = await asyncio.to_thread(self.service.body)
        await message.reply_text(
            f"<b>{escape(title)}</b>\n\n" + body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._footer_markup(),
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
            # Ambiguous Telegram results consume today's claim and never loop-send.
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
            await self.show_center(update.effective_message, notice="广播文案已保存")
            return True
        if kind == "time":
            try:
                normalized = self.service.set_time(text)
            except ValueError:
                await update.effective_message.reply_text("时间格式不正确，请发送例如：09:00。")
                return True
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await self.show_center(update.effective_message, notice=f"广播时间已保存：{normalized}")
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

        if action in {"center", "open"}:
            await self.show_center(query.message)
            return True
        if action == "today":
            await self._render_today(query.message, title="今日广播")
            return True
        if action == "preview":
            await self._render_today(query.message, title="广播预览")
            return True
        if action == "send":
            body = await asyncio.to_thread(self.service.body)
            sent = await self._send_channel(context, body, trigger_type="manual")
            await query.message.reply_text(
                f"✅ 今日广播已发布。频道消息：{escape(str(getattr(sent, 'message_id', '')))}",
                parse_mode=ParseMode.HTML,
                reply_markup=self._center_keyboard(),
            )
            return True
        if action == "time_menu":
            await query.message.reply_text("选择广播时间：", reply_markup=self._time_keyboard())
            return True
        if action == "time" and len(parts) == 3:
            choice = parts[2]
            if choice == "custom":
                context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "time"}
                await query.message.reply_text("请直接发送时间，例如：09:00。")
                return True
            if len(choice) == 4 and choice.isdigit():
                self.service.set_time(f"{choice[:2]}:{choice[2:]}")
                await self.show_center(query.message, notice="广播时间已更新")
                return True
            raise ValueError("broadcast_invalid_time_callback")
        if action == "on":
            self.service.set_enabled(True)
            await self.show_center(query.message, notice="自动广播已开启")
            return True
        if action == "off":
            self.service.set_enabled(False)
            await self.show_center(query.message, notice="自动广播已暂停")
            return True

        # Compatibility-only actions retained for previously issued admin buttons.
        if action == "tpl" and len(parts) == 3:
            self.service.set_template(parts[2])
            await self.show_center(query.message, notice="广播内容已更新")
            return True
        if action == "custom":
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "custom"}
            await query.message.reply_text("请直接发送新的广播文案。发送 /cancel 可取消。")
            return True
        if action == "fx":
            config = self.service.config()
            await query.message.reply_text(
                f"当前汇率偏移：<b>{config.fx_offset:+.2f}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=self._fx_keyboard(),
            )
            return True
        if action == "fxset" and len(parts) == 3:
            self.service.set_fx_offset(int(parts[2]) / 100.0)
            await self.show_center(query.message, notice="汇率设置已更新")
            return True
        if action == "buttons":
            await query.message.reply_text("选择广播底部按钮：", reply_markup=self._button_keyboard())
            return True
        if action == "btn" and len(parts) == 3:
            self.service.set_button(parts[2])
            await self.show_center(query.message, notice=BUTTON_LABELS[parts[2]])
            return True
        return True


__all__ = ["BROADCAST_EDIT_STATE_KEY", "BroadcastAdminController"]
