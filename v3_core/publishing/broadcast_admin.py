"""Practical Telegram broadcast center: daily service + weekly marketing."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .broadcast import BUTTON_LABELS, BroadcastService
from .marketing_broadcast import MarketingBroadcastService, TEMPLATES

BROADCAST_EDIT_STATE_KEY = "v3_publisher_broadcast_edit"


class BroadcastAdminController:
    def __init__(
        self,
        *,
        service: BroadcastService,
        channel_chat_id: str,
        timezone_name: str,
        marketing: MarketingBroadcastService | None = None,
    ):
        self.service = service
        self.marketing = marketing or MarketingBroadcastService(
            service.repository.db_path,
            user_bot_username=service.user_bot_username,
            timezone_name=timezone_name,
        )
        self.channel_chat_id = str(channel_chat_id or "").strip()
        self.timezone_name = str(timezone_name or "Asia/Phnom_Penh").strip()
        if not self.channel_chat_id:
            raise ValueError("broadcast_channel_chat_id_required")
        self.service.ensure_defaults()

    @staticmethod
    def dashboard_button() -> InlineKeyboardButton:
        return InlineKeyboardButton("📢 广播中心", callback_data="v3bc")

    @staticmethod
    def _markup(rows):
        return InlineKeyboardMarkup(rows)

    def _footer_markup(self, rows=None):
        source = rows if rows is not None else self.service.footer_rows()
        if not source:
            return None
        return InlineKeyboardMarkup([[InlineKeyboardButton(b.label, url=b.url) for b in row] for row in source])

    def _center_keyboard(self):
        return self._markup([
            [InlineKeyboardButton("🌤 每日天气汇率", callback_data="v3bc|weather"), InlineKeyboardButton("🗓 本周营销计划", callback_data="v3bc|marketing")],
            [InlineKeyboardButton("✏️ 临时广播", callback_data="v3bc|custom"), InlineKeyboardButton("📚 广播记录", callback_data="v3bc|logs")],
            [InlineKeyboardButton("⚙️ 广播设置", callback_data="v3bc|settings")],
            [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")],
        ])

    def _last_sent_row(self, template_key: str):
        local_date = self.service.local_now().date().isoformat()
        with self.service.repository._connect() as conn:
            return conn.execute(
                """SELECT id,template_key,trigger_type,local_date,created_at
                   FROM publisher_broadcast_log_v3
                   WHERE local_date=? AND template_key=? AND status='sent'
                   ORDER BY id DESC LIMIT 1""",
                (local_date, str(template_key)),
            ).fetchone()

    def _local_log_time(self, row: Any) -> str:
        if row is None:
            return ""
        raw = str(row["created_at"] or "").strip()
        if not raw:
            return ""
        try:
            value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(ZoneInfo(self.timezone_name)).strftime("%H:%M")
        except (TypeError, ValueError, ZoneInfo.KeyError):
            return raw[11:16] if len(raw) >= 16 else raw

    async def show_center(self, message: Any, *, notice: str = ""):
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        await message.reply_text(
            prefix + "<b>📢 广播中心</b>\n\n🌤 每日天气汇率：固定日常服务\n🗓 本周营销计划：周一至周日循环\n✏️ 临时广播：需要时单独发送",
            parse_mode=ParseMode.HTML,
            reply_markup=self._center_keyboard(),
        )

    async def show_daily(self, message: Any, *, notice: str = ""):
        await self.show_center(message, notice=notice)

    async def show_weather(self, message: Any, *, notice: str = ""):
        c = self.service.config()
        sent = self._last_sent_row("live")
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        today_state = f"✅ 已发送 · {self._local_log_time(sent)}" if sent is not None else "⏳ 待发送"
        await message.reply_text(
            prefix + "<b>🌤 每日天气汇率</b>\n\n"
            + f"状态：{'🟢 每日自动发送' if c.enabled else '⏸ 已暂停'}\n"
            + f"时间：{escape(c.send_time)}\n"
            + f"今日状态：{escape(today_state)}",
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup([
                [InlineKeyboardButton("👀 查看今日内容", callback_data="v3bc|today"), InlineKeyboardButton("💱 汇率设置", callback_data="v3bc|fx")],
                [InlineKeyboardButton("📤 立即发送", callback_data="v3bc|send"), InlineKeyboardButton("⏰ 修改时间", callback_data="v3bc|time_menu")],
                [InlineKeyboardButton("⏸ 暂停" if c.enabled else "▶️ 开启", callback_data="v3bc|off" if c.enabled else "v3bc|on")],
                [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
            ]),
        )

    async def show_marketing(self, message: Any, *, notice: str = ""):
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        today = self.marketing.template()
        sent = self._last_sent_row("marketing_" + today.key)
        lines = [
            "<b>🗓 本周营销计划</b>", "",
            "周一  🏠 本周找房", "周二  📋 看房准备", "周三  💰 租房预算",
            "周四  🔍 房源怎么选", "周五  📝 签约提醒", "周六  🏠 周末看房", "周日  🛡 侨联保障", "",
            f"自动营销：{'🟢 已开启' if self.marketing.enabled else '⏸ 已暂停'}",
            f"发送时间：{escape(self.marketing.send_time)}",
            f"今日状态：{'✅ 已发送 · ' + self._local_log_time(sent) if sent is not None else '⏳ 待发送'}",
        ]
        await message.reply_text(
            prefix + "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup([
                [InlineKeyboardButton("👀 查看今天", callback_data="v3bc|m_today"), InlineKeyboardButton("📅 查看整周", callback_data="v3bc|m_week")],
                [InlineKeyboardButton("📤 立即发送", callback_data="v3bc|m_send"), InlineKeyboardButton("⏰ 修改时间", callback_data="v3bc|m_time_menu")],
                [InlineKeyboardButton("⏸ 暂停营销" if self.marketing.enabled else "▶️ 开启营销", callback_data="v3bc|m_off" if self.marketing.enabled else "v3bc|m_on")],
                [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
            ]),
        )

    async def render_marketing(self, message: Any, weekday: int | None = None):
        """Preview exactly the same body and customer buttons the channel receives."""
        t = self.marketing.template(weekday)
        await message.reply_text(
            t.body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._footer_markup(self.marketing.footer_rows(t)),
        )

    async def _render_today(self, message: Any, *, title: str = ""):
        """Preview exactly the same body and customer buttons the channel receives."""
        body = await asyncio.to_thread(self.service.body, "live")
        await message.reply_text(
            body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self._footer_markup(),
        )

    async def _send_channel(self, context: Any, body: str, *, trigger_type: str, template_key: str = "live", footer=None):
        local_date = self.service.local_now().date().isoformat()
        try:
            result = await context.bot.send_message(
                chat_id=self.channel_chat_id,
                text=body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self._footer_markup(footer),
            )
        except Exception as exc:
            self.service.repository.log_delivery(
                trigger_type=trigger_type,
                template_key=template_key,
                channel_chat_id=self.channel_chat_id,
                local_date=local_date,
                status="failed_or_unknown",
                error_text=f"{type(exc).__name__}: {exc}",
            )
            raise
        self.service.repository.log_delivery(
            trigger_type=trigger_type,
            template_key=template_key,
            channel_chat_id=self.channel_chat_id,
            local_date=local_date,
            status="sent",
            channel_message_id=getattr(result, "message_id", None),
        )
        return result

    async def _send_weather_now(self, context: Any):
        body = await asyncio.to_thread(self.service.body, "live")
        return await self._send_channel(context, body, trigger_type="manual_weather", template_key="live")

    async def _send_marketing_now(self, context: Any):
        t = self.marketing.template()
        return await self._send_channel(
            context,
            t.body,
            trigger_type="manual_marketing",
            template_key="marketing_" + t.key,
            footer=self.marketing.footer_rows(t),
        )

    async def scheduled_tick(self, context: Any):
        claimed, local_date = self.service.claim_scheduled_due()
        if claimed:
            try:
                body = await asyncio.to_thread(self.service.body, "live")
                await self._send_channel(context, body, trigger_type="scheduled", template_key="live")
                self.service.mark_scheduled_sent(local_date)
            except Exception:
                pass
        m_claimed, m_date = self.marketing.claim_due()
        if m_claimed:
            t = self.marketing.template()
            try:
                await self._send_channel(
                    context,
                    t.body,
                    trigger_type="scheduled_marketing",
                    template_key="marketing_" + t.key,
                    footer=self.marketing.footer_rows(t),
                )
                self.marketing.mark_sent(m_date)
            except Exception:
                pass

    async def show_logs(self, message: Any):
        with self.service.repository._connect() as conn:
            rows = conn.execute(
                "SELECT trigger_type,template_key,status,local_date,created_at FROM publisher_broadcast_log_v3 ORDER BY id DESC LIMIT 12"
            ).fetchall()
        lines = ["<b>📚 广播记录</b>", ""]
        if not rows:
            lines.append("暂无广播记录。")
        for row in rows:
            icon = "✅" if str(row["status"]) == "sent" else "⚠️"
            kind = "🌤 天气汇率" if str(row["template_key"]) == "live" else ("🗓 营销" if str(row["template_key"]).startswith("marketing_") else "✏️ 临时广播")
            tm = self._local_log_time(row)
            when = f"{escape(str(row['local_date']))} {escape(tm)}" if tm else escape(str(row["local_date"]))
            lines.append(f"{icon} {when} · {kind}")
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup([[InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")]]),
        )

    @staticmethod
    def _time_keyboard(prefix="time"):
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("09:30", callback_data=f"v3bc|{prefix}|0930"), InlineKeyboardButton("12:30", callback_data=f"v3bc|{prefix}|1230"), InlineKeyboardButton("18:30", callback_data=f"v3bc|{prefix}|1830")],
            [InlineKeyboardButton("✏️ 其他时间", callback_data=f"v3bc|{prefix}|custom")],
            [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
        ])

    async def handle_text(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(BROADCAST_EDIT_STATE_KEY)
        if not isinstance(state, dict):
            return False
        kind = str(state.get("kind") or "")
        text = str(getattr(update.effective_message, "text", "") or "").strip()
        if kind == "custom":
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text(
                "<b>✏️ 临时广播预览</b>\n\n" + escape(text),
                parse_mode=ParseMode.HTML,
                reply_markup=self._markup([
                    [InlineKeyboardButton("📤 立即发送", callback_data="v3bc|custom_send")],
                    [InlineKeyboardButton("✏️ 重新编辑", callback_data="v3bc|custom")],
                    [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
                ]),
            )
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "custom_ready", "text": text}
            return True
        if kind == "fx":
            try:
                value = float(text.replace("+", ""))
            except ValueError:
                await update.effective_message.reply_text("格式不正确，请发送例如：+0.02、-0.05 或 0。")
                return True
            self.service.set_fx_offset(value)
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await self.show_weather(update.effective_message, notice=f"汇率人工调整已设为 {value:+.2f} CNY")
            return True
        if kind in {"time", "m_time"}:
            try:
                normalized = self.service.set_time(text) if kind == "time" else self.marketing.set_time(text)
            except ValueError:
                await update.effective_message.reply_text("时间格式不正确，请发送例如：18:30。")
                return True
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            if kind == "time":
                await self.show_weather(update.effective_message, notice=f"时间已更新：{normalized}")
            else:
                await self.show_marketing(update.effective_message, notice=f"营销时间已更新：{normalized}")
            return True
        return False

    async def handle_callback(self, update: Any, context: Any) -> bool:
        q = update.callback_query
        if q is None:
            return False
        raw = str(q.data or "")
        if not (raw == "v3bc" or raw.startswith("v3bc|")):
            return False
        p = raw.split("|")
        a = p[1] if len(p) > 1 else "center"
        if a in {"center", "open"}:
            await self.show_center(q.message); return True
        if a == "weather":
            await self.show_weather(q.message); return True
        if a in {"today", "preview"}:
            await self._render_today(q.message); return True
        if a == "fx":
            c = self.service.config()
            sign = "+" if c.fx_offset >= 0 else ""
            await q.message.reply_text(
                "<b>💱 汇率设置</b>\n\n"
                "🇺🇸 美元 / 🇨🇳 人民币\n\n"
                f"人工调整：{sign}{c.fx_offset:.2f} CNY\n"
                "广播汇率 = 自动实时汇率 + 人工调整",
                parse_mode=ParseMode.HTML,
                reply_markup=self._markup([
                    [InlineKeyboardButton("-0.10", callback_data="v3bc|fx_add|-0.10"), InlineKeyboardButton("-0.01", callback_data="v3bc|fx_add|-0.01"), InlineKeyboardButton("+0.01", callback_data="v3bc|fx_add|0.01"), InlineKeyboardButton("+0.10", callback_data="v3bc|fx_add|0.10")],
                    [InlineKeyboardButton("✏️ 直接输入", callback_data="v3bc|fx_input"), InlineKeyboardButton("↩️ 恢复自动汇率", callback_data="v3bc|fx_reset")],
                    [InlineKeyboardButton("⬅️ 返回天气汇率", callback_data="v3bc|weather")],
                ]),
            ); return True
        if a == "fx_add" and len(p) == 3:
            current = self.service.config().fx_offset
            self.service.set_fx_offset(current + float(p[2]))
            q.data = "v3bc|fx"
            return await self.handle_callback(update, context)
        if a == "fx_reset":
            self.service.set_fx_offset(0.0)
            q.data = "v3bc|fx"
            return await self.handle_callback(update, context)
        if a == "fx_input":
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "fx"}
            await q.message.reply_text("请输入人工调整值，例如：+0.02、-0.05 或 0。")
            return True
        if a == "send":
            sent = self._last_sent_row("live")
            if sent is not None:
                await q.message.reply_text(
                    "⚠️ <b>今日天气汇率已经发送过。</b>\n\n"
                    f"上次发送：{escape(self._local_log_time(sent) or '今天')}\n\n"
                    "是否仍然再发送一次？",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self._markup([
                        [InlineKeyboardButton("仍然发送一次", callback_data="v3bc|send_force")],
                        [InlineKeyboardButton("取消", callback_data="v3bc|weather")],
                    ]),
                )
                return True
            await self._send_weather_now(context)
            await self.show_weather(q.message, notice="今日天气汇率已发送"); return True
        if a == "send_force":
            await self._send_weather_now(context)
            await self.show_weather(q.message, notice="今日天气汇率已再次发送"); return True
        if a == "marketing":
            await self.show_marketing(q.message); return True
        if a == "m_today":
            await self.render_marketing(q.message); return True
        if a == "m_week":
            await q.message.reply_text(
                "<b>📅 查看整周</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=self._markup([
                    [InlineKeyboardButton("周一", callback_data="v3bc|m_day|0"), InlineKeyboardButton("周二", callback_data="v3bc|m_day|1")],
                    [InlineKeyboardButton("周三", callback_data="v3bc|m_day|2"), InlineKeyboardButton("周四", callback_data="v3bc|m_day|3")],
                    [InlineKeyboardButton("周五", callback_data="v3bc|m_day|4"), InlineKeyboardButton("周六", callback_data="v3bc|m_day|5")],
                    [InlineKeyboardButton("周日", callback_data="v3bc|m_day|6")],
                    [InlineKeyboardButton("⬅️ 返回本周计划", callback_data="v3bc|marketing")],
                ]),
            ); return True
        if a == "m_day" and len(p) == 3:
            await self.render_marketing(q.message, int(p[2])); return True
        if a == "m_send":
            t = self.marketing.template()
            sent = self._last_sent_row("marketing_" + t.key)
            if sent is not None:
                await q.message.reply_text(
                    "⚠️ <b>今天的营销广播已经发送过。</b>\n\n"
                    f"上次发送：{escape(self._local_log_time(sent) or '今天')}\n\n"
                    "是否仍然再发送一次？",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self._markup([
                        [InlineKeyboardButton("仍然发送一次", callback_data="v3bc|m_send_force")],
                        [InlineKeyboardButton("取消", callback_data="v3bc|marketing")],
                    ]),
                )
                return True
            await self._send_marketing_now(context)
            await self.show_marketing(q.message, notice="今日营销广播已发送"); return True
        if a == "m_send_force":
            await self._send_marketing_now(context)
            await self.show_marketing(q.message, notice="今日营销广播已再次发送"); return True
        if a == "m_on":
            self.marketing.set_enabled(True); await self.show_marketing(q.message, notice="自动营销已开启"); return True
        if a == "m_off":
            self.marketing.set_enabled(False); await self.show_marketing(q.message, notice="自动营销已暂停"); return True
        if a == "m_time_menu":
            await q.message.reply_text("选择营销发送时间：", reply_markup=self._time_keyboard("m_time")); return True
        if a == "m_time" and len(p) == 3:
            if p[2] == "custom":
                context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "m_time"}
                await q.message.reply_text("请发送时间，例如：18:30。"); return True
            self.marketing.set_time(f"{p[2][:2]}:{p[2][2:]}")
            await self.show_marketing(q.message, notice="营销时间已更新"); return True
        if a == "logs":
            await self.show_logs(q.message); return True
        if a == "settings":
            await q.message.reply_text(
                "<b>⚙️ 广播设置</b>\n\n天气汇率和每周营销分别管理时间与启停。",
                parse_mode=ParseMode.HTML,
                reply_markup=self._markup([
                    [InlineKeyboardButton("🌤 天气汇率设置", callback_data="v3bc|weather"), InlineKeyboardButton("🗓 营销设置", callback_data="v3bc|marketing")],
                    [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="v3bc")],
                ]),
            ); return True
        if a == "custom":
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "custom"}
            await q.message.reply_text("<b>✏️ 临时广播</b>\n\n直接发送广播文案，发送后先预览，不会立即发布。", parse_mode=ParseMode.HTML)
            return True
        if a == "custom_send":
            st = context.user_data.get(BROADCAST_EDIT_STATE_KEY) or {}
            text = str(st.get("text") or "").strip()
            if text:
                await self._send_channel(context, escape(text), trigger_type="manual_custom", template_key="custom")
                context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
                await self.show_center(q.message, notice="临时广播已发送")
            return True
        if a == "time_menu":
            await q.message.reply_text("选择天气汇率发送时间：", reply_markup=self._time_keyboard("time")); return True
        if a == "time" and len(p) == 3:
            if p[2] == "custom":
                context.user_data[BROADCAST_EDIT_STATE_KEY] = {"kind": "time"}
                await q.message.reply_text("请发送时间，例如：09:30。"); return True
            self.service.set_time(f"{p[2][:2]}:{p[2][2:]}")
            await self.show_weather(q.message, notice="时间已更新"); return True
        if a == "on":
            self.service.set_enabled(True); await self.show_weather(q.message, notice="每日天气汇率已开启"); return True
        if a == "off":
            self.service.set_enabled(False); await self.show_weather(q.message, notice="每日天气汇率已暂停"); return True
        if a == "tpl" and len(p) == 3:
            self.service.set_template(p[2]); await self.show_center(q.message, notice="旧广播模板已保存"); return True
        if a == "btn" and len(p) == 3:
            self.service.set_button(p[2]); await self.show_center(q.message, notice=BUTTON_LABELS[p[2]]); return True
        return True


__all__ = ["BROADCAST_EDIT_STATE_KEY", "BroadcastAdminController"]
