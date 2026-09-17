"""Boss-facing publication center UI layered over the existing broadcast domain."""
from __future__ import annotations

from html import escape
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .broadcast import BUTTON_LABELS
from .broadcast_admin import BROADCAST_EDIT_STATE_KEY, BroadcastAdminController
from .marketing_broadcast import TEMPLATES


class PublisherCenterAdminController(BroadcastAdminController):
    """Keep broadcast capabilities intact while presenting them as a publication center."""

    @staticmethod
    def dashboard_button() -> InlineKeyboardButton:
        return InlineKeyboardButton("📢 发布中心", callback_data="v3bc")

    def _center_keyboard(self):
        return self._markup(
            [
                [
                    InlineKeyboardButton("🌤 每日天气汇率", callback_data="v3bc|weather"),
                    InlineKeyboardButton("🗓 本周营销计划", callback_data="v3bc|marketing"),
                ],
                [
                    InlineKeyboardButton("✏️ 临时发布", callback_data="v3bc|custom"),
                    InlineKeyboardButton("📚 发布记录", callback_data="v3bc|logs"),
                ],
                [InlineKeyboardButton("⚙️ 发布设置", callback_data="v3bc|settings")],
                [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")],
            ]
        )

    async def show_center(self, message: Any, *, notice: str = ""):
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        await message.reply_text(
            prefix
            + "<b>📢 发布中心</b>\n\n"
            + "管理频道日常内容和自动发布。",
            parse_mode=ParseMode.HTML,
            reply_markup=self._center_keyboard(),
        )

    async def show_weather(self, message: Any, *, notice: str = ""):
        c = self.service.config()
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        today_state = self._today_delivery_state("live")
        await message.reply_text(
            prefix
            + "<b>🌤 每日天气汇率</b>\n\n"
            + f"自动发布：{'🟢 已开启' if c.enabled else '⏸ 已暂停'}\n"
            + f"发送时间：{escape(c.send_time)}\n"
            + f"今日状态：{escape(today_state)}",
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup(
                [
                    [
                        InlineKeyboardButton("👀 查看今日内容", callback_data="v3bc|today"),
                        InlineKeyboardButton("✏️ 修改模板", callback_data="v3bc|daily_copy"),
                    ],
                    [
                        InlineKeyboardButton("🔘 按钮设置", callback_data="v3bc|daily_button"),
                        InlineKeyboardButton("💱 汇率设置", callback_data="v3bc|fx"),
                    ],
                    [
                        InlineKeyboardButton("📤 立即发送", callback_data="v3bc|send"),
                        InlineKeyboardButton("⏰ 修改时间", callback_data="v3bc|time_menu"),
                    ],
                    [
                        InlineKeyboardButton(
                            "⏸ 暂停自动发布" if c.enabled else "▶️ 开启自动发布",
                            callback_data="v3bc|off" if c.enabled else "v3bc|on",
                        )
                    ],
                    [InlineKeyboardButton("⬅️ 返回发布中心", callback_data="v3bc")],
                ]
            ),
        )

    async def show_marketing(self, message: Any, *, notice: str = ""):
        prefix = f"✅ {escape(notice)}\n\n" if notice else ""
        today = self.marketing.template()
        today_state = self._today_delivery_state("marketing_" + today.key)
        lines = [
            "<b>🗓 本周营销计划</b>",
            "",
            "周一  🏠 本周找房",
            "周二  📋 看房准备",
            "周三  💰 租房预算",
            "周四  🔍 房源怎么选",
            "周五  📝 签约提醒",
            "周六  🏠 周末看房",
            "周日  🛡 侨联保障",
            "",
            f"自动营销：{'🟢 已开启' if self.marketing.enabled else '⏸ 已暂停'}",
            f"发送时间：{escape(self.marketing.send_time)}",
            f"今日状态：{escape(today_state)}",
        ]
        await message.reply_text(
            prefix + "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup(
                [
                    [
                        InlineKeyboardButton("👀 查看今天", callback_data="v3bc|m_today"),
                        InlineKeyboardButton("📅 查看整周", callback_data="v3bc|m_week"),
                    ],
                    [
                        InlineKeyboardButton("✏️ 编辑模板", callback_data="v3bc|m_templates"),
                        InlineKeyboardButton("🔘 按钮设置", callback_data="v3bc|m_buttons"),
                    ],
                    [
                        InlineKeyboardButton("📤 立即发送", callback_data="v3bc|m_send"),
                        InlineKeyboardButton("⏰ 修改时间", callback_data="v3bc|m_time_menu"),
                    ],
                    [
                        InlineKeyboardButton(
                            "⏸ 暂停营销" if self.marketing.enabled else "▶️ 开启营销",
                            callback_data="v3bc|m_off" if self.marketing.enabled else "v3bc|m_on",
                        )
                    ],
                    [InlineKeyboardButton("⬅️ 返回发布中心", callback_data="v3bc")],
                ]
            ),
        )

    async def show_logs(self, message: Any):
        with self.service.repository._connect() as conn:
            rows = conn.execute(
                "SELECT trigger_type,template_key,status,local_date,created_at "
                "FROM publisher_broadcast_log_v3 ORDER BY id DESC LIMIT 12"
            ).fetchall()
        lines = ["<b>📚 发布记录</b>", ""]
        if not rows:
            lines.append("暂无频道发布记录。")
        for row in rows:
            icon = "✅" if str(row["status"]) == "sent" else "⚠️"
            key = str(row["template_key"])
            kind = "🌤 天气汇率" if key == "live" else ("🗓 营销" if key.startswith("marketing_") else "✏️ 临时发布")
            tm = self._local_log_time(row)
            when = f"{escape(str(row['local_date']))} {escape(tm)}" if tm else escape(str(row["local_date"]))
            lines.append(f"{icon} {when} · {kind}")
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=self._markup([[InlineKeyboardButton("⬅️ 返回发布中心", callback_data="v3bc")]]),
        )

    @staticmethod
    def _weekday_keyboard(action: str, *, back: str) -> InlineKeyboardMarkup:
        rows = []
        names = "一二三四五六日"
        for start in (0, 2, 4):
            rows.append(
                [
                    InlineKeyboardButton(f"周{names[idx]}", callback_data=f"v3bc|{action}|{idx}")
                    for idx in range(start, min(start + 2, 7))
                ]
            )
        rows.append([InlineKeyboardButton("周日", callback_data=f"v3bc|{action}|6")])
        rows.append([InlineKeyboardButton("⬅️ 返回本周计划", callback_data=back)])
        return InlineKeyboardMarkup(rows)

    async def handle_text(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(BROADCAST_EDIT_STATE_KEY)
        if isinstance(state, dict) and str(state.get("kind") or "") == "custom":
            text = str(getattr(update.effective_message, "text", "") or "").strip()
            context.user_data.pop(BROADCAST_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text(
                "<b>✏️ 临时发布预览</b>\n\n" + escape(text),
                parse_mode=ParseMode.HTML,
                reply_markup=self._markup(
                    [
                        [InlineKeyboardButton("📤 立即发送", callback_data="v3bc|custom_send")],
                        [InlineKeyboardButton("🔘 按钮设置", callback_data="v3bc|custom_button")],
                        [InlineKeyboardButton("✏️ 重新编辑", callback_data="v3bc|custom")],
                        [InlineKeyboardButton("⬅️ 返回发布中心", callback_data="v3bc")],
                    ]
                ),
            )
            context.user_data[BROADCAST_EDIT_STATE_KEY] = {
                "kind": "custom_ready",
                "text": text,
                "button_key": self.service.config().button_key,
            }
            return True
        return await super().handle_text(update, context)

    async def handle_callback(self, update: Any, context: Any) -> bool:
        q = update.callback_query
        raw = str(getattr(q, "data", "") or "") if q is not None else ""
        if raw == "v3bc|m_templates":
            await q.message.reply_text(
                "<b>✏️ 编辑营销模板</b>\n\n请选择要修改的日期：",
                parse_mode=ParseMode.HTML,
                reply_markup=self._weekday_keyboard("m_edit", back="v3bc|marketing"),
            )
            return True
        if raw == "v3bc|m_buttons":
            await q.message.reply_text(
                "<b>🔘 营销按钮设置</b>\n\n请选择要修改的日期：",
                parse_mode=ParseMode.HTML,
                reply_markup=self._weekday_keyboard("m_button", back="v3bc|marketing"),
            )
            return True
        return await super().handle_callback(update, context)


__all__ = ["PublisherCenterAdminController"]
