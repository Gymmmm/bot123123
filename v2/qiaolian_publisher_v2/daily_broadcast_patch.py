from __future__ import annotations

"""Production daily-broadcast contract for the unified Publisher Bot.

The legacy autopilot helper still contains historical weekly/custom templates for
backward compatibility.  Production installs this patch at process start so the
only reachable daily-broadcast product is the locked live card:

- date
- Phnom Penh live weather
- USD/CNY reference = live market rate - 0.20

No weekly rotation, KHR, news, motivational copy, or custom scheduled body is
reachable from the production Publisher Bot.
"""

import asyncio
import html
import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode


_INSTALLED = False


def install_daily_broadcast_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    import autopilot_publish_bot as ap

    def _locked_daily_keyboard(on: bool) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("👀 预览今天", callback_data="daily:preview")],
            [
                InlineKeyboardButton("⏰ 09:30", callback_data="daily:time:0930"),
                InlineKeyboardButton("⏰ 12:30", callback_data="daily:time:1230"),
            ],
            [
                InlineKeyboardButton("⏰ 18:30", callback_data="daily:time:1830"),
                InlineKeyboardButton("✏️ 其他时间", callback_data="daily:time:custom"),
            ],
            [InlineKeyboardButton(
                "⏸ 暂停每日广播" if on else "▶️ 开启每日广播",
                callback_data="daily:off" if on else "daily:on",
            )],
            [InlineKeyboardButton("⬅️ 返回首页", callback_data="cmd:quick_help")],
        ])

    def _locked_defaults() -> None:
        if not ap._get_setting(ap.KEY_DAILY_TIME, "").strip():
            ap._set_setting(ap.KEY_DAILY_TIME, "09:30")
        # Keep an explicit marker so old persisted weekly/custom settings cannot
        # become active again if an old callback is pressed.
        ap._set_setting(ap.KEY_DAILY_TEMPLATE, "live")
        ap._set_setting(ap.KEY_DAILY_DYNAMIC, "1")

    async def cmd_daily(update, context) -> None:
        if not ap._is_admin(update.effective_user.id):
            return
        _locked_defaults()
        on = ap._get_setting(ap.KEY_DAILY_ON, "0").strip().lower() in {"1", "true", "yes"}
        tm = ap._get_setting(ap.KEY_DAILY_TIME, "09:30").strip() or "09:30"
        await update.effective_message.reply_text(
            "📢 <b>每日广播</b>\n\n"
            f"状态：<b>{'已开启' if on else '未开启'}</b>\n"
            f"发送时间：<b>{html.escape(tm)}</b>（{html.escape(ap.TZ_NAME)}）\n\n"
            "固定内容：<b>日期 + 金边实时天气 + USD/CNY</b>\n"
            "汇率展示规则：<b>实时 USD/CNY − 0.20</b>。\n\n"
            "不会发送 KHR、新闻、周历内容或其他自动文案。",
            parse_mode=ParseMode.HTML,
            reply_markup=_locked_daily_keyboard(on),
        )

    async def on_daily_callback(update, context) -> None:
        q = update.callback_query
        if q is None:
            return
        await q.answer()
        if not ap._is_admin(update.effective_user.id):
            return
        _locked_defaults()
        data = str(q.data or "")
        on = ap._get_setting(ap.KEY_DAILY_ON, "0").strip().lower() in {"1", "true", "yes"}

        if data == "daily:preview":
            body = await asyncio.to_thread(ap._fetch_phnom_penh_daily_info)
            await q.message.reply_text(
                "<b>👀 频道发送预览</b>\n\n" + body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return

        if data.startswith("daily:time:"):
            choice = data.rsplit(":", 1)[1]
            if choice == "custom":
                context.user_data["await"] = "daily_time"
                await q.message.reply_text("请直接发送发送时间，例如：09:00。仅保存时间，不会立即发送。")
                return
            if re.fullmatch(r"\d{4}", choice):
                hhmm = f"{choice[:2]}:{choice[2:]}"
                if ap._parse_hhmm(hhmm) is not None:
                    ap._set_setting(ap.KEY_DAILY_TIME, hhmm)
                    await q.edit_message_text(
                        f"✅ <b>发送时间已设为 {hhmm}</b>（{html.escape(ap.TZ_NAME)}）\n\n"
                        "广播内容固定为当天实时天气与 USD/CNY。",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_locked_daily_keyboard(on),
                    )
            return

        if data == "daily:on":
            ap._set_setting(ap.KEY_DAILY_ON, "1")
            await q.edit_message_text(
                "✅ <b>每日广播已开启</b>\n\n"
                f"每天 {html.escape(ap._get_setting(ap.KEY_DAILY_TIME, '09:30'))} 发送当天实时信息。",
                parse_mode=ParseMode.HTML,
                reply_markup=_locked_daily_keyboard(True),
            )
            return

        if data == "daily:off":
            ap._set_setting(ap.KEY_DAILY_ON, "0")
            await q.edit_message_text(
                "⏸ <b>每日广播已暂停</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=_locked_daily_keyboard(False),
            )
            return

        # Historical weekly/template/custom callbacks can survive on old admin
        # messages.  They are accepted but cannot reactivate the old product.
        if data.startswith(("daily:weekly", "daily:plan", "daily:tpl:", "daily:custom")):
            await q.edit_message_text(
                "ℹ️ <b>这个旧广播选项已经停用</b>\n\n"
                "现在每日广播固定发送：日期、金边实时天气和 USD/CNY。",
                parse_mode=ParseMode.HTML,
                reply_markup=_locked_daily_keyboard(on),
            )

    async def scheduled_daily_broadcast(context) -> None:
        if not ap.CHANNEL_ID:
            return
        if ap._get_setting(ap.KEY_DAILY_ON, "0").strip().lower() not in {"1", "true", "yes"}:
            return
        body = await asyncio.to_thread(ap._fetch_phnom_penh_daily_info)
        if not body:
            ap.logger.info("每日广播：实时正文为空，跳过")
            return
        if not ap._direct_publish_enabled():
            ap.logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
            return
        kb = ap.build_channel_menu_keyboard()
        try:
            await context.bot.send_message(
                chat_id=ap.CHANNEL_ID,
                text=body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=kb if kb.inline_keyboard else None,
            )
            ap.logger.info("每日广播已发送：live weather + USD/CNY")
        except Exception:
            ap.logger.exception("每日广播发送失败")

    async def cmd_daily_text(update, context) -> None:
        if not ap._is_admin(update.effective_user.id):
            return
        await update.effective_message.reply_text(
            "每日广播已锁定为实时内容，不能设置自定义自动正文。\n"
            "固定内容：日期、金边天气、USD/CNY。"
        )

    ap._daily_keyboard = _locked_daily_keyboard
    ap._ensure_daily_defaults = _locked_defaults
    ap.cmd_daily = cmd_daily
    ap.on_daily_callback = on_daily_callback
    ap.scheduled_daily_broadcast = scheduled_daily_broadcast
    ap.cmd_daily_text = cmd_daily_text
    _INSTALLED = True
