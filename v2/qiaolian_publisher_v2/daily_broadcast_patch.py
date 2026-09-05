from __future__ import annotations

"""Production broadcast center for the unified Publisher Bot.

Daily weather/FX remains a fixed live template. Admins may only adjust the
USD/CNY display offset, preview it, send it now, or enable its daily schedule.
Other broadcast cards are simple ready-to-send presets.
"""

import asyncio
import html
import re
from types import SimpleNamespace

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode


_INSTALLED = False
_FX_KEY = "daily_broadcast_fx_offset"
_BUTTON_KEY = "daily_broadcast_button"
_BUTTON_LABELS = {
    "none": "不带按钮",
    "find": "帮我找房",
    "latest": "最新房源",
    "contact": "联系我们",
    "combo": "组合按钮",
}


def install_daily_broadcast_patch() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    import autopilot_publish_bot as ap

    original_live_builder = ap._fetch_phnom_penh_daily_info
    original_text_handler = ap.on_text_private

    templates: dict[str, tuple[str, str]] = {
        "weekly": (
            "📅 每周找房提醒",
            "<b>📅 侨联地产｜本周找房提醒</b>\n\n"
            "这周准备找房或换房，可以直接把 <b>区域 + 月预算 + 户型 + 入住时间</b> 发给我们。\n\n"
            "我们按当前在租房源帮你筛，不需要先翻一大堆无关房源。\n\n"
            "<b>💎 侨联地产｜您在金边的自己人</b>",
        ),
        "weekend": (
            "🏠 周末看房",
            "<b>🏠 周末看房安排</b>\n\n"
            "周末准备集中看房的，可以提前把 <b>区域、预算、户型、方便时间</b> 发过来。\n\n"
            "我们先确认房态，再安排实际可看的房源；不方便到现场也可以先约视频看房。\n\n"
            "<b>💎 侨联地产｜您在金边的自己人</b>",
        ),
        "viewing": (
            "📋 看房前准备",
            "<b>📋 看房前，先把这几项定下来</b>\n\n"
            "• 想住的区域\n"
            "• 每月预算\n"
            "• 户型\n"
            "• 预计入住时间\n\n"
            "这几项明确以后，找房和排看房都会快很多。\n\n"
            "<b>💎 侨联地产｜您在金边的自己人</b>",
        ),
        "contract": (
            "📝 签约提醒",
            "<b>📝 签约前再确认一次</b>\n\n"
            "租期、押付方式、水电、物业、网络、停车，以及提前退租和维修责任，最好都在签约前确认清楚。\n\n"
            "入住当天再把房屋现状、家具家电、表计读数、钥匙和门卡做好留档。\n\n"
            "<b>💎 侨联地产｜您在金边的自己人</b>",
        ),
    }

    def _on() -> bool:
        return ap._get_setting(ap.KEY_DAILY_ON, "0").strip().lower() in {"1", "true", "yes"}

    def _fx_offset() -> float:
        try:
            value = float(ap._get_setting(_FX_KEY, "-0.20").strip())
        except (TypeError, ValueError):
            value = -0.20
        return max(-2.0, min(2.0, value))

    def _fx_label() -> str:
        value = _fx_offset()
        return f"{value:+.2f}" if value else "0.00"

    def _live_body() -> str:
        """Use the existing live fetcher and only alter its displayed FX offset."""
        body = original_live_builder()
        desired = _fx_offset()
        # The legacy live builder uses market - 0.20. Convert that displayed
        # number to the admin-selected offset without duplicating the network fetch.
        delta = desired + 0.20
        pattern = re.compile(r"(💵 美元/人民币：1 USD ≈ )([0-9]+(?:\.[0-9]+)?)( CNY)")

        def repl(match: re.Match[str]) -> str:
            try:
                current = float(match.group(2))
                adjusted = max(0.0, current + delta)
                return f"{match.group(1)}{adjusted:.2f}{match.group(3)}"
            except (TypeError, ValueError):
                return match.group(0)

        return pattern.sub(repl, body, count=1)

    def _selected_key() -> str:
        value = ap._get_setting(ap.KEY_DAILY_TEMPLATE, "live").strip().lower()
        return value if value == "live" or value in templates or value == "custom" else "live"

    def _current_body() -> str:
        key = _selected_key()
        if key == "live":
            return _live_body()
        return ap._get_setting(ap.KEY_DAILY_TEXT, "").strip() or templates.get(key, ("", ""))[1]

    def _footer_keyboard() -> InlineKeyboardMarkup | None:
        key = ap._get_setting(_BUTTON_KEY, "none").strip().lower()
        username = str(ap.DEEPLINK_BOT_USERNAME or "").strip().lstrip("@")
        if not username or key == "none":
            return None
        base = f"https://t.me/{username}?start="
        buttons = {
            "find": InlineKeyboardButton("🔍 帮我找房", url=base + "find_home"),
            "latest": InlineKeyboardButton("🏠 查看最新房源", url=base + "latest"),
            "contact": InlineKeyboardButton("💬 联系我们", url=base + "advisor"),
        }
        if key == "combo":
            return InlineKeyboardMarkup([[buttons["find"], buttons["latest"]], [buttons["contact"]]])
        return InlineKeyboardMarkup([[buttons[key]]]) if key in buttons else None

    async def _send_channel(context, body: str) -> bool:
        if not ap.CHANNEL_ID:
            return False
        kb = _footer_keyboard()
        await context.bot.send_message(
            chat_id=ap.CHANNEL_ID,
            text=body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        return True

    def _center_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🌤 每日广播", callback_data="daily:open")],
            [
                InlineKeyboardButton("📅 每周找房", callback_data="daily:tpl:weekly"),
                InlineKeyboardButton("🏠 周末看房", callback_data="daily:tpl:weekend"),
            ],
            [
                InlineKeyboardButton("📋 看房准备", callback_data="daily:tpl:viewing"),
                InlineKeyboardButton("📝 签约提醒", callback_data="daily:tpl:contract"),
            ],
            [InlineKeyboardButton("⬅️ 返回后台", callback_data="cmd:quick_help")],
        ])

    def _daily_keyboard(on: bool) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 选择模板", callback_data="daily:center"), InlineKeyboardButton("✏️ 编辑文案", callback_data="daily:custom")],
            [InlineKeyboardButton("💱 调整汇率", callback_data="daily:fx"), InlineKeyboardButton("🔘 底部按钮", callback_data="daily:buttons")],
            [
                InlineKeyboardButton("👀 预览", callback_data="daily:preview"),
                InlineKeyboardButton("📤 立即发送", callback_data="daily:send"),
            ],
            [
                InlineKeyboardButton("⏰ 09:30", callback_data="daily:time:0930"),
                InlineKeyboardButton("⏰ 12:30", callback_data="daily:time:1230"),
            ],
            [
                InlineKeyboardButton("⏰ 18:30", callback_data="daily:time:1830"),
                InlineKeyboardButton("✏️ 其他时间", callback_data="daily:time:custom"),
            ],
            [InlineKeyboardButton(
                "⏸ 暂停定时" if on else "▶️ 开启定时",
                callback_data="daily:off" if on else "daily:on",
            )],
            [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="daily:center")],
        ])

    def _fx_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("-0.30", callback_data="daily:fxset:-30"),
                InlineKeyboardButton("-0.20", callback_data="daily:fxset:-20"),
                InlineKeyboardButton("-0.10", callback_data="daily:fxset:-10"),
            ],
            [
                InlineKeyboardButton("0.00", callback_data="daily:fxset:0"),
                InlineKeyboardButton("+0.10", callback_data="daily:fxset:10"),
                InlineKeyboardButton("+0.20", callback_data="daily:fxset:20"),
            ],
            [InlineKeyboardButton("⬅️ 返回每日广播", callback_data="daily:open")],
        ])

    def _button_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("不带按钮", callback_data="daily:btn:none")],
            [InlineKeyboardButton("🔍 帮我找房", callback_data="daily:btn:find"), InlineKeyboardButton("🏠 查看最新房源", callback_data="daily:btn:latest")],
            [InlineKeyboardButton("💬 联系我们", callback_data="daily:btn:contact"), InlineKeyboardButton("组合按钮", callback_data="daily:btn:combo")],
            [InlineKeyboardButton("⬅️ 返回广播页面", callback_data="daily:open")],
        ])

    def _template_keyboard(key: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("👀 预览", callback_data=f"daily:tplpreview:{key}"),
                InlineKeyboardButton("📤 立即发送", callback_data=f"daily:tplsend:{key}"),
            ],
            [InlineKeyboardButton("⬅️ 返回广播中心", callback_data="daily:center")],
        ])

    def _locked_defaults() -> None:
        if not ap._get_setting(ap.KEY_DAILY_TIME, "").strip():
            ap._set_setting(ap.KEY_DAILY_TIME, "09:30")
        if not ap._get_setting(_FX_KEY, "").strip():
            ap._set_setting(_FX_KEY, "-0.20")
        if not ap._get_setting(ap.KEY_DAILY_TEMPLATE, "").strip():
            ap._set_setting(ap.KEY_DAILY_TEMPLATE, "live")
        if not ap._get_setting(_BUTTON_KEY, "").strip():
            ap._set_setting(_BUTTON_KEY, "none")

    async def _show_center(target) -> None:
        await target.edit_message_text(
            "📢 <b>广播中心</b>\n\n"
            "每日天气汇率是固定模板；其他模板按需要直接预览或发送。",
            parse_mode=ParseMode.HTML,
            reply_markup=_center_keyboard(),
        )

    async def _show_daily(target, *, notice: str = "") -> None:
        tm = ap._get_setting(ap.KEY_DAILY_TIME, "09:30").strip() or "09:30"
        key = _selected_key()
        title = "每日天气汇率" if key == "live" else (templates.get(key, ("自定义文案", ""))[0] if key != "custom" else "自定义文案")
        await target.edit_message_text(
            (f"✅ 已选择：<b>{html.escape(notice)}</b>\n\n" if notice else "")
            + "📢 <b>广播页面</b>\n\n"
            f"当前模板：<b>{html.escape(title)}</b>\n"
            f"汇率偏移：<b>{html.escape(_fx_label())}</b>\n"
            f"定时：<b>{'已开启' if _on() else '未开启'}</b> · {html.escape(tm)}（{html.escape(ap.TZ_NAME)}）\n\n"
            f"底部按钮：<b>{html.escape(_BUTTON_LABELS.get(ap._get_setting(_BUTTON_KEY, 'none'), '不带按钮'))}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=_daily_keyboard(_on()),
        )

    async def cmd_daily(update, context) -> None:
        if not ap._is_admin(update.effective_user.id):
            return
        _locked_defaults()
        await update.effective_message.reply_text(
            "📢 <b>广播中心</b>\n\n"
            "每日天气汇率是固定模板；其他模板按需要直接预览或发送。",
            parse_mode=ParseMode.HTML,
            reply_markup=_center_keyboard(),
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

        if data == "daily:center":
            await _show_center(q)
            return
        if data == "daily:open":
            await _show_daily(q)
            return
        if data == "daily:fx":
            await q.edit_message_text(
                "💱 <b>汇率偏移</b>\n\n"
                f"当前：<b>{html.escape(_fx_label())}</b>\n\n"
                "例如 -0.20 = 实时市场 USD/CNY 减 0.20。",
                parse_mode=ParseMode.HTML,
                reply_markup=_fx_keyboard(),
            )
            return
        if data == "daily:buttons":
            await q.edit_message_text("🔘 <b>选择帖子底部按钮</b>", parse_mode=ParseMode.HTML, reply_markup=_button_keyboard())
            return
        if data.startswith("daily:btn:"):
            choice = data.rsplit(":", 1)[1]
            if choice in {"none", "find", "latest", "contact", "combo"}:
                ap._set_setting(_BUTTON_KEY, choice)
                await _show_daily(q, notice=_BUTTON_LABELS[choice])
            return
        if data.startswith("daily:fxset:"):
            raw = data.rsplit(":", 1)[1]
            try:
                offset = int(raw) / 100.0
            except ValueError:
                return
            ap._set_setting(_FX_KEY, f"{offset:.2f}")
            await _show_daily(q)
            return

        if data == "daily:preview":
            body = await asyncio.to_thread(_current_body)
            await q.message.reply_text(
                "<b>👀 频道发送预览</b>\n\n" + body,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True, reply_markup=_footer_keyboard(),
            )
            return

        if data == "daily:send":
            body = await asyncio.to_thread(_current_body)
            try:
                if await _send_channel(context, body):
                    await q.edit_message_text(
                        "✅ <b>每日广播已发送</b>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_daily_keyboard(_on()),
                    )
                else:
                    await q.message.reply_text("❌ 未配置频道，无法发送。")
            except Exception:
                ap.logger.exception("每日广播手动发送失败")
                await q.message.reply_text("❌ 发送失败，请稍后重试。")
            return

        if data.startswith("daily:tpl:"):
            key = data.rsplit(":", 1)[1]
            tpl = templates.get(key)
            if not tpl:
                return
            _title, body = tpl
            ap._set_setting(ap.KEY_DAILY_TEMPLATE, key)
            ap._set_setting(ap.KEY_DAILY_TEXT, body)
            await _show_daily(q)
            return

        if data.startswith("daily:tplpreview:"):
            key = data.rsplit(":", 1)[1]
            tpl = templates.get(key)
            if not tpl:
                return
            await q.message.reply_text(
                "<b>👀 频道发送预览</b>\n\n" + tpl[1],
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=_footer_keyboard(),
            )
            return

        if data.startswith("daily:tplsend:"):
            key = data.rsplit(":", 1)[1]
            tpl = templates.get(key)
            if not tpl:
                return
            try:
                if await _send_channel(context, tpl[1]):
                    await q.edit_message_text(
                        f"✅ <b>{html.escape(tpl[0])}已发送</b>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_template_keyboard(key),
                    )
                else:
                    await q.message.reply_text("❌ 未配置频道，无法发送。")
            except Exception:
                ap.logger.exception("广播模板手动发送失败: %s", key)
                await q.message.reply_text("❌ 发送失败，请稍后重试。")
            return

        if data.startswith("daily:time:"):
            choice = data.rsplit(":", 1)[1]
            if choice == "custom":
                context.user_data["await"] = "daily_time"
                await q.message.reply_text("请直接发送时间，例如：09:00。仅保存时间，不会立即发送。")
                return
            if re.fullmatch(r"\d{4}", choice):
                hhmm = f"{choice[:2]}:{choice[2:]}"
                if ap._parse_hhmm(hhmm) is not None:
                    ap._set_setting(ap.KEY_DAILY_TIME, hhmm)
                    await _show_daily(q)
            return

        if data == "daily:custom":
            context.user_data["await"] = "daily_patch_text"
            await q.message.reply_text("请发送新的广播文案。保存后会返回广播页面，不会立即发送。")
            return

        if data == "daily:on":
            ap._set_setting(ap.KEY_DAILY_ON, "1")
            await _show_daily(q)
            return

        if data == "daily:off":
            ap._set_setting(ap.KEY_DAILY_ON, "0")
            await _show_daily(q)
            return

        # Old callbacks can remain on historical admin messages. Keep them safe.
        if data.startswith(("daily:weekly", "daily:plan", "daily:custom")):
            await _show_center(q)

    async def scheduled_daily_broadcast(context) -> None:
        if not ap.CHANNEL_ID:
            return
        if not _on():
            return
        body = await asyncio.to_thread(_current_body)
        if not body:
            ap.logger.info("每日广播：实时正文为空，跳过")
            return
        if not ap._direct_publish_enabled():
            ap.logger.warning("Direct publish via autopilot blocked. Set AUTOPILOT_DIRECT_PUBLISH_ENABLED=yes to enable.")
            return
        try:
            await _send_channel(context, body)
            ap.logger.info("每日广播已发送：live weather + USD/CNY offset=%s", _fx_label())
        except Exception:
            ap.logger.exception("每日广播发送失败")

    async def cmd_daily_text(update, context) -> None:
        if not ap._is_admin(update.effective_user.id):
            return
        context.user_data["await"] = "daily_patch_text"
        await update.effective_message.reply_text("请发送新的广播文案。保存后会返回广播页面，不会立即发送。")

    async def on_text_private(update, context) -> None:
        mode = str(context.user_data.get("await") or "")
        text = str(getattr(update.message, "text", "") or "").strip()
        if mode == "daily_patch_text":
            ap._set_setting(ap.KEY_DAILY_TEMPLATE, "custom")
            ap._set_setting(ap.KEY_DAILY_TEXT, text[:12000])
            context.user_data.pop("await", None)
            await update.message.reply_text("✅ 广播文案已保存。")
            await _show_daily(SimpleNamespace(edit_message_text=update.message.reply_text))
            return
        if mode == "daily_time":
            parsed = ap._parse_hhmm(text)
            if parsed:
                ap._set_setting(ap.KEY_DAILY_TIME, f"{parsed[0]:02d}:{parsed[1]:02d}")
                context.user_data.pop("await", None)
                await update.message.reply_text("✅ 定时时间已保存。")
                await _show_daily(SimpleNamespace(edit_message_text=update.message.reply_text))
                return
        await original_text_handler(update, context)

    ap._fetch_phnom_penh_daily_info = _live_body
    ap._daily_keyboard = _daily_keyboard
    ap._ensure_daily_defaults = _locked_defaults
    ap.cmd_daily = cmd_daily
    ap.on_daily_callback = on_daily_callback
    ap.scheduled_daily_broadcast = scheduled_daily_broadcast
    ap.cmd_daily_text = cmd_daily_text
    ap.on_text_private = on_text_private
    _INSTALLED = True
