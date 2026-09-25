"""Action-first room-status dashboard for the Publisher bot.

The existing inventory operator keeps the inventory writes and channel-sync
execution layer. This controller makes the owner-facing workflow practical:
pending inventory is the primary daily queue and large batches can be selected
without opening dozens of individual pages.
"""
from __future__ import annotations

import math
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .inventory_operator_ui import INVENTORY_BATCH_STATE_KEY, PublisherInventoryAdminController


_BATCH_PAGE_SIZE = 12


class PublisherInventoryDashboardController(PublisherInventoryAdminController):
    """Daily workbench over the existing inventory management actions."""

    async def show_listing_categories(self, message: Any) -> None:
        c = self._inventory_counts()
        repo = getattr(self, "repository", None)
        exception_count = int(repo.exception_count() or 0) if repo is not None else 0
        lines = [
            "<b>🔵 房态工作台</b>",
            "",
            "<b>今天先处理</b>",
            f"🔵 待确认 {c['pending']}｜10套一组　　⏰ 超3天 {c['overdue']}",
            f"🟡 已有预约 {c['reserved']}　　🆕 今日发布 {c['today']}",
            f"⚠️ 待处理异常 {exception_count}",
            "",
            "<b>当前库存</b>",
            f"🟢 可预约 {c['active']}　　🔴 已租出 {c['rented']}　　⚫ 已下架 {c['offline']}",
            "",
            "待确认可以直接批量处理，不需要逐套打开。",
        ]
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(f"🔵 待确认 {c['pending']}｜10套一组", callback_data="v3smp|pbat|0"),
                        InlineKeyboardButton(f"⏰ 超时 {c['overdue']}", callback_data="v3smp|inv_rows|overdue"),
                    ],
                    [
                        InlineKeyboardButton(
                            f"⚠️ 待处理异常 ({exception_count})",
                            callback_data="v3smp|exceptions|all",
                        )
                    ],
                    [
                        InlineKeyboardButton(f"🟡 已有预约 {c['reserved']}", callback_data="v3smp|inv_rows|reserved"),
                        InlineKeyboardButton(f"🆕 今日发布 {c['today']}", callback_data="v3smp|inv_rows|today"),
                    ],
                    [
                        InlineKeyboardButton("🕘 最近发布", callback_data="v3smp|inv_rows|recent"),
                        InlineKeyboardButton("🔍 搜索房源", callback_data="v3smp|inv_search"),
                    ],
                    [
                        InlineKeyboardButton(f"🟢 可预约 {c['active']}", callback_data="v3smp|inv_rows|active"),
                        InlineKeyboardButton(f"🔴 已租出 {c['rented']}", callback_data="v3smp|inv_rows|rented"),
                    ],
                    [InlineKeyboardButton(f"⚫ 已下架 {c['offline']}", callback_data="v3smp|inv_rows|offline")],
                    self.home_row(),
                ]
            ),
        )

    async def show_batch_menu(self, message: Any) -> None:
        c = self._inventory_counts()
        await message.reply_text(
            "<b>☑️ 批量改房态</b>\n\n"
            "最常用的是直接处理全部待确认房源。\n"
            "也可以只处理超时或最近发布的范围。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            f"🔵 待确认 {c['pending']}｜10套一组",
                            callback_data="v3smp|pbat|0",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            f"⏰ 超3天未确认 {c['overdue']}",
                            callback_data="v3smp|inv_batch_scope|overdue",
                        )
                    ],
                    [
                        InlineKeyboardButton("🆕 今天发布", callback_data="v3smp|inv_batch_scope|today"),
                        InlineKeyboardButton("📅 昨天发布", callback_data="v3smp|inv_batch_scope|yesterday"),
                    ],
                    [InlineKeyboardButton("⬅️ 返回房态工作台", callback_data="v3smp|listings")],
                ]
            ),
        )

    def _batch_scope_rows(self, scope: str) -> list[dict[str, Any]]:
        clean = str(scope or "").strip().lower()
        if clean == "pending_all":
            return self._pending_batch_rows(0)
        return super()._batch_scope_rows(clean)

    @staticmethod
    def _batch_scope_title(scope: str) -> str:
        return {
            "pending_all": "🟠 全部待确认",
            "overdue": "⏰ 超3天未确认",
            "today": "🆕 今天发布",
            "yesterday": "📅 昨天发布",
        }.get(str(scope or ""), "批量房源")

    async def show_batch_scope(self, message: Any, context: Any, scope: str) -> None:
        rows = self._batch_scope_rows(scope)
        state = context.user_data.setdefault(INVENTORY_BATCH_STATE_KEY, {})
        if state.get("scope") != scope:
            state.clear()
            state.update({"scope": scope, "selected": [], "page": 0})

        chosen = {str(value) for value in state.get("selected", [])}
        total = len(rows)
        page_count = max(1, math.ceil(total / _BATCH_PAGE_SIZE))
        page = max(0, min(int(state.get("page") or 0), page_count - 1))
        state["page"] = page
        start = page * _BATCH_PAGE_SIZE
        visible = rows[start : start + _BATCH_PAGE_SIZE]

        lines = [
            f"<b>☑️ 批量改房态 · {self._batch_scope_title(scope)}</b>",
            "",
            f"共 {total} 套　｜　第 {page + 1}/{page_count} 页　｜　已选 {len(chosen)} 套",
            "",
        ]
        buttons: list[list[InlineKeyboardButton]] = []
        if not visible:
            lines.append("这个范围当前没有房源。")

        for offset, row in enumerate(visible, start=start + 1):
            listing_id = str(row["listing_id"])
            mark = "☑️" if listing_id in chosen else "☐"
            label = self._button_title(row)
            lines.append(f"{mark} {offset}. {label}")
            buttons.append(
                [InlineKeyboardButton(f"{mark} {offset}. {label}"[:60], callback_data=f"v3smp|inv_toggle|{listing_id}")]
            )

        if visible:
            buttons.append(
                [
                    InlineKeyboardButton("☑️ 选择本页", callback_data="v3smp|inv_select_page"),
                    InlineKeyboardButton(f"☑️ 全选全部 {total}", callback_data="v3smp|inv_select_all"),
                ]
            )

        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data="v3smp|inv_page|prev"))
        if page + 1 < page_count:
            nav.append(InlineKeyboardButton("➡️ 下一页", callback_data="v3smp|inv_page|next"))
        if nav:
            buttons.append(nav)

        if chosen:
            buttons.extend(
                [
                    [
                        InlineKeyboardButton("🟢 设为可预约", callback_data="v3smp|inv_apply|active"),
                        InlineKeyboardButton("🟡 设为已有预约", callback_data="v3smp|inv_apply|reserved"),
                    ],
                    [
                        InlineKeyboardButton("🔴 设为已租出", callback_data="v3smp|inv_apply|rented"),
                        InlineKeyboardButton("⚫ 下架", callback_data="v3smp|inv_apply|offline"),
                    ],
                    [InlineKeyboardButton("❌ 清空选择", callback_data="v3smp|inv_clear")],
                ]
            )

        buttons.append([InlineKeyboardButton("⬅️ 返回批量管理", callback_data="v3smp|inv_batch")])
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def _show_batch_confirmation(self, message: Any, context: Any, status: str) -> None:
        state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
        selected = [str(value) for value in state.get("selected", [])]
        label = {
            "active": "🟢 可预约",
            "reserved": "🟡 已有预约",
            "rented": "🔴 已租出",
            "offline": "⚫ 已下架",
        }.get(status, status)
        await message.reply_text(
            f"<b>确认批量修改</b>\n\n"
            f"已选择：<b>{len(selected)} 套</b>\n"
            f"目标房态：<b>{label}</b>\n\n"
            "确认后会同步修改这些房源的房态。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(f"✅ 确认修改 {len(selected)} 套", callback_data=f"v3smp|inv_apply_confirm|{status}")],
                    [InlineKeyboardButton("⬅️ 返回选择", callback_data="v3smp|inv_batch_return")],
                ]
            ),
        )

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else ""

        if action == "inv_batch_scope" and len(parts) == 3 and parts[2] == "pending_all":
            await self.show_pending_batch(query.message, 0)
            return True

        if action == "inv_select_page":
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            scope = str(state.get("scope") or "pending_all")
            rows = self._batch_scope_rows(scope)
            page = max(0, int(state.get("page") or 0))
            start = page * _BATCH_PAGE_SIZE
            visible = rows[start : start + _BATCH_PAGE_SIZE]
            chosen = {str(value) for value in state.get("selected", [])}
            chosen.update(str(row["listing_id"]) for row in visible)
            state["selected"] = list(chosen)
            context.user_data[INVENTORY_BATCH_STATE_KEY] = state
            await self.show_batch_scope(query.message, context, scope)
            return True

        if action == "inv_select_all":
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            scope = str(state.get("scope") or "pending_all")
            rows = self._batch_scope_rows(scope)
            state["selected"] = [str(row["listing_id"]) for row in rows]
            context.user_data[INVENTORY_BATCH_STATE_KEY] = state
            await self.show_batch_scope(query.message, context, scope)
            return True

        if action == "inv_page" and len(parts) == 3:
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            scope = str(state.get("scope") or "pending_all")
            page = max(0, int(state.get("page") or 0))
            page += -1 if parts[2] == "prev" else 1
            state["page"] = max(0, page)
            context.user_data[INVENTORY_BATCH_STATE_KEY] = state
            await self.show_batch_scope(query.message, context, scope)
            return True

        if action == "inv_apply" and len(parts) == 3:
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            if not state.get("selected"):
                await query.message.reply_text("请先选择要处理的房源。")
                return True
            await self._show_batch_confirmation(query.message, context, parts[2])
            return True

        if action == "inv_apply_confirm" and len(parts) == 3:
            updated, skipped = self._apply_selected(context, parts[2])
            await query.message.reply_text(
                f"✅ 已处理 {updated} 套。" + (f"\n⏭ {skipped} 套房态已变化，未覆盖。" if skipped else ""),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ 返回房态工作台", callback_data="v3smp|listings")]]
                ),
            )
            return True

        if action == "inv_batch_return":
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            await self.show_batch_scope(query.message, context, str(state.get("scope") or "pending_all"))
            return True

        return await super().handle_callback(update, context)


__all__ = ["PublisherInventoryDashboardController"]
