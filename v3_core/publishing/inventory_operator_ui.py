"""Boss-facing inventory management UI for the V3 Publisher.

This module changes only operator interaction. Existing inventory writes,
publication records and channel synchronization remain the execution layer.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .pending_batch_admin import BATCH_STATUS_SYNC_KEY, PendingBatchOperatorPublisherAdminController
from .simple_admin import SIMPLE_EDIT_STATE_KEY


INVENTORY_BATCH_STATE_KEY = "v3_inventory_batch_selection"
_LOCAL_TZ = ZoneInfo("Asia/Phnom_Penh")


class PublisherInventoryAdminController(PendingBatchOperatorPublisherAdminController):
    """Compact daily inventory console for a non-technical operator."""

    @staticmethod
    def home_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("➕ 发布房源", callback_data="v3smp|new"),
                    InlineKeyboardButton("📢 发布中心", callback_data="v3bc"),
                ],
                [
                    InlineKeyboardButton("🔵 房态管理", callback_data="v3smp|listings"),
                    InlineKeyboardButton("📡 采集源", callback_data="v3smp|sources"),
                ],
                [InlineKeyboardButton("📚 发布记录", callback_data="v3smp|logs")],
            ]
        )

    async def show_home(self, message: Any) -> None:
        await message.reply_text(
            "<b>📣 侨联发布助手</b>\n\n"
            "发布房源、管理频道内容和房态。",
            parse_mode=ParseMode.HTML,
            reply_markup=self.home_keyboard(),
        )

    def _inventory_counts(self) -> dict[str, int]:
        counts = self._status_counts()
        with sqlite3.connect(self.db_path) as conn:
            today = conn.execute(
                """SELECT COUNT(DISTINCT pi.listing_id)
                     FROM publication_instances pi
                    WHERE pi.platform='telegram'
                      AND pi.publish_status='published'
                      AND date(pi.published_at, '+7 hours')=date('now', '+7 hours')"""
            ).fetchone()
            overdue = conn.execute(
                """SELECT COUNT(*) FROM listings_v3
                    WHERE inventory_status='pending'
                      AND datetime(updated_at) <= datetime('now','-3 days')"""
            ).fetchone()
        counts["today"] = int((today or [0])[0] or 0)
        counts["overdue"] = int((overdue or [0])[0] or 0)
        return counts

    async def show_listing_categories(self, message: Any) -> None:
        c = self._inventory_counts()
        lines = [
            "<b>🔵 房态管理</b>",
            "",
            f"🟠 待确认        {c['pending']}",
            f"🟢 可预约       {c['active']}",
            f"🟡 已有预约      {c['reserved']}",
            f"🔴 已租出        {c['rented']}",
            f"⚫ 已下架        {c['offline']}",
            f"🆕 今天新发布    {c['today']}",
            f"⏰ 超3天未确认   {c['overdue']}",
        ]
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(f"🟠 待确认 {c['pending']}", callback_data="v3smp|inv_rows|pending"),
                        InlineKeyboardButton(f"🟢 可预约 {c['active']}", callback_data="v3smp|inv_rows|active"),
                    ],
                    [
                        InlineKeyboardButton(f"🟡 已有预约 {c['reserved']}", callback_data="v3smp|inv_rows|reserved"),
                        InlineKeyboardButton(f"🔴 已租出 {c['rented']}", callback_data="v3smp|inv_rows|rented"),
                    ],
                    [InlineKeyboardButton(f"⚫ 已下架 {c['offline']}", callback_data="v3smp|inv_rows|offline")],
                    [
                        InlineKeyboardButton("🆕 今天新发布", callback_data="v3smp|inv_rows|today"),
                        InlineKeyboardButton("⏰ 超时未确认", callback_data="v3smp|inv_rows|overdue"),
                    ],
                    [
                        InlineKeyboardButton("🔍 搜索房源", callback_data="v3smp|inv_search"),
                        InlineKeyboardButton("☑️ 批量管理", callback_data="v3smp|inv_batch"),
                    ],
                    self.home_row(),
                ]
            ),
        )

    def _inventory_rows(self, category: str, *, limit: int = 30, query: str = "") -> list[dict[str, Any]]:
        clean = str(category or "").strip().lower()
        params: list[Any] = []
        where: list[str] = []
        if clean in {"pending", "active", "reserved", "rented"}:
            where.append("l.inventory_status=?")
            params.append(clean)
        elif clean == "offline":
            where.append("l.inventory_status IN ('offline','inactive')")
        elif clean == "today":
            where.append(
                "EXISTS (SELECT 1 FROM publication_instances px WHERE px.listing_id=l.listing_id "
                "AND px.platform='telegram' AND px.publish_status='published' "
                "AND date(px.published_at, '+7 hours')=date('now', '+7 hours'))"
            )
        elif clean == "overdue":
            where.append("l.inventory_status='pending'")
            where.append("datetime(l.updated_at) <= datetime('now','-3 days')")
        elif clean == "recent":
            pass
        else:
            return []
        token = str(query or "").strip()
        if token:
            like = f"%{token}%"
            where.append("(l.public_listing_id LIKE ? OR l.project_name LIKE ? OR l.display_title LIKE ? OR l.layout LIKE ?)")
            params.extend([like, like, like, like])
        clause = " AND ".join(where) if where else "1=1"
        sql = f"""SELECT l.listing_id,l.public_listing_id,l.project_name,l.display_title,l.layout,
                         l.property_type,l.public_location_display,l.floor,l.inventory_status,l.updated_at,
                         (SELECT o.monthly_rent_usd FROM listing_offers o
                           WHERE o.listing_id=l.listing_id AND o.offer_type='rent'
                           ORDER BY CASE WHEN o.offer_status='active' THEN 0 ELSE 1 END,o.rowid DESC LIMIT 1) AS monthly_rent_usd,
                         (SELECT pi.published_at FROM publication_instances pi
                           WHERE pi.listing_id=l.listing_id AND pi.platform='telegram'
                             AND pi.publish_status='published'
                           ORDER BY pi.published_at DESC,pi.id DESC LIMIT 1) AS published_at,
                         (SELECT pi.channel_message_id FROM publication_instances pi
                           WHERE pi.listing_id=l.listing_id AND pi.platform='telegram'
                             AND pi.publish_status='published'
                           ORDER BY pi.published_at DESC,pi.id DESC LIMIT 1) AS channel_message_id
                    FROM listings_v3 l
                   WHERE {clause}
                   ORDER BY COALESCE(published_at,l.updated_at) DESC,l.listing_id DESC
                   LIMIT ?"""
        params.append(max(1, int(limit)))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    @staticmethod
    def _price(row: dict[str, Any]) -> str:
        value = row.get("monthly_rent_usd")
        if value in (None, ""):
            return "价格待确认"
        try:
            return f"${int(float(value)):,}"
        except (TypeError, ValueError):
            return f"${value}"

    @staticmethod
    def _primary_name(row: dict[str, Any]) -> str:
        return str(row.get("project_name") or row.get("display_title") or "未命名房源").strip()

    @classmethod
    def _button_title(cls, row: dict[str, Any]) -> str:
        project = cls._primary_name(row)
        layout = str(row.get("layout") or "户型待确认").strip()
        return f"{project}｜{layout}｜{cls._price(row)}"[:58]

    @staticmethod
    def _local_time_label(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "时间待确认"
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(_LOCAL_TZ)
            now = datetime.now(_LOCAL_TZ)
            if local.date() == now.date():
                return f"今天 {local:%H:%M}"
            if local.date() == (now.date() - timedelta(days=1)):
                return f"昨天 {local:%H:%M}"
            return local.strftime("%m-%d %H:%M")
        except ValueError:
            return raw[:16]

    @staticmethod
    def _category_title(category: str) -> str:
        return {
            "pending": "🟠 待确认",
            "active": "🟢 可预约",
            "reserved": "🟡 已有预约",
            "rented": "🔴 已租出",
            "offline": "⚫ 已下架",
            "today": "🆕 今天新发布",
            "overdue": "⏰ 超3天未确认",
            "recent": "🕘 最近发布",
            "search": "🔍 搜索结果",
        }.get(category, category)

    async def _render_inventory_rows(self, message: Any, rows: list[dict[str, Any]], *, title: str) -> None:
        lines = [f"<b>{escape(title)} · {len(rows)}套</b>", ""]
        buttons: list[list[InlineKeyboardButton]] = []
        if not rows:
            lines.append("当前没有房源。")
        for index, row in enumerate(rows, start=1):
            public_id = str(row.get("public_listing_id") or "房源")
            lines.append(f"{index}. {escape(self._button_title(row))}")
            lines.append(f"   {escape(public_id)} · {escape(self._local_time_label(row.get('published_at') or row.get('updated_at')))}")
            buttons.append(
                [InlineKeyboardButton(f"{index}. {self._button_title(row)}", callback_data=f"v3smp|listing|{row['listing_id']}")]
            )
        buttons.append([InlineKeyboardButton("☑️ 批量处理", callback_data="v3smp|inv_batch")])
        buttons.append([InlineKeyboardButton("⬅️ 房态管理", callback_data="v3smp|listings")])
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def show_inventory_rows(self, message: Any, category: str) -> None:
        rows = self._inventory_rows(category)
        await self._render_inventory_rows(message, rows, title=self._category_title(category))

    async def show_listing_rows(self, message: Any, category: str) -> None:
        await self.show_inventory_rows(message, category)

    async def show_pending_batch(self, message: Any, page: int = 0) -> None:
        await self.show_inventory_rows(message, "pending")

    def _channel_post_url(self, message_id: Any) -> str:
        mid = str(message_id or "").strip()
        chat = str(self.channel_chat_id or "").strip()
        if not mid:
            return ""
        if chat.startswith("@"):
            return f"https://t.me/{chat[1:]}/{mid}"
        if chat.startswith("-100") and chat[4:].isdigit():
            return f"https://t.me/c/{chat[4:]}/{mid}"
        return ""

    async def show_listing(self, message: Any, listing_id: str) -> None:
        row = self._listing_detail(str(listing_id))
        extra = self._inventory_rows("recent", limit=200)
        meta = next((item for item in extra if str(item.get("listing_id")) == str(listing_id)), {})
        public_id = str(row.get("public_listing_id") or listing_id)
        project = str(row.get("project_name") or row.get("display_title") or "未命名房源")
        layout = str(row.get("layout") or "户型待确认")
        status = str(row.get("inventory_status") or "pending").strip().lower()
        status_label = {
            "pending": "🟠 待确认",
            "active": "🟢 可预约",
            "reserved": "🟡 已有预约",
            "rented": "🔴 已租出",
            "offline": "⚫ 已下架",
            "inactive": "⚫ 已下架",
        }.get(status, "🟠 待确认")
        price = self._price(row)
        area = str(row.get("public_location_display") or "位置待确认")
        property_type = str(row.get("property_type") or "类型待确认")
        floor = str(row.get("floor") or "楼层待确认")
        payment = str(row.get("deposit_payment_terms") or row.get("payment_terms") or "押付待确认")
        contract = str(row.get("contract_term_display") or row.get("contract_term") or "租期待确认")
        published = self._local_time_label(meta.get("published_at") or row.get("updated_at"))
        source = "自动采集" if not str(public_id).startswith("manual_") else "手动发布"
        lines = [
            f"<b>🏠 {escape(project)}｜{escape(layout)}</b>",
            f"💵 {escape(price)}/月" if price.startswith("$") else f"💵 {escape(price)}",
            "",
            escape(public_id),
            f"发布时间：{escape(published)}",
            f"来源：{source}",
            f"当前房态：{status_label}",
            "",
            f"📍 {escape(area)}",
            f"🏢 {escape(property_type)} · {escape(floor)}",
            f"🗝 {escape(payment)} · {escape(contract)}",
        ]
        rows: list[list[InlineKeyboardButton]] = [
            [
                InlineKeyboardButton("🟢 可预约", callback_data=f"v3smp|status|active|{listing_id}"),
                InlineKeyboardButton("🟡 已有预约", callback_data=f"v3smp|status|reserved|{listing_id}"),
            ],
            [
                InlineKeyboardButton("🔴 已租出", callback_data=f"v3smp|status|rented|{listing_id}"),
                InlineKeyboardButton("⚫ 下架", callback_data=f"v3smp|status|offline|{listing_id}"),
            ],
        ]
        if public_id:
            rows.append(
                [InlineKeyboardButton("📋 查看完整资料", url=f"https://t.me/{self.user_bot_username}?start=property_{public_id}_details")]
            )
        post_url = self._channel_post_url(meta.get("channel_message_id"))
        if post_url:
            rows.append([InlineKeyboardButton("📨 查看频道原帖", url=post_url)])
        rows.extend(
            [
                [InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")],
                self.home_row(),
            ]
        )
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))

    async def show_batch_menu(self, message: Any) -> None:
        await message.reply_text(
            "<b>☑️ 批量管理</b>\n\n请选择要处理的房源：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🆕 今天发布", callback_data="v3smp|inv_batch_scope|today")],
                    [InlineKeyboardButton("📅 昨天发布", callback_data="v3smp|inv_batch_scope|yesterday")],
                    [InlineKeyboardButton("⏰ 超3天未确认", callback_data="v3smp|inv_batch_scope|overdue")],
                    [InlineKeyboardButton("⬅️ 返回", callback_data="v3smp|listings")],
                ]
            ),
        )

    def _batch_scope_rows(self, scope: str) -> list[dict[str, Any]]:
        clean = str(scope or "").strip().lower()
        if clean == "overdue":
            return self._inventory_rows("overdue", limit=40)
        day_offset = 0 if clean == "today" else -1 if clean == "yesterday" else None
        if day_offset is None:
            return []
        target = (datetime.now(_LOCAL_TZ).date() + timedelta(days=day_offset)).isoformat()
        rows = self._inventory_rows("pending", limit=100)
        selected = []
        for row in rows:
            raw = str(row.get("published_at") or "").strip()
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt.astimezone(_LOCAL_TZ).date().isoformat() == target:
                    selected.append(row)
            except ValueError:
                continue
        return selected[:40]

    async def show_batch_scope(self, message: Any, context: Any, scope: str) -> None:
        rows = self._batch_scope_rows(scope)
        state = context.user_data.setdefault(INVENTORY_BATCH_STATE_KEY, {})
        if state.get("scope") != scope:
            state.clear()
            state.update({"scope": scope, "selected": []})
        chosen = {str(value) for value in state.get("selected", [])}
        buttons: list[list[InlineKeyboardButton]] = []
        lines = ["<b>☑️ 批量管理</b>", "", f"已选择：{len(chosen)}套", ""]
        if not rows:
            lines.append("这个范围当前没有待确认房源。")
        for index, row in enumerate(rows, start=1):
            listing_id = str(row["listing_id"])
            mark = "☑️" if listing_id in chosen else "☐"
            label = self._button_title(row)
            lines.append(f"{mark} {index}. {escape(label)}")
            buttons.append(
                [InlineKeyboardButton(f"{mark} {index}. {label}"[:58], callback_data=f"v3smp|inv_toggle|{listing_id}")]
            )
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
                    [InlineKeyboardButton("❌ 取消选择", callback_data="v3smp|inv_clear")],
                ]
            )
        buttons.append([InlineKeyboardButton("⬅️ 返回批量管理", callback_data="v3smp|inv_batch")])
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    def _apply_selected(self, context: Any, status: str) -> tuple[int, int]:
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"active", "reserved", "rented", "offline"}:
            raise ValueError("unsupported inventory status")
        state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
        selected = [str(value) for value in state.get("selected", [])]
        updated = 0
        skipped = 0
        sync_queue = context.user_data.setdefault(BATCH_STATUS_SYNC_KEY, [])
        with sqlite3.connect(self.db_path) as conn:
            for listing_id in selected:
                cur = conn.execute(
                    """UPDATE listings_v3 SET inventory_status=?,updated_at=CURRENT_TIMESTAMP
                        WHERE listing_id=? AND inventory_status='pending'""",
                    (clean_status, listing_id),
                )
                if cur.rowcount == 1:
                    updated += 1
                    sync_queue.append({"listing_id": listing_id, "status": clean_status})
                else:
                    skipped += 1
            conn.commit()
        context.user_data.pop(INVENTORY_BATCH_STATE_KEY, None)
        return updated, skipped

    async def handle_text(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(SIMPLE_EDIT_STATE_KEY)
        if isinstance(state, dict) and str(state.get("kind") or "") == "inventory_search":
            text = str(getattr(update.effective_message, "text", "") or "").strip()
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            rows: list[dict[str, Any]] = []
            for category in ("pending", "active", "reserved", "rented", "offline"):
                rows.extend(self._inventory_rows(category, limit=20, query=text))
            unique: dict[str, dict[str, Any]] = {str(row["listing_id"]): row for row in rows}
            await self._render_inventory_rows(
                update.effective_message,
                list(unique.values())[:30],
                title=f"🔍 搜索：{text}",
            )
            return True
        return await super().handle_text(update, context)

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else ""
        if action == "inv_rows" and len(parts) == 3:
            await self.show_inventory_rows(query.message, parts[2])
            return True
        if action == "inv_search":
            context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "inventory_search"}
            await query.message.reply_text("发送项目名、QL编号、户型或关键词：")
            return True
        if action == "inv_batch":
            context.user_data.pop(INVENTORY_BATCH_STATE_KEY, None)
            await self.show_batch_menu(query.message)
            return True
        if action == "inv_batch_scope" and len(parts) == 3:
            await self.show_batch_scope(query.message, context, parts[2])
            return True
        if action == "inv_toggle" and len(parts) == 3:
            state = context.user_data.setdefault(INVENTORY_BATCH_STATE_KEY, {"scope": "today", "selected": []})
            chosen = {str(value) for value in state.get("selected", [])}
            listing_id = str(parts[2])
            if listing_id in chosen:
                chosen.remove(listing_id)
            else:
                chosen.add(listing_id)
            state["selected"] = list(chosen)
            await self.show_batch_scope(query.message, context, str(state.get("scope") or "today"))
            return True
        if action == "inv_clear":
            state = context.user_data.get(INVENTORY_BATCH_STATE_KEY) or {}
            scope = str(state.get("scope") or "today")
            context.user_data[INVENTORY_BATCH_STATE_KEY] = {"scope": scope, "selected": []}
            await self.show_batch_scope(query.message, context, scope)
            return True
        if action == "inv_apply" and len(parts) == 3:
            updated, skipped = self._apply_selected(context, parts[2])
            await query.message.reply_text(
                f"✅ 已处理 {updated} 套。" + (f"\n⏭ {skipped} 套房态已变化，未覆盖。" if skipped else ""),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回房态管理", callback_data="v3smp|listings")]]),
            )
            return True
        if action == "pbat":
            await self.show_inventory_rows(query.message, "pending")
            return True
        return await super().handle_callback(update, context)


__all__ = ["PublisherInventoryAdminController", "INVENTORY_BATCH_STATE_KEY"]
