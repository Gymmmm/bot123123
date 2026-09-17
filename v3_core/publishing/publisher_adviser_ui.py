"""Publisher-side authoritative controls for 💬 侨联说.

Publisher owns generation and final approval of adviser copy. Manual publishing
may keep auto copy, replace it with 1-2 operator lines, or hide it. The final
choice is frozen into the publication package for User Bot to consume verbatim.
"""
from __future__ import annotations

import asyncio
from html import escape
from pathlib import Path
import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.adviser_copy import generate_adviser_text
from .inventory_dashboard_ui import PublisherInventoryDashboardController
from .operator_flow import BLOCKER_LABELS, DISPLAY_FIELDS, OPTIONAL_FIELDS
from .package_service import _publisher_adviser_facts
from .package_store import FrozenPackage
from .simple_admin import NEW_LISTING_STATE_KEY, SIMPLE_EDIT_STATE_KEY
from .telegram_adapter import TelegramChannelAdapter, deliver_approved_package


class PublisherAdviserAdminController(PublisherInventoryDashboardController):
    """Boss-facing Publisher UI with one authoritative adviser-copy decision."""

    @staticmethod
    def _adviser_mode(state: dict[str, Any]) -> str:
        mode = str(state.get("adviser_mode") or "auto").strip().lower()
        return mode if mode in {"auto", "manual", "hidden"} else "auto"

    @staticmethod
    def _invalidate_preview(state: dict[str, Any]) -> None:
        """Invalidate any old frozen preview after adviser-copy changes."""
        state.pop("package_id", None)
        state["mode"] = "confirm"

    def _auto_adviser_copy(self, detail: Any) -> str:
        facts = dict(detail.canonical.get("facts") or {})
        public_id = str(detail.listing.get("public_listing_id") or detail.listing.get("listing_id") or "")
        return generate_adviser_text(
            _publisher_adviser_facts(facts),
            seed=public_id,
            max_points=2,
            allow_fallback=False,
        ).strip()

    def _adviser_display(self, detail: Any, state: dict[str, Any]) -> str:
        mode = self._adviser_mode(state)
        if mode == "hidden":
            return "暂不显示"
        if mode == "manual":
            return str(state.get("adviser_copy") or "").strip() or "暂不显示"
        return self._auto_adviser_copy(detail) or "暂不显示"

    def _adviser_override(self, state: dict[str, Any]) -> str | None:
        mode = self._adviser_mode(state)
        if mode == "auto":
            return None
        if mode == "hidden":
            return ""
        return str(state.get("adviser_copy") or "").strip()

    def _repair_manual_layout_if_needed(self, detail: Any, state: dict[str, Any]) -> Any:
        """Recover common admin-copy layouts such as ``房型：2+1``.

        The generic canonical parser deliberately stays conservative. Publisher
        manual intake, however, receives structured copy from agents where a bare
        ``2+1`` is commonly prefixed by 房型/户型. Treat that labelled value as an
        explicit operator fact instead of forcing the owner to re-enter it.
        """
        if str(detail.listing.get("layout") or "").strip():
            return detail
        raw = str(state.get("text") or "")
        match = re.search(r"(?:房型|户型)\s*[:：]\s*(\d{1,2}\s*\+\s*\d{1,2})(?!\s*(?:房|卫))", raw)
        if not match:
            return detail
        layout = re.sub(r"\s+", "", match.group(1))
        self.workflow.edit_review_field(
            review_id=str(detail.review["review_id"]),
            field_name="layout",
            value=layout,
            operator_user_id="system:manual_layout_repair",
        )
        self._invalidate_preview(state)
        return self.workflow.review_detail(str(detail.review["review_id"]))

    async def _manual_blockers(self, detail: Any) -> tuple[list[str], Any]:
        blockers, media = await super()._manual_blockers(detail)
        blockers = [code for code in blockers if code != "listing_not_publishable"]
        return blockers, media

    async def show_manual_confirmation(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        detail = self._repair_manual_layout_if_needed(detail, state)
        values = self._manual_values(detail)
        try:
            blockers, _media = await self._manual_blockers(detail)
        except Exception:
            blockers = ["unreadable_media"]
        optional_missing = [name for name in OPTIONAL_FIELDS if values.get(name) in (None, "")]

        lines = ["<b>🏠 房源资料已整理</b>", ""]
        for name, label in DISPLAY_FIELDS:
            lines.append(f"{label}：{escape(self._display_value(name, values.get(name)))}")
        lines.extend(["", f"📷 图片：{len(state.get('images') or [])} 张", f"🎬 视频：{len(state.get('videos') or [])} 条", "", "💬 <b>侨联说</b>"])
        adviser = self._adviser_display(detail, state)
        lines.extend(escape(line) for line in adviser.splitlines() if line.strip())

        if blockers:
            required = "、".join(BLOCKER_LABELS.get(code, code) for code in blockers)
            lines.extend(["", f"🔴 发布前需要补充：{escape(required)}"])
        elif optional_missing:
            labels = {name: label.split(" ", 1)[-1] for name, label in DISPLAY_FIELDS}
            lines.extend(["", "✅ 当前资料已经可以发布", "💡 可选补充：" + "、".join(labels.get(name, name) for name in optional_missing)])
        else:
            lines.extend(["", "✅ 房源资料已满足发布条件。"])

        buttons: list[list[InlineKeyboardButton]] = []
        if blockers or optional_missing:
            buttons.append([InlineKeyboardButton("➕ 补充资料", callback_data="v3smp|manual_supplement"), InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit")])
        else:
            buttons.append([InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit")])
        buttons.append([InlineKeyboardButton("💬 调整侨联说", callback_data="v3smp|manual_adviser")])
        buttons.append([InlineKeyboardButton("📤 检查并发布" if blockers else "👀 生成预览", callback_data="v3smp|manual_preview")])
        buttons.append([InlineKeyboardButton("❌ 取消", callback_data="v3smp|manual_cancel")])
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def show_adviser_editor(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        current = self._adviser_display(detail, state)
        context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "manual_adviser"}
        await message.reply_text(
            "<b>💬 调整侨联说</b>\n\n当前内容：\n" + escape(current) + "\n\n直接发送 1–2 句新文案，换行分开。\n保存后必须重新生成预览，发布的一定是预览里看到的版本。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🙈 不显示", callback_data="v3smp|manual_adviser_hide"), InlineKeyboardButton("↩️ 恢复自动", callback_data="v3smp|manual_adviser_auto")], [InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")]]),
        )

    async def prepare_manual_preview(self, message: Any, context: Any, *, review_id: str, offer_id: str, style: str | None = None, advance_cover: bool = False) -> None:
        state = context.user_data.setdefault(NEW_LISTING_STATE_KEY, {})
        detail = self.workflow.review_detail(review_id)
        detail = self._repair_manual_layout_if_needed(detail, state)
        blockers, media = await self._manual_blockers(detail)
        if blockers:
            await self.show_manual_confirmation(message, context)
            return
        gallery = tuple(media.gallery_paths)
        if advance_cover and gallery:
            state["cover_index"] = (int(state.get("cover_index") or 0) + 1) % len(gallery)
        index = int(state.get("cover_index") or 0) % max(1, len(gallery))
        cover_path = gallery[index] if gallery else None
        if str(detail.review.get("review_status") or "") != "approved":
            await asyncio.to_thread(self.workflow.approve_review, review_id=review_id, operator_user_id="system:manual_preview")

        original_status = str(detail.listing.get("inventory_status") or "pending")
        preview_status_changed = original_status == "pending"
        if preview_status_changed:
            self.repository.set_listing_status(str(detail.listing["listing_id"]), "active")
        try:
            package = await asyncio.to_thread(
                self.workflow.build_package_for_review,
                review_id=review_id,
                cover_style=style,
                manual_cover_path=cover_path,
                adviser_copy_override=self._adviser_override(state),
            )
        finally:
            if preview_status_changed:
                self.repository.mark_listing_pending_for_auto_publish(str(detail.listing["listing_id"]))

        self.repository.set_item(offer_id, state="preview_ready", package_id=package.package_id, origin="manual")
        state["package_id"] = package.package_id
        state["offer_id"] = offer_id
        state["review_id"] = review_id
        state["listing_id"] = str(detail.listing["listing_id"])
        state["mode"] = "preview"
        await self.send_manual_preview(message, package, review_id=review_id, offer_id=offer_id)

    async def send_manual_preview(self, message: Any, package: FrozenPackage, *, review_id: str, offer_id: str) -> None:
        with Path(package.cover_path).open("rb") as handle:
            await message.reply_photo(
                photo=handle,
                caption=package.post_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 确认发布到频道", callback_data="v3smp|manual_send")],
                    [InlineKeyboardButton("💬 调整侨联说", callback_data="v3smp|manual_adviser")],
                    [InlineKeyboardButton("🖼 更换封面图片", callback_data="v3smp|manual_cover"), InlineKeyboardButton("🎨 更换封面模板", callback_data="v3smp|manual_templates")],
                    [InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")],
                ]),
            )

    async def handle_text(self, update: Any, context: Any) -> bool:
        edit = context.user_data.get(SIMPLE_EDIT_STATE_KEY)
        if isinstance(edit, dict) and str(edit.get("kind") or "") == "manual_adviser":
            raw = str(update.effective_message.text or "").strip()
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            if raw in {"不显示", "隐藏"}:
                if isinstance(state, dict):
                    state["adviser_mode"] = "hidden"
                    state["adviser_copy"] = ""
                    self._invalidate_preview(state)
                context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
                await self.show_manual_confirmation(update.effective_message, context)
                return True
            if raw in {"恢复自动", "自动"}:
                if isinstance(state, dict):
                    state["adviser_mode"] = "auto"
                    state.pop("adviser_copy", None)
                    self._invalidate_preview(state)
                context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
                await self.show_manual_confirmation(update.effective_message, context)
                return True
            lines = [line.strip() for line in raw.splitlines() if line.strip()]
            if not 1 <= len(lines) <= 2:
                await update.effective_message.reply_text("请发送 1–2 句侨联说，换行分开。")
                return True
            if not isinstance(state, dict):
                context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
                return True
            state["adviser_mode"] = "manual"
            state["adviser_copy"] = "\n".join(lines)
            self._invalidate_preview(state)
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await self.show_manual_confirmation(update.effective_message, context)
            return True
        return await super().handle_text(update, context)

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = getattr(update, "callback_query", None)
        raw = str(getattr(query, "data", "") or "") if query is not None else ""
        parts = raw.split("|")
        if raw == "v3smp|manual_adviser":
            await self.show_adviser_editor(query.message, context)
            return True
        if raw == "v3smp|manual_adviser_hide":
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            if isinstance(state, dict):
                state["adviser_mode"] = "hidden"
                state["adviser_copy"] = ""
                self._invalidate_preview(state)
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await self.show_manual_confirmation(query.message, context)
            return True
        if raw == "v3smp|manual_adviser_auto":
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            if isinstance(state, dict):
                state["adviser_mode"] = "auto"
                state.pop("adviser_copy", None)
                self._invalidate_preview(state)
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await self.show_manual_confirmation(query.message, context)
            return True
        if raw in {"v3smp|manual_send", "v3smp|manual_cover", "v3smp|manual_templates"} or (len(parts) == 3 and parts[1] == "manual_style"):
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            current_package = str(state.get("package_id") or "") if isinstance(state, dict) else ""
            current_offer = str(state.get("offer_id") or "") if isinstance(state, dict) else ""
            current_review = str(state.get("review_id") or "") if isinstance(state, dict) else ""
            current_listing = str(state.get("listing_id") or "") if isinstance(state, dict) else ""
            valid_session = all((current_package, current_offer, current_review, current_listing))
            if valid_session:
                try:
                    detail = self.workflow.review_detail(current_review)
                    package = self.workflow.package(current_package)
                    valid_session = (
                        str(detail.listing.get("listing_id") or "") == current_listing
                        and str(detail.offer.get("offer_id") or "") == current_offer
                        and str(package.listing_id) == current_listing
                        and str(package.offer_id) == current_offer
                    )
                except (KeyError, ValueError):
                    valid_session = False
            if not valid_session:
                await query.message.reply_text(
                    "⚠️ 这张预览已经失效。侨联说或资料有过调整，请重新生成预览后再发布。",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👀 重新生成预览", callback_data="v3smp|manual_preview")]]),
                )
                return True
            if raw == "v3smp|manual_cover":
                await self.prepare_manual_preview(query.message, context, review_id=current_review, offer_id=current_offer, advance_cover=True)
                return True
            if raw == "v3smp|manual_templates":
                await query.message.reply_text(
                    "选择封面模板：",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("右侧价格牌", callback_data="v3smp|manual_style|right_price")],
                        [InlineKeyboardButton("黑金模板", callback_data="v3smp|manual_style|black_gold")],
                        [InlineKeyboardButton("经典蓝卡", callback_data="v3smp|manual_style|classic_blue")],
                        self.home_row(),
                    ]),
                )
                return True
            if len(parts) == 3 and parts[1] == "manual_style":
                await self.prepare_manual_preview(query.message, context, review_id=current_review, offer_id=current_offer, style=parts[2])
                return True

            package = await asyncio.to_thread(self.workflow.approve_package, package_id=current_package, approved_by="system:manual_publish")
            result = await deliver_approved_package(
                coordinator=self.workflow.delivery,
                adapter=TelegramChannelAdapter(context.bot),
                package_id=package.package_id,
                channel_chat_id=self.channel_chat_id,
                inventory_status_override="active",
            )
            listing_id = str(state.get("listing_id") or package.listing_id) if isinstance(state, dict) else str(package.listing_id)
            if listing_id:
                self.repository.set_listing_status(listing_id, "active")
            self.repository.set_item(current_offer, state="published", package_id=package.package_id, channel_message_id=str(result.publication.channel_message_id), origin="manual")
            context.user_data.pop(NEW_LISTING_STATE_KEY, None)
            await query.message.reply_text(
                f"✅ 已发布到频道。频道消息：{escape(str(result.publication.channel_message_id))}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return True
        return await super().handle_callback(update, context)


__all__ = ["PublisherAdviserAdminController"]
