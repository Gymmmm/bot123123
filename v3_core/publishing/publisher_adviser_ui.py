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

from v3_core.adviser_copy import build_adviser_copy, validate_adviser_copy
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
        text = build_adviser_copy(
            _publisher_adviser_facts(facts, listing=dict(detail.listing or {})),
            seed=public_id,
            max_points=2,
        ).strip()
        validate_adviser_copy(text)
        return text

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

    @staticmethod
    def _manual_cover_candidates(media: Any) -> tuple[str, ...]:
        candidates: list[str] = []
        preferred = str(getattr(media, "cover_source_path", "") or "").strip()
        if preferred:
            candidates.append(preferred)
        for item in tuple(getattr(media, "ranking", ()) or ()):
            if item.get("reject"):
                continue
            path = str(item.get("file") or "").strip()
            if path and path not in candidates:
                candidates.append(path)
        return tuple(candidates)

    async def show_manual_cover_picker(self, message: Any, context: Any, *, review_id: str) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return
        media = await asyncio.to_thread(self.workflow.review_media, review_id=review_id)
        candidates = self._manual_cover_candidates(media)
        if not candidates:
            await message.reply_text("⚠️ 当前没有可用的封面照片。")
            return
        state["cover_candidates"] = list(candidates)
        selected = str(state.get("selected_cover_path") or "")
        await message.reply_text(
            "<b>🖼 选择主图</b>\n\n"
            "点下方按钮，选一张原图作为频道封面底图。\n"
            "系统会按所选「封面格式」生成封面（横版 1200×900）。\n"
            "更多实拍相册仍用全部照片，并加上侨联角标。",
            parse_mode=ParseMode.HTML,
        )
        for index, path in enumerate(candidates):
            with Path(path).open("rb") as handle:
                marker = " · 当前主图" if path == selected or (not selected and index == 0) else ""
                await message.reply_photo(
                    photo=handle,
                    caption=f"候选 {index + 1}/{len(candidates)}{marker}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            f"✅ 设为主图 {index + 1}",
                            callback_data=f"v3smp|manual_cover_pick|{index}",
                        )
                    ]]),
                )

    async def prepare_manual_preview(self, message: Any, context: Any, *, review_id: str, offer_id: str, style: str | None = None, advance_cover: bool = False) -> None:
        state = context.user_data.setdefault(NEW_LISTING_STATE_KEY, {})
        detail = self.workflow.review_detail(review_id)
        detail = self._repair_manual_layout_if_needed(detail, state)
        blockers, media = await self._manual_blockers(detail)
        if blockers:
            await self.show_manual_confirmation(message, context)
            return
        candidates = self._manual_cover_candidates(media)
        if not candidates:
            await self.show_manual_confirmation(message, context)
            return
        selected_cover = str(state.get("selected_cover_path") or "").strip()
        if selected_cover not in candidates:
            selected_cover = ""
        if advance_cover:
            current_index = candidates.index(selected_cover) if selected_cover in candidates else int(state.get("cover_index") or 0) % len(candidates)
            current_index = (current_index + 1) % len(candidates)
            selected_cover = candidates[current_index]
            state["cover_index"] = current_index
            state["selected_cover_path"] = selected_cover
        elif selected_cover:
            state["cover_index"] = candidates.index(selected_cover)
        else:
            state["cover_index"] = 0
            selected_cover = candidates[0]
        state["cover_candidates"] = list(candidates)
        cover_path = selected_cover
        effective_style = str(style or state.get("cover_style") or "").strip() or None
        if style:
            state["cover_style"] = str(style)
        if str(detail.review.get("review_status") or "") != "approved":
            await asyncio.to_thread(self.workflow.approve_review, review_id=review_id, operator_user_id="system:manual_preview")

        original_status = str(detail.listing.get("inventory_status") or "pending")
        status_override = "active" if original_status == "pending" else None
        package = await asyncio.to_thread(
            self.workflow.build_package_for_review,
            review_id=review_id,
            cover_style=effective_style,
            manual_cover_path=cover_path,
            adviser_copy_override=self._adviser_override(state),
            inventory_status_override=status_override,
        )

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
                    [
                        InlineKeyboardButton("🖼 选主图", callback_data="v3smp|manual_cover"),
                        InlineKeyboardButton("🎨 选封面格式", callback_data="v3smp|manual_templates"),
                    ],
                    [InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")],
                ]),
            )

    def _preview_ready_rows(self, *, limit: int = 10) -> list[dict[str, Any]]:
        import sqlite3

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT a.offer_id,a.package_id,a.listing_id,a.updated_at,
                          l.public_listing_id,l.display_title,p.status AS package_status
                   FROM publisher_auto_items_v3 a
                   LEFT JOIN listings_v3 l ON l.listing_id=a.listing_id
                   LEFT JOIN publication_packages_v3 p ON p.package_id=a.package_id
                   WHERE a.state='preview_ready' AND a.ignored=0
                     AND COALESCE(a.package_id,'')<>''
                   ORDER BY a.updated_at DESC LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def _preview_ready_package(self, offer_id: str) -> tuple[str, str]:
        import sqlite3

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """SELECT package_id, offer_id FROM publisher_auto_items_v3
                   WHERE offer_id=? AND state='preview_ready' AND ignored=0
                     AND COALESCE(package_id,'')<>'' LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
        if row is None:
            raise ValueError("preview_ready_not_found")
        return str(row[0]), str(row[1])

    async def show_preview_ready(self, message: Any) -> None:
        rows = self._preview_ready_rows()
        if not rows:
            await message.reply_text(
                "<b>📤 待确认发布</b>\n\n当前没有可继续发布的预览包。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
        lines = ["<b>📤 待确认发布</b>", "", "这些预览已冻结在库里，即使会话丢失也可直接发布。", ""]
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or "")
            label = " · ".join(value for value in (public_id, title) if value)[:58]
            offer_id = str(row.get("offer_id") or "")
            callback = f"v3smp|prsend|{offer_id}"
            if len(callback.encode("utf-8")) > 64:
                continue
            lines.append(f"• {escape(public_id)}｜包状态 {escape(str(row.get('package_status') or ''))}")
            buttons.append([InlineKeyboardButton(f"📤 {label}", callback_data=callback)])
        if not buttons:
            lines.append("当前记录的 offer id 过长，请重新生成预览后再发。")
        buttons.append(self.home_row())
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def _deliver_preview_package(self, message: Any, context: Any, *, package_id: str, offer_id: str) -> None:
        package = self.workflow.package(package_id)
        status = str(getattr(package, "status", "") or "")
        if status == "package_ready" or not status:
            package = await asyncio.to_thread(
                self.workflow.approve_package,
                package_id=package_id,
                approved_by="system:manual_publish",
            )
        elif status != "approved":
            raise ValueError(f"package_not_publishable:{status}")
        if str(getattr(package, "offer_id", "") or "") != str(offer_id):
            raise ValueError("preview_offer_package_mismatch")
        result = await deliver_approved_package(
            coordinator=self.workflow.delivery,
            adapter=TelegramChannelAdapter(context.bot),
            package_id=str(getattr(package, "package_id", package_id)),
            channel_chat_id=self.channel_chat_id,
            inventory_status_override="active",
        )
        listing_id = str(getattr(package, "listing_id", "") or "")
        if listing_id:
            self.repository.set_listing_status(listing_id, "active")
        self.repository.set_item(
            offer_id,
            state="published",
            package_id=str(getattr(package, "package_id", package_id)),
            channel_message_id=str(result.publication.channel_message_id),
            origin="manual",
        )
        context.user_data.pop(NEW_LISTING_STATE_KEY, None)
        await message.reply_text(
            f"✅ 已发布到频道。频道消息：{escape(str(result.publication.channel_message_id))}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([self.home_row()]),
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
            manual_copy = "\n".join(lines)
            try:
                validate_adviser_copy(manual_copy)
            except ValueError as exc:
                await update.effective_message.reply_text(f"侨联说未通过校验：{exc}")
                return True
            if not isinstance(state, dict):
                context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
                return True
            state["adviser_mode"] = "manual"
            state["adviser_copy"] = manual_copy
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
        if raw == "v3smp|preview_ready":
            await self.show_preview_ready(query.message)
            return True
        if len(parts) == 3 and parts[1] == "prsend":
            try:
                package_id, offer_id = self._preview_ready_package(parts[2])
                await self._deliver_preview_package(
                    query.message,
                    context,
                    package_id=package_id,
                    offer_id=offer_id,
                )
            except Exception as exc:
                await query.message.reply_text(
                    "发布失败：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("📤 待确认发布", callback_data="v3smp|preview_ready")],
                            self.home_row(),
                        ]
                    ),
                )
            return True
        if raw in {"v3smp|manual_send", "v3smp|manual_cover", "v3smp|manual_templates"} or (
            len(parts) == 3 and parts[1] in {"manual_style", "manual_cover_pick"}
        ):
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
                    "⚠️ 这张预览已经失效。可从「待确认发布」继续，或重新生成预览。",
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("📤 待确认发布", callback_data="v3smp|preview_ready")],
                            [InlineKeyboardButton("👀 重新生成预览", callback_data="v3smp|manual_preview")],
                        ]
                    ),
                )
                return True
            if raw == "v3smp|manual_cover":
                await self.show_manual_cover_picker(query.message, context, review_id=current_review)
                return True
            if len(parts) == 3 and parts[1] == "manual_cover_pick":
                try:
                    index = int(parts[2])
                except ValueError:
                    index = -1
                candidates = tuple(state.get("cover_candidates") or ()) if isinstance(state, dict) else ()
                if index < 0 or index >= len(candidates) or not Path(str(candidates[index])).is_file():
                    await query.message.reply_text("⚠️ 封面候选已失效，请重新选择封面照片。")
                    return True
                state["cover_index"] = index
                state["selected_cover_path"] = str(candidates[index])
                await self.prepare_manual_preview(query.message, context, review_id=current_review, offer_id=current_offer)
                return True
            if raw == "v3smp|manual_templates":
                await query.message.reply_text(
                    "<b>🎨 选择封面格式</b>\n\n"
                    "四套风格；画幅跟主图自动走：\n"
                    "横图 → 1080×864 · 竖图 → 1200×1500\n"
                    "普通房默认「极简实拍」；别墅建议「黑金」。",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("极简实拍渐变（默认）", callback_data="v3smp|manual_style|premium_photo")],
                        [InlineKeyboardButton("右侧价格牌", callback_data="v3smp|manual_style|right_price")],
                        [InlineKeyboardButton("黑金高级感（别墅）", callback_data="v3smp|manual_style|black_gold")],
                        [InlineKeyboardButton("经典蓝卡", callback_data="v3smp|manual_style|classic_blue")],
                        self.home_row(),
                    ]),
                )
                return True
            if len(parts) == 3 and parts[1] == "manual_style":
                await self.prepare_manual_preview(query.message, context, review_id=current_review, offer_id=current_offer, style=parts[2])
                return True
            if raw == "v3smp|manual_send":
                try:
                    await self._deliver_preview_package(
                        query.message,
                        context,
                        package_id=current_package,
                        offer_id=current_offer,
                    )
                except Exception as exc:
                    await query.message.reply_text(
                        "发布失败：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup([self.home_row()]),
                    )
                return True
            return True
        return await super().handle_callback(update, context)


__all__ = ["PublisherAdviserAdminController"]