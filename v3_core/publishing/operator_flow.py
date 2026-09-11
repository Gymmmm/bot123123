"""Task-oriented Telegram workflow for day-to-day publishing operators.

Operators provide Telegram material, verify parsed facts, optionally supplement
or correct them, preview the exact channel package, and publish.  Collector
exceptions and pipeline internals stay out of this UI.
"""
from __future__ import annotations

import asyncio
from html import escape
import hashlib
import os
from pathlib import Path
import sqlite3
from typing import Any
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.ingest.intake_service import SourceIntake
from v3_core.ingest.source_registry import AdminSourceRegistry
from v3_core.publishing.admin_operations import EDITABLE_FACT_FIELDS
from .autopilot import ERROR_LABELS
from .package_store import FrozenPackage
from .simple_admin import NEW_LISTING_STATE_KEY, SIMPLE_EDIT_STATE_KEY
from .simple_admin_production import ProductionSimplePublisherAdminController


FIELD_CODES = {
    "pn": "project_name",
    "pt": "property_type",
    "ar": "public_location_display",
    "ly": "layout",
    "sz": "size_sqm",
    "fl": "floor",
    "pr": "monthly_rent_usd",
    "dp": "deposit_payment_terms",
    "ct": "contract_term_display",
    "av": "available_date",
    "hi": "highlights",
}
FIELD_CODE_BY_NAME = {value: key for key, value in FIELD_CODES.items()}

DISPLAY_FIELDS = (
    ("project_name", "📍 项目"),
    ("public_location_display", "📌 区域"),
    ("layout", "🏠 户型"),
    ("property_type", "🏢 类型"),
    ("size_sqm", "📐 面积"),
    ("floor", "🏙 楼层"),
    ("monthly_rent_usd", "💵 租金"),
    ("deposit_payment_terms", "🔑 押付"),
    ("contract_term_display", "📅 租期"),
)
OPTIONAL_FIELDS = ("size_sqm", "floor", "deposit_payment_terms", "contract_term_display", "available_date")
BLOCKER_LABELS = {
    "missing_rent": "租金",
    "missing_location": "项目或区域",
    "missing_listing_info": "户型",
    "unreadable_media": "可用封面图片",
    "listing_not_publishable": "房态",
    "canonical_error": "关键房源资料",
}


class OperatorPublisherAdminController(ProductionSimplePublisherAdminController):
    @staticmethod
    def home_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("➕ 发布房源", callback_data="v3smp|new"),
                    InlineKeyboardButton("📢 广播中心", callback_data="v3bc"),
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
            "发送房源素材、发布广播、修改房态和管理采集源。",
            parse_mode=ParseMode.HTML,
            reply_markup=self.home_keyboard(),
        )

    async def show_listing_categories(self, message: Any) -> None:
        await message.reply_text(
            "<b>🔵 房态管理</b>\n\n请选择要处理的房源状态：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🟢 当前可预约", callback_data="v3smp|ls|active"),
                        InlineKeyboardButton("🟡 已有预约", callback_data="v3smp|ls|reserved"),
                    ],
                    [
                        InlineKeyboardButton("🔴 已租出", callback_data="v3smp|ls|rented"),
                        InlineKeyboardButton("⚫ 已下架", callback_data="v3smp|ls|offline"),
                    ],
                    [InlineKeyboardButton("🕘 最近发布", callback_data="v3smp|ls|recent")],
                    self.home_row(),
                ]
            ),
        )

    def _source_registry(self) -> AdminSourceRegistry:
        base = str(os.getenv("COLLECTOR_SOURCES_JSON") or (self.repo_root / "sources.json"))
        return AdminSourceRegistry(base_path=base, db_path=self.db_path)

    async def show_sources(self, message: Any) -> None:
        configured = self._source_registry().rows()
        runtime = {str(row.get("source_name") or ""): row for row in self.runtime.source_rows()}
        lines = ["<b>📡 采集源</b>", "", "这里只管理采集来源；后台采集、过滤、去重和发布条件保持不变。", ""]
        buttons: list[list[InlineKeyboardButton]] = []
        for row in configured:
            name = str(row.get("source_name") or "")
            state = runtime.get(name, {})
            enabled = bool(int(state.get("enabled", 1) or 0))
            lines.append(f"{'✅' if enabled else '⏸'} {escape(name)}")
            buttons.append(
                [
                    InlineKeyboardButton(
                        "⏸ 暂停" if enabled else "▶️ 启用",
                        callback_data=f"v3smp|source_toggle|{'0' if enabled else '1'}|{name}",
                    ),
                    InlineKeyboardButton("🗑 删除", callback_data=f"v3smp|source_remove|{name}"),
                ]
            )
        if not configured:
            lines.append("当前没有采集源。")
        buttons.append([InlineKeyboardButton("➕ 添加采集源", callback_data="v3smp|source_add")])
        buttons.append(self.home_row())
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def start_new_listing(self, message: Any, context: Any) -> None:
        old = context.user_data.pop(NEW_LISTING_STATE_KEY, None)
        if isinstance(old, dict):
            task = old.get("_album_task")
            if task and not task.done():
                task.cancel()
        context.user_data[NEW_LISTING_STATE_KEY] = {
            "session_id": "manual_" + uuid.uuid4().hex[:18],
            "text": "",
            "images": [],
            "videos": [],
            "source_post_pk": None,
            "offer_id": "",
            "review_id": "",
            "cover_index": 0,
            "mode": "collecting",
        }
        await message.reply_text(
            "<b>➕ 发布房源</b>\n\n"
            "直接发送房源素材和文字：\n"
            "• 单张图片 + 文字\n"
            "• 多张图片相册 + 文字\n"
            "• 单条视频 + 文字\n\n"
            "系统会自动解析并整理房源资料，再让你确认。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ 取消发布", callback_data="v3smp|manual_cancel")], self.home_row()]
            ),
        )

    async def _download_photo(self, message: Any, context: Any) -> dict[str, Any] | None:
        photos = list(getattr(message, "photo", None) or [])
        if not photos:
            return None
        photo = photos[-1]
        target = self.manual_media_dir / f"{uuid.uuid4().hex}.jpg"
        telegram_file = await context.bot.get_file(photo.file_id)
        await telegram_file.download_to_drive(custom_path=str(target))
        return {
            "local_path": str(target.resolve()),
            "file_hash": hashlib.sha256(target.read_bytes()).hexdigest(),
            "telegram_file_id": str(photo.file_id),
            "telegram_file_unique_id": str(photo.file_unique_id or ""),
        }

    async def _download_video(self, message: Any, context: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        video = getattr(message, "video", None)
        if video is None:
            return None, None
        suffix = Path(str(getattr(video, "file_name", "") or "video.mp4")).suffix or ".mp4"
        target = self.manual_media_dir / f"{uuid.uuid4().hex}{suffix}"
        telegram_file = await context.bot.get_file(video.file_id)
        await telegram_file.download_to_drive(custom_path=str(target))
        video_item = {
            "local_path": str(target.resolve()),
            "file_hash": hashlib.sha256(target.read_bytes()).hexdigest(),
            "telegram_file_id": str(video.file_id),
            "telegram_file_unique_id": str(video.file_unique_id or ""),
        }
        thumb_item = None
        thumb = getattr(video, "thumbnail", None)
        if thumb is not None:
            thumb_path = self.manual_media_dir / f"{uuid.uuid4().hex}_video.jpg"
            thumb_file = await context.bot.get_file(thumb.file_id)
            await thumb_file.download_to_drive(custom_path=str(thumb_path))
            thumb_item = {
                "local_path": str(thumb_path.resolve()),
                "file_hash": hashlib.sha256(thumb_path.read_bytes()).hexdigest(),
                "telegram_file_id": str(thumb.file_id),
                "telegram_file_unique_id": str(thumb.file_unique_id or ""),
                "from_video_thumbnail": True,
            }
        return video_item, thumb_item

    @staticmethod
    def _append_unique(target: list[dict[str, Any]], item: dict[str, Any] | None) -> None:
        if not item:
            return
        digest = str(item.get("file_hash") or "")
        if digest and digest in {str(x.get("file_hash") or "") for x in target if isinstance(x, dict)}:
            return
        target.append(item)

    async def consume_new_listing_message(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return False
        message = update.effective_message
        text = str(getattr(message, "caption", None) or getattr(message, "text", None) or "").strip()
        if text:
            existing = str(state.get("text") or "").strip()
            state["text"] = text if not existing else existing + "\n" + text

        photo = await self._download_photo(message, context)
        self._append_unique(state.setdefault("images", []), photo)
        video, thumbnail = await self._download_video(message, context)
        self._append_unique(state.setdefault("videos", []), video)
        self._append_unique(state.setdefault("images", []), thumbnail)

        if not text and photo is None and video is None:
            return False

        media_group_id = str(getattr(message, "media_group_id", None) or "")
        if media_group_id:
            previous = state.get("_album_task")
            if previous and not previous.done():
                previous.cancel()
            state["_album_task"] = asyncio.create_task(
                self._flush_album_after_delay(
                    message=message,
                    context=context,
                    user_id=int(update.effective_user.id),
                    session_id=str(state.get("session_id") or ""),
                )
            )
            return True

        await message.reply_text(
            f"已收到：图片 {len(state.get('images') or [])} 张 · 视频 {len(state.get('videos') or [])} 条。\n正在整理房源资料…"
        )
        await self._materialize_manual(message, context, user_id=int(update.effective_user.id))
        return True

    async def _flush_album_after_delay(self, *, message: Any, context: Any, user_id: int, session_id: str) -> None:
        try:
            await asyncio.sleep(1.4)
        except asyncio.CancelledError:
            return
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or str(state.get("session_id") or "") != session_id:
            return
        await message.reply_text(
            f"相册已收齐：图片 {len(state.get('images') or [])} 张。\n正在整理房源资料…"
        )
        await self._materialize_manual(message, context, user_id=user_id)

    async def _materialize_manual(self, message: Any, context: Any, *, user_id: int) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return
        source = SourceIntake(
            source_type="admin_manual",
            source_name="publisher_admin",
            source_post_id=str(state["session_id"]),
            source_url="",
            source_author=str(user_id),
            raw_text=str(state.get("text") or ""),
            raw_images=list(state.get("images") or []),
            raw_videos=list(state.get("videos") or []),
            meta={"origin": "publisher_admin", "telegram_user_id": int(user_id)},
        )
        result = await asyncio.to_thread(self.intake.persist, source)
        state["source_post_pk"] = result.source_post_pk
        if result.source_post_pk is None:
            return
        worker_result = await asyncio.to_thread(self.worker.process_one, int(result.source_post_pk))
        if worker_result.status != "materialized":
            await message.reply_text(
                "暂时没能整理出完整房源资料。可以继续发送文字或素材补充。",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("➕ 继续补充", callback_data="v3smp|manual_supplement")], self.home_row()]
                ),
            )
            return
        self.repository.sync_candidates()
        with self.repository._connect() as conn:
            row = conn.execute(
                """SELECT a.offer_id,a.review_id,o.offer_type FROM publisher_auto_items_v3 a
                   JOIN listing_offers o ON o.offer_id=a.offer_id
                   WHERE a.listing_id=? ORDER BY CASE o.offer_type WHEN 'rent' THEN 0 ELSE 1 END LIMIT 1""",
                (worker_result.listing_id,),
            ).fetchone()
        if row is None:
            await message.reply_text("暂时没能识别出可发布的租赁信息，可以继续补充文字。")
            return
        state["offer_id"] = str(row["offer_id"])
        state["review_id"] = str(row["review_id"])
        state["mode"] = "confirm"
        if str(row["offer_type"]) != "rent":
            self.repository.set_item(
                str(row["offer_id"]), state="exception", reason_code="sale_store_only",
                reason_text=ERROR_LABELS["sale_store_only"], origin="manual",
            )
            await message.reply_text(
                "✅ 已识别为出售房源并保存。出售房源不会发布到租赁频道。",
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            context.user_data.pop(NEW_LISTING_STATE_KEY, None)
            return
        await self.show_manual_confirmation(message, context)

    def _manual_values(self, detail: Any) -> dict[str, Any]:
        facts = dict(detail.canonical.get("facts") or {})
        listing = dict(detail.listing or {})
        offer = dict(detail.offer or {})
        values = dict(facts)
        values.update({key: value for key, value in listing.items() if value not in (None, "")})
        for key in ("monthly_rent_usd", "deposit_payment_terms", "contract_term_display"):
            if offer.get(key) not in (None, ""):
                values[key] = offer.get(key)
        return values

    async def _manual_blockers(self, detail: Any) -> tuple[list[str], Any]:
        media = await asyncio.to_thread(self.workflow.review_media, review_id=str(detail.review["review_id"]))
        item = {
            "offer_type": detail.offer.get("offer_type"),
            "monthly_rent_usd": detail.offer.get("monthly_rent_usd"),
            "project_name": detail.listing.get("project_name"),
            "public_location_display": detail.listing.get("public_location_display"),
            "layout": detail.listing.get("layout"),
            "inventory_status": detail.listing.get("inventory_status"),
        }
        blockers = self.autopilot._strict_blockers(item, dict(detail.canonical.get("facts") or {}), media)
        # Manual Telegram publishing explicitly accepts one photo/video.  This
        # exemption is local to the operator flow; collector min-media remains unchanged.
        blockers = [code for code in blockers if code != "insufficient_media"]
        return blockers, media

    @staticmethod
    def _display_value(name: str, value: Any) -> str:
        if value in (None, ""):
            return "未识别"
        if name == "monthly_rent_usd":
            try:
                return f"${int(float(value)):,}/月"
            except (TypeError, ValueError):
                return str(value)
        if name == "size_sqm":
            return f"{value}㎡"
        return str(value)

    async def show_manual_confirmation(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        try:
            blockers, _media = await self._manual_blockers(detail)
        except Exception:
            blockers = ["unreadable_media"]
        optional_missing = [name for name in OPTIONAL_FIELDS if values.get(name) in (None, "")]
        lines = ["<b>🏠 房源资料已整理</b>", ""]
        for name, label in DISPLAY_FIELDS:
            lines.append(f"{label}：{escape(self._display_value(name, values.get(name)))}")
        lines.extend(
            [
                "",
                f"📷 图片：{len(state.get('images') or [])} 张",
                f"🎬 视频：{len(state.get('videos') or [])} 条",
            ]
        )
        if blockers:
            required = "、".join(BLOCKER_LABELS.get(code, ERROR_LABELS.get(code, code)) for code in blockers)
            lines.extend(["", f"🔴 发布前需要补充：{escape(required)}"])
        elif optional_missing:
            labels = {name: label.split(" ", 1)[-1] for name, label in DISPLAY_FIELDS}
            lines.extend(
                ["", "✅ 当前资料已经可以发布", "💡 可选补充：" + "、".join(labels.get(name, name) for name in optional_missing)]
            )
        else:
            lines.extend(["", "✅ 房源资料已满足发布条件。"])

        buttons: list[list[InlineKeyboardButton]] = []
        if blockers or optional_missing:
            buttons.append(
                [
                    InlineKeyboardButton("➕ 补充资料", callback_data="v3smp|manual_supplement"),
                    InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit"),
                ]
            )
        else:
            buttons.append([InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit")])
        if not blockers:
            buttons.append([InlineKeyboardButton("👀 生成预览", callback_data="v3smp|manual_preview")])
        buttons.append([InlineKeyboardButton("❌ 取消", callback_data="v3smp|manual_cancel")])
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def show_supplement(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        missing = [name for name in OPTIONAL_FIELDS if values.get(name) in (None, "")]
        try:
            blockers, _media = await self._manual_blockers(detail)
        except Exception:
            blockers = ["unreadable_media"]
        lines = ["<b>➕ 补充资料</b>", ""]
        if blockers:
            lines.append("发布前还需要：" + "、".join(BLOCKER_LABELS.get(code, ERROR_LABELS.get(code, code)) for code in blockers))
        if missing:
            label_map = {name: label.split(" ", 1)[-1] for name, label in DISPLAY_FIELDS}
            lines.append("可选补充：" + "、".join(label_map.get(name, name) for name in missing))
        lines.extend(
            [
                "",
                "可以直接发一句话，例如：",
                "<code>面积120㎡，18楼，押2付1</code>",
                "",
                "也可以继续发送文字、单图、多图相册或单条视频。",
            ]
        )
        state["mode"] = "supplement"
        await message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ 不再补充", callback_data="v3smp|manual_back_confirm")],
                    [InlineKeyboardButton("❌ 取消发布", callback_data="v3smp|manual_cancel")],
                ]
            ),
        )

    async def show_manual_edit_menu(self, message: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict) or not state.get("review_id"):
            return
        detail = self.workflow.review_detail(str(state["review_id"]))
        values = self._manual_values(detail)
        rows: list[list[InlineKeyboardButton]] = []
        current: list[InlineKeyboardButton] = []
        for name, label in DISPLAY_FIELDS:
            code = FIELD_CODE_BY_NAME.get(name)
            if not code:
                continue
            short_label = label.split(" ", 1)[-1]
            value = self._display_value(name, values.get(name))
            current.append(InlineKeyboardButton(f"{short_label}：{value}"[:28], callback_data=f"v3smp|manual_field|{code}"))
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")])
        await message.reply_text(
            "<b>✏️ 修改资料</b>\n\n选择要修改的项目，然后直接发送新值。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def prepare_manual_preview(
        self,
        message: Any,
        context: Any,
        *,
        review_id: str,
        offer_id: str,
        style: str | None = None,
        advance_cover: bool = False,
    ) -> None:
        detail = self.workflow.review_detail(review_id)
        blockers, media = await self._manual_blockers(detail)
        if blockers:
            await self.show_manual_confirmation(message, context)
            return
        state = context.user_data.setdefault(NEW_LISTING_STATE_KEY, {})
        gallery = tuple(media.gallery_paths)
        if advance_cover and gallery:
            state["cover_index"] = (int(state.get("cover_index") or 0) + 1) % len(gallery)
        index = int(state.get("cover_index") or 0) % max(1, len(gallery))
        cover_path = gallery[index] if gallery else None
        if str(detail.review.get("review_status") or "") != "approved":
            await asyncio.to_thread(
                self.workflow.approve_review,
                review_id=review_id,
                operator_user_id="system:manual_preview",
            )
        package = await asyncio.to_thread(
            self.workflow.build_package_for_review,
            review_id=review_id,
            cover_style=style,
            manual_cover_path=cover_path,
        )
        self.repository.set_item(offer_id, state="preview_ready", package_id=package.package_id, origin="manual")
        state["package_id"] = package.package_id
        state["mode"] = "preview"
        await self.send_manual_preview(message, package, review_id=review_id, offer_id=offer_id)

    async def send_manual_preview(self, message: Any, package: FrozenPackage, *, review_id: str, offer_id: str) -> None:
        with Path(package.cover_path).open("rb") as handle:
            await message.reply_photo(
                photo=handle,
                caption=package.post_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("✅ 发布到 Telegram", callback_data=f"v3smp|manual_send|{package.package_id}|{offer_id}")],
                        [
                            InlineKeyboardButton("✏️ 修改资料", callback_data="v3smp|manual_edit"),
                            InlineKeyboardButton("🖼 调整封面", callback_data=f"v3smp|manual_cover|{review_id}|{offer_id}"),
                        ],
                        [InlineKeyboardButton("⬅️ 返回资料确认", callback_data="v3smp|manual_back_confirm")],
                        [InlineKeyboardButton("❌ 取消", callback_data="v3smp|manual_cancel")],
                    ]
                ),
            )

    async def handle_text(self, update: Any, context: Any) -> bool:
        edit = context.user_data.get(SIMPLE_EDIT_STATE_KEY)
        if isinstance(edit, dict) and str(edit.get("kind") or "") == "manual_field":
            raw = str(update.effective_message.text or "").strip()
            if not raw:
                await update.effective_message.reply_text("请发送新值。")
                return True
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            if not isinstance(state, dict) or not state.get("review_id"):
                context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
                return True
            await asyncio.to_thread(
                self.workflow.edit_review_field,
                review_id=str(state["review_id"]),
                field_name=str(edit["field_name"]),
                value=raw,
                operator_user_id=str(update.effective_user.id),
            )
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text("✅ 已修改，正在重新整理。")
            await self.show_manual_confirmation(update.effective_message, context)
            return True
        if isinstance(edit, dict) and str(edit.get("kind") or "") == "source_add":
            raw = str(update.effective_message.text or "").strip()
            try:
                row = self._source_registry().add(raw)
            except ValueError as exc:
                await update.effective_message.reply_text(str(exc))
                return True
            self.runtime.ensure_source(str(row["source_name"]), enabled=True)
            self.runtime.set_source_enabled(str(row["source_name"]), True)
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await update.effective_message.reply_text("✅ 采集源已添加，采集器会自动加载。")
            await self.show_sources(update.effective_message)
            return True
        return await super().handle_text(update, context)

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = update.callback_query
        if query is None:
            return False
        raw = str(query.data or "")
        if not raw.startswith("v3smp|"):
            return False
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else ""
        if action == "manual_supplement":
            await self.show_supplement(query.message, context)
            return True
        if action == "manual_back_confirm":
            await self.show_manual_confirmation(query.message, context)
            return True
        if action == "manual_edit":
            await self.show_manual_edit_menu(query.message, context)
            return True
        if action == "manual_field" and len(parts) == 3:
            field_name = FIELD_CODES.get(parts[2])
            if not field_name:
                return True
            label = EDITABLE_FACT_FIELDS.get(field_name, (field_name, "text"))[0]
            context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "manual_field", "field_name": field_name}
            await query.message.reply_text(f"发送新的“{label}”值：")
            return True
        if action == "manual_preview":
            state = context.user_data.get(NEW_LISTING_STATE_KEY)
            if isinstance(state, dict) and state.get("review_id") and state.get("offer_id"):
                await self.prepare_manual_preview(
                    query.message,
                    context,
                    review_id=str(state["review_id"]),
                    offer_id=str(state["offer_id"]),
                )
            return True
        if action == "manual_cancel":
            state = context.user_data.pop(NEW_LISTING_STATE_KEY, None)
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            if isinstance(state, dict):
                task = state.get("_album_task")
                if task and not task.done():
                    task.cancel()
            await query.message.reply_text("已取消本次房源发布。", reply_markup=InlineKeyboardMarkup([self.home_row()]))
            return True
        if action == "source_add":
            context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "source_add"}
            await query.message.reply_text(
                "<b>➕ 添加采集源</b>\n\n发送频道 @username、t.me/username 或频道数字 ID。\n只增加来源，不会改变后台采集条件。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return True
        if action == "source_remove" and len(parts) >= 3:
            name = "|".join(parts[2:])
            await query.message.reply_text(
                f"确认删除采集源 <b>{escape(name)}</b>？",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("🗑 确认删除", callback_data=f"v3smp|source_remove_yes|{name}")],
                        [InlineKeyboardButton("⬅️ 返回采集源", callback_data="v3smp|sources")],
                    ]
                ),
            )
            return True
        if action == "source_remove_yes" and len(parts) >= 3:
            name = "|".join(parts[2:])
            self._source_registry().remove(name)
            self.runtime.set_source_enabled(name, False)
            await query.message.reply_text("✅ 已删除采集源。")
            await self.show_sources(query.message)
            return True
        return await super().handle_callback(update, context)


__all__ = ["OperatorPublisherAdminController"]
