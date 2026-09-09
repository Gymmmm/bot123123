"""Simple administrator console for the V3 Publisher.

The controller deliberately hides review/package/delivery implementation terms.
It reuses the existing canonical worker, package builder, approval service and
delivery coordinator for every manual or automatic publication.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from html import escape
import hashlib
from pathlib import Path
import sqlite3
from typing import Any
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_reader import SourceReader
from v3_core.ingest.source_repository import SourceRepository
from v3_core.ops.runtime_state import RuntimeStateRepository
from v3_core.parser.worker import CanonicalWorker
from .autopilot import AutoPublishRepository, AutoPublishService, ERROR_LABELS
from .package_store import FrozenPackage
from .telegram_adapter import TelegramChannelAdapter, deliver_approved_package


SIMPLE_EDIT_STATE_KEY = "v3_simple_admin_edit"
NEW_LISTING_STATE_KEY = "v3_simple_new_listing"


class SimplePublisherAdminController:
    def __init__(
        self,
        *,
        db_path: str,
        repo_root: Path,
        workflow: Any,
        autopilot: AutoPublishService,
        repository: AutoPublishRepository,
        runtime: RuntimeStateRepository,
        user_bot_username: str,
        channel_chat_id: str,
        cover_output_dir: str,
    ):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.repo_root = Path(repo_root).resolve()
        self.workflow = workflow
        self.autopilot = autopilot
        self.repository = repository
        self.runtime = runtime
        self.user_bot_username = str(user_bot_username).lstrip("@")
        self.channel_chat_id = str(channel_chat_id)
        self.cover_output_dir = Path(cover_output_dir).expanduser().resolve()
        self.manual_media_dir = self.cover_output_dir.parent / "publisher_manual_uploads"
        self.manual_media_dir.mkdir(parents=True, exist_ok=True)
        self.sources = SourceRepository(self.db_path)
        self.source_reader = SourceReader(self.db_path)
        self.intake = IntakeService(self.sources, min_listing_images=self.repository.config().min_media)
        self.worker = CanonicalWorker(self.db_path)

    @staticmethod
    def home_keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("➕ 新建房源", callback_data="v3smp|new")],
                [InlineKeyboardButton("🟢 运行状态", callback_data="v3smp|runtime")],
                [InlineKeyboardButton("📣 每日广播", callback_data="v3bc")],
                [InlineKeyboardButton("📊 今日统计", callback_data="v3smp|stats")],
                [InlineKeyboardButton("🏠 房源状态", callback_data="v3smp|listings")],
                [InlineKeyboardButton("🕒 发帖时段", callback_data="v3smp|windows")],
                [InlineKeyboardButton("📡 采集源", callback_data="v3smp|sources")],
            ]
        )

    @staticmethod
    def home_row() -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton("🏠 返回首页", callback_data="v3h")]

    async def show_home(self, message: Any) -> None:
        cfg = self.repository.config()
        await message.reply_text(
            "<b>侨联 V3 发布机器人</b>\n\n"
            f"自动发布：<b>{'运行中' if cfg.enabled else '已暂停'}</b>\n"
            "正常采集房源会自动解析、检查、生成封面和文案，并在允许时段自动发布。\n"
            "不符合条件的房源只进入异常列表，不会降低门槛发布。",
            parse_mode=ParseMode.HTML,
            reply_markup=self.home_keyboard(),
        )

    @staticmethod
    def _fresh(row: dict[str, Any] | None, *, seconds: int = 120) -> bool:
        if not row:
            return False
        raw = str(row.get("last_seen_at") or "").strip()
        if not raw:
            return False
        try:
            seen = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return False
        return (datetime.now(timezone.utc) - seen).total_seconds() <= seconds

    async def show_runtime(self, message: Any) -> None:
        components = self.runtime.component_rows()
        cfg = self.repository.config()
        sources = self.runtime.source_rows()
        last_collected = max((str(row.get("last_collected_at") or "") for row in sources), default="") or "暂无"
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT MAX(published_at) FROM publication_instances WHERE platform='telegram'").fetchone()
        last_published = str(row[0] or "暂无") if row else "暂无"
        text = (
            "<b>🟢 运行状态</b>\n\n"
            f"自动采集：{'运行中' if self._fresh(components.get('collector')) else '已停止'}\n"
            f"自动解析：{'运行中' if self._fresh(components.get('canonical')) else '已停止'}\n"
            f"自动发布：{'运行中' if cfg.enabled and self._fresh(components.get('publisher')) else ('已暂停' if not cfg.enabled else '已停止')}\n"
            f"用户机器人：{'运行中' if self._fresh(components.get('user')) else '已停止'}\n\n"
            f"最近采集时间：{escape(last_collected)}\n"
            f"最近发布时间：{escape(last_published)}\n"
            f"当前待处理数量：{self.repository.queue_count()}\n"
            f"当前异常数量：{self.repository.exception_count()}"
        )
        button = "暂停自动发布" if cfg.enabled else "恢复自动发布"
        action = "pause" if cfg.enabled else "resume"
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton(button, callback_data=f"v3smp|{action}")],
                    [InlineKeyboardButton("刷新状态", callback_data="v3smp|runtime")],
                    self.home_row(),
                ]
            ),
        )

    async def show_stats(self, message: Any) -> None:
        stats = self.repository.today_stats()
        await message.reply_text(
            "<b>📊 今日统计</b>\n"
            "时区：Asia/Phnom_Penh\n\n"
            f"今日采集：{stats['collected']}\n"
            f"解析成功：{stats['parsed']}\n"
            f"自动发布：{stats['auto_published']}\n"
            f"手动发布：{stats['manual_published']}\n"
            f"重复跳过：{stats['duplicates']}\n"
            f"异常房源：{stats['exceptions']}\n"
            f"发布失败：{stats['publish_failed']}\n"
            f"当前可预约房源：{stats['bookable']}\n"
            f"已出租房源：{stats['rented']}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([self.home_row()]),
        )

    async def show_listing_categories(self, message: Any) -> None:
        await message.reply_text(
            "<b>🏠 房源状态</b>\n\n请选择要查看的房源：",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("当前可预约", callback_data="v3smp|ls|active")],
                    [InlineKeyboardButton("已出租", callback_data="v3smp|ls|rented")],
                    [InlineKeyboardButton("已下架", callback_data="v3smp|ls|offline")],
                    [InlineKeyboardButton(f"待处理异常 ({self.repository.exception_count()})", callback_data="v3smp|exceptions")],
                    [InlineKeyboardButton("最近发布", callback_data="v3smp|ls|recent")],
                    self.home_row(),
                ]
            ),
        )

    async def show_listing_rows(self, message: Any, category: str) -> None:
        rows = self.repository.listing_rows(category)
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or "房源")
            title = str(row.get("display_title") or row.get("project_name") or "未命名")
            buttons.append([
                InlineKeyboardButton(f"{public_id} · {title}"[:58], callback_data=f"v3smp|listing|{row['listing_id']}")
            ])
        buttons.append([InlineKeyboardButton("⬅️ 返回房源状态", callback_data="v3smp|listings")])
        buttons.append(self.home_row())
        await message.reply_text(
            f"<b>{escape({'active':'当前可预约','rented':'已出租','offline':'已下架','recent':'最近发布'}.get(category, category))}</b>\n\n"
            + ("请选择房源：" if rows else "当前没有房源。"),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    def _listing_detail(self, listing_id: str) -> dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT l.*,o.offer_id,o.monthly_rent_usd,o.offer_status
                   FROM listings_v3 l LEFT JOIN listing_offers o
                     ON o.listing_id=l.listing_id AND o.offer_type='rent' AND o.offer_status='active'
                   WHERE l.listing_id=? LIMIT 1""",
                (str(listing_id),),
            ).fetchone()
        if row is None:
            raise KeyError(listing_id)
        return dict(row)

    async def show_listing(self, message: Any, listing_id: str) -> None:
        row = self._listing_detail(listing_id)
        public_id = str(row.get("public_listing_id") or "")
        status = str(row.get("inventory_status") or "")
        status_label = {"active":"当前可预约","reserved":"当前可预约","rented":"已出租","inactive":"已下架","offline":"已下架"}.get(status, "待确认")
        rent = row.get("monthly_rent_usd")
        price = f"${int(rent):,}/月" if rent not in (None, "") else "未识别"
        rows: list[list[InlineKeyboardButton]] = []
        if public_id:
            rows.append([InlineKeyboardButton("查看房源", url=f"https://t.me/{self.user_bot_username}?start=property_{public_id}_details")])
        if status in {"active", "reserved"}:
            rows.append([InlineKeyboardButton("停止发布", callback_data=f"v3smp|status|inactive|{listing_id}")])
            rows.append([InlineKeyboardButton("标记已出租", callback_data=f"v3smp|status|rented|{listing_id}")])
            rows.append([InlineKeyboardButton("标记已下架", callback_data=f"v3smp|status|offline|{listing_id}")])
        else:
            rows.append([InlineKeyboardButton("恢复房源", callback_data=f"v3smp|status|active|{listing_id}")])
        offer_id = str(row.get("offer_id") or "")
        if offer_id:
            rows.append([InlineKeyboardButton("重新检查并发布", callback_data=f"v3smp|recheck|{offer_id}")])
        rows.append([InlineKeyboardButton("⬅️ 返回房源状态", callback_data="v3smp|listings")])
        rows.append(self.home_row())
        await message.reply_text(
            f"<b>{escape(public_id or listing_id)}</b>\n"
            f"{escape(str(row.get('display_title') or row.get('project_name') or '未命名房源'))}\n"
            f"状态：{escape(status_label)}\n"
            f"租金：{escape(price)}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def show_exceptions(self, message: Any) -> None:
        rows = self.repository.exception_rows(limit=20)
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or "房源")
            reason = str(row.get("reason_text") or ERROR_LABELS.get(str(row.get("reason_code") or ""), "异常"))
            buttons.append([
                InlineKeyboardButton(f"{public_id} · {reason}"[:58], callback_data=f"v3smp|exception|{row['offer_id']}")
            ])
        buttons.append([InlineKeyboardButton("⬅️ 返回房源状态", callback_data="v3smp|listings")])
        buttons.append(self.home_row())
        await message.reply_text(
            "<b>待处理异常</b>\n\n" + ("请选择房源：" if rows else "当前没有异常房源。"),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    def _exception_detail(self, offer_id: str) -> dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT a.*,l.public_listing_id,l.display_title,l.canonical_record_id,r.review_id,
                          c.source_post_id
                   FROM publisher_auto_items_v3 a
                   JOIN listings_v3 l ON l.listing_id=a.listing_id
                   JOIN review_items r ON r.offer_id=a.offer_id
                   JOIN canonical_records c ON c.canonical_record_id=l.canonical_record_id
                   WHERE a.offer_id=? ORDER BY r.updated_at DESC LIMIT 1""",
                (str(offer_id),),
            ).fetchone()
        if row is None:
            raise KeyError(offer_id)
        return dict(row)

    async def show_exception(self, message: Any, offer_id: str) -> None:
        row = self._exception_detail(offer_id)
        reason = str(row.get("reason_text") or ERROR_LABELS.get(str(row.get("reason_code") or ""), "异常"))
        await message.reply_text(
            f"<b>{escape(str(row.get('public_listing_id') or '异常房源'))}</b>\n"
            f"{escape(str(row.get('display_title') or '未命名房源'))}\n\n"
            f"原因：<b>{escape(reason)}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("查看原始房源", callback_data=f"v3smp|raw|{offer_id}")],
                    [InlineKeyboardButton("补充或修改资料", callback_data=f"v3edit|{row['review_id']}")],
                    [InlineKeyboardButton("补充图片", callback_data=f"v3smp|addmedia|{offer_id}")],
                    [InlineKeyboardButton("标记忽略", callback_data=f"v3smp|ignore|{offer_id}")],
                    [InlineKeyboardButton("重新检查并发布", callback_data=f"v3smp|recheck|{offer_id}")],
                    [InlineKeyboardButton("⬅️ 返回异常列表", callback_data="v3smp|exceptions")],
                    self.home_row(),
                ]
            ),
        )

    async def show_raw(self, message: Any, offer_id: str) -> None:
        row = self._exception_detail(offer_id)
        source = self.source_reader.source_post(int(row["source_post_id"]))
        text = str(source.get("raw_text") or "").strip() or "（原消息没有文字）"
        text = text[:3200]
        buttons: list[list[InlineKeyboardButton]] = []
        url = str(source.get("source_url") or "").strip()
        if url.startswith("https://"):
            buttons.append([InlineKeyboardButton("打开来源消息", url=url)])
        buttons.append([InlineKeyboardButton("⬅️ 返回异常房源", callback_data=f"v3smp|exception|{offer_id}")])
        buttons.append(self.home_row())
        await message.reply_text(
            "<b>原始房源</b>\n\n" + escape(text),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def show_windows(self, message: Any) -> None:
        cfg = self.repository.config()
        windows = self.repository.windows()
        lines = ["<b>🕒 发帖时段</b>", "", f"自动发帖：<b>{'已开启' if cfg.enabled else '已暂停'}</b>"]
        if windows:
            lines.append("当前时段：")
            lines.extend(f"• {escape(str(row['start_time']))}–{escape(str(row['end_time']))}" for row in windows)
        else:
            lines.append("当前没有允许自动发帖的时段。")
        buttons: list[list[InlineKeyboardButton]] = [
            [InlineKeyboardButton("➕ 增加时段", callback_data="v3smp|window_add")]
        ]
        for row in windows:
            buttons.append([
                InlineKeyboardButton(
                    f"删除 {row['start_time']}–{row['end_time']}",
                    callback_data=f"v3smp|window_del|{row['window_id']}",
                )
            ])
        buttons.append([
            InlineKeyboardButton("暂停全部自动发帖" if cfg.enabled else "恢复自动发帖", callback_data="v3smp|pause" if cfg.enabled else "v3smp|resume")
        ])
        buttons.append(self.home_row())
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def show_sources(self, message: Any) -> None:
        rows = self.runtime.source_rows()
        buttons: list[list[InlineKeyboardButton]] = []
        lines = ["<b>📡 采集源</b>", ""]
        for row in rows:
            enabled = bool(int(row.get("enabled") or 0))
            name = str(row.get("source_name") or "")
            lines.extend(
                [
                    f"<b>{escape(name)}</b> · {'启用' if enabled else '暂停'}",
                    f"最近采集：{escape(str(row.get('last_collected_at') or '暂无'))}",
                    f"今日采集：{int(row.get('today_count') or 0)}",
                    f"最近错误：{escape(str(row.get('last_error') or '无')[:120])}",
                    "",
                ]
            )
            buttons.append([
                InlineKeyboardButton(
                    ("暂停 " if enabled else "启用 ") + name,
                    callback_data=f"v3smp|source_toggle|{'0' if enabled else '1'}|{name}",
                )
            ])
        buttons.append([InlineKeyboardButton("刷新状态", callback_data="v3smp|sources")])
        buttons.append(self.home_row())
        if not rows:
            lines.append("当前还没有采集源运行记录。")
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def start_new_listing(self, message: Any, context: Any) -> None:
        session_id = "manual_" + uuid.uuid4().hex[:18]
        context.user_data[NEW_LISTING_STATE_KEY] = {
            "session_id": session_id,
            "text": "",
            "images": [],
            "source_post_pk": None,
            "offer_id": "",
            "review_id": "",
            "cover_index": 0,
        }
        await message.reply_text(
            "<b>➕ 新建房源</b>\n\n"
            "直接发送房源文字、图片、带文字的多张图片，或转发房源消息。\n"
            "系统会自动合并、解析和检查；资料满足发布条件后会直接显示最终预览。\n"
            "如果资料不完整，可以继续补发，不需要重新开始。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([self.home_row()]),
        )

    async def _download_photo(self, message: Any, context: Any) -> dict[str, Any] | None:
        photos = list(getattr(message, "photo", None) or [])
        if not photos:
            return None
        photo = photos[-1]
        target = self.manual_media_dir / f"{uuid.uuid4().hex}.jpg"
        telegram_file = await context.bot.get_file(photo.file_id)
        await telegram_file.download_to_drive(custom_path=str(target))
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        return {
            "local_path": str(target.resolve()),
            "file_hash": digest,
            "telegram_file_id": str(photo.file_id),
            "telegram_file_unique_id": str(photo.file_unique_id or ""),
        }

    async def consume_new_listing_message(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return False
        message = update.effective_message
        text = str(getattr(message, "caption", None) or getattr(message, "text", None) or "").strip()
        if text:
            state["text"] = text
        photo = await self._download_photo(message, context)
        if photo:
            images = state.setdefault("images", [])
            if photo["file_hash"] not in {str(x.get("file_hash") or "") for x in images if isinstance(x, dict)}:
                images.append(photo)
        await message.reply_text(
            f"已收到：文字 {'有' if state.get('text') else '无'} · 图片 {len(state.get('images') or [])} 张。\n正在自动检查…"
        )
        await self._materialize_manual(update, context)
        return True

    async def _materialize_manual(self, update: Any, context: Any) -> None:
        state = context.user_data.get(NEW_LISTING_STATE_KEY)
        if not isinstance(state, dict):
            return
        source = SourceIntake(
            source_type="admin_manual",
            source_name="publisher_admin",
            source_post_id=str(state["session_id"]),
            source_url="",
            source_author=str(update.effective_user.id),
            raw_text=str(state.get("text") or ""),
            raw_images=list(state.get("images") or []),
            raw_videos=[],
            meta={"origin": "publisher_admin", "telegram_user_id": int(update.effective_user.id)},
        )
        result = await asyncio.to_thread(self.intake.persist, source)
        state["source_post_pk"] = result.source_post_pk
        if result.source_post_pk is None:
            return
        worker_result = await asyncio.to_thread(self.worker.process_one, int(result.source_post_pk))
        if worker_result.status != "materialized":
            await update.effective_message.reply_text(
                "解析失败，房源没有发布。可以继续补充资料后再次发送。\n" + escape(worker_result.error[:500]),
                parse_mode=ParseMode.HTML,
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
            await update.effective_message.reply_text("房源信息不完整，暂时不能生成发布预览。可以继续补充资料。")
            return
        offer_id = str(row["offer_id"])
        review_id = str(row["review_id"])
        state["offer_id"], state["review_id"] = offer_id, review_id
        if str(row["offer_type"]) != "rent":
            self.repository.set_item(offer_id, state="exception", reason_code="sale_store_only", reason_text=ERROR_LABELS["sale_store_only"], origin="manual")
            await update.effective_message.reply_text("✅ 已识别为出售房源并保存归档。出售房源不会发布到租赁频道。", reply_markup=InlineKeyboardMarkup([self.home_row()]))
            return
        await self.prepare_manual_preview(update.effective_message, context, review_id=review_id, offer_id=offer_id)

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
        media = await asyncio.to_thread(self.workflow.review_media, review_id=review_id)
        state = context.user_data.setdefault(NEW_LISTING_STATE_KEY, {})
        gallery = tuple(media.gallery_paths)
        if advance_cover and gallery:
            state["cover_index"] = (int(state.get("cover_index") or 0) + 1) % len(gallery)
        index = int(state.get("cover_index") or 0) % max(1, len(gallery))
        cover_path = gallery[index] if gallery else None
        item = {
            "offer_type": detail.offer.get("offer_type"),
            "monthly_rent_usd": detail.offer.get("monthly_rent_usd"),
            "project_name": detail.listing.get("project_name"),
            "public_location_display": detail.listing.get("public_location_display"),
            "layout": detail.listing.get("layout"),
            "inventory_status": detail.listing.get("inventory_status"),
        }
        blockers = self.autopilot._strict_blockers(item, dict(detail.canonical.get("facts") or {}), media)
        if blockers:
            code = blockers[0]
            reason = ERROR_LABELS.get(code, code)
            self.repository.set_item(offer_id, state="exception", reason_code=code, reason_text=reason, origin="manual")
            await message.reply_text(
                f"当前不能发布：<b>{escape(reason)}</b>\n\n可以继续发送补充资料或图片，系统会自动重新检查。",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
            return
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
        state["offer_id"] = offer_id
        state["review_id"] = review_id
        await self.send_manual_preview(message, package, review_id=review_id, offer_id=offer_id)

    async def send_manual_preview(self, message: Any, package: FrozenPackage, *, review_id: str, offer_id: str) -> None:
        with Path(package.cover_path).open("rb") as handle:
            await message.reply_photo(
                photo=handle,
                caption=package.post_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("发布到频道", callback_data=f"v3smp|manual_send|{package.package_id}|{offer_id}")],
                        [
                            InlineKeyboardButton("更换封面图片", callback_data=f"v3smp|manual_cover|{review_id}|{offer_id}"),
                            InlineKeyboardButton("更换封面模板", callback_data=f"v3smp|manual_templates|{review_id}|{offer_id}"),
                        ],
                        self.home_row(),
                    ]
                ),
            )

    async def append_exception_media(self, message: Any, context: Any, offer_id: str) -> None:
        row = self._exception_detail(offer_id)
        source = self.source_reader.source_post(int(row["source_post_id"]))
        context.user_data[NEW_LISTING_STATE_KEY] = {
            "session_id": str(source["source_post_id"]),
            "text": str(source.get("raw_text") or ""),
            "images": list(source.get("raw_images") or []),
            "source_post_pk": int(source["id"]),
            "offer_id": offer_id,
            "review_id": str(row["review_id"]),
            "cover_index": 0,
        }
        await message.reply_text("请直接发送要补充的图片。收到后会自动合并并重新检查。")

    async def handle_text(self, update: Any, context: Any) -> bool:
        state = context.user_data.get(SIMPLE_EDIT_STATE_KEY)
        if not isinstance(state, dict):
            return False
        kind = str(state.get("kind") or "")
        text = str(update.effective_message.text or "").strip()
        if kind == "window":
            if "-" not in text:
                await update.effective_message.reply_text("请发送例如：09:00-12:00")
                return True
            start, end = [part.strip() for part in text.split("-", 1)]
            try:
                self.repository.add_window(start, end)
            except ValueError as exc:
                await update.effective_message.reply_text(str(exc))
                return True
            context.user_data.pop(SIMPLE_EDIT_STATE_KEY, None)
            await self.show_windows(update.effective_message)
            return True
        return False

    async def handle_callback(self, update: Any, context: Any) -> bool:
        query = update.callback_query
        if query is None:
            return False
        raw = str(query.data or "")
        if not raw.startswith("v3smp|"):
            return False
        parts = raw.split("|")
        action = parts[1] if len(parts) > 1 else ""
        if action == "new":
            await self.start_new_listing(query.message, context)
        elif action == "runtime":
            await self.show_runtime(query.message)
        elif action == "stats":
            await self.show_stats(query.message)
        elif action == "listings":
            await self.show_listing_categories(query.message)
        elif action == "ls" and len(parts) == 3:
            await self.show_listing_rows(query.message, parts[2])
        elif action == "listing" and len(parts) == 3:
            await self.show_listing(query.message, parts[2])
        elif action == "exceptions":
            await self.show_exceptions(query.message)
        elif action == "exception" and len(parts) == 3:
            await self.show_exception(query.message, parts[2])
        elif action == "raw" and len(parts) == 3:
            await self.show_raw(query.message, parts[2])
        elif action == "ignore" and len(parts) == 3:
            self.repository.ignore(parts[2])
            await query.message.reply_text("✅ 已忽略该异常房源。", reply_markup=InlineKeyboardMarkup([self.home_row()]))
        elif action == "addmedia" and len(parts) == 3:
            await self.append_exception_media(query.message, context, parts[2])
        elif action == "recheck" and len(parts) == 3:
            result = await self.autopilot.process_one(bot=context.bot, force_offer_id=parts[2])
            if result.status == "published":
                await query.message.reply_text(f"✅ 已重新检查并发布。频道消息：{escape(result.channel_message_id)}", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([self.home_row()]))
            elif result.status == "already_published":
                await query.message.reply_text("该房源已经发布，不会重复发送。", reply_markup=InlineKeyboardMarkup([self.home_row()]))
            else:
                await query.message.reply_text(f"未发布：{escape(result.reason_text or result.status)}", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([self.home_row()]))
        elif action == "status" and len(parts) == 4:
            status, listing_id = parts[2], parts[3]
            self.repository.set_listing_status(listing_id, status)
            row = self._listing_detail(listing_id)
            if status == "active" and row.get("offer_id"):
                self.repository.requeue(str(row["offer_id"]))
            await self.show_listing(query.message, listing_id)
        elif action == "windows":
            await self.show_windows(query.message)
        elif action == "window_add":
            context.user_data[SIMPLE_EDIT_STATE_KEY] = {"kind": "window"}
            await query.message.reply_text("发送新的发帖时段，例如：09:00-12:00")
        elif action == "window_del" and len(parts) == 3:
            self.repository.delete_window(parts[2])
            await self.show_windows(query.message)
        elif action == "pause":
            self.repository.set_enabled(False)
            await self.show_runtime(query.message)
        elif action == "resume":
            self.repository.set_enabled(True)
            await self.show_runtime(query.message)
        elif action == "sources":
            await self.show_sources(query.message)
        elif action == "source_toggle" and len(parts) >= 4:
            enabled = parts[2] == "1"
            source_name = "|".join(parts[3:])
            self.runtime.set_source_enabled(source_name, enabled)
            await self.show_sources(query.message)
        elif action == "manual_cover" and len(parts) == 4:
            await self.prepare_manual_preview(query.message, context, review_id=parts[2], offer_id=parts[3], advance_cover=True)
        elif action == "manual_templates" and len(parts) == 4:
            await query.message.reply_text(
                "选择封面模板：",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("右侧价格牌", callback_data=f"v3smp|manual_style|right_price|{parts[2]}|{parts[3]}")],
                        [InlineKeyboardButton("黑金模板", callback_data=f"v3smp|manual_style|black_gold|{parts[2]}|{parts[3]}")],
                        [InlineKeyboardButton("经典蓝卡", callback_data=f"v3smp|manual_style|classic_blue|{parts[2]}|{parts[3]}")],
                        self.home_row(),
                    ]
                ),
            )
        elif action == "manual_style" and len(parts) == 5:
            await self.prepare_manual_preview(query.message, context, review_id=parts[3], offer_id=parts[4], style=parts[2])
        elif action == "manual_send" and len(parts) == 4:
            package = await asyncio.to_thread(
                self.workflow.approve_package,
                package_id=parts[2],
                approved_by="system:manual_publish",
            )
            result = await deliver_approved_package(
                coordinator=self.workflow.delivery,
                adapter=TelegramChannelAdapter(context.bot),
                package_id=package.package_id,
                channel_chat_id=self.channel_chat_id,
            )
            self.repository.set_item(
                parts[3], state="published", package_id=package.package_id,
                channel_message_id=str(result.publication.channel_message_id), origin="manual",
            )
            context.user_data.pop(NEW_LISTING_STATE_KEY, None)
            await query.message.reply_text(
                f"✅ 已发布到频道。频道消息：{escape(str(result.publication.channel_message_id))}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self.home_row()]),
            )
        else:
            return False
        return True


__all__ = [
    "NEW_LISTING_STATE_KEY",
    "SIMPLE_EDIT_STATE_KEY",
    "SimplePublisherAdminController",
]
