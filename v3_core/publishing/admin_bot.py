"""Production-shaped V3 Publisher admin bot.

The bot remains private/admin-only and Telegram-facing only. Business mutations
live in ``PublisherWorkflowService`` / ``PublisherAdminOperations``.  There is
no legacy Draft model, no runtime monkey patch, and no discussion-thread path.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from html import escape
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from v3_core.ingest.source_reader import SourceReader
from v3_core.media.cover_service import CoverRenderService
from v3_core.media.service import MediaPreparationService
from v3_core.publishing.admin_operations import EDITABLE_FACT_FIELDS
from v3_core.publishing.admin_workflow import PublisherWorkflowService, ReviewDetail
from v3_core.publishing.delivery_coordinator import PublicationDeliveryCoordinator
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_service import PackageApprovalService, PackageBuildService
from v3_core.publishing.package_store import FrozenPackage, FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.publishing.telegram_adapter import TelegramChannelAdapter, deliver_approved_package
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

PAGE_SIZE = 8
EDIT_STATE_KEY = "v3_publisher_edit"
MEDIA_STATE_KEY = "v3_publisher_media"

FIELD_CODES = {
    "pn": "project_name",
    "pa": "project_alias",
    "pt": "property_type",
    "ar": "public_location_display",
    "ly": "layout",
    "sz": "size_sqm",
    "fl": "floor",
    "pr": "monthly_rent_usd",
    "sp": "sale_price_usd",
    "dp": "deposit_payment_terms",
    "ct": "contract_term_display",
    "av": "available_date",
    "hi": "highlights",
}
FIELD_CODE_BY_NAME = {value: key for key, value in FIELD_CODES.items()}

REVIEW_STATUS_LABELS = {
    "pending": "待审核",
    "approved": "已批准事实",
    "hold": "暂缓",
    "rejected": "已拒绝",
}
PACKAGE_STATUS_LABELS = {
    "package_ready": "待冻结发布包",
    "approved": "待发布",
    "published": "已发布",
    "superseded": "已替换",
}
DELIVERY_STATE_LABELS = {
    "prepared": "待发送",
    "sending": "发送中",
    "sent": "已发送待提交",
    "committed": "已提交",
    "failed_before_send": "发送前失败",
    "unknown": "状态未知",
}


@dataclass(frozen=True)
class PublisherAdminSettings:
    token: str
    admin_ids: frozenset[int]
    db_path: str
    user_bot_username: str
    channel_chat_id: str
    cover_output_dir: str


def _parse_admin_ids(raw: str) -> frozenset[int]:
    result: set[int] = set()
    for part in str(raw or "").split(","):
        value = part.strip()
        if value:
            result.add(int(value))
    return frozenset(result)


def load_settings() -> PublisherAdminSettings:
    token = str(os.getenv("PUBLISHER_BOT_TOKEN", "")).strip()
    admins = _parse_admin_ids(os.getenv("ADMIN_IDS", ""))
    channel = str(os.getenv("CHANNEL_ID", "")).strip()
    user_bot = (
        str(os.getenv("DEEPLINK_BOT_USERNAME", "")).strip().lstrip("@")
        or str(os.getenv("USER_BOT_USERNAME", "")).strip().lstrip("@")
    )
    db = Path(os.getenv("DB_PATH") or os.getenv("SQLITE_PATH") or REPO_ROOT / "data/qiaolian_dual_bot.db")
    if not db.is_absolute():
        db = (REPO_ROOT / db).resolve()
    cover_dir = Path(os.getenv("V3_COVER_OUTPUT_DIR") or (REPO_ROOT / "media" / "covers_v3"))
    if not cover_dir.is_absolute():
        cover_dir = (REPO_ROOT / cover_dir).resolve()
    missing = []
    if not token:
        missing.append("PUBLISHER_BOT_TOKEN")
    if not admins:
        missing.append("ADMIN_IDS")
    if not channel:
        missing.append("CHANNEL_ID")
    if not user_bot:
        missing.append("DEEPLINK_BOT_USERNAME/USER_BOT_USERNAME")
    if missing:
        raise RuntimeError("missing_v3_publisher_settings:" + ",".join(missing))
    return PublisherAdminSettings(
        token=token,
        admin_ids=admins,
        db_path=str(db),
        user_bot_username=user_bot,
        channel_chat_id=channel,
        cover_output_dir=str(cover_dir),
    )


def build_workflow(settings: PublisherAdminSettings) -> PublisherWorkflowService:
    reader = InventoryReader(settings.db_path)
    inventory = InventoryRepository(settings.db_path)
    source_reader = SourceReader(settings.db_path)
    media = MediaPreparationService(source_reader)
    covers = CoverRenderService(reader=reader, output_dir=settings.cover_output_dir)
    packages = FrozenPackageStore(settings.db_path)
    package_builder = PackageBuildService(
        reader=reader,
        store=packages,
        user_bot_username=settings.user_bot_username,
    )
    package_approver = PackageApprovalService(
        reader=reader,
        inventory_repository=inventory,
        store=packages,
    )
    deliveries = PublicationDeliveryStateRepository(settings.db_path)
    publications = PublicationInstanceRepository(settings.db_path)
    delivery = PublicationDeliveryCoordinator(
        reader=reader,
        packages=packages,
        deliveries=deliveries,
        publications=publications,
    )
    return PublisherWorkflowService(
        reader=reader,
        inventory=inventory,
        media=media,
        covers=covers,
        packages=packages,
        package_builder=package_builder,
        package_approver=package_approver,
        delivery=delivery,
    )


def _money(offer: dict[str, Any]) -> str:
    if str(offer.get("offer_type") or "") == "rent":
        value = offer.get("monthly_rent_usd")
        return f"${int(value):,}/月" if value not in (None, "") else "未识别租金"
    value = offer.get("sale_price_usd")
    return f"${int(value):,}" if value not in (None, "") else "未识别售价"


def _short(value: Any, limit: int = 28) -> str:
    text = str(value if value not in (None, "") else "未填写").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def review_text(detail: ReviewDetail) -> str:
    review, listing, offer = detail.review, detail.listing, detail.offer
    status = REVIEW_STATUS_LABELS.get(str(review.get("review_status") or ""), str(review.get("review_status") or ""))
    lines = [
        f"<b>{escape(str(listing.get('public_listing_id') or listing.get('listing_id') or '房源'))}</b>",
        escape(str(listing.get("display_title") or listing.get("project_name") or "未命名房源")),
        f"状态：<b>{escape(status)}</b>",
        f"位置：{escape(str(listing.get('public_location_display') or '待确认'))}",
        f"户型：{escape(str(listing.get('layout') or '待确认'))}",
        f"面积：{escape(str(listing.get('size_sqm') or '待确认'))}㎡",
        f"楼层：{escape(str(listing.get('floor') or '待确认'))}",
        f"价格：{escape(_money(offer))}",
        f"交易：{escape(str(offer.get('offer_type') or ''))}",
        f"发布策略：{escape(str(offer.get('publication_policy') or ''))}",
        f"审核分：{escape(str(review.get('review_score') or 0))}",
    ]
    note = str(review.get("review_note") or "").strip()
    if note:
        lines.append(f"提示：{escape(note)}")
    if str(offer.get("offer_type") or "") == "sale":
        lines.append("\nℹ️ 出售房源仅存档，当前不会进入租赁频道发布链。")
    return "\n".join(lines)


class PublisherAdminBot:
    def __init__(self, settings: PublisherAdminSettings):
        self.settings = settings
        self.workflow = build_workflow(settings)

    def _authorized(self, update: Update) -> bool:
        user = update.effective_user
        chat = update.effective_chat
        return bool(user and not user.is_bot and user.id in self.settings.admin_ids and chat and chat.type == "private")

    async def _require_admin(self, update: Update) -> bool:
        if self._authorized(update):
            return True
        chat = update.effective_chat
        if chat and chat.type == "private" and update.effective_message:
            await update.effective_message.reply_text("⛔ 你没有权限使用这个发布 Bot。")
        return False

    @staticmethod
    def _home_button() -> list[InlineKeyboardButton]:
        return [InlineKeyboardButton("🏠 发布后台", callback_data="v3h")]

    def _dashboard_keyboard(self) -> InlineKeyboardMarkup:
        counts = self.workflow.queue_counts()
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(f"📋 待审核 {counts.pending}", callback_data="v3ql|pending|0"),
                    InlineKeyboardButton(f"✅ 已批准 {counts.approved}", callback_data="v3ql|approved|0"),
                ],
                [
                    InlineKeyboardButton(f"⏸ 暂缓 {counts.hold}", callback_data="v3ql|hold|0"),
                    InlineKeyboardButton(f"❌ 已拒绝 {counts.rejected}", callback_data="v3ql|rejected|0"),
                ],
                [
                    InlineKeyboardButton(f"📦 待冻结 {counts.package_ready}", callback_data="v3pq|package_ready|0"),
                    InlineKeyboardButton(f"📤 待发布 {counts.package_approved}", callback_data="v3pq|approved|0"),
                ],
                [
                    InlineKeyboardButton(f"✅ 已发布 {counts.published}", callback_data="v3pq|published|0"),
                    InlineKeyboardButton("⚠️ 发布恢复", callback_data="v3err"),
                ],
            ]
        )

    async def _send_dashboard(self, message: Any) -> None:
        await message.reply_text(
            "<b>侨联 V3 发布后台</b>\n\n"
            "审核事实 → 人工纠偏 → 选择封面/实拍 → 最终预览 → 冻结 → 发布。\n"
            "批准后的发布包保持冻结；状态未知的 Telegram 发送不会自动重发。",
            parse_mode=ParseMode.HTML,
            reply_markup=self._dashboard_keyboard(),
        )

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop(EDIT_STATE_KEY, None)
        await self._send_dashboard(update.effective_message)

    async def queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        await self._send_review_queue(update.effective_message, "pending", 0)

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        await self._send_dashboard(update.effective_message)

    async def _send_review_queue(self, message: Any, status: str, page: int) -> None:
        page = max(0, int(page))
        offset = page * PAGE_SIZE
        rows = self.workflow.reader.reviews_by_status(status, limit=PAGE_SIZE + 1, offset=offset)
        has_next = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or row.get("project_name") or "")
            offer_type = str(row.get("offer_type") or "")
            price = (
                f"${int(row['monthly_rent_usd']):,}/月"
                if row.get("monthly_rent_usd") not in (None, "")
                else (f"${int(row['sale_price_usd']):,}" if row.get("sale_price_usd") not in (None, "") else "")
            )
            marker = "租" if offer_type == "rent" else "售"
            label = " · ".join(value for value in (marker, public_id, title, price) if value)
            buttons.append([InlineKeyboardButton(label[:58], callback_data=f"v3r|{row['review_id']}")])
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"v3ql|{status}|{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"v3ql|{status}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append(self._home_button())
        label = REVIEW_STATUS_LABELS.get(status, status)
        text = f"<b>{escape(label)}</b> · 第 {page + 1} 页"
        if not rows:
            text += "\n\n当前没有记录。"
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def _send_package_queue(self, message: Any, status: str, page: int) -> None:
        page = max(0, int(page))
        offset = page * PAGE_SIZE
        rows = self.workflow.package_rows(status, limit=PAGE_SIZE + 1, offset=offset)
        has_next = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or "")
            price = f"${int(row['monthly_rent_usd']):,}/月" if row.get("monthly_rent_usd") not in (None, "") else ""
            label = " · ".join(value for value in (public_id, title, price) if value)
            buttons.append([InlineKeyboardButton(label[:58], callback_data=f"v3pkg|{row['package_id']}")])
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"v3pq|{status}|{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"v3pq|{status}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append(self._home_button())
        label = PACKAGE_STATUS_LABELS.get(status, status)
        text = f"<b>{escape(label)}</b> · 第 {page + 1} 页"
        if not rows:
            text += "\n\n当前没有记录。"
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def _send_delivery_queue(self, message: Any, state: str, page: int) -> None:
        page = max(0, int(page))
        offset = page * PAGE_SIZE
        rows = self.workflow.delivery_rows(state, limit=PAGE_SIZE + 1, offset=offset)
        has_next = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        buttons: list[list[InlineKeyboardButton]] = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or "")
            label = " · ".join(value for value in (public_id, title, _short(row.get("error_text"), 18) if row.get("error_text") else "") if value)
            buttons.append([InlineKeyboardButton(label[:58], callback_data=f"v3d|{row['attempt_id']}")])
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"v3dq|{state}|{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data=f"v3dq|{state}|{page+1}"))
        if nav:
            buttons.append(nav)
        buttons.append([InlineKeyboardButton("⚠️ 返回恢复中心", callback_data="v3err")])
        label = DELIVERY_STATE_LABELS.get(state, state)
        text = f"<b>{escape(label)}</b> · 第 {page + 1} 页"
        if not rows:
            text += "\n\n当前没有记录。"
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))

    async def _send_review_detail(self, message: Any, review_id: str) -> None:
        detail = self.workflow.review_detail(review_id)
        status = str(detail.review.get("review_status") or "")
        offer_type = str(detail.offer.get("offer_type") or "")
        rows: list[list[InlineKeyboardButton]] = []
        if status == "pending":
            rows.append([
                InlineKeyboardButton("✅ 批准事实", callback_data=f"v3a|{review_id}"),
                InlineKeyboardButton("✏️ 修改资料", callback_data=f"v3edit|{review_id}"),
            ])
            rows.append([InlineKeyboardButton("🖼 封面 / 实拍", callback_data=f"v3media|{review_id}")])
            rows.append([
                InlineKeyboardButton("⏸ 暂缓", callback_data=f"v3hold|{review_id}"),
                InlineKeyboardButton("❌ 拒绝", callback_data=f"v3reject|{review_id}"),
            ])
        elif status == "approved":
            rows.append([InlineKeyboardButton("✏️ 修改资料", callback_data=f"v3edit|{review_id}")])
            if offer_type == "rent" and str(detail.offer.get("publication_policy") or "") == "telegram_rent":
                rows.append([
                    InlineKeyboardButton("🖼 封面 / 实拍", callback_data=f"v3media|{review_id}"),
                    InlineKeyboardButton("🎨 生成发布预览", callback_data=f"v3styles|{review_id}"),
                ])
            rows.append([InlineKeyboardButton("⏸ 暂缓", callback_data=f"v3hold|{review_id}")])
        elif status in {"hold", "rejected"}:
            rows.append([
                InlineKeyboardButton("↩️ 恢复待审核", callback_data=f"v3reopen|{review_id}"),
                InlineKeyboardButton("✏️ 修改资料", callback_data=f"v3edit|{review_id}"),
            ])
        rows.append(self._home_button())
        await message.reply_text(review_text(detail), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))

    async def _send_edit_menu(self, message: Any, review_id: str) -> None:
        detail = self.workflow.review_detail(review_id)
        facts = dict(detail.canonical.get("facts") or {})
        buttons: list[list[InlineKeyboardButton]] = []
        row: list[InlineKeyboardButton] = []
        for field_name, (label, _kind) in EDITABLE_FACT_FIELDS.items():
            code = FIELD_CODE_BY_NAME.get(field_name)
            if not code:
                continue
            value = facts.get(field_name)
            if field_name == "monthly_rent_usd" and value in (None, ""):
                value = detail.offer.get("monthly_rent_usd")
            if field_name == "sale_price_usd" and value in (None, ""):
                value = detail.offer.get("sale_price_usd")
            row.append(InlineKeyboardButton(f"{label}：{_short(value, 12)}", callback_data=f"v3ef|{code}|{review_id}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("⬅️ 返回房源", callback_data=f"v3r|{review_id}")])
        await message.reply_text(
            "<b>修改房源事实</b>\n\n选择字段后直接发送新值。\n修改会记录 override、重算 canonical、重新物化，并把审核退回待审核。",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    def _media_state(self, context: ContextTypes.DEFAULT_TYPE, review_id: str) -> dict[str, Any]:
        root = context.user_data.setdefault(MEDIA_STATE_KEY, {})
        state = root.setdefault(str(review_id), {"cover_path": "", "excluded": []})
        state.setdefault("cover_path", "")
        state.setdefault("excluded", [])
        return state

    async def _send_media(self, message: Any, context: ContextTypes.DEFAULT_TYPE, review_id: str, index: int = 0) -> None:
        state = self._media_state(context, review_id)
        media = await asyncio.to_thread(
            self.workflow.review_media,
            review_id=review_id,
            manual_cover_path=str(state.get("cover_path") or "") or None,
        )
        candidates = tuple(media.gallery_paths)
        if not candidates:
            raise ValueError("publisher_media_has_no_gallery")
        index = int(index) % len(candidates)
        candidate = str(candidates[index])
        active_cover = str(state.get("cover_path") or media.cover_source_path)
        excluded = {str(value) for value in state.get("excluded") or []}
        retained = [path for path in candidates if str(path) not in excluded]
        is_cover = candidate == active_cover
        is_excluded = candidate in excluded
        caption = (
            f"<b>封面 / 实拍管理</b>\n"
            f"第 {index + 1}/{len(candidates)} 张\n"
            f"当前封面：{'是' if is_cover else '否'}\n"
            f"实拍状态：{'已移除' if is_excluded else '保留'}\n"
            f"最终实拍：{len(retained)} 张"
        )
        rows: list[list[InlineKeyboardButton]] = []
        if len(candidates) > 1:
            rows.append([
                InlineKeyboardButton("⬅️ 上一张", callback_data=f"v3m|{review_id}|{(index-1)%len(candidates)}"),
                InlineKeyboardButton("下一张 ➡️", callback_data=f"v3m|{review_id}|{(index+1)%len(candidates)}"),
            ])
        if is_cover:
            rows.append([InlineKeyboardButton("✅ 当前封面", callback_data="v3noop")])
        else:
            rows.append([InlineKeyboardButton("🖼 设为封面", callback_data=f"v3c|{review_id}|{index}")])
        if is_cover:
            rows.append([InlineKeyboardButton("封面图必须保留在实拍", callback_data="v3noop")])
        else:
            toggle = "↩️ 恢复到实拍" if is_excluded else "🗑 从实拍移除"
            rows.append([InlineKeyboardButton(toggle, callback_data=f"v3g|{review_id}|{index}")])
        rows.append([
            InlineKeyboardButton("🎨 生成发布预览", callback_data=f"v3styles|{review_id}"),
            InlineKeyboardButton("⬅️ 返回房源", callback_data=f"v3r|{review_id}"),
        ])
        with Path(candidate).open("rb") as handle:
            await message.reply_photo(photo=handle, caption=caption, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))

    @staticmethod
    def _style_keyboard(review_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("经典蓝卡", callback_data=f"v3b|classic_blue|{review_id}"),
                    InlineKeyboardButton("右侧价格牌", callback_data=f"v3b|right_price|{review_id}"),
                ],
                [InlineKeyboardButton("黑金高级感", callback_data=f"v3b|black_gold|{review_id}")],
                [InlineKeyboardButton("⬅️ 封面 / 实拍", callback_data=f"v3media|{review_id}")],
            ]
        )

    def _package_keyboard(self, package: FrozenPackage, *, review_id: str = "") -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = [
            [
                InlineKeyboardButton("🏠 房源详情", url=package.actions["details"]),
                InlineKeyboardButton("📸 更多实拍", url=package.actions["photos"]),
            ],
            [InlineKeyboardButton("📅 预约看房", url=package.actions["book"])],
        ]
        if package.status == "package_ready":
            rows.append([InlineKeyboardButton("✅ 批准并冻结发布包", callback_data=f"v3p|{package.package_id}")])
            if review_id:
                rows.append([
                    InlineKeyboardButton("🖼 调整封面/实拍", callback_data=f"v3media|{review_id}"),
                    InlineKeyboardButton("✏️ 修改资料", callback_data=f"v3edit|{review_id}"),
                ])
        elif package.status == "approved":
            rows.append([InlineKeyboardButton("📤 发布到频道", callback_data=f"v3s|{package.package_id}")])
        rows.append(self._home_button())
        return InlineKeyboardMarkup(rows)

    async def _send_package_preview(self, message: Any, package: FrozenPackage, *, review_id: str = "") -> None:
        header = (
            f"最终发布预览 · v{package.package_version}\n"
            f"封面样式：{package.cover_style}\n"
            f"实拍：{len(package.gallery)} 张\n\n"
        )
        caption = header + package.post_text
        if len(caption) > 1024:
            caption = package.post_text
        with Path(package.cover_path).open("rb") as handle:
            await message.reply_photo(
                photo=handle,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=self._package_keyboard(package, review_id=review_id),
            )

    async def _send_delivery_detail(self, message: Any, attempt_id: str) -> None:
        attempt = self.workflow.delivery.deliveries.get(attempt_id)
        if attempt is None:
            raise KeyError(attempt_id)
        label = DELIVERY_STATE_LABELS.get(attempt.state, attempt.state)
        text = (
            f"<b>发布发送记录</b>\n"
            f"状态：<b>{escape(label)}</b>\n"
            f"attempt：<code>{escape(attempt.attempt_id)}</code>\n"
            f"package：<code>{escape(attempt.package_id)}</code>"
        )
        if attempt.channel_message_id:
            text += f"\nmessage：<code>{escape(str(attempt.channel_message_id))}</code>"
        if attempt.error_text:
            text += f"\n错误：{escape(str(attempt.error_text))}"
        rows: list[list[InlineKeyboardButton]] = []
        if attempt.state == "failed_before_send":
            rows.append([InlineKeyboardButton("🔄 安全重试", callback_data=f"v3retry|{attempt.package_id}")])
        elif attempt.state == "sent":
            rows.append([InlineKeyboardButton("✅ 使用已保存 receipt 完成提交", callback_data=f"v3final|{attempt.attempt_id}")])
        elif attempt.state == "unknown":
            text += "\n\n⚠️ 发送结果未知，禁止自动重发。请先人工核对频道。"
        rows.append([InlineKeyboardButton("⚠️ 返回恢复中心", callback_data="v3err")])
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))

    async def on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        state = context.user_data.get(EDIT_STATE_KEY)
        if not isinstance(state, dict):
            return
        review_id = str(state.get("review_id") or "")
        field_name = str(state.get("field_name") or "")
        if not review_id or not field_name:
            context.user_data.pop(EDIT_STATE_KEY, None)
            return
        raw_value = str(update.effective_message.text or "").strip()
        if not raw_value:
            await update.effective_message.reply_text("请输入新值。")
            return
        try:
            detail = await asyncio.to_thread(
                self.workflow.edit_review_field,
                review_id=review_id,
                field_name=field_name,
                value=raw_value,
                operator_user_id=str(update.effective_user.id),
            )
        except Exception as exc:
            await update.effective_message.reply_text(
                "修改失败，当前编辑状态已保留，可直接重新输入：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop(EDIT_STATE_KEY, None)
        await update.effective_message.reply_text(
            "✅ 已修改。事实审核已自动退回待审核，旧的未冻结预览包已作废。\n\n" + review_text(detail),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("继续审核", callback_data=f"v3r|{review_id}")]]),
        )

    async def on_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        if not await self._require_admin(update):
            await query.answer()
            return
        raw = str(query.data or "")
        if raw == "v3noop":
            await query.answer("当前操作不可用")
            return
        await query.answer()
        parts = raw.split("|")
        action = parts[0]
        try:
            if action == "v3h":
                await self._send_dashboard(query.message)
                return
            if action == "v3ql" and len(parts) == 3:
                await self._send_review_queue(query.message, parts[1], int(parts[2]))
                return
            if action == "v3pq" and len(parts) == 3:
                await self._send_package_queue(query.message, parts[1], int(parts[2]))
                return
            if action == "v3dq" and len(parts) == 3:
                await self._send_delivery_queue(query.message, parts[1], int(parts[2]))
                return
            if action == "v3err":
                await query.message.reply_text(
                    "<b>发布恢复中心</b>\n\n"
                    "发送前失败可以安全重试；已发送待提交只 finalize receipt；状态未知禁止自动重发。",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("🔄 发送前失败", callback_data="v3dq|failed_before_send|0")],
                            [InlineKeyboardButton("✅ 已发送待提交", callback_data="v3dq|sent|0")],
                            [InlineKeyboardButton("⚠️ 状态未知", callback_data="v3dq|unknown|0")],
                            self._home_button(),
                        ]
                    ),
                )
                return
            if action == "v3r" and len(parts) == 2:
                await self._send_review_detail(query.message, parts[1])
                return
            if action == "v3a" and len(parts) == 2:
                detail = await asyncio.to_thread(
                    self.workflow.approve_review,
                    review_id=parts[1],
                    operator_user_id=str(update.effective_user.id),
                )
                await query.message.reply_text(
                    "✅ 事实审核已批准。\n\n" + review_text(detail),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖼 选择封面 / 实拍", callback_data=f"v3media|{parts[1]}")], self._home_button()]),
                )
                return
            if action in {"v3hold", "v3reject", "v3reopen"} and len(parts) == 2:
                target = {"v3hold": "hold", "v3reject": "rejected", "v3reopen": "pending"}[action]
                detail = await asyncio.to_thread(
                    self.workflow.set_review_status,
                    review_id=parts[1],
                    status=target,
                    operator_user_id=str(update.effective_user.id),
                    note={"hold": "admin_hold", "rejected": "admin_rejected", "pending": "admin_reopened"}[target],
                )
                await query.message.reply_text(
                    "状态已更新。\n\n" + review_text(detail),
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("查看房源", callback_data=f"v3r|{parts[1]}")], self._home_button()]),
                )
                return
            if action == "v3edit" and len(parts) == 2:
                await self._send_edit_menu(query.message, parts[1])
                return
            if action == "v3ef" and len(parts) == 3:
                field_name = FIELD_CODES.get(parts[1])
                if not field_name:
                    raise ValueError("publisher_unknown_edit_field")
                context.user_data[EDIT_STATE_KEY] = {"review_id": parts[2], "field_name": field_name}
                label = EDITABLE_FACT_FIELDS[field_name][0]
                await query.message.reply_text(
                    f"✏️ 正在修改：<b>{escape(label)}</b>\n\n直接发送新值。发送 /cancel 可取消。",
                    parse_mode=ParseMode.HTML,
                )
                return
            if action == "v3media" and len(parts) == 2:
                await self._send_media(query.message, context, parts[1], 0)
                return
            if action == "v3m" and len(parts) == 3:
                await self._send_media(query.message, context, parts[1], int(parts[2]))
                return
            if action in {"v3c", "v3g"} and len(parts) == 3:
                review_id, index = parts[1], int(parts[2])
                state = self._media_state(context, review_id)
                media = await asyncio.to_thread(
                    self.workflow.review_media,
                    review_id=review_id,
                    manual_cover_path=str(state.get("cover_path") or "") or None,
                )
                candidates = tuple(media.gallery_paths)
                if not candidates:
                    raise ValueError("publisher_media_has_no_gallery")
                index %= len(candidates)
                candidate = str(candidates[index])
                excluded = {str(value) for value in state.get("excluded") or []}
                active_cover = str(state.get("cover_path") or media.cover_source_path)
                if action == "v3c":
                    state["cover_path"] = candidate
                    excluded.discard(candidate)
                else:
                    if candidate == active_cover:
                        raise ValueError("publisher_cover_cannot_be_removed_from_gallery")
                    if candidate in excluded:
                        excluded.remove(candidate)
                    else:
                        if len([path for path in candidates if str(path) not in excluded]) <= 1:
                            raise ValueError("publisher_gallery_cannot_be_empty")
                        excluded.add(candidate)
                state["excluded"] = sorted(excluded)
                await self._send_media(query.message, context, review_id, index)
                return
            if action == "v3styles" and len(parts) == 2:
                detail = self.workflow.review_detail(parts[1])
                if str(detail.review.get("review_status") or "") != "approved":
                    raise ValueError("review_must_be_approved_before_package")
                await query.message.reply_text("选择正式封面样式：", reply_markup=self._style_keyboard(parts[1]))
                return
            if action == "v3b" and len(parts) == 3:
                style, review_id = parts[1], parts[2]
                state = self._media_state(context, review_id)
                package = await asyncio.to_thread(
                    self.workflow.build_package_for_review,
                    review_id=review_id,
                    cover_style=style,
                    manual_cover_path=str(state.get("cover_path") or "") or None,
                    excluded_gallery_paths=tuple(state.get("excluded") or ()),
                )
                await self._send_package_preview(query.message, package, review_id=review_id)
                return
            if action == "v3pkg" and len(parts) == 2:
                package = self.workflow.package(parts[1])
                await self._send_package_preview(query.message, package)
                return
            if action == "v3p" and len(parts) == 2:
                package = await asyncio.to_thread(
                    self.workflow.approve_package,
                    package_id=parts[1],
                    approved_by=str(update.effective_user.id),
                )
                await query.message.reply_text(
                    f"✅ 发布包已冻结批准：<code>{escape(package.package_id)}</code>\n"
                    "发送时不会重新生成文案、封面或 gallery。",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📤 发布到频道", callback_data=f"v3s|{package.package_id}")], self._home_button()]),
                )
                return
            if action in {"v3s", "v3retry"} and len(parts) == 2:
                package_id = parts[1]
                adapter = TelegramChannelAdapter(context.bot)
                result = await deliver_approved_package(
                    coordinator=self.workflow.delivery,
                    adapter=adapter,
                    package_id=package_id,
                    channel_chat_id=self.settings.channel_chat_id,
                )
                await query.message.reply_text(
                    "✅ 已发布\n"
                    f"package: <code>{escape(package_id)}</code>\n"
                    f"message: <code>{escape(str(result.publication.channel_message_id))}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([self._home_button()]),
                )
                return
            if action == "v3d" and len(parts) == 2:
                await self._send_delivery_detail(query.message, parts[1])
                return
            if action == "v3final" and len(parts) == 2:
                result = await asyncio.to_thread(self.workflow.delivery.finalize_saved_receipt, attempt_id=parts[1])
                await query.message.reply_text(
                    "✅ 已使用持久化 Telegram receipt 完成提交，没有重新发送。\n"
                    f"message: <code>{escape(str(result.publication.channel_message_id))}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([self._home_button()]),
                )
                return
            await query.message.reply_text("这个 V3 操作已经失效，请重新打开发布后台。")
        except Exception as exc:
            await query.message.reply_text(
                "V3 操作失败，未自动绕过：\n" + escape(type(exc).__name__ + ": " + str(exc)),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([self._home_button()]),
            )

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        context.user_data.pop(EDIT_STATE_KEY, None)
        await update.effective_message.reply_text("已取消当前编辑。", reply_markup=InlineKeyboardMarkup([self._home_button()]))

    def build_application(self) -> Application:
        app = Application.builder().token(self.settings.token).build()
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("queue", self.queue))
        app.add_handler(CommandHandler("status", self.status))
        app.add_handler(CommandHandler("cancel", self.cancel))
        app.add_handler(CallbackQueryHandler(self.on_callback, pattern=r"^v3"))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text))
        return app


def run() -> None:
    settings = load_settings()
    PublisherAdminBot(settings).build_application().run_polling(drop_pending_updates=False)


__all__ = [
    "PublisherAdminBot",
    "PublisherAdminSettings",
    "build_workflow",
    "load_settings",
    "review_text",
    "run",
]
