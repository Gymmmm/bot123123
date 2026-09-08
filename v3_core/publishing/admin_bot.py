"""Side-by-side V3 Publisher admin bot.

The bot is private/admin-only.  It reviews V3 inventory and frozen publication
packages; it never creates a second Draft model and it has no discussion-thread
handlers.
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
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from v3_core.ingest.source_reader import SourceReader
from v3_core.media.cover_service import CoverRenderService
from v3_core.media.service import MediaPreparationService
from v3_core.publishing.admin_workflow import PublisherWorkflowService, ReviewDetail
from v3_core.publishing.delivery_coordinator import PublicationDeliveryCoordinator
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_service import PackageApprovalService, PackageBuildService
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.publishing.telegram_adapter import TelegramChannelAdapter, deliver_approved_package
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")


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
    cover_dir = Path(
        os.getenv("V3_COVER_OUTPUT_DIR")
        or (REPO_ROOT / "media" / "covers_v3")
    )
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
    covers = CoverRenderService(
        reader=reader,
        output_dir=settings.cover_output_dir,
    )
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
        return f"${value}/月" if value not in (None, "") else "未识别租金"
    value = offer.get("sale_price_usd")
    return f"${value}" if value not in (None, "") else "未识别售价"


def review_text(detail: ReviewDetail) -> str:
    review, listing, offer = detail.review, detail.listing, detail.offer
    lines = [
        f"<b>{escape(str(listing.get('public_listing_id') or listing.get('listing_id') or '房源'))}</b>",
        escape(str(listing.get("display_title") or listing.get("project_name") or "未命名房源")),
        f"位置：{escape(str(listing.get('public_location_display') or '待确认'))}",
        f"户型：{escape(str(listing.get('layout') or '待确认'))}",
        f"面积：{escape(str(listing.get('size_sqm') or '待确认'))}㎡",
        f"楼层：{escape(str(listing.get('floor') or '待确认'))}",
        f"价格：{escape(_money(offer))}",
        f"类型：{escape(str(offer.get('offer_type') or ''))}",
        f"审核分：{escape(str(review.get('review_score') or 0))}",
    ]
    note = str(review.get("review_note") or "").strip()
    if note:
        lines.append(f"提示：{escape(note)}")
    return "\n".join(lines)


class PublisherAdminBot:
    def __init__(self, settings: PublisherAdminSettings):
        self.settings = settings
        self.workflow = build_workflow(settings)

    def _authorized(self, update: Update) -> bool:
        user = update.effective_user
        chat = update.effective_chat
        return bool(
            user
            and not user.is_bot
            and user.id in self.settings.admin_ids
            and chat
            and chat.type == "private"
        )

    async def _require_admin(self, update: Update) -> bool:
        if self._authorized(update):
            return True
        chat = update.effective_chat
        if chat and chat.type == "private" and update.effective_message:
            await update.effective_message.reply_text("⛔ 你没有权限使用这个发布 Bot。")
        return False

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📋 待审核房源", callback_data="v3q|pending")]]
        )
        await update.effective_message.reply_text(
            "<b>侨联 V3 发布后台</b>\n\n"
            "这里只处理：事实审核 → 封面/发布包 → 批准 → 频道发布。\n"
            "不会创建旧 Draft，也不会发讨论区。",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )

    async def queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_admin(update):
            return
        await self._send_queue(update.effective_message)

    async def _send_queue(self, message: Any) -> None:
        rows = self.workflow.pending_reviews(limit=12)
        if not rows:
            await message.reply_text("当前没有待审核 V3 房源。")
            return
        buttons = []
        for row in rows:
            public_id = str(row.get("public_listing_id") or row.get("listing_id") or "房源")
            title = str(row.get("display_title") or row.get("project_name") or "")
            price = (
                f"${row.get('monthly_rent_usd')}/月"
                if row.get("monthly_rent_usd") not in (None, "")
                else ""
            )
            label = " · ".join(value for value in (public_id, title, price) if value)
            buttons.append(
                [InlineKeyboardButton(label[:55], callback_data=f"v3r|{row['review_id']}")]
            )
        await message.reply_text(
            f"待审核：{len(rows)} 条",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def on_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        if not await self._require_admin(update):
            await query.answer()
            return
        await query.answer()
        raw = str(query.data or "")
        parts = raw.split("|")
        action = parts[0]
        try:
            if action == "v3q":
                await self._send_queue(query.message)
                return
            if action == "v3r" and len(parts) == 2:
                detail = self.workflow.review_detail(parts[1])
                status = str(detail.review.get("review_status") or "")
                if status == "pending":
                    keyboard = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("✅ 批准事实", callback_data=f"v3a|{parts[1]}")]]
                    )
                else:
                    keyboard = self._style_keyboard(parts[1])
                await query.message.reply_text(
                    review_text(detail),
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
                return
            if action == "v3a" and len(parts) == 2:
                review_id = parts[1]
                detail = self.workflow.approve_review(
                    review_id=review_id,
                    operator_user_id=str(update.effective_user.id),
                )
                await query.message.reply_text(
                    "事实审核已批准。选择正式封面样式：\n\n" + review_text(detail),
                    parse_mode=ParseMode.HTML,
                    reply_markup=self._style_keyboard(review_id),
                )
                return
            if action == "v3b" and len(parts) == 3:
                style, review_id = parts[1], parts[2]
                package = await asyncio.to_thread(
                    self.workflow.build_package_for_review,
                    review_id=review_id,
                    cover_style=style,
                )
                keyboard = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("✅ 批准这个发布包", callback_data=f"v3p|{package.package_id}")]]
                )
                with Path(package.cover_path).open("rb") as handle:
                    await query.message.reply_photo(
                        photo=handle,
                        caption=package.post_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                    )
                return
            if action == "v3p" and len(parts) == 2:
                package = self.workflow.approve_package(
                    package_id=parts[1],
                    approved_by=str(update.effective_user.id),
                )
                await query.message.reply_text(
                    f"发布包已冻结批准：{package.package_id}\n"
                    "批准后不会在发送时重新生成文案或封面。",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("📤 发布到频道", callback_data=f"v3s|{package.package_id}")]]
                    ),
                )
                return
            if action == "v3s" and len(parts) == 2:
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
                    f"package: {package_id}\n"
                    f"message: {result.publication.channel_message_id}"
                )
                return
            await query.message.reply_text("这个 V3 操作已经失效，请重新打开待审核队列。")
        except Exception as exc:
            await query.message.reply_text(
                "V3 操作失败，未自动绕过：\n"
                f"{escape(type(exc).__name__ + ': ' + str(exc))}",
                parse_mode=ParseMode.HTML,
            )

    @staticmethod
    def _style_keyboard(review_id: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("经典蓝卡", callback_data=f"v3b|classic_blue|{review_id}"),
                    InlineKeyboardButton("右侧价格牌", callback_data=f"v3b|right_price|{review_id}"),
                ],
                [InlineKeyboardButton("黑金高级感", callback_data=f"v3b|black_gold|{review_id}")],
            ]
        )

    def build_application(self) -> Application:
        app = Application.builder().token(self.settings.token).build()
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("queue", self.queue))
        app.add_handler(CallbackQueryHandler(self.on_callback, pattern=r"^v3"))
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
