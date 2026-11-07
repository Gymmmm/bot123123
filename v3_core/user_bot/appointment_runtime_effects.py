"""Outer best-effort effects after a durable V3 appointment transaction."""
from __future__ import annotations

from dataclasses import dataclass
from html import escape
import logging
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .admin_notification_plans import appointment_notification
from .admin_notifications import AdminNotification, AdminNotificationResult, TelegramAdminNotifier
from .appointment_availability import (
    AppointmentAvailabilityResult,
    AppointmentAvailabilityService,
)
from .appointment_submit_executor import AppointmentSubmitExecution
from .channel_status_sync import ChannelStatusSyncResult, V3AppointmentChannelSynchronizer
from .lead_service import LeadUser
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AppointmentRuntimeEffectResult:
    availability: AppointmentAvailabilityResult | None
    channel: ChannelStatusSyncResult | None
    admin: AdminNotificationResult | None
    availability_error: str = ""
    channel_alert: AdminNotificationResult | None = None


def _admin_appointment_keyboard(appointment_id: int, user_id: int) -> InlineKeyboardMarkup:
    aid = int(appointment_id)
    uid = int(user_id)
    rows = [
        [
            InlineKeyboardButton("✅ 确认预约", callback_data=f"adminq:appointment:confirm:{aid}"),
            InlineKeyboardButton("☎️ 标记已联系", callback_data=f"adminq:appointment:contacted:{aid}"),
        ],
        [
            InlineKeyboardButton("❌ 无法安排", callback_data=f"adminq:appointment:cancel:{aid}"),
            InlineKeyboardButton("📋 查看预约", callback_data=f"adminq:appointment:{aid}"),
        ],
    ]
    if uid > 0:
        rows.append([InlineKeyboardButton("👤 联系用户", url=f"tg://user?id={uid}")])
    return InlineKeyboardMarkup(rows)


class AppointmentRuntimeEffectExecutor:
    def __init__(
        self,
        *,
        availability: AppointmentAvailabilityService,
        channel: V3AppointmentChannelSynchronizer,
        admins: TelegramAdminNotifier,
        inventory: PublicInventoryReader,
    ):
        self.availability = availability
        self.channel = channel
        self.admins = admins
        self.inventory = inventory

    def _subject(self, public_listing_id: str) -> str:
        view = self.inventory.resolve(public_listing_id)
        if view is None:
            return str(public_listing_id or "").strip()
        listing = view.frozen_listing or view.listing
        project = str(
            listing.get("project_name")
            or listing.get("project")
            or listing.get("public_location_display")
            or ""
        ).strip()
        layout = str(listing.get("layout") or listing.get("property_type") or "").strip()
        parts = [value for value in (project, layout) if value]
        return "｜".join(parts) or str(public_listing_id or "").strip()

    async def execute(
        self,
        *,
        bot: Any,
        user: LeadUser,
        execution: AppointmentSubmitExecution,
        draft: PublicAppointmentDraft,
    ) -> AppointmentRuntimeEffectResult:
        availability_result = None
        availability_error = ""
        if execution.effects.includes("recompute_listing_availability"):
            try:
                availability_result = self.availability.recompute(execution.listing_id)
            except Exception as exc:
                availability_error = str(exc)

        channel_result = None
        if execution.effects.includes("sync_channel"):
            channel_result = await self.channel.sync(execution.listing_id)

        admin_result = None
        if execution.effects.includes("notify_admin"):
            admin_result = await self.admins.send(
                bot,
                appointment_notification(
                    user,
                    execution,
                    draft,
                    subject=self._subject(execution.public_listing_id),
                    reply_markup=_admin_appointment_keyboard(
                        execution.submission.appointment_id,
                        user.user_id,
                    ),
                ),
            )

        # The ordinary appointment notification is not evidence of a channel
        # update. Surface channel failures independently, using the existing
        # durable admin notification outbox for transient send failures.
        channel_alert = None
        if channel_result is not None and not channel_result.synced:
            queued = "retry_queue_failed" not in str(channel_result.error or "")
            try:
                channel_alert = await self.admins.send(
                    bot,
                    AdminNotification(
                        title="频道房态同步异常",
                        lines=(
                            f"房源：{escape(str(execution.public_listing_id))}",
                            "预约数据已经保存，但频道状态未确认同步。",
                            "已持久化等待按最新房态重试。" if queued else "重试待办保存失败，需要人工检查。",
                            "请勿重新发送房源帖；只核对原频道消息。",
                        ),
                    ),
                )
            except Exception:
                logger.exception("Unable to notify admins about channel sync failure")

        return AppointmentRuntimeEffectResult(
            availability=availability_result,
            channel=channel_result,
            admin=admin_result,
            availability_error=availability_error,
            channel_alert=channel_alert,
        )


__all__ = [
    "AppointmentRuntimeEffectExecutor",
    "AppointmentRuntimeEffectResult",
    "_admin_appointment_keyboard",
]
