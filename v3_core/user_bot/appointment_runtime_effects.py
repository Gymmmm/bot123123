"""Outer best-effort effects after a durable V3 appointment transaction."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .admin_notification_plans import appointment_notification
from .admin_notifications import AdminNotificationResult, TelegramAdminNotifier
from .appointment_availability import (
    AppointmentAvailabilityResult,
    AppointmentAvailabilityService,
)
from .appointment_submit_executor import AppointmentSubmitExecution
from .channel_status_sync import ChannelStatusSyncResult, V3AppointmentChannelSynchronizer
from .lead_service import LeadUser
from .public_appointment import PublicAppointmentDraft
from .public_inventory import PublicInventoryReader


@dataclass(frozen=True)
class AppointmentRuntimeEffectResult:
    availability: AppointmentAvailabilityResult | None
    channel: ChannelStatusSyncResult | None
    admin: AdminNotificationResult | None
    availability_error: str = ""


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
                # Appointment persistence has already committed. Match production's
                # non-rollback outer-effect boundary and continue to channel/admin.
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
                ),
            )

        return AppointmentRuntimeEffectResult(
            availability=availability_result,
            channel=channel_result,
            admin=admin_result,
            availability_error=availability_error,
        )


__all__ = ["AppointmentRuntimeEffectExecutor", "AppointmentRuntimeEffectResult"]
