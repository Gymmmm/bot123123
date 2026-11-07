from __future__ import annotations

from types import SimpleNamespace

import pytest

from v3_core.user_bot.admin_notifications import AdminNotificationResult
from v3_core.user_bot.appointment_runtime_effects import AppointmentRuntimeEffectExecutor
from v3_core.user_bot.channel_status_sync import ChannelStatusSyncResult
from v3_core.user_bot.lead_service import LeadUser
from v3_core.user_bot.public_appointment import PublicAppointmentDraft


class _Effects:
    def includes(self, kind):
        return kind in {"sync_channel", "notify_admin"}


class _Channel:
    async def sync(self, listing_id):
        assert listing_id == "l_1"
        return ChannelStatusSyncResult(
            listing_id="l_1", attempted=True, synced=False, error="temporary-network"
        )


class _Admins:
    def __init__(self):
        self.notifications = []

    async def send(self, bot, notification):
        self.notifications.append(notification)
        return AdminNotificationResult((123,), (123,), ())


class _Inventory:
    def resolve(self, public_listing_id):
        return None


@pytest.mark.asyncio
async def test_channel_failure_alert_is_separate_from_booking_notification():
    admins = _Admins()
    executor = AppointmentRuntimeEffectExecutor(
        availability=None, channel=_Channel(), admins=admins, inventory=_Inventory()
    )
    execution = SimpleNamespace(
        effects=_Effects(),
        listing_id="l_1",
        public_listing_id="QL-RF-A2B3",
        submission=SimpleNamespace(appointment_id=42, kind="created"),
    )
    draft = PublicAppointmentDraft(
        public_listing_id="QL-RF-A2B3", mode="offline", date="11-10", time="pm"
    )
    result = await executor.execute(
        bot=object(),
        user=LeadUser(user_id=123, username="test", display_name="Test User"),
        execution=execution,
        draft=draft,
    )

    assert result.channel is not None and not result.channel.synced
    assert result.admin is not None and result.admin.ok
    assert result.channel_alert is not None and result.channel_alert.ok
    assert len(admins.notifications) == 2
    assert "预约" in admins.notifications[0].title
    assert admins.notifications[1].title == "频道房态同步异常"
    assert "请勿重新发送房源帖" in admins.notifications[1].text
