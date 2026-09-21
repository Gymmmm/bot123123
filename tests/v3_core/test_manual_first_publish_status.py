from types import SimpleNamespace

import pytest

from v3_core.publishing.telegram_adapter import build_channel_keyboard, deliver_approved_package


ACTIONS = {
    "details": "https://t.me/UserBot?start=property_QL-PP-A2B3_details__ch",
    "photos": "https://t.me/UserBot?start=property_QL-PP-A2B3_photos__ch",
    "book": "https://t.me/UserBot?start=property_QL-PP-A2B3_book__ch",
    "consult": "https://t.me/advisor?text=你好，我想咨询这套房：QL-PP-A2B3",
}


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


class FakeCoordinator:
    def __init__(self):
        self.override = None
        self.sent = None

    def prepare_send(self, *, package_id, channel_chat_id, inventory_status_override=None):
        self.override = inventory_status_override
        return SimpleNamespace(
            attempt_id="DLV_1",
            package_id=package_id,
            channel_chat_id=channel_chat_id,
            cover_path="/tmp/unused",
            caption="caption",
            actions=dict(ACTIONS),
            inventory_status=inventory_status_override or "pending",
        )

    def mark_sending(self, attempt_id):
        assert attempt_id == "DLV_1"

    def mark_unknown(self, attempt_id, error):
        raise AssertionError(error)

    def record_sent(self, *, attempt_id, telegram_result):
        self.sent = telegram_result
        return SimpleNamespace(publication=SimpleNamespace(channel_message_id="99"))


class FakeAdapter:
    def __init__(self):
        self.command = None

    async def send(self, command):
        self.command = command
        labels = _labels(build_channel_keyboard(command.actions, inventory_status=command.inventory_status))
        assert labels == ["📷 房源详情", "📅 预约看房", "💬 中文顾问"]
        return {"caption_message_id": "99"}


@pytest.mark.asyncio
async def test_manual_first_publish_can_send_as_active_without_prechanging_inventory():
    coordinator = FakeCoordinator()
    adapter = FakeAdapter()

    await deliver_approved_package(
        coordinator=coordinator,
        adapter=adapter,
        package_id="PKG_1",
        channel_chat_id="-100123",
        inventory_status_override="active",
    )

    assert coordinator.override == "active"
    assert adapter.command.inventory_status == "active"


def test_pending_keyboard_still_hides_booking_for_normal_non_manual_send():
    labels = _labels(build_channel_keyboard(dict(ACTIONS), inventory_status="pending"))
    assert "📅 预约看房" not in labels
    assert "📷 房源详情" not in labels
    assert labels == ["🔎 看相近房源", "💬 中文顾问"]
