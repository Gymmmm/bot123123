import pytest

from v3_core.publishing.delivery_coordinator import TelegramSendCommand
from v3_core.publishing.telegram_adapter import (
    TelegramChannelAdapter,
    build_channel_keyboard,
    deliver_approved_package,
)


ACTIONS = {
    "details": "https://t.me/TestBot?start=property_QL-RF-A2B3_details",
    "photos": "https://t.me/TestBot?start=property_QL-RF-A2B3_photos",
    "book": "https://t.me/TestBot?start=property_QL-RF-A2B3_book",
}


class FakeMessage:
    message_id = 777


class FakeBot:
    def __init__(self, *, error=None):
        self.calls = []
        self.error = error

    async def send_photo(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return FakeMessage()


class FakeCoordinator:
    def __init__(self, command):
        self.command = command
        self.events = []

    def prepare_send(self, **kwargs):
        self.events.append(("prepare", kwargs))
        return self.command

    def mark_sending(self, attempt_id):
        self.events.append(("sending", attempt_id))

    def mark_unknown(self, attempt_id, error, telegram_result=None):
        self.events.append(("unknown", attempt_id, error, telegram_result))

    def record_sent(self, *, attempt_id, telegram_result):
        self.events.append(("sent", attempt_id, telegram_result))
        return telegram_result


def _command(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    return TelegramSendCommand(
        attempt_id="DLV_1",
        package_id="PKG3_1",
        channel_chat_id="-100123",
        cover_path=str(cover),
        caption="🏠 <b>富力城｜2房1厅</b>",
        actions=dict(ACTIONS),
        inventory_status="active",
    )


def test_keyboard_contract_is_two_plus_one_and_ordered():
    keyboard = build_channel_keyboard(dict(ACTIONS))
    rows = keyboard.inline_keyboard
    assert [button.text for button in rows[0]] == ["🏠 房源详情", "📸 更多实拍"]
    assert [button.text for button in rows[1]] == ["📅 预约看房"]
    assert rows[0][0].url == ACTIONS["details"]
    assert rows[0][1].url == ACTIONS["photos"]
    assert rows[1][0].url == ACTIONS["book"]


@pytest.mark.asyncio
async def test_adapter_sends_only_one_cover_and_returns_durable_receipt(tmp_path):
    bot = FakeBot()
    adapter = TelegramChannelAdapter(bot)
    receipt = await adapter.send(_command(tmp_path))

    assert len(bot.calls) == 1
    call = bot.calls[0]
    assert call["chat_id"] == "-100123"
    assert call["caption"].startswith("🏠")
    assert len(call["reply_markup"].inline_keyboard) == 2
    assert receipt["media_message_ids"] == ["777"]
    assert receipt["caption_message_id"] == "777"
    assert receipt["button_message_id"] == "777"
    assert receipt["single_cover"] is True
    assert receipt["discussion_published"] is False


@pytest.mark.asyncio
async def test_exception_after_sending_boundary_is_marked_unknown(tmp_path):
    command = _command(tmp_path)
    coordinator = FakeCoordinator(command)
    adapter = TelegramChannelAdapter(FakeBot(error=TimeoutError("socket timeout")))

    with pytest.raises(TimeoutError):
        await deliver_approved_package(
            coordinator=coordinator,
            adapter=adapter,
            package_id=command.package_id,
            channel_chat_id=command.channel_chat_id,
        )

    assert coordinator.events[0][0] == "prepare"
    assert coordinator.events[1] == ("sending", command.attempt_id)
    assert coordinator.events[2][0] == "unknown"
    assert "socket timeout" in coordinator.events[2][2]
    assert not any(event[0] == "sent" for event in coordinator.events)
