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
    "consult": "https://t.me/advisor?text=你好，我想咨询这套房：QL-RF-A2B3",
}


class FakeMessage:
    def __init__(self, message_id=777, media_group_id=""):
        self.message_id = message_id
        self.media_group_id = media_group_id


class FakeBot:
    def __init__(self, *, error=None):
        self.calls = []
        self.error = error

    async def send_photo(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return FakeMessage()

    async def send_media_group(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return [
            FakeMessage(700 + index, media_group_id="album-1")
            for index, _ in enumerate(kwargs["media"])
        ]

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return FakeMessage(799)


class ButtonTimeoutBot(FakeBot):
    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        raise TimeoutError("button response timeout")


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


def _command(tmp_path, *, gallery_count=0):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = []
    for index in range(gallery_count):
        path = tmp_path / f"gallery-{index}.jpg"
        path.write_bytes(f"gallery-{index}".encode())
        gallery.append(str(path))
    return TelegramSendCommand(
        attempt_id="DLV_1",
        package_id="PKG3_1",
        channel_chat_id="-100123",
        cover_path=str(cover),
        caption="🏠 <b>富力城｜2房1厅</b>",
        actions=dict(ACTIONS),
        inventory_status="active",
        gallery_paths=tuple(gallery),
    )


def test_bookable_keyboard_contract_is_locked_matrix_and_ordered():
    keyboard = build_channel_keyboard(dict(ACTIONS))
    rows = keyboard.inline_keyboard
    assert [[button.text for button in row] for row in rows] == [
        ["🏠 房源详情", "📅 预约看房"],
        ["💬 中文顾问"],
    ]
    assert rows[0][0].url == ACTIONS["details"]
    assert rows[0][1].url == ACTIONS["book"]
    assert rows[1][0].url == ACTIONS["consult"]


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
async def test_adapter_sends_four_independent_images_then_linked_action_bar(tmp_path):
    bot = FakeBot()
    adapter = TelegramChannelAdapter(bot)
    receipt = await adapter.send(_command(tmp_path, gallery_count=5))

    assert len(bot.calls) == 2
    album_call, button_call = bot.calls
    assert album_call["chat_id"] == "-100123"
    assert len(album_call["media"]) == 4
    assert album_call["media"][0].caption.startswith("🏠")
    assert all(item.caption is None for item in album_call["media"][1:])
    assert button_call["text"] == "👇 选择房源服务"
    assert button_call["reply_to_message_id"] == 700
    assert len(button_call["reply_markup"].inline_keyboard) == 2
    assert receipt["media_message_ids"] == ["700", "701", "702", "703"]
    assert receipt["caption_message_id"] == "700"
    assert receipt["button_message_id"] == "799"
    assert receipt["media_group_id"] == "album-1"
    assert receipt["single_cover"] is False


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


@pytest.mark.asyncio
async def test_button_timeout_after_album_is_unknown_and_never_auto_retried(tmp_path):
    command = _command(tmp_path, gallery_count=3)
    coordinator = FakeCoordinator(command)
    adapter = TelegramChannelAdapter(ButtonTimeoutBot())

    with pytest.raises(TimeoutError, match="button response timeout"):
        await deliver_approved_package(
            coordinator=coordinator,
            adapter=adapter,
            package_id=command.package_id,
            channel_chat_id=command.channel_chat_id,
        )

    assert coordinator.events[1] == ("sending", command.attempt_id)
    assert coordinator.events[2][0] == "unknown"
    assert not any(event[0] == "sent" for event in coordinator.events)
