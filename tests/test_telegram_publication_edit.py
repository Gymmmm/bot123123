import asyncio
from pathlib import Path

from v3_core.publishing.delivery_coordinator import TelegramSendCommand
from v3_core.publishing.telegram_adapter import TelegramChannelAdapter, build_channel_keyboard


ACTIONS = {
    "details": "https://t.me/example_bot?start=details",
    "photos": "https://t.me/example_bot?start=photos",
    "book": "https://t.me/example_bot?start=book",
}


class FakeBot:
    def __init__(self):
        self.calls = []

    async def send_photo(self, **kwargs):
        self.calls.append(("send_photo", kwargs))
        return type("Message", (), {"message_id": 99})()

    async def edit_message_media(self, **kwargs):
        self.calls.append(("edit_message_media", kwargs))
        return True

    async def edit_message_caption(self, **kwargs):
        self.calls.append(("edit_message_caption", kwargs))
        return True

    async def edit_message_text(self, **kwargs):
        self.calls.append(("edit_message_text", kwargs))
        return True

    async def edit_message_reply_markup(self, **kwargs):
        self.calls.append(("edit_message_reply_markup", kwargs))
        return True


def _command(tmp_path: Path, status: str) -> TelegramSendCommand:
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"fake-image-bytes")
    return TelegramSendCommand(
        attempt_id="edit:PUB_1",
        package_id="PKG_1",
        channel_chat_id="-100123",
        cover_path=str(cover),
        caption="🏡 测试房源\n\n🔵 房态待确认　QL-TEST",
        actions=dict(ACTIONS),
        inventory_status=status,
    )


def test_keyboard_uses_live_inventory_status():
    active = build_channel_keyboard(dict(ACTIONS), inventory_status="active")
    pending = build_channel_keyboard(dict(ACTIONS), inventory_status="pending")
    assert len(active.inline_keyboard) == 2
    assert active.inline_keyboard[1][0].text == "📅 预约看房"
    assert len(pending.inline_keyboard) == 1


def test_edit_reuses_normal_command_and_updates_media_caption_buttons(tmp_path):
    bot = FakeBot()
    adapter = TelegramChannelAdapter(bot)
    result = asyncio.run(adapter.edit(_command(tmp_path, "active"), message_id="3208"))
    assert result["edit_mode"] == "media"
    assert result["media_message_ids"] == ["3208"]
    name, kwargs = bot.calls[0]
    assert name == "edit_message_media"
    assert kwargs["chat_id"] == "-100123"
    assert kwargs["message_id"] == 3208
    keyboard = kwargs["reply_markup"]
    assert keyboard.inline_keyboard[1][0].text == "📅 预约看房"
