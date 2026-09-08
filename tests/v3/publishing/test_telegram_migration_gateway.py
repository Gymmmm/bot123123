from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from tools.migrations.telegram_migration_gateway import TelegramMigrationGateway


class Button:
    def __init__(self, text: str, url: str) -> None:
        self.text = text
        self.url = url


class Row:
    def __init__(self, buttons) -> None:
        self.buttons = buttons


class Markup:
    def __init__(self, keyboard) -> None:
        self.rows = [Row([Button(text, url)]) for text, url in keyboard]


class FakeMessage:
    def __init__(self, *, caption: str, keyboard, photo_id: int) -> None:
        self.message = caption
        self.entities = []
        self.reply_markup = Markup(keyboard)
        self.photo = SimpleNamespace(id=photo_id)
        self.document = None
        self.media = object()


class InMemoryLiveGateway(TelegramMigrationGateway):
    def __init__(self, *, receipt_path: Path, message: FakeMessage, media_bytes: bytes) -> None:
        super().__init__(
            api_id=1,
            api_hash="hash",
            session_path=str(receipt_path.parent / "session"),
            publisher_bot_token="123456:TESTTOKEN",
            cover_dhash_max=0,
            receipt_path=receipt_path,
        )
        self.message = message
        self.media_bytes = media_bytes
        self.edit_calls = 0

    def _fetch_message(self, *, channel_id: str, message_id: int):
        return self.message, self.media_bytes

    def _edit(self, *, channel_id: str, message_id: int, cover: str, caption: str, keyboard):
        self.edit_calls += 1
        self.message = FakeMessage(caption=caption, keyboard=keyboard, photo_id=999)
        self.media_bytes = Path(cover).read_bytes()
        return SimpleNamespace(message_id=message_id)


def png(path: Path, value: int) -> None:
    Image.new("RGB", (64, 64), (value, value, value)).save(path)


def keyboard():
    return (
        ("🏠 房源详情", "https://t.me/QiaoLianBot?start=details_QL-RF-A2B3"),
        ("📸 更多实拍", "https://t.me/QiaoLianBot?start=photos_QL-RF-A2B3"),
        ("📅 预约看房", "https://t.me/QiaoLianBot?start=book_QL-RF-A2B3"),
    )


def test_gateway_live_before_fingerprint_edit_and_verified_after_receipt(tmp_path):
    old_cover = tmp_path / "old.png"
    new_cover = tmp_path / "new.png"
    png(old_cover, 20)
    png(new_cover, 220)
    gateway = InMemoryLiveGateway(
        receipt_path=tmp_path / "receipt.json",
        message=FakeMessage(caption="旧测试帖", keyboard=(), photo_id=10),
        media_bytes=old_cover.read_bytes(),
    )

    before = gateway.inspect_migration_post(channel_id="-100123", message_id=501)
    before_hash = before["content_hash"]
    assert before["state"] == "live_fingerprint"

    result = gateway.edit_migration_post(
        channel_id="-100123",
        message_id=501,
        cover=str(new_cover),
        caption="新真实 RENT",
        keyboard=[{"text": text, "url": url} for text, url in keyboard()],
        expected_before_hash=before_hash,
        expected_after_hash="PKG_AFTER_HASH",
    )
    assert result["ok"] is True
    assert gateway.edit_calls == 1
    assert (tmp_path / "receipt.json").is_file()

    after = gateway.inspect_migration_post(channel_id="-100123", message_id=501)
    assert after == {"content_hash": "PKG_AFTER_HASH", "state": "after_verified"}


def test_prepared_receipt_recovers_after_process_crash_without_editing_again(tmp_path):
    new_cover = tmp_path / "new.png"
    png(new_cover, 180)
    receipt = tmp_path / "receipt.json"
    expected_keyboard = keyboard()

    first = InMemoryLiveGateway(
        receipt_path=receipt,
        message=FakeMessage(caption="旧", keyboard=(), photo_id=1),
        media_bytes=new_cover.read_bytes(),
    )
    first._save_prepared_receipt(
        channel_id="-100123",
        message_id=777,
        cover=str(new_cover),
        caption="已迁移",
        keyboard=expected_keyboard,
        expected_after_hash="AFTER_777",
    )

    # Simulate Telegram having applied the edit, then the process dying before
    # the V3 DB mapping transaction is committed.
    restarted = InMemoryLiveGateway(
        receipt_path=receipt,
        message=FakeMessage(caption="已迁移", keyboard=expected_keyboard, photo_id=2),
        media_bytes=new_cover.read_bytes(),
    )
    inspected = restarted.inspect_migration_post(channel_id="-100123", message_id=777)
    assert inspected == {"content_hash": "AFTER_777", "state": "after_verified"}
    assert restarted.edit_calls == 0


def test_prepared_receipt_never_claims_after_when_live_render_does_not_match(tmp_path):
    new_cover = tmp_path / "new.png"
    old_cover = tmp_path / "old.png"
    png(new_cover, 230)
    png(old_cover, 30)
    receipt = tmp_path / "receipt.json"

    gateway = InMemoryLiveGateway(
        receipt_path=receipt,
        message=FakeMessage(caption="仍是旧帖", keyboard=(), photo_id=1),
        media_bytes=old_cover.read_bytes(),
    )
    gateway._save_prepared_receipt(
        channel_id="-100123",
        message_id=888,
        cover=str(new_cover),
        caption="应当的新帖",
        keyboard=keyboard(),
        expected_after_hash="AFTER_888",
    )
    inspected = gateway.inspect_migration_post(channel_id="-100123", message_id=888)
    assert inspected["state"] == "live_fingerprint"
    assert inspected["content_hash"] != "AFTER_888"
