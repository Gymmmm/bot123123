from __future__ import annotations

import asyncio
import hashlib
import html as html_std
import io
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from PIL import Image
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telethon import TelegramClient
from telethon.extensions import html as telethon_html


class TelegramMigrationGatewayError(RuntimeError):
    pass


def _stable_hash(payload: object) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _entity_signature(entities: Iterable[Any] | None) -> tuple[tuple[Any, ...], ...]:
    result: list[tuple[Any, ...]] = []
    for entity in entities or ():
        result.append((
            type(entity).__name__,
            int(getattr(entity, "offset", 0) or 0),
            int(getattr(entity, "length", 0) or 0),
            str(getattr(entity, "url", "") or ""),
            str(getattr(entity, "user_id", "") or ""),
            str(getattr(entity, "language", "") or ""),
            str(getattr(entity, "document_id", "") or ""),
        ))
    return tuple(result)


def _keyboard_from_telethon(message: Any) -> tuple[tuple[str, str], ...]:
    markup = getattr(message, "reply_markup", None)
    rows = getattr(markup, "rows", None) or ()
    output: list[tuple[str, str]] = []
    for row in rows:
        for button in getattr(row, "buttons", None) or ():
            text = getattr(button, "text", None)
            url = getattr(button, "url", None)
            if text and url:
                output.append((str(text), str(url)))
    return tuple(output)


def _normalize_keyboard(value: Iterable[Mapping[str, Any]] | Iterable[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    output: list[tuple[str, str]] = []
    for item in value:
        if isinstance(item, Mapping):
            output.append((str(item.get("text") or ""), str(item.get("url") or "")))
        else:
            text, url = item
            output.append((str(text), str(url)))
    return tuple(output)


def _expected_caption_signature(caption_html: str) -> tuple[str, tuple[tuple[Any, ...], ...]]:
    try:
        text, entities = telethon_html.parse(str(caption_html or ""))
    except Exception as exc:
        raise TelegramMigrationGatewayError(f"invalid_expected_caption_html:{exc}") from exc
    return str(text), _entity_signature(entities)


def _message_caption_signature(message: Any) -> tuple[str, tuple[tuple[Any, ...], ...]]:
    return str(getattr(message, "message", "") or ""), _entity_signature(getattr(message, "entities", None))


def _media_identity(message: Any) -> dict[str, str]:
    photo = getattr(message, "photo", None)
    document = getattr(message, "document", None)
    if photo is not None:
        return {"type": "photo", "id": str(getattr(photo, "id", "") or "")}
    if document is not None:
        return {"type": "document", "id": str(getattr(document, "id", "") or "")}
    return {"type": "none", "id": ""}


def _dhash_bytes(data: bytes) -> int:
    with Image.open(io.BytesIO(data)) as image:
        gray = image.convert("L").resize((9, 8))
        pixels = list(gray.getdata())
    value = 0
    for row in range(8):
        for col in range(8):
            left = pixels[row * 9 + col]
            right = pixels[row * 9 + col + 1]
            value = (value << 1) | int(left > right)
    return value


def _dhash_file(path: str | Path) -> int:
    with open(path, "rb") as handle:
        return _dhash_bytes(handle.read())


def _hamming(a: int, b: int) -> int:
    return (int(a) ^ int(b)).bit_count()


class TelegramMigrationGateway:
    """Phase-11 live adapter.

    Telethon is used for read-only inspection because the Bot API cannot fetch an
    arbitrary historical channel message. The publisher bot is used only for the
    exact known-message media edit.
    """

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        session_path: str,
        publisher_bot_token: str,
        cover_dhash_max: int = 8,
    ) -> None:
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.session_path = str(session_path)
        self.publisher_bot_token = str(publisher_bot_token)
        self.cover_dhash_max = max(0, int(cover_dhash_max))
        if not self.api_id or not self.api_hash or not self.session_path or not self.publisher_bot_token:
            raise TelegramMigrationGatewayError("missing_migration_gateway_credentials")

    @staticmethod
    def _run(coro):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        raise TelegramMigrationGatewayError("migration_gateway_requires_sync_cli_context")

    async def _fetch_message_async(self, *, channel_id: str, message_id: int):
        client = TelegramClient(self.session_path, self.api_id, self.api_hash)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise TelegramMigrationGatewayError("telethon_session_not_authorized")
            message = await client.get_messages(int(channel_id), ids=int(message_id))
            if message is None:
                raise TelegramMigrationGatewayError(f"telegram_message_not_found:{channel_id}:{message_id}")
            media_bytes = b""
            if getattr(message, "media", None) is not None:
                downloaded = await client.download_media(message, file=bytes)
                if isinstance(downloaded, (bytes, bytearray)):
                    media_bytes = bytes(downloaded)
            return message, media_bytes
        finally:
            await client.disconnect()

    def _fetch_message(self, *, channel_id: str, message_id: int):
        return self._run(self._fetch_message_async(channel_id=channel_id, message_id=message_id))

    async def _edit_async(
        self,
        *,
        channel_id: str,
        message_id: int,
        cover: str,
        caption: str,
        keyboard: tuple[tuple[str, str], ...],
    ):
        cover_path = Path(cover)
        if not cover_path.is_file():
            raise TelegramMigrationGatewayError(f"cover_not_found:{cover}")
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(text=text, url=url)] for text, url in keyboard
        ])
        async with Bot(self.publisher_bot_token) as bot:
            with cover_path.open("rb") as handle:
                media = InputMediaPhoto(media=handle, caption=caption, parse_mode="HTML")
                return await bot.edit_message_media(
                    chat_id=channel_id,
                    message_id=int(message_id),
                    media=media,
                    reply_markup=markup,
                )

    def _edit(self, **kwargs):
        return self._run(self._edit_async(**kwargs))

    def _live_fingerprint(self, *, channel_id: str, message_id: int, message: Any) -> str:
        payload = {
            "channel_id": str(channel_id),
            "message_id": int(message_id),
            "caption": _message_caption_signature(message),
            "keyboard": _keyboard_from_telethon(message),
            "media": _media_identity(message),
        }
        return _stable_hash(payload)

    def _matches_expected_render(
        self,
        *,
        message: Any,
        media_bytes: bytes,
        expected_cover: str,
        expected_caption: str,
        expected_keyboard: tuple[tuple[str, str], ...],
    ) -> bool:
        if _message_caption_signature(message) != _expected_caption_signature(expected_caption):
            return False
        if _keyboard_from_telethon(message) != expected_keyboard:
            return False
        if _media_identity(message).get("type") != "photo" or not media_bytes:
            return False
        cover_path = Path(expected_cover)
        if not cover_path.is_file():
            raise TelegramMigrationGatewayError(f"cover_not_found:{expected_cover}")
        try:
            return _hamming(_dhash_bytes(media_bytes), _dhash_file(cover_path)) <= self.cover_dhash_max
        except Exception as exc:
            raise TelegramMigrationGatewayError(f"cover_verification_failed:{exc}") from exc

    def inspect_migration_post(
        self,
        *,
        channel_id: str,
        message_id: int,
        expected_cover: str | None = None,
        expected_caption: str | None = None,
        expected_keyboard: Iterable[Mapping[str, Any]] | Iterable[tuple[str, str]] | None = None,
        expected_after_hash: str | None = None,
    ) -> dict[str, Any]:
        message, media_bytes = self._fetch_message(channel_id=channel_id, message_id=message_id)
        keyboard = _normalize_keyboard(expected_keyboard or ())
        if expected_after_hash and expected_cover and expected_caption is not None and keyboard:
            if self._matches_expected_render(
                message=message,
                media_bytes=media_bytes,
                expected_cover=expected_cover,
                expected_caption=expected_caption,
                expected_keyboard=keyboard,
            ):
                return {"content_hash": str(expected_after_hash), "state": "after_verified"}
        return {
            "content_hash": self._live_fingerprint(
                channel_id=channel_id, message_id=message_id, message=message
            ),
            "state": "live_fingerprint",
        }

    def edit_migration_post(
        self,
        *,
        channel_id: str,
        message_id: int,
        cover: str,
        caption: str,
        keyboard: Iterable[Mapping[str, Any]],
        expected_before_hash: str,
        expected_after_hash: str,
    ) -> dict[str, Any]:
        normalized_keyboard = _normalize_keyboard(keyboard)
        before = self.inspect_migration_post(channel_id=channel_id, message_id=message_id)
        if str(before.get("content_hash") or "") != str(expected_before_hash):
            raise TelegramMigrationGatewayError(
                f"before_hash_conflict:{channel_id}:{message_id}:{before.get('content_hash')}"
            )
        result = self._edit(
            channel_id=channel_id,
            message_id=message_id,
            cover=cover,
            caption=caption,
            keyboard=normalized_keyboard,
        )
        after = self.inspect_migration_post(
            channel_id=channel_id,
            message_id=message_id,
            expected_cover=cover,
            expected_caption=caption,
            expected_keyboard=normalized_keyboard,
            expected_after_hash=expected_after_hash,
        )
        if str(after.get("content_hash") or "") != str(expected_after_hash):
            raise TelegramMigrationGatewayError(f"post_edit_verification_failed:{channel_id}:{message_id}")
        returned_id = int(getattr(result, "message_id", 0) or message_id)
        if returned_id != int(message_id):
            raise TelegramMigrationGatewayError("telegram_edit_returned_wrong_message_id")
        return {"ok": True, "message_id": int(message_id), "verified_after_hash": str(expected_after_hash)}


def _resolve_session_path() -> str:
    explicit = str(os.getenv("TELETHON_SESSION_PATH") or "").strip()
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    name = str(os.getenv("COLLECTOR_SESSION_NAME") or "qiaolian_collector").strip()
    return str((Path.cwd() / "telethon_sessions" / name).resolve())


def build_gateway() -> TelegramMigrationGateway:
    api_id = int(str(os.getenv("TG_API_ID") or "0").strip() or 0)
    api_hash = str(os.getenv("TG_API_HASH") or "").strip()
    token = str(os.getenv("PUBLISHER_BOT_TOKEN") or "").strip()
    threshold = int(str(os.getenv("MIGRATION_COVER_DHASH_MAX") or "8").strip() or 8)
    return TelegramMigrationGateway(
        api_id=api_id,
        api_hash=api_hash,
        session_path=_resolve_session_path(),
        publisher_bot_token=token,
        cover_dhash_max=threshold,
    )


__all__ = ["TelegramMigrationGateway", "TelegramMigrationGatewayError", "build_gateway"]
