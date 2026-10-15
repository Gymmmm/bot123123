"""Thin Telegram adapter for one frozen V3 publication command.

The adapter owns only python-telegram-bot API shapes.  It does not read the DB,
rebuild captions, choose media, or touch discussion threads.  The durable state
machine remains in ``delivery_coordinator`` / ``delivery_state``.

Both new sends and existing-post edits consume the same frozen command so
caption, actions and live inventory status cannot drift into separate Telegram
implementations.
"""
from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Any, Protocol

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.constants import ParseMode

from .channel_contract import official_channel_button_spec
from .delivery_coordinator import TelegramSendCommand


class TelegramBotLike(Protocol):
    async def send_photo(self, **kwargs: Any) -> Any: ...
    async def send_media_group(self, **kwargs: Any) -> Any: ...
    async def send_message(self, **kwargs: Any) -> Any: ...
    async def edit_message_media(self, **kwargs: Any) -> Any: ...
    async def edit_message_caption(self, **kwargs: Any) -> Any: ...
    async def edit_message_text(self, **kwargs: Any) -> Any: ...
    async def edit_message_reply_markup(self, **kwargs: Any) -> Any: ...


def build_channel_keyboard(
    actions: dict[str, str], *, inventory_status: object = "active", area: object = ""
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, url=url) for label, url in row]
        for row in official_channel_button_spec(
            actions, inventory_status=inventory_status, area=area
        )
    ]
    return InlineKeyboardMarkup(rows)


def _telegram_error_text(exc: Exception) -> str:
    return str(exc or "").strip().lower()


def _message_not_modified(exc: Exception) -> bool:
    return "message is not modified" in _telegram_error_text(exc)


def _media_edit_requires_text_fallback(exc: Exception) -> bool:
    text = _telegram_error_text(exc)
    return any(
        token in text
        for token in (
            "there is no media",
            "message can't be edited",
            "message can\u2019t be edited",
            "message to edit not found",
            "message is not a media message",
        )
    )


class TelegramChannelAdapter:
    def __init__(self, bot: TelegramBotLike):
        self.bot = bot

    @staticmethod
    def _validated(
        command: TelegramSendCommand,
    ) -> tuple[Path, tuple[Path, ...], InlineKeyboardMarkup]:
        cover = Path(str(command.cover_path)).expanduser().resolve()
        if not cover.is_file():
            raise FileNotFoundError(f"frozen_cover_missing:{cover}")
        gallery: list[Path] = []
        for raw_path in command.gallery_paths:
            path = Path(str(raw_path)).expanduser().resolve()
            if not path.is_file():
                raise FileNotFoundError(f"frozen_gallery_missing:{path}")
            if path != cover and path not in gallery:
                gallery.append(path)
        if len(str(command.caption)) > 1024:
            raise ValueError("telegram_caption_too_long")
        keyboard = build_channel_keyboard(
            command.actions,
            inventory_status=command.inventory_status,
            area=command.area_key,
        )
        return cover, tuple(gallery), keyboard

    async def send(self, command: TelegramSendCommand) -> dict[str, Any]:
        cover, gallery, keyboard = self._validated(command)
        # Telegram albums need at least two items and cannot carry an inline
        # keyboard.  Keep the complete frozen caption on the first image, then
        # send one compact action bar immediately after the album.
        if gallery:
            paths = (cover, *gallery[:3])
            with ExitStack() as stack:
                handles = [stack.enter_context(path.open("rb")) for path in paths]
                media = [
                    InputMediaPhoto(
                        media=handle,
                        caption=command.caption if index == 0 else None,
                        parse_mode=ParseMode.HTML if index == 0 else None,
                    )
                    for index, handle in enumerate(handles)
                ]
                messages = await self.bot.send_media_group(
                    chat_id=command.channel_chat_id,
                    media=media,
                )
            message_ids = [
                str(getattr(message, "message_id", "") or "") for message in messages
            ]
            if len(message_ids) != len(paths) or any(not value for value in message_ids):
                raise RuntimeError("telegram_album_returned_incomplete_message_ids")
            button_message = await self.bot.send_message(
                chat_id=command.channel_chat_id,
                text="👇 选择房源服务",
                reply_markup=keyboard,
                reply_to_message_id=int(message_ids[0]),
            )
            button_message_id = str(
                getattr(button_message, "message_id", "") or ""
            )
            if not button_message_id:
                raise RuntimeError("telegram_button_send_returned_no_message_id")
            media_group_id = str(
                getattr(messages[0], "media_group_id", "") or ""
            )
            return {
                "media_message_ids": message_ids,
                "caption_message_id": message_ids[0],
                "button_message_id": button_message_id,
                "media_group_id": media_group_id,
                "caption": command.caption,
                "single_cover": False,
                "discussion_published": False,
            }

        with cover.open("rb") as handle:
            message = await self.bot.send_photo(
                chat_id=command.channel_chat_id,
                photo=handle,
                caption=command.caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )

        message_id = getattr(message, "message_id", None)
        if message_id in (None, ""):
            raise RuntimeError("telegram_send_returned_no_message_id")
        return {
            "media_message_ids": [str(message_id)],
            "caption_message_id": str(message_id),
            "button_message_id": str(message_id),
            "caption": command.caption,
            "single_cover": True,
            "discussion_published": False,
        }

    async def edit(self, command: TelegramSendCommand, *, message_id: str | int) -> dict[str, Any]:
        """Replace one existing Telegram publication using the normal send contract."""
        cover, _, keyboard = self._validated(command)
        target = int(message_id)
        mode = "media"
        try:
            with cover.open("rb") as handle:
                media = InputMediaPhoto(
                    media=handle,
                    caption=command.caption,
                    parse_mode=ParseMode.HTML,
                )
                await self.bot.edit_message_media(
                    chat_id=command.channel_chat_id,
                    message_id=target,
                    media=media,
                    reply_markup=keyboard,
                )
        except Exception as exc:
            if _message_not_modified(exc):
                await self.bot.edit_message_reply_markup(
                    chat_id=command.channel_chat_id,
                    message_id=target,
                    reply_markup=keyboard,
                )
                mode = "reply_markup"
            elif _media_edit_requires_text_fallback(exc):
                try:
                    await self.bot.edit_message_text(
                        chat_id=command.channel_chat_id,
                        message_id=target,
                        text=command.caption,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard,
                    )
                    mode = "text"
                except Exception as text_exc:
                    if not _message_not_modified(text_exc):
                        raise
                    await self.bot.edit_message_reply_markup(
                        chat_id=command.channel_chat_id,
                        message_id=target,
                        reply_markup=keyboard,
                    )
                    mode = "reply_markup"
            else:
                raise

        return {
            "media_message_ids": [str(target)],
            "caption_message_id": str(target),
            "button_message_id": str(target),
            "caption": command.caption,
            "single_cover": True,
            "discussion_published": False,
            "edit_mode": mode,
        }


async def deliver_approved_package(
    *,
    coordinator: Any,
    adapter: TelegramChannelAdapter,
    package_id: str,
    channel_chat_id: str,
    inventory_status_override: str | None = None,
) -> Any:
    """Execute one safe delivery attempt.

    Any exception after ``mark_sending`` is treated as unknown because Telegram
    may have accepted the request before the client observed the failure.  This
    intentionally blocks automatic retry until reconciliation.
    """
    command = coordinator.prepare_send(
        package_id=str(package_id),
        channel_chat_id=str(channel_chat_id),
        inventory_status_override=inventory_status_override,
    )
    coordinator.mark_sending(command.attempt_id)
    try:
        receipt = await adapter.send(command)
    except Exception as exc:
        coordinator.mark_unknown(command.attempt_id, str(exc))
        raise
    return coordinator.record_sent(
        attempt_id=command.attempt_id,
        telegram_result=receipt,
    )


__all__ = [
    "TelegramChannelAdapter",
    "build_channel_keyboard",
    "deliver_approved_package",
]
