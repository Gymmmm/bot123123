"""Thin Telegram adapter for one frozen V3 publication command.

The adapter owns only python-telegram-bot API shapes.  It does not read the DB,
rebuild captions, choose media, or touch discussion threads.  The durable state
machine remains in ``delivery_coordinator`` / ``delivery_state``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .channel_contract import official_channel_button_spec
from .delivery_coordinator import TelegramSendCommand


class TelegramBotLike(Protocol):
    async def send_photo(self, **kwargs: Any) -> Any: ...


def build_channel_keyboard(
    actions: dict[str, str], *, inventory_status: object = "active"
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, url=url) for label, url in row]
        for row in official_channel_button_spec(
            actions, inventory_status=inventory_status
        )
    ]
    return InlineKeyboardMarkup(rows)


class TelegramChannelAdapter:
    def __init__(self, bot: TelegramBotLike):
        self.bot = bot

    async def send(self, command: TelegramSendCommand) -> dict[str, Any]:
        cover = Path(str(command.cover_path)).expanduser().resolve()
        if not cover.is_file():
            raise FileNotFoundError(f"frozen_cover_missing:{cover}")
        if len(str(command.caption)) > 1024:
            raise ValueError("telegram_caption_too_long")
        keyboard = build_channel_keyboard(
            command.actions,
            inventory_status=command.inventory_status,
        )

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


async def deliver_approved_package(
    *,
    coordinator: Any,
    adapter: TelegramChannelAdapter,
    package_id: str,
    channel_chat_id: str,
) -> Any:
    """Execute one safe delivery attempt.

    Any exception after ``mark_sending`` is treated as unknown because Telegram
    may have accepted the request before the client observed the failure.  This
    intentionally blocks automatic retry until reconciliation.
    """
    command = coordinator.prepare_send(
        package_id=str(package_id),
        channel_chat_id=str(channel_chat_id),
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
