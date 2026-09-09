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

from v3_core.user_bot.listing_presenter import inventory_status_bookable

from .delivery_coordinator import TelegramSendCommand
from .package_store import CHANNEL_ACTION_ORDER


class TelegramBotLike(Protocol):
    async def send_photo(self, **kwargs: Any) -> Any: ...


def build_channel_keyboard(
    actions: dict[str, str], *, inventory_status: object = "active"
) -> InlineKeyboardMarkup:
    if tuple(actions) != CHANNEL_ACTION_ORDER:
        raise ValueError("telegram_actions_must_be_details_photos_book")
    if any(not str(actions[key] or "").strip() for key in CHANNEL_ACTION_ORDER):
        raise ValueError("telegram_action_url_missing")
    rows = [
        [
            InlineKeyboardButton("🏠 房源详情", url=actions["details"]),
            InlineKeyboardButton("📸 更多实拍", url=actions["photos"]),
        ]
    ]
    if inventory_status_bookable(inventory_status):
        rows.append([InlineKeyboardButton("📅 预约看房", url=actions["book"])])
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
