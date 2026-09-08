"""Thin async Telegram callback handler for the side-by-side V3 User Bot.

Only callbacks in the explicit ``v3u:`` namespace are handled. The router and
response adapter make every business decision before this layer. This handler
performs Telegram edit/send operations for details, photos, and search cards,
and returns transition intents untouched for later orchestration.

The search-card renderer is public so initial search results and later card
navigation share one Telegram/session contract. This module is deliberately not
registered in the production Application yet.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import InputMediaPhoto
from telegram.constants import ParseMode

from .callback_router import CallbackRouter
from .callbacks import PREFIX
from .telegram_callback_response import (
    TelegramCallbackResponse,
    adapt_callback_response,
)


SEARCH_SESSION_KEY = "v3_find_card_public_ids"
SEARCH_ANCHOR_KEY = "v3_find_card_anchor"


@dataclass(frozen=True)
class TelegramCallbackHandlerOutcome:
    handled: bool
    response: TelegramCallbackResponse | None = None


def _session_ids(context: Any) -> tuple[str, ...]:
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return ()
    values = data.get(SEARCH_SESSION_KEY) or ()
    if not isinstance(values, (list, tuple)):
        return ()
    return tuple(str(value) for value in values)


def _chat_id(update: Any) -> int | str:
    chat = getattr(update, "effective_chat", None)
    value = getattr(chat, "id", None)
    if value is None:
        raise ValueError("telegram_effective_chat_missing")
    return value


async def _render_details(query: Any, response: TelegramCallbackResponse) -> None:
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=response.text,
            parse_mode=ParseMode.HTML,
            reply_markup=response.keyboard,
        )
        return
    await query.edit_message_text(
        response.text,
        parse_mode=ParseMode.HTML,
        reply_markup=response.keyboard,
    )


async def _render_photos(update: Any, context: Any, response: TelegramCallbackResponse) -> None:
    chat_id = _chat_id(update)
    bot = context.bot
    for group in response.media_groups:
        if len(group) == 1:
            path = Path(group[0])
            with path.open("rb") as handle:
                await bot.send_photo(chat_id=chat_id, photo=handle)
            continue
        media = []
        for raw in group:
            path = Path(raw)
            media.append(InputMediaPhoto(media=path.read_bytes()))
        await bot.send_media_group(chat_id=chat_id, media=media)
    await bot.send_message(
        chat_id=chat_id,
        text=response.text,
        parse_mode=ParseMode.HTML,
        reply_markup=response.keyboard,
    )


async def render_search_card_response(
    update: Any,
    context: Any,
    response: TelegramCallbackResponse,
    *,
    query: Any | None = None,
) -> None:
    """Render one search card and persist only its refreshed public-id session."""
    if response.kind != "card":
        raise ValueError("search_card_renderer_requires_card_response")

    message = getattr(query, "message", None) if query is not None else None
    has_photo = bool(getattr(message, "photo", None))
    photo_path = Path(response.photo_path) if response.photo_path else None
    sent = None

    if query is not None and has_photo and photo_path is not None:
        await query.edit_message_media(
            media=InputMediaPhoto(
                media=photo_path.read_bytes(),
                caption=response.text,
                parse_mode=ParseMode.HTML,
            ),
            reply_markup=response.keyboard,
        )
    elif query is not None and has_photo:
        await query.edit_message_caption(
            caption=response.text,
            parse_mode=ParseMode.HTML,
            reply_markup=response.keyboard,
        )
    elif query is not None and photo_path is None:
        await query.edit_message_text(
            response.text,
            parse_mode=ParseMode.HTML,
            reply_markup=response.keyboard,
        )
    elif photo_path is not None:
        with photo_path.open("rb") as handle:
            sent = await context.bot.send_photo(
                chat_id=_chat_id(update),
                photo=handle,
                caption=response.text,
                parse_mode=ParseMode.HTML,
                reply_markup=response.keyboard,
            )
    else:
        sent = await context.bot.send_message(
            chat_id=_chat_id(update),
            text=response.text,
            parse_mode=ParseMode.HTML,
            reply_markup=response.keyboard,
        )

    user_data = getattr(context, "user_data", None)
    if isinstance(user_data, dict):
        user_data[SEARCH_SESSION_KEY] = list(response.session_public_listing_ids)
        if sent is not None:
            sent_chat_id = getattr(sent, "chat_id", _chat_id(update))
            sent_message_id = getattr(sent, "message_id", None)
            if sent_message_id is not None:
                user_data[SEARCH_ANCHOR_KEY] = {
                    "chat_id": sent_chat_id,
                    "message_id": sent_message_id,
                }


async def handle_v3_callback(
    update: Any,
    context: Any,
    *,
    router: CallbackRouter,
) -> TelegramCallbackHandlerOutcome:
    """Handle one V3 callback and leave transition orchestration to the caller."""
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    if query is None or not raw.startswith(f"{PREFIX}:"):
        return TelegramCallbackHandlerOutcome(handled=False)

    dispatched = router.dispatch(
        raw,
        session_public_listing_ids=_session_ids(context),
    )
    response = adapt_callback_response(dispatched)

    # Always stop Telegram's callback spinner for callbacks owned by this
    # handler. Error/transition copy is intentionally handled elsewhere.
    await query.answer()

    if response.kind == "details":
        await _render_details(query, response)
    elif response.kind == "photos":
        await _render_photos(update, context, response)
    elif response.kind == "card":
        await render_search_card_response(update, context, response, query=query)

    return TelegramCallbackHandlerOutcome(
        handled=True,
        response=response,
    )


__all__ = [
    "SEARCH_ANCHOR_KEY",
    "SEARCH_SESSION_KEY",
    "TelegramCallbackHandlerOutcome",
    "handle_v3_callback",
    "render_search_card_response",
]
