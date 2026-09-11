"""Thin async Telegram callback handler for the side-by-side V3 User Bot.

Only callbacks owned by the existing listing/card router are handled here.
Renderable details/photos/cards are applied directly. Side-effect-free transition
entries (book, similar, change-search) may also be rendered when a
``TransitionViewService`` is injected; session mutations are applied only after
the Telegram edit succeeds.

Child callbacks in the explicit ``v3u:t:`` transition namespace are deliberately
not claimed by this router handler. They belong to the separate transition-action
handler. Consult also remains an intent when no direct advisor handoff is
configured.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from telegram import InputMediaPhoto
from telegram.constants import ParseMode

from .callback_router import CallbackRouter
from .callbacks import PREFIX, encode_card_callback
from .telegram_callback_response import (
    TelegramCallbackResponse,
    adapt_callback_response,
)
from .telegram_navigation import polish_listing_keyboard
from .telegram_transition_ui import build_transition_keyboard
from .transition_callbacks import parse_transition_callback
from .transition_plan import build_transition_plan
from .transition_session import apply_session_mutation, build_transition_session
from .transition_views import TransitionView, TransitionViewService


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


async def _render_transition_view(query: Any, view: TransitionView) -> None:
    keyboard = build_transition_keyboard(view)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return
    await query.edit_message_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
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


def _error_alert(response: TelegramCallbackResponse) -> str:
    if response.status == "expired":
        return "搜索结果已更新，请重新查找。"
    if response.status == "blocked":
        return "这套房当前状态已变化，请查看最新房态。"
    if response.status == "not_found":
        return "房源信息已更新，请重新打开。"
    return "这个操作已失效，请重新进入。"


async def handle_v3_callback(
    update: Any,
    context: Any,
    *,
    router: CallbackRouter,
    transition_views: TransitionViewService | None = None,
    advisor_url: str = "",
    channel_url: str = "",
) -> TelegramCallbackHandlerOutcome:
    """Handle one router-owned V3 callback without swallowing child transitions."""
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    if query is None or not raw.startswith(f"{PREFIX}:"):
        return TelegramCallbackHandlerOutcome(handled=False)

    if parse_transition_callback(raw) is not None:
        return TelegramCallbackHandlerOutcome(handled=False)

    session_ids = _session_ids(context)
    dispatched = router.dispatch(
        raw,
        session_public_listing_ids=session_ids,
    )
    response = adapt_callback_response(dispatched)

    if response.kind == "error":
        await query.answer(_error_alert(response), show_alert=True)
        return TelegramCallbackHandlerOutcome(handled=True, response=response)

    # Product polish happens only at the Telegram transport boundary. A configured
    # advisor becomes a one-tap public-username handoff with the listing ID already
    # drafted. Details/photos also receive real exits back to channel/home.
    if response.kind in {"details", "photos", "card"}:
        back_to_search_callback = ""
        callback = dispatched.callback
        if response.kind == "details" and callback is not None:
            public_id = str(getattr(callback, "public_listing_id", "") or "").strip()
            if public_id and public_id in session_ids:
                back_to_search_callback = encode_card_callback(
                    session_ids.index(public_id),
                    public_id,
                )
        response = replace(
            response,
            keyboard=polish_listing_keyboard(
                response.keyboard,
                advisor_url=advisor_url,
                channel_url=channel_url,
                back_to_search_callback=back_to_search_callback,
                add_home=response.kind in {"details", "photos"},
                add_channel=response.kind in {"details", "photos"},
            ),
        )

    await query.answer()

    if response.kind == "details":
        await _render_details(query, response)
    elif response.kind == "photos":
        await _render_photos(update, context, response)
    elif response.kind == "card":
        await render_search_card_response(update, context, response, query=query)
    elif (
        response.kind == "transition"
        and response.transition in {"book", "similar", "change_search"}
        and transition_views is not None
    ):
        plan = build_transition_plan(response)
        mutation = build_transition_session(plan)
        view = transition_views.build(plan)
        await _render_transition_view(query, view)
        user_data = getattr(context, "user_data", None)
        if not isinstance(user_data, dict):
            raise ValueError("telegram_user_data_missing_for_transition")
        apply_session_mutation(user_data, mutation)

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
