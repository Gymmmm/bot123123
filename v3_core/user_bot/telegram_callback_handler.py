"""Thin async Telegram callback handler for the side-by-side V3 User Bot."""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from telegram import InputMediaPhoto
from telegram.constants import ParseMode

from .callback_router import CallbackRouter
from .callbacks import PREFIX, encode_card_callback
from .telegram_callback_response import TelegramCallbackResponse, adapt_callback_response
from .telegram_navigation import polish_listing_keyboard
from .telegram_transition_ui import build_transition_keyboard
from .transition_callbacks import parse_transition_callback
from .transition_plan import build_transition_plan
from .transition_session import apply_session_mutation, build_transition_session
from .transition_views import TransitionView, TransitionViewService


SEARCH_SESSION_KEY = "v3_find_card_public_ids"
SEARCH_ANCHOR_KEY = "v3_find_card_anchor"
LISTING_SOURCE_KEY = "v3_listing_source"
LISTING_TOUCHPOINT_KEY = "v3_listing_touchpoint"


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


def _listing_source(context: Any) -> str:
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return "listing_callback"
    return str(data.get(LISTING_SOURCE_KEY) or "").strip() or "listing_callback"


def _listing_touchpoint(context: Any) -> str:
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return ""
    return str(data.get(LISTING_TOUCHPOINT_KEY) or "").strip()


def _set_listing_touchpoint(context: Any, value: str) -> None:
    data = getattr(context, "user_data", None)
    if isinstance(data, dict):
        data[LISTING_TOUCHPOINT_KEY] = str(value or "").strip()


def _chat_id(update: Any) -> int | str:
    chat = getattr(update, "effective_chat", None)
    value = getattr(chat, "id", None)
    if value is None:
        raise ValueError("telegram_effective_chat_missing")
    return value


async def _render_details(query: Any, response: TelegramCallbackResponse) -> None:
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(caption=response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)
        return
    await query.edit_message_text(response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)


async def _render_photos(
    update: Any,
    context: Any,
    response: TelegramCallbackResponse,
    *,
    query: Any | None = None,
) -> None:
    """Render one photo + detail caption; flip in place via editMessageMedia."""
    chat_id = _chat_id(update)
    bot = context.bot
    photo_path = str(getattr(response, "photo_path", "") or "").strip()
    if not photo_path and response.media_groups:
        first = response.media_groups[0]
        if first:
            photo_path = str(first[0] or "").strip()
    path = Path(photo_path) if photo_path else None
    if path is not None and not path.is_file():
        path = None

    message = getattr(query, "message", None) if query is not None else None
    has_photo = bool(getattr(message, "photo", None))

    if query is not None and has_photo and path is not None:
        await query.edit_message_media(
            media=InputMediaPhoto(
                media=path.read_bytes(),
                caption=response.text,
                parse_mode=ParseMode.HTML,
            ),
            reply_markup=response.keyboard,
        )
        return
    if query is not None and has_photo and path is None:
        await query.edit_message_caption(
            caption=response.text,
            parse_mode=ParseMode.HTML,
            reply_markup=response.keyboard,
        )
        return
    if path is not None:
        with path.open("rb") as handle:
            await bot.send_photo(
                chat_id=chat_id,
                photo=handle,
                caption=response.text,
                parse_mode=ParseMode.HTML,
                reply_markup=response.keyboard,
            )
        return
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
        await query.edit_message_caption(caption=view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    await query.edit_message_text(view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def render_search_card_response(update: Any, context: Any, response: TelegramCallbackResponse, *, query: Any | None = None) -> None:
    if response.kind != "card":
        raise ValueError("search_card_renderer_requires_card_response")

    message = getattr(query, "message", None) if query is not None else None
    has_photo = bool(getattr(message, "photo", None))
    photo_path = Path(response.photo_path) if response.photo_path else None
    sent = None

    if query is not None and has_photo and photo_path is not None:
        await query.edit_message_media(media=InputMediaPhoto(media=photo_path.read_bytes(), caption=response.text, parse_mode=ParseMode.HTML), reply_markup=response.keyboard)
    elif query is not None and has_photo:
        await query.edit_message_caption(caption=response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)
    elif query is not None and photo_path is None:
        await query.edit_message_text(response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)
    elif photo_path is not None:
        with photo_path.open("rb") as handle:
            sent = await context.bot.send_photo(chat_id=_chat_id(update), photo=handle, caption=response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)
    else:
        sent = await context.bot.send_message(chat_id=_chat_id(update), text=response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)

    user_data = getattr(context, "user_data", None)
    if isinstance(user_data, dict):
        user_data[SEARCH_SESSION_KEY] = list(response.session_public_listing_ids)
        user_data[LISTING_SOURCE_KEY] = "search_result"
        user_data[LISTING_TOUCHPOINT_KEY] = "search_result"
        if sent is not None:
            sent_chat_id = getattr(sent, "chat_id", _chat_id(update))
            sent_message_id = getattr(sent, "message_id", None)
            if sent_message_id is not None:
                user_data[SEARCH_ANCHOR_KEY] = {"chat_id": sent_chat_id, "message_id": sent_message_id}


def _error_alert(response: TelegramCallbackResponse) -> str:
    if response.status == "expired":
        return "搜索结果已更新，请重新查找。"
    if response.status == "blocked":
        return "这套房当前状态已变化，请查看最新房态。"
    if response.status == "not_found":
        return "房源信息已更新，请重新打开。"
    return "这个操作已失效，请重新进入。"


def _dispatch(router: CallbackRouter, raw: str, context: Any):
    """Use the attributed contract while accepting older injected adapters."""
    kwargs = {
        "session_public_listing_ids": _session_ids(context),
        "source": _listing_source(context),
        "touchpoint": _listing_touchpoint(context),
    }
    try:
        return router.dispatch(raw, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument" not in str(exc):
            raise
        return router.dispatch(
            raw,
            session_public_listing_ids=kwargs["session_public_listing_ids"],
        )


async def handle_v3_callback(
    update: Any,
    context: Any,
    *,
    router: CallbackRouter,
    transition_views: TransitionViewService | None = None,
    advisor_url: str = "",
    channel_url: str = "",
) -> TelegramCallbackHandlerOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    if query is None or not raw.startswith(f"{PREFIX}:"):
        return TelegramCallbackHandlerOutcome(handled=False)
    if parse_transition_callback(raw) is not None:
        return TelegramCallbackHandlerOutcome(handled=False)

    session_ids = _session_ids(context)
    dispatched = _dispatch(router, raw, context)
    response = adapt_callback_response(dispatched)

    if response.kind == "error":
        await query.answer(_error_alert(response), show_alert=True)
        return TelegramCallbackHandlerOutcome(handled=True, response=response)

    if response.kind in {"details", "photos", "card"}:
        back_to_search_callback = ""
        callback = dispatched.callback
        if response.kind in {"details", "photos"} and callback is not None:
            public_id = str(getattr(callback, "public_listing_id", "") or "").strip()
            if public_id and public_id in session_ids:
                back_to_search_callback = encode_card_callback(
                    session_ids.index(public_id), public_id
                )
        response = replace(
            response,
            keyboard=polish_listing_keyboard(
                response.keyboard,
                advisor_url=advisor_url,
                channel_url=channel_url,
                back_to_search_callback=back_to_search_callback,
                listing_summary=str(getattr(response, "listing_summary", "") or ""),
                add_home=response.kind in {"details", "photos"},
                add_channel=response.kind in {"details", "photos"},
            ),
        )
    await query.answer()

    if response.kind == "details":
        await _render_details(query, response)
        _set_listing_touchpoint(context, "listing_details")
    elif response.kind == "photos":
        await _render_photos(update, context, response, query=query)
        action = str(getattr(dispatched, "action", "") or "")
        _set_listing_touchpoint(
            context,
            "listing_details" if action == "details" else "listing_photos",
        )
    elif response.kind == "card":
        await render_search_card_response(update, context, response, query=query)
    elif response.kind == "transition" and response.transition in {"book", "similar", "change_search"} and transition_views is not None:
        plan = build_transition_plan(response)
        mutation = build_transition_session(plan)
        view = transition_views.build(plan)
        await _render_transition_view(query, view)
        user_data = getattr(context, "user_data", None)
        if not isinstance(user_data, dict):
            raise ValueError("telegram_user_data_missing_for_transition")
        apply_session_mutation(user_data, mutation)
        if response.transition == "similar":
            user_data[LISTING_TOUCHPOINT_KEY] = "similar_listing"

    return TelegramCallbackHandlerOutcome(handled=True, response=response)


__all__ = [
    "LISTING_SOURCE_KEY",
    "LISTING_TOUCHPOINT_KEY",
    "SEARCH_ANCHOR_KEY",
    "SEARCH_SESSION_KEY",
    "TelegramCallbackHandlerOutcome",
    "handle_v3_callback",
    "render_search_card_response",
]
