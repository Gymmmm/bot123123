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
from .transition_views import TransitionChoice, TransitionView, TransitionViewService


SEARCH_SESSION_KEY = "v3_find_card_public_ids"
SEARCH_ANCHOR_KEY = "v3_find_card_anchor"
SEARCH_CONTEXT_KEY = "v3_find_card_context"

# V4.0 error copy table (异常与系统提示).
BUTTON_EXPIRED_TEXT = "这个入口已经更新，请返回重新选择。"
NO_SIMILAR_TEXT = "目前也没有找到合适的相似房源，可以让顾问继续帮你找。"
LISTING_SOURCE_KEY = "v3_listing_source"
LISTING_TOUCHPOINT_KEY = "v3_listing_touchpoint"
PHOTOS_ALBUM_KEY = "v3_listing_photos_album"


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


def _album_state(update: Any, context: Any, public_listing_id: str) -> dict[str, Any]:
    """Return mutable album state for ``(chat_id, public_listing_id)``."""
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return {}
    albums = data.get(PHOTOS_ALBUM_KEY)
    if not isinstance(albums, dict):
        albums = {}
        data[PHOTOS_ALBUM_KEY] = albums
    try:
        chat_key = str(_chat_id(update))
    except Exception:
        chat_key = ""
    key = f"{chat_key}::{public_listing_id}"
    state = albums.get(key)
    if not isinstance(state, dict):
        state = {}
        albums[key] = state
    return state


def _album_message_ids(state: dict[str, Any]) -> list[str]:
    raw = state.get("message_ids") or []
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw if str(value).strip()]


def _remember_album_sent(state: dict[str, Any], *, message_ids: list[str], page: int) -> None:
    state["message_ids"] = list(message_ids)
    state["page"] = int(page)


def remember_photos_album(
    update: Any,
    context: Any,
    public_listing_id: str,
    *,
    message_ids: list[int | str],
    page: int = 0,
) -> None:
    """Record a rendered album so the next page can delete it cleanly."""
    public_id = str(public_listing_id or "").strip()
    if not public_id:
        return
    state = _album_state(update, context, public_id)
    _remember_album_sent(
        state,
        message_ids=[str(value) for value in message_ids if str(value).strip()],
        page=int(page),
    )


async def _delete_album_photos(
    bot: Any,
    chat_id_value: int | str,
    state: dict[str, Any],
    *,
    keep_action_bar: bool,
) -> None:
    """Delete the prior album messages recorded under ``state``.

    ``keep_action_bar=True`` deletes only the 4 photo frames and leaves the
    action bar message in place so it can be edited in-place. The action bar
    is always the LAST recorded id (see ``send_listing_photos_album``).
    ``keep_action_bar=False`` deletes every recorded id, used when the next
    step replaces the action bar with a brand-new message.
    """
    prior_ids = _album_message_ids(state)
    if not prior_ids:
        return
    if keep_action_bar and prior_ids:
        prior_ids = list(prior_ids[:-1])
    for raw in prior_ids:
        try:
            await bot.delete_message(chat_id=chat_id_value, message_id=int(raw))
        except Exception:
            continue


async def _render_photos(
    update: Any,
    context: Any,
    response: TelegramCallbackResponse,
    *,
    query: Any | None = None,
    page: int = 0,
    public_listing_id: str = "",
) -> None:
    """Send native album (+ optional action bar). Never flips in place.

    For paged callbacks (``page`` > 0 or paging explicitly requested) the prior
    album frame message ids stored under ``PHOTOS_ALBUM_KEY`` are deleted before
    the new album is sent, so repeated ◀/▶ presses never leave residual album
    spam. ``page`` matching the recorded last-page is a no-op (just answers the
    callback) — keeps paged buttons safe under flaky connections.
    """
    from .telegram_photos_render import send_listing_photos_album

    bot = getattr(context, "bot", None)
    chat_id_value = _chat_id(update)
    state: dict[str, Any] = {}
    if public_listing_id and bot is not None:
        state = _album_state(update, context, public_listing_id)
        last_page = int(state["page"]) if "page" in state else -1
        if int(page) == last_page and _album_message_ids(state):
            # Idempotent: same page, album already up-to-date.
            return
        await _delete_album_photos(
            bot, chat_id_value, state, keep_action_bar=False
        )

    sent_ids = await send_listing_photos_album(
        bot,
        chat_id=chat_id_value,
        media_groups=response.media_groups,
        media_caption=str(getattr(response, "media_caption", "") or ""),
        photo_path=str(getattr(response, "photo_path", "") or ""),
        text=response.text,
        reply_markup=response.keyboard,
        expand_only=bool(getattr(response, "expand_only", False)),
    )

    if public_listing_id and bot is not None:
        state = _album_state(update, context, public_listing_id)
        _remember_album_sent(state, message_ids=[int(s) for s in sent_ids], page=int(page))


async def _render_details(query: Any, response: TelegramCallbackResponse) -> None:
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(caption=response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)
        return
    await query.edit_message_text(response.text, parse_mode=ParseMode.HTML, reply_markup=response.keyboard)


async def _render_transition_view(query: Any, view: TransitionView) -> None:
    keyboard = build_transition_keyboard(view)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(caption=view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    await query.edit_message_text(view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def _send_transition_view(update: Any, context: Any, query: Any, view: TransitionView) -> None:
    keyboard = build_transition_keyboard(view)
    message = getattr(query, "message", None)
    reply = getattr(message, "reply_text", None)
    if callable(reply):
        await reply(view.text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    await context.bot.send_message(
        chat_id=_chat_id(update),
        text=view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


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
        return "这套房目前不再展示，看看其他选择吧。"
    return BUTTON_EXPIRED_TEXT


def _search_context(context: Any) -> dict | None:
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return None
    value = data.get(SEARCH_CONTEXT_KEY)
    return dict(value) if isinstance(value, dict) else None


def no_similar_view() -> TransitionView:
    return TransitionView(
        kind="similar_none",
        text=f"🏘️ {NO_SIMILAR_TEXT}",
        rows=(
            (TransitionChoice("💬 帮我找房", "home", "contact"),),
            (TransitionChoice("🔄 调整条件", "change_search"),),
        ),
    )


def _dispatch(router: CallbackRouter, raw: str, context: Any):
    """Use the attributed contract while accepting older injected adapters."""
    kwargs = {
        "session_public_listing_ids": _session_ids(context),
        "source": _listing_source(context),
        "touchpoint": _listing_touchpoint(context),
    }
    search_context = _search_context(context)
    if search_context is not None:
        try:
            return router.dispatch(raw, search_context=search_context, **kwargs)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
    try:
        return router.dispatch(raw, **kwargs)
    except TypeError as exc:
        if "unexpected keyword argument" not in str(exc):
            raise
        return router.dispatch(
            raw,
            session_public_listing_ids=kwargs["session_public_listing_ids"],
        )


async def _exit_album_to_other_surface(
    update: Any,
    context: Any,
    public_listing_id: str,
    *,
    keep_action_bar: bool,
) -> None:
    """Drop the 4 album photo frames before switching to details/transition.

    No-op when the user isn't currently sitting on a paged album for this
    listing (e.g. consult invoked from a search card). The lookup must not
    create any new ``PHOTOS_ALBUM_KEY`` entry — callers like ``consult`` have
    tests that assert ``user_data`` stays empty.

    Cleanup of ``message_ids`` / ``page`` happens on EVERY exit path, not only
    when the action bar is deleted. The next 「📷 查看实拍」entry must always
    re-send the 4 photos + action bar; otherwise the paged-album idempotent
    guard in ``_render_photos`` (same ``page == last_page``) would short-circuit
    and never re-issue the album.

    ``public_listing_id`` may be empty for callbacks that don't carry a target
    listing id (e.g. ``v3u:change_search``). In that case we fall back to the
    only listing currently held in the album state — a user only ever sits in
    one paged album at a time within a single chat.
    """
    bot = getattr(context, "bot", None)
    if bot is None:
        return
    data = getattr(context, "user_data", None)
    if not isinstance(data, dict):
        return
    albums = data.get(PHOTOS_ALBUM_KEY)
    if not isinstance(albums, dict) or not albums:
        return
    try:
        chat_key = str(_chat_id(update))
    except Exception:
        return
    state, resolved_public_id = _resolve_album_state(albums, chat_key, public_listing_id)
    if not isinstance(state, dict) or not _album_message_ids(state):
        return
    if not resolved_public_id:
        return
    await _delete_album_photos(
        bot, chat_key, state, keep_action_bar=keep_action_bar
    )
    # Always clear the recorded state so the next album entry is never
    # blocked by the page-idempotency guard. Even when the action bar is
    # kept in chat, the recorded message_ids no longer reflect reality
    # (the 4 photo frames are gone), so the next render must send fresh.
    state.pop("message_ids", None)
    state.pop("page", None)


def _resolve_album_state(albums: dict, chat_key: str, public_listing_id: str) -> tuple[Any, str]:
    """Return (state, public_listing_id) for the given chat.

    If ``public_listing_id`` is provided, look up ``<chat>::<public_id>``.
    If it's empty, fall back to the single active listing in this chat's
    album map (assumes one-album-per-chat at any time).
    """
    if public_listing_id:
        return albums.get(f"{chat_key}::{public_listing_id}"), public_listing_id
    matches = [
        (key.split("::", 1)[1] if "::" in key else "", state)
        for key, state in albums.items()
        if key.startswith(f"{chat_key}::") and isinstance(state, dict) and _album_message_ids(state)
    ]
    if len(matches) == 1:
        return matches[0][1], matches[0][0]
    return None, ""


def _callback_public_listing_id(dispatched) -> str:
    callback = getattr(dispatched, "callback", None)
    if callback is None:
        return ""
    return str(getattr(callback, "public_listing_id", "") or "").strip()


async def handle_v3_callback(
    update: Any,
    context: Any,
    *,
    router: CallbackRouter,
    transition_views: TransitionViewService | None = None,
    search_executor=None,
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
                add_home=(
                    response.kind == "details"
                    and not bool(getattr(response, "expand_only", False))
                ),
                add_channel=False,
            ),
        )
    await query.answer()

    if response.kind == "details":
        # Returning to listing details from the paged album: keep the action
        # bar in place and delete only the 4 photo frames so we can edit the
        # action bar to the details body.
        public_id_for_exit = _callback_public_listing_id(dispatched)
        await _exit_album_to_other_surface(
            update,
            context,
            public_id_for_exit,
            keep_action_bar=True,
        )
        await _render_details(query, response)
        _set_listing_touchpoint(context, "listing_details")
    elif response.kind == "photos":
        public_id_for_album = ""
        page_for_album = 0
        callback_obj = dispatched.callback
        if callback_obj is not None:
            public_id_for_album = str(
                getattr(callback_obj, "public_listing_id", "") or ""
            ).strip()
            if getattr(callback_obj, "page_index", None) is not None:
                page_for_album = int(getattr(callback_obj, "page_index") or 0)
        await _render_photos(
            update,
            context,
            response,
            query=query,
            page=page_for_album,
            public_listing_id=public_id_for_album,
        )
        action = str(getattr(dispatched, "action", "") or "")
        callback = dispatched.callback
        is_expand = bool(
            getattr(response, "expand_only", False)
            or (
                callback is not None
                and getattr(callback, "target_index", None) is not None
                and int(getattr(callback, "target_index") or 0) > 0
            )
        )
        if is_expand:
            _set_listing_touchpoint(context, "listing_photos_expand")
        else:
            _set_listing_touchpoint(
                context,
                "listing_details" if action == "details" else "listing_photos",
            )
    elif response.kind == "card":
        await render_search_card_response(update, context, response, query=query)
    elif response.kind == "transition" and response.transition == "consult":
        # Consult is a pure intent — no transition view is rendered here. The
        # higher-level intake handler turns the intent into the actual handoff.
        # We still need to clear the paged album if the user invoked consult
        # from inside the album.
        public_id_for_exit = _callback_public_listing_id(dispatched)
        if not public_id_for_exit and getattr(response, "consult_intent", None) is not None:
            public_id_for_exit = str(
                getattr(response.consult_intent, "public_listing_id", "") or ""
            ).strip()
        await _exit_album_to_other_surface(
            update,
            context,
            public_id_for_exit,
            keep_action_bar=False,
        )
    elif response.kind == "transition" and response.transition in {"book", "similar", "change_search"} and transition_views is not None:
        # Book / similar / change_search from the paged album: drop only the
        # 4 photo frames and KEEP the action bar in chat. The downstream
        # _render_transition_view uses query.edit_message_text/caption to
        # rewrite that action bar in place — deleting it would leave the
        # view to render against an already-deleted message.
        public_id_for_exit = _callback_public_listing_id(dispatched)
        await _exit_album_to_other_surface(
            update,
            context,
            public_id_for_exit,
            keep_action_bar=True,
        )
        plan = build_transition_plan(response)
        mutation = build_transition_session(plan)
        view = transition_views.build(plan)
        
        # Direct similar search for rented/offline listings
        if response.transition == "similar" and plan.next_step == "search_submit" and search_executor is not None:
            from .search_flow import SearchFlowService
            from .search_query import SearchCriteria
            from .search_no_match_view import build_search_no_match_view
            from .telegram_search_results import present_search_flow_result
            intent = response.similar_intent
            if intent is not None:
                criteria = SearchCriteria(
                    location_keys=tuple(intent.location_keys) if intent.location_keys else (),
                    budget_min=int(intent.budget_min) if intent.budget_min else None,
                    budget_max=int(intent.budget_max) if intent.budget_max else None,
                    property_type=str(intent.property_type or "").strip() or "",
                    room_type=str(intent.room_type or "").strip() or "",
                    raw_text="",
                )
                flow = SearchFlowService(search_executor.flow)
                similar_result = flow.similar(criteria, limit=5)
                await _render_transition_view(query, view)
                similar_presentation = await present_search_flow_result(update, context, similar_result)
                if not similar_presentation.matched:
                    await _render_transition_view(query, no_similar_view())
                user_data = getattr(context, "user_data", None)
                if isinstance(user_data, dict):
                    apply_session_mutation(user_data, mutation)
                    user_data[LISTING_TOUCHPOINT_KEY] = "similar_listing"
                return TelegramCallbackHandlerOutcome(handled=True, response=response)
        
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
    "SEARCH_CONTEXT_KEY",
    "SEARCH_SESSION_KEY",
    "TelegramCallbackHandlerOutcome",
    "handle_v3_callback",
    "remember_photos_album",
    "render_search_card_response",
]
