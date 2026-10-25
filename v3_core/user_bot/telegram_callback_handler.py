"""Thin async Telegram callback handler for the side-by-side V3 User Bot."""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from html import escape as html_escape, unescape as html_unescape
import re
from io import BytesIO
from PIL import Image, ImageOps
from typing import Any

from telegram import InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from .callback_router import CallbackRouter
from .callbacks import PREFIX, encode_card_callback
from v3_core.publishing.public_ids import normalize_public_id
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
# Album state ``page`` marker for the listing detail surface (photos + text).
DETAILS_ALBUM_PAGE = -2


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
        # Keep the existing page visible until the replacement has actually
        # been sent. Deleting it first makes Telegram jump to an empty screen,
        # and a transient send failure otherwise leaves the customer stranded.
        stale_ids = _album_message_ids(state)

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
        if not sent_ids:
            return
        _remember_album_sent(state, message_ids=[int(s) for s in sent_ids], page=int(page))
        for old_id in stale_ids:
            if str(old_id) in {str(new_id) for new_id in sent_ids}:
                continue
            try:
                await bot.delete_message(chat_id=chat_id_value, message_id=int(old_id))
            except Exception:
                continue


def _single_detail_caption(text: str, index: int, total: int) -> str:
    """Keep the complete useful detail in the same Telegram photo message."""
    begin = index * 4 + 1
    end = min(begin + 3, total)
    heading = f"📷 实拍 {begin}–{end}/{total} · 点击查看大图\n\n"
    content = str(text or "").strip()
    if len(heading + content) <= 1024:
        return heading + content
    # No truncated HTML entities/tags: Telegram rejects malformed HTML.
    plain = html_unescape(re.sub(r"<[^>]*>", "", content))
    return heading + html_escape(plain[: 970 - len(heading)])


def _single_detail_keyboard(
    markup: Any, public_id: str, index: int, total: int,
) -> InlineKeyboardMarkup:
    """Photo controls and listing actions live on one persistent message."""
    rows: list[list[InlineKeyboardButton]] = []
    pages = (total + 3) // 4
    if pages > 1:
        photo_row = []
        if index > 0:
            photo_row.append(InlineKeyboardButton(
                "⬅️ 上一组", callback_data=f"v3u:detailphoto:{public_id}:{index - 1}"
            ))
        photo_row.append(InlineKeyboardButton(
            f"{index + 1}/{pages}", callback_data=f"v3u:detailphoto:{public_id}:{index}"
        ))
        if index + 1 < pages:
            photo_row.append(InlineKeyboardButton(
                "下一组 ➡️", callback_data=f"v3u:detailphoto:{public_id}:{index + 1}"
            ))
        rows.append(photo_row)
    if total > 1:
        rows.append([InlineKeyboardButton(
            "🔎 查看高清原图", callback_data=f"v3u:listing:photos:{public_id}:pg:{index}"
        )])
    for row in (getattr(markup, "inline_keyboard", None) or ()):
        clean = [
            button for button in row
            if str(getattr(button, "text", "") or "") != "📸 下一组实拍"
        ]
        if clean:
            rows.append(clean)
    return InlineKeyboardMarkup(rows)


def _detail_grid(paths: tuple[str, ...], page: int) -> bytes:
    """Photo-first responsive collage: large hero plus proportionate supporting shots.

    Keep pictures free from logos and text; adapt the lower row to the count.
    Avoid four equal squares, which reduce legibility on Telegram phones.
    """
    selected = paths[page * 4: page * 4 + 4]
    if not selected:
        raise ValueError("detail_grid_has_no_photos")

    width, gutter = 1200, 6
    if len(selected) == 1:
        layout = ((0, 0, 1200, 800),)
        height = 800
    elif len(selected) == 2:
        layout = ((0, 0, 1200, 660), (0, 666, 1200, 540))
        height = 1206
    else:
        hero_height, row_height = 715, 348
        count = len(selected) - 1
        cell_width = (width - gutter * (count - 1)) // count
        layout = [(0, 0, width, hero_height)]
        for i in range(count):
            x = i * (cell_width + gutter)
            cell = width - x if i == count - 1 else cell_width
            layout.append((x, hero_height + gutter, cell, row_height))
        height = hero_height + gutter + row_height

    canvas = Image.new("RGB", (width, height), "white")
    for source, (x, y, w, h) in zip(selected, layout):
        with Image.open(source) as original:
            corrected = ImageOps.exif_transpose(original).convert("RGB")
            photo = ImageOps.fit(
                corrected, (w, h), method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.40 if y == 0 else 0.5),
            )
            canvas.paste(photo, (x, y))
    output = BytesIO()
    canvas.save(output, format="JPEG", quality=89, optimize=True)
    return output.getvalue()


async def send_listing_details(
    update: Any,
    context: Any,
    *,
    public_listing_id: str,
    media_groups: tuple[tuple[str, ...], ...],
    media_caption: str,
    text: str,
    reply_markup: Any,
    photo_paths: tuple[str, ...] = (),
    replace_message: Any = None,
    force: bool = False,
) -> bool:
    """One editable Telegram photo+caption+keyboard; never a second detail bubble.

    A channel deep link sends one message. Callbacks from an existing photo card
    edit that same message whenever Telegram permits. Photo navigation then only
    edits that message's media, keeping its ID and scroll position stable.
    """
    bot = getattr(context, "bot", None)
    if bot is None:
        raise ValueError("telegram_bot_missing_for_listing_details")
    chat_id_value = _chat_id(update)
    public_id = str(public_listing_id or "").strip()
    state = _album_state(update, context, public_id) if public_id else {}
    previous = _album_message_ids(state)
    replace_id = str(getattr(replace_message, "message_id", "") or "").strip()
    if (replace_id and replace_id in previous
            and state.get("page") == DETAILS_ALBUM_PAGE
            and state.get("detail_index") == 0 and not force):
        return False
    candidates = photo_paths or tuple(
        path for group in media_groups or () for path in group
    )
    paths = tuple(dict.fromkeys(
        str(path) for path in candidates if Path(str(path)).is_file()
    ))
    ids: list[str] = []
    if paths:
        caption = _single_detail_caption(text, 0, len(paths))
        keyboard = _single_detail_keyboard(reply_markup, public_id, 0, len(paths))
        media = InputMediaPhoto(
            media=_detail_grid(paths, 0),
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
        if replace_id and bool(getattr(replace_message, "photo", None)):
            edit = getattr(bot, "edit_message_media", None)
            if callable(edit):
                try:
                    await edit(
                        chat_id=chat_id_value,
                        message_id=int(replace_id),
                        media=media,
                        reply_markup=keyboard,
                    )
                    ids = [replace_id]
                except Exception:
                    # An album member cannot always become a standalone photo.
                    # In that case send one new message and retire the old.
                    pass
        if not ids:
            sent = await bot.send_photo(
                chat_id=chat_id_value,
                photo=_detail_grid(paths, 0),
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            ids = [str(getattr(sent, "message_id"))]
    else:
        if replace_id and not getattr(replace_message, "photo", None):
            try:
                await bot.edit_message_text(
                    chat_id=chat_id_value,
                    message_id=int(replace_id),
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
                ids = [replace_id]
            except Exception:
                pass
        if not ids:
            sent = await bot.send_message(
                chat_id=chat_id_value, text=text, parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
            ids = [str(getattr(sent, "message_id"))]

    stale = list(previous)
    if replace_id and replace_id not in stale:
        stale.append(replace_id)
    for old_id in stale:
        if old_id in ids:
            continue
        try:
            await bot.delete_message(chat_id=chat_id_value, message_id=int(old_id))
        except Exception:
            continue
    if public_id:
        _remember_album_sent(state, message_ids=ids, page=DETAILS_ALBUM_PAGE)
        state["detail_index"] = 0
    return True


async def _handle_detail_photo(
    update: Any, context: Any, router: CallbackRouter, raw: str,
    *, advisor_url: str = "", channel_url: str = "",
) -> bool:
    matched = re.fullmatch(r"v3u:detailphoto:(QL-[A-Z0-9-]+):(\d+)", raw)
    if matched is None:
        return False
    query = update.callback_query
    public_id = normalize_public_id(matched.group(1))
    if public_id is None:
        await query.answer("房源入口无效", show_alert=True)
        return True
    # Re-resolve the published listing, so status/booking rules stay current.
    listings = getattr(router, "listings", None)
    result = listings.resolve_action(public_id, "details") if listings else None
    details = getattr(result, "details", None) if getattr(result, "ok", False) else None
    if details is None:
        await query.answer("房源已更新，请重新进入", show_alert=True)
        return True
    paths = tuple(path for path in details.photo_paths if Path(path).is_file())
    index = int(matched.group(2))
    if not paths or index >= (len(paths) + 3) // 4:
        await query.answer("这张实拍暂时无法打开", show_alert=True)
        return True
    state = _album_state(update, context, public_id)
    current_id = str(getattr(getattr(query, "message", None), "message_id", "") or "")
    if not current_id or not getattr(query.message, "photo", None):
        await query.answer("请重新进入房源详情", show_alert=True)
        return True
    # No dependency on in-memory album state: older live buttons still work
    # after a Bot restart, because the listing is re-resolved from storage.
    shown_caption = str(getattr(query.message, "caption", "") or "")
    if (shown_caption.startswith(f"📷 实拍 {index * 4 + 1}–{min(index * 4 + 4, len(paths))}/{len(paths)}")
            or (current_id in _album_message_ids(state)
                and int(state.get("detail_index", -1)) == index)):
        await query.answer()
        return True
    from .telegram_ui import build_action_keyboard
    base = polish_listing_keyboard(
        build_action_keyboard(details.action_rows),
        advisor_url=advisor_url, channel_url=channel_url, add_home=True,
        listing_summary=details.listing_summary,
    )
    keyboard = _single_detail_keyboard(base, public_id, index, len(paths))
    caption = _single_detail_caption(details.text, index, len(paths))
    await query.answer()
    await query.edit_message_media(
        media=InputMediaPhoto(
            media=_detail_grid(paths, index),
            caption=caption,
            parse_mode=ParseMode.HTML,
        ),
        reply_markup=keyboard,
    )
    _remember_album_sent(state, message_ids=[current_id], page=DETAILS_ALBUM_PAGE)
    state["detail_index"] = index
    return True


async def _clear_details_behind(update: Any, context: Any, query: Any) -> None:
    """After 「返回搜索结果」from a photo detail, drop that detail's photos + text.

    The result card is re-sent as a new photo message, so the old detail
    surface (album frames + text) would otherwise stay in the chat.
    """
    bot = getattr(context, "bot", None)
    data = getattr(context, "user_data", None)
    message_id = str(getattr(getattr(query, "message", None), "message_id", "") or "").strip()
    if bot is None or not isinstance(data, dict) or not message_id:
        return
    albums = data.get(PHOTOS_ALBUM_KEY)
    if not isinstance(albums, dict):
        return
    try:
        chat_key = str(_chat_id(update))
    except Exception:
        return
    for key, state in albums.items():
        if not key.startswith(f"{chat_key}::") or not isinstance(state, dict):
            continue
        if state.get("page") != DETAILS_ALBUM_PAGE or message_id not in _album_message_ids(state):
            continue
        await _delete_album_photos(bot, chat_key, state, keep_action_bar=False)
        state.pop("message_ids", None)
        state.pop("page", None)


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

    # A search card may replace the detail photo in place. Invalidate the
    # old detail state or the next '房源详情' tap can be incorrectly no-op'd.
    if message is not None and has_photo and sent is None:
        current_id = str(getattr(message, "message_id", "") or "")
        albums = getattr(context, "user_data", {})
        albums = albums.get(PHOTOS_ALBUM_KEY) if isinstance(albums, dict) else None
        if isinstance(albums, dict):
            for state in albums.values():
                if (isinstance(state, dict)
                        and state.get("page") == DETAILS_ALBUM_PAGE
                        and current_id in _album_message_ids(state)):
                    state.pop("page", None)
                    state.pop("message_ids", None)
                    state.pop("detail_index", None)

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
    if raw.startswith("v3u:detailphoto:"):
        handled = await _handle_detail_photo(
            update, context, router, raw,
            advisor_url=advisor_url, channel_url=channel_url,
        )
        if handled:
            return TelegramCallbackHandlerOutcome(handled=True)

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
        if response.media_groups and getattr(context, "bot", None) is not None and public_id_for_exit:
            # V4.1: detail = curated real photos + text/buttons right below.
            await send_listing_details(
                update,
                context,
                public_listing_id=public_id_for_exit,
                media_groups=response.media_groups,
                photo_paths=response.photo_paths,
                media_caption=response.media_caption,
                text=response.text,
                reply_markup=response.keyboard,
                replace_message=getattr(query, "message", None),
            )
        else:
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
        sent_as_new = bool(response.photo_path) and not bool(getattr(getattr(query, "message", None), "photo", None))
        await render_search_card_response(update, context, response, query=query)
        if sent_as_new:
            await _clear_details_behind(update, context, query)
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
            keep_action_bar=True,
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
                similar_result = flow.similar(criteria, limit=5, from_listing=True)
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
    "send_listing_details",
    "DETAILS_ALBUM_PAGE",
    "render_search_card_response",
]
