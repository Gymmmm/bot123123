"""Send V3 listing photo albums via Telegram native MediaGroup.

Album frames go out as sendMediaGroup / sendPhoto. The status + inline keyboard
is always a separate follow-up message. MediaGroup failures fall back to the
first frame so the action bar still reaches the user.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from telegram import InputMediaPhoto
from telegram.constants import ParseMode


def _readable_paths(groups: tuple[tuple[str, ...], ...] | list) -> list[str]:
    paths: list[str] = []
    for group in groups or ():
        for raw in group:
            path = Path(str(raw or "").strip())
            if path.is_file():
                paths.append(str(path))
    return paths


async def send_listing_photos_album(
    bot: Any,
    *,
    chat_id: int | str,
    media_groups: tuple[tuple[str, ...], ...] | list = (),
    media_caption: str = "",
    photo_path: str = "",
    text: str = "",
    reply_markup: Any = None,
    expand_only: bool = False,
) -> list[int]:
    """Send album frames, then optional action message.

    Returns the list of message ids (album frames + optional status message)
    so callers can revoke them on subsequent paging without leaving stale
    media in the chat.

    Rules:
    - 0 frames: action / fallback text only
    - 1 frame: sendPhoto (MediaGroup requires ≥2)
    - 2–10 frames: sendMediaGroup; caption only on the first frame
    - MediaGroup errors → first frame + action bar
    """
    paths = _readable_paths(media_groups)
    if not paths:
        fallback = str(photo_path or "").strip()
        if fallback and Path(fallback).is_file():
            paths = [fallback]

    caption = str(media_caption or "").strip()
    sent_media_ids: list[int] = []

    if len(paths) == 1:
        with Path(paths[0]).open("rb") as handle:
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=handle,
                caption=caption or None,
                parse_mode=ParseMode.HTML if caption else None,
            )
        media_id = getattr(sent, "message_id", None)
        if media_id is not None:
            sent_media_ids.append(int(media_id))
    elif len(paths) >= 2:
        media: list[InputMediaPhoto] = []
        for index, raw in enumerate(paths[:10]):
            data = Path(raw).read_bytes()
            if index == 0 and caption:
                media.append(
                    InputMediaPhoto(
                        media=data,
                        caption=caption,
                        parse_mode=ParseMode.HTML,
                    )
                )
            else:
                media.append(InputMediaPhoto(media=data))
        try:
            sent_group = await bot.send_media_group(chat_id=chat_id, media=media)
            for msg in sent_group or []:
                mid = getattr(msg, "message_id", None)
                if mid is not None:
                    sent_media_ids.append(int(mid))
        except Exception:
            # Do not crash the whole photos flow on a MediaGroup rejection.
            with Path(paths[0]).open("rb") as handle:
                sent = await bot.send_photo(
                    chat_id=chat_id,
                    photo=handle,
                    caption=caption or None,
                    parse_mode=ParseMode.HTML if caption else None,
                )
            media_id = getattr(sent, "message_id", None)
            if media_id is not None:
                sent_media_ids.append(int(media_id))

    if expand_only:
        return sent_media_ids

    body = str(text or "").strip()
    if not body and reply_markup is None:
        if not sent_media_ids:
            await bot.send_message(
                chat_id=chat_id,
                text="这套房的实拍暂时没有加载出来。",
            )
        return sent_media_ids

    action_msg = await bot.send_message(
        chat_id=chat_id,
        text=body or "📷 实拍",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )
    action_mid = getattr(action_msg, "message_id", None)
    if action_mid is not None:
        sent_media_ids.append(int(action_mid))
    return sent_media_ids


__all__ = ["send_listing_photos_album"]
