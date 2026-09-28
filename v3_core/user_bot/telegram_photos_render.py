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
) -> None:
    """Send album frames, then optional action message.

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
    sent_media = False

    if len(paths) == 1:
        with Path(paths[0]).open("rb") as handle:
            await bot.send_photo(
                chat_id=chat_id,
                photo=handle,
                caption=caption or None,
                parse_mode=ParseMode.HTML if caption else None,
            )
        sent_media = True
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
            await bot.send_media_group(chat_id=chat_id, media=media)
            sent_media = True
        except Exception:
            # Do not crash the whole photos flow on a MediaGroup rejection.
            with Path(paths[0]).open("rb") as handle:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=handle,
                    caption=caption or None,
                    parse_mode=ParseMode.HTML if caption else None,
                )
            sent_media = True

    if expand_only:
        return

    body = str(text or "").strip()
    if not body and reply_markup is None:
        if not sent_media:
            await bot.send_message(
                chat_id=chat_id,
                text="这套房的实拍暂时没有加载出来。",
            )
        return

    await bot.send_message(
        chat_id=chat_id,
        text=body or "📷 实拍",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )


__all__ = ["send_listing_photos_album"]
