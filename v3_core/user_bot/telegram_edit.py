"""Safe Telegram edit helpers for edit-first User Bot navigation."""
from __future__ import annotations

from typing import Any

from telegram.error import BadRequest


def _is_not_modified(error: BaseException) -> bool:
    return "message is not modified" in str(error or "").lower()


async def edit_query_text(query: Any, text: str, **kwargs: Any) -> bool:
    try:
        await query.edit_message_text(text, **kwargs)
        return True
    except BadRequest as exc:
        if _is_not_modified(exc):
            return False
        raise


async def edit_query_caption(query: Any, caption: str, **kwargs: Any) -> bool:
    try:
        await query.edit_message_caption(caption=caption, **kwargs)
        return True
    except BadRequest as exc:
        if _is_not_modified(exc):
            return False
        raise


async def edit_query_panel(query: Any, *, text: str, **kwargs: Any) -> bool:
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        return await edit_query_caption(query, text, **kwargs)
    return await edit_query_text(query, text, **kwargs)


__all__ = ["edit_query_caption", "edit_query_panel", "edit_query_text"]
