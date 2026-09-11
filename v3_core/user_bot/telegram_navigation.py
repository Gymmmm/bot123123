"""Telegram-only navigation polish for public V3 listing surfaces.

This module never decides listing availability or mutates business state. It only
turns an already-valid advisor URL into a direct chat handoff with a prepared
listing reference and appends safe return buttons to listing keyboards.
"""
from __future__ import annotations

from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


_HOME_CALLBACK = "v3u:t:home"
_CONSULT_PREFIX = "v3u:listing:consult:"


def advisor_handoff_url(advisor_url: object, *, public_listing_id: object = "") -> str:
    """Return a direct Telegram advisor chat URL with a prepared listing message.

    Telegram public-username links support ``?text=<draft_text>``. Unknown/non-
    Telegram URLs are returned unchanged rather than being rewritten unsafely.
    """
    raw = str(advisor_url or "").strip()
    if not raw:
        return ""
    public_id = str(public_listing_id or "").strip()
    if not public_id:
        return raw
    message = f"你好，我想咨询这套房：{public_id}"
    parts = urlsplit(raw)
    host = parts.netloc.lower()
    if parts.scheme in {"http", "https"} and host in {
        "t.me",
        "www.t.me",
        "telegram.me",
        "www.telegram.me",
        "telegram.dog",
        "www.telegram.dog",
    }:
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["text"] = message
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    if raw.startswith("tg://resolve?"):
        separator = "&" if "?" in raw else "?"
        return f"{raw}{separator}text={quote(message)}"
    return raw


def polish_listing_keyboard(
    markup: InlineKeyboardMarkup | None,
    *,
    advisor_url: str = "",
    channel_url: str = "",
    back_to_search_callback: str = "",
    add_home: bool = False,
    add_channel: bool = False,
) -> InlineKeyboardMarkup | None:
    """Upgrade consult buttons and append non-dead return routes."""
    if markup is None:
        rows: list[list[InlineKeyboardButton]] = []
    else:
        rows = []
        for row in markup.inline_keyboard:
            upgraded: list[InlineKeyboardButton] = []
            for button in row:
                callback_data = str(getattr(button, "callback_data", "") or "")
                label = "💬 联系中文顾问" if str(button.text or "") == "💬 联系我们" else str(button.text or "")
                if callback_data.startswith(_CONSULT_PREFIX) and str(advisor_url or "").strip():
                    public_id = callback_data[len(_CONSULT_PREFIX):].strip()
                    upgraded.append(
                        InlineKeyboardButton(
                            label or "💬 联系中文顾问",
                            url=advisor_handoff_url(advisor_url, public_listing_id=public_id),
                        )
                    )
                elif label != str(button.text or ""):
                    upgraded.append(
                        InlineKeyboardButton(
                            label,
                            callback_data=button.callback_data,
                            url=button.url,
                        )
                    )
                else:
                    upgraded.append(button)
            if upgraded:
                rows.append(upgraded)

    existing_labels = {str(button.text or "") for row in rows for button in row}
    back_search = str(back_to_search_callback or "").strip()
    if back_search and "⬅️ 返回搜索结果" not in existing_labels:
        rows.append([InlineKeyboardButton("⬅️ 返回搜索结果", callback_data=back_search)])
        existing_labels.add("⬅️ 返回搜索结果")
    clean_channel = str(channel_url or "").strip()
    if add_channel and clean_channel and "📣 返回房源频道" not in existing_labels:
        rows.append([InlineKeyboardButton("📣 返回房源频道", url=clean_channel)])
    if add_home and "🏠 返回首页" not in existing_labels:
        rows.append([InlineKeyboardButton("🏠 返回首页", callback_data=_HOME_CALLBACK)])
    return InlineKeyboardMarkup(rows) if rows else None


__all__ = ["advisor_handoff_url", "polish_listing_keyboard"]
