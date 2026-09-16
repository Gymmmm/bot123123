"""Telegram-only navigation polish for public V3 listing surfaces.

This module never decides listing availability or mutates business state. It
builds advisor handoff URLs where a completed contact effect already exists and
appends safe return buttons to listing keyboards.
"""
from __future__ import annotations

import re

from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


_HOME_CALLBACK = "v3u:t:home"


def build_advisor_handoff_text(
    *,
    public_listing_id: object = "",
    listing_summary: object = "",
) -> str:
    """Compose the prefilled advisor chat text (id + which listing)."""
    public_id = str(public_listing_id or "").strip()
    summary = re.sub(r"\s+", " ", str(listing_summary or "").strip())
    if public_id and summary:
        return f"你好，我想咨询这套房：{public_id}\n（{summary}）"
    if public_id:
        return f"你好，我想咨询这套房：{public_id}"
    if summary:
        return f"你好，我想咨询这套房：\n（{summary}）"
    return "你好，我想咨询租房。"


def advisor_handoff_url(
    advisor_url: object,
    *,
    public_listing_id: object = "",
    listing_summary: object = "",
) -> str:
    """Return a direct Telegram advisor chat URL with a prepared listing message.

    Telegram public-username links support ``?text=<draft_text>``. Unknown/non-
    Telegram URLs are returned unchanged rather than being rewritten unsafely.
    """
    raw = str(advisor_url or "").strip()
    if not raw:
        return ""
    message = build_advisor_handoff_text(
        public_listing_id=public_listing_id,
        listing_summary=listing_summary,
    )
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
    listing_summary: str = "",
    add_home: bool = False,
    add_channel: bool = False,
) -> InlineKeyboardMarkup | None:
    """Normalize labels and append non-dead return routes.

    Consult callbacks deliberately remain callbacks so the listing-specific
    lead effect executes before the rendered contact page exposes a direct URL.
    """
    _ = advisor_url, listing_summary
    if markup is None:
        rows: list[list[InlineKeyboardButton]] = []
    else:
        rows = []
        for row in markup.inline_keyboard:
            upgraded: list[InlineKeyboardButton] = []
            for button in row:
                label = "💬 联系中文顾问" if str(button.text or "") == "💬 联系我们" else str(button.text or "")
                if label != str(button.text or ""):
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


__all__ = ["advisor_handoff_url", "build_advisor_handoff_text", "polish_listing_keyboard"]
