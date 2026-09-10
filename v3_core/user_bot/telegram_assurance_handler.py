"""Telegram adapter for V3 Qiaolian assurance callbacks."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ParseMode

from .assurance_views import (
    AssuranceView,
    assurance_asset_bundle,
    build_moving_view,
)


@dataclass(frozen=True)
class TelegramAssuranceOutcome:
    handled: bool
    action: str = ""
    rendered: bool = False
    assets_sent: bool = False


def build_assurance_keyboard(view: AssuranceView) -> InlineKeyboardMarkup | None:
    if not view.rows:
        return None
    rows = []
    for row in view.rows:
        buttons = []
        for choice in row:
            if choice.url:
                buttons.append(InlineKeyboardButton(choice.label, url=choice.url))
            else:
                buttons.append(InlineKeyboardButton(choice.label, callback_data=choice.callback_data))
        rows.append(buttons)
    return InlineKeyboardMarkup(rows)


async def render_assurance_view(query: Any, view: AssuranceView) -> None:
    markup = build_assurance_keyboard(view)
    message = getattr(query, "message", None)
    if getattr(message, "photo", None):
        await query.edit_message_caption(
            caption=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
        return
    await query.edit_message_text(
        view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


async def send_assurance_bundle(
    update: Any,
    context: Any,
    *,
    repo_root: str | Path,
    kind: str,
) -> None:
    """One customer click sends the guidance, preview image and full PDF."""
    bundle = assurance_asset_bundle(repo_root, kind)
    if not bundle.image_path.is_file():
        raise FileNotFoundError(str(bundle.image_path))
    if not bundle.pdf_path.is_file():
        raise FileNotFoundError(str(bundle.pdf_path))
    chat = getattr(update, "effective_chat", None)
    if chat is None or getattr(chat, "id", None) is None:
        raise ValueError("telegram_chat_missing_for_assurance_assets")
    chat_id = int(chat.id)
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("💬 联系我们", callback_data="v3u:home:contact")],
            [InlineKeyboardButton("⬅️ 返回侨联保障", callback_data="v3u:home:rental")],
        ]
    )
    await context.bot.send_message(
        chat_id=chat_id,
        text=bundle.instruction,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )
    with bundle.image_path.open("rb") as image:
        await context.bot.send_photo(chat_id=chat_id, photo=image)
    with bundle.pdf_path.open("rb") as document:
        await context.bot.send_document(
            chat_id=chat_id,
            document=InputFile(document, filename=bundle.filename),
        )


async def handle_v3_assurance_callback(
    update: Any,
    context: Any,
    *,
    repo_root: str | Path,
) -> TelegramAssuranceOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    prefix = "v3u:assure:"
    if query is None or not raw.startswith(prefix):
        return TelegramAssuranceOutcome(handled=False)
    action = raw[len(prefix):].strip().lower()
    if action not in {"handover", "deposit", "moving"}:
        return TelegramAssuranceOutcome(handled=False)
    await query.answer()
    if action in {"handover", "deposit"}:
        try:
            await query.message.delete()
        except Exception:
            pass
        await send_assurance_bundle(update, context, repo_root=repo_root, kind=action)
        return TelegramAssuranceOutcome(
            handled=True,
            action=action,
            rendered=True,
            assets_sent=True,
        )
    await render_assurance_view(query, build_moving_view())
    return TelegramAssuranceOutcome(handled=True, action=action, rendered=True)


__all__ = [
    "TelegramAssuranceOutcome",
    "build_assurance_keyboard",
    "handle_v3_assurance_callback",
    "render_assurance_view",
    "send_assurance_bundle",
]
