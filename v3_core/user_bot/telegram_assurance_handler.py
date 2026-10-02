"""Telegram adapter for public V3 rental-service callbacks."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.constants import ParseMode

from .telegram_edit import edit_query_panel
from .assurance_views import (
    AssuranceView,
    assurance_asset_bundle,
    build_deposit_view,
    build_handover_view,
    build_moving_view,
    build_signing_view,
)
from .service_views import handover_record_view
from .telegram_service_handler import build_service_keyboard


@dataclass(frozen=True)
class TelegramAssuranceOutcome:
    handled: bool
    action: str = ""
    rendered: bool = False
    assets_sent: bool = False


def build_assurance_keyboard(view: AssuranceView, *, advisor_url: str = "") -> InlineKeyboardMarkup | None:
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


async def render_assurance_view(query: Any, view: AssuranceView, *, advisor_url: str = "") -> None:
    markup = build_assurance_keyboard(view, advisor_url=advisor_url)
    await edit_query_panel(
        query,
        text=view.text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


async def send_assurance_pdf(update: Any, context: Any, *, repo_root: str | Path, kind: str) -> None:
    bundle = assurance_asset_bundle(repo_root, kind)
    if not bundle.pdf_path.is_file():
        raise FileNotFoundError(str(bundle.pdf_path))
    chat = getattr(update, "effective_chat", None)
    if chat is None or getattr(chat, "id", None) is None:
        raise ValueError("telegram_chat_missing_for_assurance_assets")
    with bundle.pdf_path.open("rb") as document:
        await context.bot.send_document(
            chat_id=int(chat.id),
            document=InputFile(document, filename=bundle.filename),
        )


# Backward-compatible callable name for internal imports. It intentionally sends
# only the explicitly requested document; first-click auto-send is forbidden.
async def send_assurance_bundle(
    update: Any,
    context: Any,
    *,
    repo_root: str | Path,
    kind: str,
    advisor_url: str = "",
) -> None:
    await send_assurance_pdf(update, context, repo_root=repo_root, kind=kind)


async def handle_v3_assurance_callback(
    update: Any,
    context: Any,
    *,
    repo_root: str | Path,
    advisor_url: str = "",
) -> TelegramAssuranceOutcome:
    query = getattr(update, "callback_query", None)
    raw = str(getattr(query, "data", "") or "") if query is not None else ""
    if query is None:
        return TelegramAssuranceOutcome(handled=False)
    # New locked surface: 看房与交接 / 入住留档 (Section 7 + 8).
    if raw.startswith("v3u:rental:"):
        return await _handle_v3_rental_callback(update, context, raw=raw, repo_root=repo_root, advisor_url=advisor_url)
    prefix = "v3u:assure:"
    if not raw.startswith(prefix):
        return TelegramAssuranceOutcome(handled=False)
    action = raw[len(prefix):].strip().lower()
    allowed = {
        "signing", "handover", "deposit", "deposit_tenant", "moving",
        "handover_download", "deposit_download",
    }
    if action not in allowed:
        return TelegramAssuranceOutcome(handled=False)

    await query.answer()
    if action == "signing":
        await render_assurance_view(query, build_signing_view(), advisor_url=advisor_url)
        return TelegramAssuranceOutcome(True, action, True, False)
    if action == "handover":
        await render_assurance_view(query, build_handover_view(), advisor_url=advisor_url)
        return TelegramAssuranceOutcome(True, action, True, False)
    if action == "deposit":
        await render_assurance_view(query, build_deposit_view(), advisor_url=advisor_url)
        return TelegramAssuranceOutcome(True, action, True, False)
    if action == "deposit_tenant":
        await render_assurance_view(
            query,
            build_deposit_view(
                back_label="⬅️ 返回退租",
                back_callback="v3u:service:tenant_terminate",
            ),
            advisor_url=advisor_url,
        )
        return TelegramAssuranceOutcome(True, action, True, False)
    if action == "moving":
        await render_assurance_view(query, build_moving_view(), advisor_url=advisor_url)
        return TelegramAssuranceOutcome(True, action, True, False)

    kind = {
        "handover_download": "handover",
        "deposit_download": "deposit",
    }[action]
    await send_assurance_pdf(update, context, repo_root=repo_root, kind=kind)
    return TelegramAssuranceOutcome(True, action, False, True)


async def _handle_v3_rental_callback(
    update: Any,
    context: Any,
    *,
    raw: str,
    repo_root: str | Path,
    advisor_url: str = "",
) -> TelegramAssuranceOutcome:
    """Locked copy: 看房与交接 (Section 7) + 入住留档 (Section 8).

    Only the two locked callbacks live under v3u:rental::
      - handover         -> render 入住留档 page
      - handover_download -> send the existing 入住交接清单.pdf asset
    The asset path/filename stay frozen so we don't drift from the PDF that
    the rest of the system already produces.
    """
    query = getattr(update, "callback_query", None)
    if query is None:
        return TelegramAssuranceOutcome(handled=False)
    action = raw[len("v3u:rental:"):].strip().lower()
    allowed = {"handover", "handover_download"}
    if action not in allowed:
        return TelegramAssuranceOutcome(handled=False)
    await query.answer()
    if action == "handover":
        view = handover_record_view()
        await edit_query_panel(
            query,
            text=view.text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_service_keyboard(view, advisor_url=advisor_url),
        )
        return TelegramAssuranceOutcome(True, action, True, False)
    await send_assurance_pdf(update, context, repo_root=repo_root, kind="handover")
    return TelegramAssuranceOutcome(True, action, False, True)


__all__ = [
    "TelegramAssuranceOutcome", "build_assurance_keyboard", "handle_v3_assurance_callback",
    "render_assurance_view", "send_assurance_bundle", "send_assurance_pdf",
]
