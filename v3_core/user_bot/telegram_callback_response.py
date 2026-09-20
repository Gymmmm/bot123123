"""Telegram-facing response adapter for already-decided V3 callbacks.

The callback router owns business decisions.  This module only turns successful
renderable results into Telegram UI payloads and exposes transition intents for
flows that require a later handler (book, consult, similar search, change search).
It never queries storage, mutates session state, starts appointments, or writes
leads/admin notifications.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from telegram import InlineKeyboardMarkup

from .callback_router import CallbackDispatchResult
from .consult import ConsultIntent
from .public_flow import PublicBookIntent
from .similar_intent import SimilarSearchIntent
from .telegram_ui import build_action_keyboard


TelegramResponseKind = Literal[
    "details",
    "photos",
    "card",
    "transition",
    "error",
]
TransitionAction = Literal["book", "consult", "similar", "change_search"]


@dataclass(frozen=True)
class TelegramCallbackResponse:
    kind: TelegramResponseKind
    status: str
    reason: str = ""
    text: str = ""
    photo_path: str = ""
    media_groups: tuple[tuple[str, ...], ...] = ()
    detail_text: str = ""
    send_detail: bool = False
    keyboard: InlineKeyboardMarkup | None = None
    transition: TransitionAction | None = None
    book_intent: PublicBookIntent | None = None
    consult_intent: ConsultIntent | None = None
    similar_intent: SimilarSearchIntent | None = None
    session_public_listing_ids: tuple[str, ...] = ()
    requested_removed: bool = False
    listing_summary: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok"


def adapt_callback_response(
    dispatched: CallbackDispatchResult,
) -> TelegramCallbackResponse:
    """Convert one router result without adding new business decisions."""
    if not dispatched.ok:
        return TelegramCallbackResponse(
            kind="error",
            status=str(dispatched.status),
            reason=str(dispatched.reason or ""),
        )

    if dispatched.change_search:
        return TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="change_search",
        )

    if dispatched.action == "show_card":
        navigation = dispatched.navigation
        if navigation is None or navigation.card is None:
            raise ValueError("successful_card_dispatch_missing_card")
        card = navigation.card
        return TelegramCallbackResponse(
            kind="card",
            status="ok",
            text=card.text,
            photo_path=card.photo_path,
            keyboard=build_action_keyboard(card.action_rows),
            session_public_listing_ids=navigation.public_listing_ids,
            requested_removed=navigation.requested_removed,
        )

    if dispatched.action in {"details", "photos", "book"}:
        listing = dispatched.listing
        if listing is None:
            raise ValueError("successful_listing_dispatch_missing_result")
        if dispatched.action in {"details", "photos"}:
            photos = listing.photos
            if photos is not None and (photos.has_media or dispatched.action == "photos"):
                photo_path = str(getattr(photos, "photo_path", "") or "").strip()
                if not photo_path and photos.media_groups:
                    photo_path = str(photos.media_groups[0][0] or "").strip()
                return TelegramCallbackResponse(
                    kind="photos",
                    status="ok",
                    text=photos.text,
                    photo_path=photo_path,
                    media_groups=photos.media_groups if photos.has_media else (),
                    detail_text=str(getattr(photos, "detail_text", "") or ""),
                    # Open 📷 房源详情 must send sectioned body as its own bubble.
                    # Photo flipper navigation (action=photos) must not re-send it.
                    send_detail=dispatched.action == "details",
                    keyboard=build_action_keyboard(photos.action_rows),
                    listing_summary=str(getattr(photos, "listing_summary", "") or ""),
                )
            if listing.details is None:
                raise ValueError("successful_details_dispatch_missing_response")
            return TelegramCallbackResponse(
                kind="details",
                status="ok",
                text=listing.details.text,
                keyboard=build_action_keyboard(listing.details.action_rows),
                listing_summary=str(getattr(listing.details, "listing_summary", "") or ""),
            )
        if listing.book is None:
            raise ValueError("successful_book_dispatch_missing_intent")
        return TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="book",
            book_intent=listing.book,
        )

    if dispatched.action == "consult":
        if dispatched.consult is None or dispatched.consult.intent is None:
            raise ValueError("successful_consult_dispatch_missing_intent")
        return TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="consult",
            consult_intent=dispatched.consult.intent,
        )

    if dispatched.action == "similar":
        if dispatched.similar is None or dispatched.similar.intent is None:
            raise ValueError("successful_similar_dispatch_missing_intent")
        return TelegramCallbackResponse(
            kind="transition",
            status="ok",
            transition="similar",
            similar_intent=dispatched.similar.intent,
        )

    raise ValueError(f"unsupported_successful_callback_action:{dispatched.action}")


__all__ = [
    "TelegramCallbackResponse",
    "adapt_callback_response",
]
