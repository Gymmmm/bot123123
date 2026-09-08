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
    keyboard: InlineKeyboardMarkup | None = None
    transition: TransitionAction | None = None
    book_intent: PublicBookIntent | None = None
    consult_intent: ConsultIntent | None = None
    similar_intent: SimilarSearchIntent | None = None
    session_public_listing_ids: tuple[str, ...] = ()
    requested_removed: bool = False

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
        if dispatched.action == "details":
            if listing.details is None:
                raise ValueError("successful_details_dispatch_missing_response")
            return TelegramCallbackResponse(
                kind="details",
                status="ok",
                text=listing.details.text,
                keyboard=build_action_keyboard(listing.details.action_rows),
            )
        if dispatched.action == "photos":
            if listing.photos is None:
                raise ValueError("successful_photos_dispatch_missing_response")
            return TelegramCallbackResponse(
                kind="photos",
                status="ok",
                text=listing.photos.text,
                media_groups=listing.photos.media_groups,
                keyboard=build_action_keyboard(listing.photos.action_rows),
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
