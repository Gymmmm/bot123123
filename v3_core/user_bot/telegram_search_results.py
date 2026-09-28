"""Telegram presentation for an initial V3 search result set.

Initial search results and later ``v3u:card`` navigation must share the same
public-id sequence and the same card renderer.  This module validates that a
``SearchFlowResult`` is internally consistent, presents only the first card, and
stores only QL public ids in the Telegram user session.

No-match copy is intentionally not invented here; callers receive a ``no_match``
outcome and may render the separately locked search UI.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from v3_core.publishing.public_ids import normalize_public_id

from .search_flow import SearchFlowResult
from .telegram_callback_handler import (
    SEARCH_ANCHOR_KEY,
    SEARCH_SESSION_KEY,
    render_search_card_response,
)
from .telegram_callback_response import TelegramCallbackResponse
from .telegram_ui import build_action_keyboard


SearchPresentationStatus = Literal["presented", "no_match"]


@dataclass(frozen=True)
class TelegramSearchPresentation:
    status: SearchPresentationStatus
    mode: str
    has_similar: bool
    public_listing_ids: tuple[str, ...] = ()
    response: TelegramCallbackResponse | None = None

    @property
    def matched(self) -> bool:
        return self.status == "presented"


def _validated_ids(result: SearchFlowResult) -> tuple[str, ...]:
    ids = tuple(str(value or "").strip() for value in result.public_listing_ids)
    card_ids = tuple(card.public_listing_id for card in result.cards)
    if ids != card_ids:
        raise ValueError("search_flow_card_identity_mismatch")
    normalized: list[str] = []
    seen: set[str] = set()
    for value in ids:
        public_id = normalize_public_id(value)
        if public_id is None or public_id != value:
            raise ValueError("search_flow_contains_invalid_public_id")
        if public_id in seen:
            raise ValueError("search_flow_contains_duplicate_public_id")
        normalized.append(public_id)
        seen.add(public_id)
    return tuple(normalized)


async def present_search_flow_result(
    update: Any,
    context: Any,
    result: SearchFlowResult,
) -> TelegramSearchPresentation:
    ids = _validated_ids(result)
    if not result.cards:
        user_data = getattr(context, "user_data", None)
        if isinstance(user_data, dict):
            user_data.pop(SEARCH_SESSION_KEY, None)
            user_data.pop(SEARCH_ANCHOR_KEY, None)
        return TelegramSearchPresentation(
            status="no_match",
            mode=result.mode,
            has_similar=result.has_similar,
        )

    first = result.cards[0]
    response = TelegramCallbackResponse(
        kind="card",
        status="ok",
        text=first.text,
        photo_path=first.photo_path,
        keyboard=build_action_keyboard(first.action_rows),
        session_public_listing_ids=ids,
    )
    await render_search_card_response(
        update,
        context,
        response,
        query=getattr(update, "callback_query", None),
    )
    return TelegramSearchPresentation(
        status="presented",
        mode=result.mode,
        has_similar=result.has_similar,
        public_listing_ids=ids,
        response=response,
    )


__all__ = ["TelegramSearchPresentation", "present_search_flow_result"]
