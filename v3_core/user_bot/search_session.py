"""Live-refresh search-card session behavior for the V3 User Bot.

Fixed-SHA search navigation validates the callback against the original result
sequence, then re-checks availability before rendering the target card. V3 keeps
that order while replacing internal listing ids with public ids and resolving
only durably published inventory.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .callbacks import UserBotCallback, validate_card_navigation
from .public_inventory import PublicInventoryReader, PublishedListingView
from .search_cards import SearchCardResponse, build_search_card


SessionNavigationStatus = Literal["ok", "invalid_callback", "expired"]


@dataclass(frozen=True)
class SearchSessionNavigation:
    status: SessionNavigationStatus
    public_listing_ids: tuple[str, ...] = ()
    card: SearchCardResponse | None = None
    requested_public_listing_id: str = ""
    requested_removed: bool = False

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.card is not None


class SearchSessionService:
    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    @staticmethod
    def _normalized_session(values: tuple[str, ...] | list[str]) -> tuple[str, ...] | None:
        output: list[str] = []
        seen: set[str] = set()
        for value in values:
            public_id = normalize_public_id(value)
            if public_id is None or public_id in seen:
                return None
            output.append(public_id)
            seen.add(public_id)
        return tuple(output)

    def refresh(
        self,
        session_public_listing_ids: tuple[str, ...] | list[str],
    ) -> tuple[PublishedListingView, ...]:
        ids = self._normalized_session(session_public_listing_ids)
        if ids is None:
            return ()
        views: list[PublishedListingView] = []
        for public_id in ids:
            view = self.inventory.resolve(public_id)
            if view is None or not view.bookable:
                continue
            views.append(view)
        return tuple(views)

    def navigate(
        self,
        callback: UserBotCallback,
        session_public_listing_ids: tuple[str, ...] | list[str],
    ) -> SearchSessionNavigation:
        # Validate against the untouched session ordering before any availability
        # filtering, matching the fixed-SHA findcard safety check.
        if not validate_card_navigation(callback, session_public_listing_ids):
            return SearchSessionNavigation(status="invalid_callback")

        original = self._normalized_session(session_public_listing_ids)
        if original is None or callback.target_index is None:
            return SearchSessionNavigation(status="invalid_callback")

        old_index = int(callback.target_index)
        requested_id = original[old_index]
        views = self.refresh(original)
        if not views:
            return SearchSessionNavigation(
                status="expired",
                requested_public_listing_id=requested_id,
            )

        refreshed_ids = tuple(view.public_listing_id for view in views)
        if requested_id in refreshed_ids:
            new_index = refreshed_ids.index(requested_id)
            removed = False
        else:
            # Fixed SHA uses min(old index, len(valid)-1) when the requested
            # listing changed state between rendering and clicking.
            new_index = min(old_index, len(refreshed_ids) - 1)
            removed = True

        card = build_search_card(views, new_index)
        return SearchSessionNavigation(
            status="ok",
            public_listing_ids=refreshed_ids,
            card=card,
            requested_public_listing_id=requested_id,
            requested_removed=removed,
        )


__all__ = ["SearchSessionNavigation", "SearchSessionService"]
