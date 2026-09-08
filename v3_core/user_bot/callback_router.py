"""Finite callback dispatcher for the side-by-side V3 User Bot.

Unlike fixed-SHA's catch-all callback entry, V3 parses one explicit callback
contract and delegates only to already-extracted services. The router has no
Telegram calls, no database writes and no lead/session mutation.

Consult and similar-listing callbacks are intentionally recognized but not wired
here until their own services have been extracted; they fail explicitly instead
of falling through to legacy behavior.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .callbacks import UserBotCallback, parse_callback
from .public_flow import PublicListingFlowResult, PublicListingFlowService
from .search_session import SearchSessionNavigation, SearchSessionService


CallbackDispatchStatus = Literal[
    "ok",
    "invalid_callback",
    "not_found",
    "blocked",
    "expired",
    "unsupported",
]


@dataclass(frozen=True)
class CallbackDispatchResult:
    status: CallbackDispatchStatus
    callback: UserBotCallback | None = None
    action: str = ""
    reason: str = ""
    listing: PublicListingFlowResult | None = None
    navigation: SearchSessionNavigation | None = None
    change_search: bool = False

    @property
    def ok(self) -> bool:
        return self.status == "ok"


class CallbackRouter:
    def __init__(
        self,
        *,
        listings: PublicListingFlowService,
        search_sessions: SearchSessionService,
    ):
        self.listings = listings
        self.search_sessions = search_sessions

    def dispatch(
        self,
        raw_callback: object,
        *,
        session_public_listing_ids: tuple[str, ...] | list[str] = (),
    ) -> CallbackDispatchResult:
        callback = parse_callback(raw_callback)
        if callback is None:
            return CallbackDispatchResult(
                status="invalid_callback",
                reason="unsupported_or_malformed_callback",
            )

        if callback.kind == "change_search":
            return CallbackDispatchResult(
                status="ok",
                callback=callback,
                action="change_search",
                change_search=True,
            )

        if callback.kind == "card":
            navigation = self.search_sessions.navigate(
                callback,
                session_public_listing_ids,
            )
            if navigation.status == "invalid_callback":
                return CallbackDispatchResult(
                    status="invalid_callback",
                    callback=callback,
                    action="show_card",
                    reason="stale_or_tampered_search_callback",
                    navigation=navigation,
                )
            if navigation.status == "expired":
                return CallbackDispatchResult(
                    status="expired",
                    callback=callback,
                    action="show_card",
                    reason="search_session_expired",
                    navigation=navigation,
                )
            return CallbackDispatchResult(
                status="ok",
                callback=callback,
                action="show_card",
                navigation=navigation,
            )

        if callback.kind == "listing":
            if callback.action not in {"details", "photos", "book"}:
                return CallbackDispatchResult(
                    status="unsupported",
                    callback=callback,
                    action=callback.action,
                    reason="unsupported_not_wired",
                )

            listing = self.listings.resolve_action(
                callback.public_listing_id,
                callback.action,
                source="listing_callback",
            )
            if listing.ok:
                return CallbackDispatchResult(
                    status="ok",
                    callback=callback,
                    action=callback.action,
                    listing=listing,
                )
            if listing.status == "not_found":
                status: CallbackDispatchStatus = "not_found"
            elif listing.status == "blocked":
                status = "blocked"
            else:
                status = "invalid_callback"
            return CallbackDispatchResult(
                status=status,
                callback=callback,
                action=callback.action,
                reason=listing.reason,
                listing=listing,
            )

        raise AssertionError(f"unhandled_v3_callback_kind:{callback.kind}")


__all__ = ["CallbackDispatchResult", "CallbackRouter"]
