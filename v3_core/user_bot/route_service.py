"""Public channel route decisions for the side-by-side V3 User Bot.

This layer contains no Telegram calls.  It parses the official public payload,
resolves only durably published rent inventory, and decides whether the
requested public action is currently allowed.  Presentation handlers consume
the decision instead of reimplementing status rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .deeplink import PublicChannelRoute, parse_channel_start_payload
from .public_inventory import PublicInventoryReader, PublishedListingView


DecisionStatus = Literal["ok", "invalid_link", "not_found", "blocked"]


@dataclass(frozen=True)
class PublicRouteDecision:
    status: DecisionStatus
    route: PublicChannelRoute | None = None
    view: PublishedListingView | None = None
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok"


class PublicRouteService:
    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def resolve(self, payload: object) -> PublicRouteDecision:
        route = parse_channel_start_payload(payload)
        if route is None:
            return PublicRouteDecision(status="invalid_link", reason="unsupported_public_payload")
        view = self.inventory.resolve(route.public_listing_id)
        if view is None:
            return PublicRouteDecision(
                status="not_found",
                route=route,
                reason="listing_not_publicly_published",
            )
        if view.action_allowed(route.action):
            return PublicRouteDecision(status="ok", route=route, view=view)
        reason = {
            "book": "listing_not_bookable",
            "photos": "frozen_gallery_unavailable",
        }.get(route.action, "action_not_allowed")
        return PublicRouteDecision(
            status="blocked",
            route=route,
            view=view,
            reason=reason,
        )


__all__ = [
    "PublicRouteDecision",
    "PublicRouteService",
]
