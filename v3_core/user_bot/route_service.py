"""Public listing action decisions for the side-by-side V3 User Bot.

Deep links and internal User Bot callbacks share one published-only action gate.
Payload parsing remains a transport concern; inventory visibility and current
action allowance are decided exactly once here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .deeplink import PUBLIC_CHANNEL_ACTIONS, PublicChannelRoute, parse_channel_start_payload
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

    def _resolve_route(self, route: PublicChannelRoute) -> PublicRouteDecision:
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

    def resolve(self, payload: object) -> PublicRouteDecision:
        route = parse_channel_start_payload(payload)
        if route is None:
            return PublicRouteDecision(
                status="invalid_link",
                reason="unsupported_public_payload",
            )
        return self._resolve_route(route)

    def resolve_action(
        self,
        public_listing_id: object,
        action: object,
    ) -> PublicRouteDecision:
        public_id = normalize_public_id(public_listing_id)
        clean_action = str(action or "").strip().lower()
        if public_id is None:
            return PublicRouteDecision(
                status="invalid_link",
                reason="invalid_public_listing_id",
            )
        if clean_action not in PUBLIC_CHANNEL_ACTIONS:
            return PublicRouteDecision(
                status="invalid_link",
                reason="unsupported_public_action",
            )
        return self._resolve_route(
            PublicChannelRoute(
                action=clean_action,
                public_listing_id=public_id,
            )
        )


__all__ = [
    "PublicRouteDecision",
    "PublicRouteService",
]
