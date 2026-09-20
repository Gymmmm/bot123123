"""Pure published-listing flow for the side-by-side V3 User Bot.

Channel deep links and internal User Bot callbacks share one published-only
route gate and one presentation path. Entry surfaces keep their own source
metadata so appointment attribution is not silently rewritten.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .listing_responses import (
    PublicDetailsResponse,
    PublicPhotosResponse,
    build_details_response,
    build_photos_response,
)
from .route_service import DecisionStatus, PublicRouteDecision, PublicRouteService


FlowAction = Literal["details", "photos", "book"]


@dataclass(frozen=True)
class PublicBookIntent:
    listing_id: str
    public_listing_id: str
    source: str
    start_payload: str


@dataclass(frozen=True)
class PublicListingFlowResult:
    status: DecisionStatus
    action: FlowAction | None = None
    public_listing_id: str = ""
    reason: str = ""
    source: str = ""
    details: PublicDetailsResponse | None = None
    photos: PublicPhotosResponse | None = None
    book: PublicBookIntent | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok"


class PublicListingFlowService:
    def __init__(self, routes: PublicRouteService):
        self.routes = routes

    @staticmethod
    def _render(
        decision: PublicRouteDecision,
        *,
        source: str,
        start_payload: str,
        photo_offset: int = 0,
    ) -> PublicListingFlowResult:
        clean_source = str(source or "").strip()
        route = decision.route
        action = route.action if route is not None else None
        public_id = route.public_listing_id if route is not None else ""
        if not decision.ok or route is None:
            details = None
            if (
                decision.status == "blocked"
                and decision.reason == "listing_not_bookable"
                and decision.view is not None
            ):
                details = build_details_response(decision.view)
            return PublicListingFlowResult(
                status=decision.status,
                action=action,  # type: ignore[arg-type]
                public_listing_id=public_id,
                reason=decision.reason,
                source=clean_source,
                details=details,
            )
        if decision.view is None:
            return PublicListingFlowResult(
                status=decision.status,
                action=action,  # type: ignore[arg-type]
                public_listing_id=public_id,
                reason=decision.reason,
                source=clean_source,
            )

        view = decision.view
        if route.action in {"details", "photos"}:
            # Both deeplinks open the same merged detail + single-photo flipper.
            merged = build_photos_response(view, offset=int(photo_offset or 0))
            details = build_details_response(view)
            if route.action == "details":
                return PublicListingFlowResult(
                    status="ok",
                    action="details",
                    public_listing_id=public_id,
                    source=clean_source,
                    details=details,
                    photos=merged,
                )
            return PublicListingFlowResult(
                status="ok",
                action="photos",
                public_listing_id=public_id,
                source=clean_source,
                details=details,
                photos=merged,
            )
        if route.action == "book":
            return PublicListingFlowResult(
                status="ok",
                action="book",
                public_listing_id=public_id,
                source=clean_source,
                book=PublicBookIntent(
                    listing_id=view.listing_id,
                    public_listing_id=public_id,
                    source=clean_source,
                    start_payload=str(start_payload or "").strip(),
                ),
            )
        raise AssertionError(f"unsupported_public_flow_action:{route.action}")

    def resolve(self, payload: object) -> PublicListingFlowResult:
        clean_payload = str(payload or "").strip()
        decision = self.routes.resolve(clean_payload)
        route_source = (
            str(getattr(decision.route, "source", "") or "").strip()
            if decision.route is not None
            else ""
        )
        return self._render(
            decision,
            source=route_source or "channel_deeplink",
            start_payload=clean_payload,
        )

    def resolve_action(
        self,
        public_listing_id: object,
        action: object,
        *,
        source: str = "listing_callback",
        photo_offset: int = 0,
    ) -> PublicListingFlowResult:
        return self._render(
            self.routes.resolve_action(public_listing_id, action),
            source=source,
            start_payload="",
            photo_offset=int(photo_offset or 0),
        )


__all__ = [
    "FlowAction",
    "PublicBookIntent",
    "PublicListingFlowResult",
    "PublicListingFlowService",
]
