"""Published-only consultation intent for the side-by-side V3 User Bot.

Consultation is intentionally separated from lead persistence and admin
notification.  Fixed-SHA allows users to contact us about a previously
published rent listing even when that listing is pending, rented, or offline.
The only visibility prerequisite here is the durable public publication
boundary enforced by ``PublicInventoryReader``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from v3_core.publishing.public_ids import normalize_public_id

from .public_inventory import PublicInventoryReader


ConsultStatus = Literal["ok", "invalid_link", "not_found"]


@dataclass(frozen=True)
class ConsultIntent:
    listing_id: str
    public_listing_id: str
    source: str
    inventory_status: str
    offer_status: str
    publication_instance_id: str


@dataclass(frozen=True)
class ConsultResult:
    status: ConsultStatus
    public_listing_id: str = ""
    reason: str = ""
    intent: ConsultIntent | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok" and self.intent is not None


class ConsultService:
    """Resolve one consultation intent without writing lead/admin state."""

    def __init__(self, inventory: PublicInventoryReader):
        self.inventory = inventory

    def resolve(
        self,
        public_listing_id: object,
        *,
        source: str = "listing_callback",
    ) -> ConsultResult:
        public_id = normalize_public_id(public_listing_id)
        if public_id is None:
            return ConsultResult(
                status="invalid_link",
                reason="invalid_public_listing_id",
            )

        view = self.inventory.resolve(public_id)
        if view is None:
            return ConsultResult(
                status="not_found",
                public_listing_id=public_id,
                reason="listing_not_publicly_published",
            )
        if not view.action_allowed("consult"):
            raise AssertionError("published_consult_must_be_allowed")

        return ConsultResult(
            status="ok",
            public_listing_id=public_id,
            intent=ConsultIntent(
                listing_id=view.listing_id,
                public_listing_id=public_id,
                source=str(source or "").strip(),
                inventory_status=str(
                    view.listing.get("inventory_status") or ""
                ).strip().lower(),
                offer_status=str(
                    view.offer.get("offer_status") or ""
                ).strip().lower(),
                publication_instance_id=str(
                    view.publication.get("instance_id") or ""
                ).strip(),
            ),
        )


__all__ = ["ConsultIntent", "ConsultResult", "ConsultService"]
