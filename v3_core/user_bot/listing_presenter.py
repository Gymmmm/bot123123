"""Pure public listing presentation model for the V3 User Bot.

Public facts come from the frozen publication snapshot. Only current
availability/bookability comes from the live listing/offer rows. Telegram
handlers can render this model without reaching back into drafts or canonical
storage.
"""
from __future__ import annotations

from dataclasses import dataclass

from v3_core.publishing.formatting import display_layout
from v3_core.status_labels import inventory_status_presentation

from .public_inventory import PublishedListingView


@dataclass(frozen=True)
class PublicListingDetails:
    listing_id: str
    public_listing_id: str
    project_name: str
    property_type: str
    layout: str
    subject: str
    location: str
    monthly_rent_usd: int | None
    size_sqm: float | None
    floor: str
    deposit_terms: str
    contract_term: str
    inventory_status: str
    status_icon: str
    status_label: str
    bookable: bool
    gallery: tuple[str, ...]

    @property
    def lease_summary(self) -> str:
        return " · ".join(value for value in (self.deposit_terms, self.contract_term) if value)


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_public_listing_details(view: PublishedListingView) -> PublicListingDetails:
    snapshot = view.snapshot
    if str(snapshot.get("schema") or "") != "v3_publication_snapshot.v1":
        raise ValueError("frozen_public_snapshot_missing")

    listing = view.frozen_listing
    offer = view.frozen_offer
    project = str(listing.get("project_name") or "").strip()
    property_type = str(listing.get("property_type") or "").strip()
    layout = display_layout(listing.get("layout") or property_type, property_type)
    subject = "｜".join(value for value in (project, layout) if value)
    location = str(listing.get("public_location_display") or "").strip()
    inventory_status = str(view.listing.get("inventory_status") or "pending").strip().lower()
    status_icon, status_label = inventory_status_presentation(inventory_status)

    return PublicListingDetails(
        listing_id=view.listing_id,
        public_listing_id=view.public_listing_id,
        project_name=project,
        property_type=property_type,
        layout=layout,
        subject=subject,
        location=location,
        monthly_rent_usd=_optional_int(offer.get("monthly_rent_usd")),
        size_sqm=_optional_float(listing.get("size_sqm")),
        floor=str(listing.get("floor") or "").strip(),
        deposit_terms=str(
            offer.get("payment_terms") or offer.get("deposit_terms") or ""
        ).strip(),
        contract_term=str(offer.get("contract_term") or "").strip(),
        inventory_status=inventory_status,
        status_icon=status_icon,
        status_label=status_label,
        bookable=view.bookable,
        gallery=view.gallery,
    )


__all__ = ["PublicListingDetails", "build_public_listing_details"]
