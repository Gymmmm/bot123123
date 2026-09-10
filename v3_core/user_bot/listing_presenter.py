"""Pure public listing presentation model for the V3 User Bot.

Public facts come from the frozen publication snapshot and its canonical
facts. Only current availability/bookability comes from the live listing/offer
rows. Telegram handlers can render this model without reaching back into
drafts or live canonical storage.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from v3_core.publishing.formatting import display_layout
from v3_core.status_labels import inventory_status_presentation

from .public_inventory import PublishedListingView


_EMPTY = {
    "",
    "\u2014",
    "-",
    "--",
    "\u6682\u65e0",
    "[\u6682\u65e0]",
    "\u672a\u77e5",
    "\u5f85\u786e\u8ba4",
    "\u5f85\u5b9a",
    "none",
    "null",
    "unknown",
}

_FEE_FIELDS = (
    ("management_fee", "\u7269\u4e1a"),
    ("internet_fee", "\u7f51\u7edc"),
    ("water_rate", "\u6c34\u8d39"),
    ("electric_rate", "\u7535\u8d39"),
    ("parking_fee", "\u505c\u8f66"),
)


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
    fee_lines: tuple[str, ...] = ()
    amenity_line: str = ""
    highlight_line: str = ""

    @property
    def lease_summary(self) -> str:
        return " \u00b7 ".join(value for value in (self.deposit_terms, self.contract_term) if value)


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


def _clean_text(value: object) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text or text.lower() in _EMPTY or text in _EMPTY:
        return ""
    return text


def _canonical_facts(snapshot: dict[str, Any]) -> dict[str, Any]:
    raw = snapshot.get("canonical_facts")
    return dict(raw) if isinstance(raw, dict) else {}


def _join_values(value: object) -> str:
    if isinstance(value, (list, tuple)):
        parts = [_clean_text(item) for item in value]
        return "\u3001".join(part for part in parts if part)
    return _clean_text(value)


def _fee_lines(canonical: dict[str, Any]) -> tuple[str, ...]:
    lines: list[str] = []
    for key, label in _FEE_FIELDS:
        value = _clean_text(canonical.get(key))
        if value:
            lines.append(f"{label}\uff1a{value}")
    return tuple(lines)


def _amenity_line(canonical: dict[str, Any]) -> str:
    parts = [
        _join_values(canonical.get(key))
        for key in ("furniture", "appliances", "amenities", "special_tags")
    ]
    return "\u3001".join(part for part in parts if part)


def _highlight_line(canonical: dict[str, Any]) -> str:
    return _join_values(canonical.get("highlights"))


def build_public_listing_details(view: PublishedListingView) -> PublicListingDetails:
    snapshot = view.snapshot
    if str(snapshot.get("schema") or "") != "v3_publication_snapshot.v1":
        raise ValueError("frozen_public_snapshot_missing")

    listing = view.frozen_listing
    offer = view.frozen_offer
    canonical = _canonical_facts(snapshot)
    project = _clean_text(listing.get("project_name"))
    property_type = _clean_text(listing.get("property_type") or canonical.get("property_type"))
    layout = display_layout(listing.get("layout") or property_type, property_type)
    layout = _clean_text(layout)
    subject = "\uff5c".join(value for value in (project, layout) if value)
    location = _clean_text(listing.get("public_location_display"))
    inventory_status = str(view.listing.get("inventory_status") or "pending").strip().lower()
    status_icon, status_label = inventory_status_presentation(inventory_status)

    return PublicListingDetails(
        listing_id=view.listing_id,
        public_listing_id=str(view.public_listing_id or "").strip(),
        project_name=project,
        property_type=property_type,
        layout=layout,
        subject=subject,
        location=location,
        monthly_rent_usd=_optional_int(offer.get("monthly_rent_usd")),
        size_sqm=_optional_float(listing.get("size_sqm")),
        floor=_clean_text(listing.get("floor")),
        deposit_terms=_clean_text(
            offer.get("payment_terms") or offer.get("deposit_terms")
        ),
        contract_term=_clean_text(offer.get("contract_term")),
        inventory_status=inventory_status,
        status_icon=status_icon,
        status_label=status_label,
        bookable=view.bookable,
        gallery=view.gallery,
        fee_lines=_fee_lines(canonical),
        amenity_line=_amenity_line(canonical),
        highlight_line=_highlight_line(canonical),
    )


__all__ = ["PublicListingDetails", "build_public_listing_details"]
