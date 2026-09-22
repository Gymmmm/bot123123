"""Adapter 1: published inventory + QL identity + frozen facts + bookable."""

from __future__ import annotations

from typing import Any


class PublicInventoryAdapter:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path
        self._reader = None

    def _load_reader(self):
        if self._reader is not None:
            return self._reader
        from v3_core.user_bot.public_inventory import PublicInventoryReader

        self._reader = PublicInventoryReader(self.db_path) if self.db_path else PublicInventoryReader
        if self.db_path:
            return self._reader
        raise RuntimeError("public_inventory_db_path_required")

    def normalize_public_id(self, value: object) -> str | None:
        from qiaolian_dual.public_listing_id import normalize_public_id

        return normalize_public_id(value)

    def resolve_internal_id(self, value: object) -> str | None:
        from qiaolian_dual.public_listing_id import resolve_listing_id

        return resolve_listing_id(value, db_path=self.db_path)

    def resolve(self, public_listing_id: object):
        ql = self.normalize_public_id(public_listing_id)
        if not ql:
            return None
        reader = self._load_reader()
        return reader.resolve(ql)

    def to_3858_listing(self, view) -> dict[str, Any] | None:
        if view is None:
            return None
        frozen = dict(getattr(view, "frozen_listing", None) or {})
        offer = dict(getattr(view, "frozen_offer", None) or {})
        listing = dict(getattr(view, "listing", None) or {})
        public_id = str(getattr(view, "public_listing_id", "") or listing.get("public_listing_id") or "")
        price = offer.get("monthly_rent_usd")
        if price is None:
            price = listing.get("price") or frozen.get("price") or 0
        return {
            "listing_id": public_id,
            "internal_listing_id": str(getattr(view, "listing_id", "") or listing.get("listing_id") or ""),
            "title": frozen.get("display_title") or listing.get("display_title") or listing.get("title") or "",
            "area": frozen.get("public_location_display")
            or listing.get("public_location_display")
            or listing.get("area")
            or "",
            "price": price,
            "layout": frozen.get("layout") or listing.get("layout") or "",
            "size_sqm": frozen.get("size_sqm") or listing.get("size_sqm") or "",
            "deposit_rule": offer.get("deposit_terms") or frozen.get("deposit_rule") or "",
            "available_date": offer.get("available_date") or frozen.get("available_date") or "",
            "bookable": bool(getattr(view, "bookable", False)),
        }

    def get_listing(self, public_listing_id: object) -> dict[str, Any] | None:
        return self.to_3858_listing(self.resolve(public_listing_id))

    def is_bookable(self, public_listing_id: object) -> bool:
        view = self.resolve(public_listing_id)
        return bool(view and getattr(view, "bookable", False))
