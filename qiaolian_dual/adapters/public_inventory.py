"""Adapter 1: published inventory + QL identity + frozen facts + bookable."""

from __future__ import annotations

from typing import Any


class PublicInventoryAdapter:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path
        self._reader = None
        self._search_reader = None

    def _load_reader(self):
        if self._reader is not None:
            return self._reader
        from v3_core.user_bot.public_inventory import PublicInventoryReader

        self._reader = PublicInventoryReader(self.db_path) if self.db_path else PublicInventoryReader
        if self.db_path:
            return self._reader
        raise RuntimeError("public_inventory_db_path_required")

    def _load_search_reader(self):
        if self._search_reader is not None:
            return self._search_reader
        if not self.db_path:
            raise RuntimeError("public_inventory_db_path_required")
        from v3_core.user_bot.public_search import PublicSearchReader

        self._search_reader = PublicSearchReader(self.db_path)
        return self._search_reader

    def normalize_public_id(self, value: object) -> str | None:
        from qiaolian_dual.public_listing_id import normalize_public_id

        return normalize_public_id(value)

    def resolve_internal_id(self, value: object) -> str | None:
        view = self.resolve(value)
        if view is None:
            return None
        return str(getattr(view, "listing_id", "") or "").strip() or None

    def public_id_for_internal(self, value: object) -> str | None:
        internal_id = str(value or "").strip()
        if not internal_id or not self.db_path:
            return None
        from pathlib import Path
        import sqlite3

        path = Path(self.db_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        uri = f"file:{path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=5) as conn:
            row = conn.execute(
                "SELECT public_listing_id FROM listings_v3 WHERE listing_id=? LIMIT 1",
                (internal_id,),
            ).fetchone()
        public_id = self.normalize_public_id(row[0]) if row and row[0] else None
        return public_id

    def resolve(self, public_listing_id: object):
        ql = self.normalize_public_id(public_listing_id)
        if not ql:
            return None
        return self._load_reader().resolve(ql)

    def to_3858_listing(self, view) -> dict[str, Any] | None:
        if view is None:
            return None

        snapshot = dict(getattr(view, "snapshot", None) or {})
        frozen = dict(getattr(view, "frozen_listing", None) or {})
        frozen_offer = dict(getattr(view, "frozen_offer", None) or {})
        canonical = dict(snapshot.get("canonical_facts") or {})
        live_listing = dict(getattr(view, "listing", None) or {})
        live_offer = dict(getattr(view, "offer", None) or {})

        public_id = str(
            getattr(view, "public_listing_id", "")
            or snapshot.get("public_listing_id")
            or live_listing.get("public_listing_id")
            or ""
        ).strip()
        internal_id = str(
            getattr(view, "listing_id", "")
            or snapshot.get("listing_id")
            or live_listing.get("listing_id")
            or ""
        ).strip()

        price = frozen_offer.get("monthly_rent_usd")
        if price in (None, ""):
            price = canonical.get("monthly_rent_usd")

        project = (
            frozen.get("project_name")
            or frozen.get("project_alias")
            or canonical.get("project_name")
            or canonical.get("project_alias")
            or canonical.get("community_name")
            or ""
        )
        community = canonical.get("community_name") or project
        area = (
            frozen.get("public_location_display")
            or frozen.get("canonical_area_display")
            or canonical.get("public_location_display")
            or canonical.get("canonical_area_display")
            or ""
        )
        property_type = (
            frozen.get("property_type")
            or canonical.get("property_type")
            or canonical.get("property_type_display")
            or ""
        )
        layout = frozen.get("layout") or canonical.get("layout") or ""
        size_sqm = frozen.get("size_sqm")
        if size_sqm in (None, ""):
            size_sqm = canonical.get("size_sqm")
        floor = frozen.get("floor")
        if floor in (None, ""):
            floor = canonical.get("floor")

        deposit = (
            frozen_offer.get("deposit_terms")
            or frozen_offer.get("payment_terms")
            or canonical.get("deposit_payment_terms")
            or ""
        )
        contract_term = frozen_offer.get("contract_term") or canonical.get("contract_term_display") or ""
        status = str(
            live_listing.get("inventory_status")
            or frozen.get("inventory_status")
            or "pending"
        ).strip().lower()
        gallery = list(getattr(view, "gallery", ()) or ())

        return {
            "listing_id": public_id,
            "public_listing_id": public_id,
            "internal_listing_id": internal_id,
            "title": canonical.get("display_title") or frozen.get("display_title") or live_listing.get("display_title") or "",
            "project": project,
            "community": community,
            "area": area,
            "property_type": property_type,
            "layout": layout,
            "bedrooms": frozen.get("bedrooms") if frozen.get("bedrooms") is not None else canonical.get("bedrooms"),
            "bathrooms": frozen.get("bathrooms") if frozen.get("bathrooms") is not None else canonical.get("bathrooms"),
            "price": price or 0,
            "size_sqm": size_sqm or "",
            "size": size_sqm or "",
            "floor": floor or "",
            "deposit": deposit,
            "deposit_rule": deposit,
            "contract_term": contract_term,
            "available_date": frozen_offer.get("available_date") or canonical.get("available_date") or "",
            "status": status,
            "inventory_status": status,
            "bookable": bool(getattr(view, "bookable", False)),
            "published": True,
            "normalized_data": canonical,
            "highlights": list(canonical.get("highlights") or []),
            "adviser_copy": str(snapshot.get("adviser_copy") or ""),
            "gallery": gallery,
            "media_files": gallery,
            "caption_variant": "a",
        }

    def get_listing(self, public_listing_id: object) -> dict[str, Any] | None:
        return self.to_3858_listing(self.resolve(public_listing_id))

    def search(
        self,
        *,
        property_type: str | None = None,
        areas: list[str] | tuple[str, ...] | None = None,
        project_terms: list[str] | tuple[str, ...] | None = None,
        room_type: str | None = None,
        budget_min: int | None = None,
        budget_max: int | None = None,
        limit: int = 6,
    ) -> list[dict[str, Any]]:
        views = self._load_search_reader().search(
            property_type=str(property_type or "").strip(),
            project_terms=tuple(
                dict.fromkeys(
                    str(value or "").strip()
                    for value in (project_terms or ())
                    if str(value or "").strip()
                )
            ),
            location_keys=tuple(
                dict.fromkeys(
                    str(area or "").strip()
                    for area in (areas or ())
                    if str(area or "").strip() and str(area or "").strip() != "不限"
                )
            ),
            room_type=str(room_type or "").strip(),
            budget_min=budget_min,
            budget_max=budget_max,
            limit=max(1, int(limit)),
        )
        return [
            item
            for item in (self.to_3858_listing(view) for view in views)
            if item is not None
        ]

    def recent(self, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.search(limit=limit)

    def is_bookable(self, public_listing_id: object) -> bool:
        view = self.resolve(public_listing_id)
        return bool(view and getattr(view, "bookable", False))
