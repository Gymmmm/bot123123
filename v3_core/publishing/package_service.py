"""V3 publication package build and approval services."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from v3_core.publishing.channel_contract import channel_action_url
from v3_core.publishing.channel_renderer import render_channel_caption
from v3_core.publishing.eligibility import evaluate_offer_eligibility
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository
from .package_store import FrozenPackage, FrozenPackageStore


class PackageBuildService:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        store: FrozenPackageStore,
        user_bot_username: str,
    ):
        self.reader = reader
        self.store = store
        self.user_bot_username = str(user_bot_username or "").strip().lstrip("@")
        if not self.user_bot_username:
            raise ValueError("user_bot_username is required")

    def build(
        self,
        *,
        listing_id: str,
        offer_id: str,
        cover_style: str,
        cover_path: str,
        gallery: list[str],
        source_identity: dict[str, Any] | None = None,
    ) -> FrozenPackage:
        listing = self.reader.listing(listing_id)
        offer = self.reader.offer(offer_id)
        if str(offer["listing_id"]) != str(listing_id):
            raise ValueError("offer_listing_mismatch")
        if str(offer.get("offer_type") or "") != "rent":
            raise ValueError("publication_package_requires_rent_offer")
        if str(offer.get("publication_policy") or "") != "telegram_rent":
            raise ValueError("publication_package_requires_telegram_rent_policy")
        if str(offer.get("offer_status") or "") != "active":
            raise ValueError("publication_package_requires_active_offer")

        canonical = self.reader.canonical(str(listing["canonical_record_id"]))
        facts = dict(canonical["facts"])
        public_id = str(listing.get("public_listing_id") or "")

        caption = render_channel_caption(
            listing=listing,
            offer=offer,
            public_listing_id=public_id,
            status=str(listing.get("inventory_status") or "active"),
        )
        actions = {
            action: channel_action_url(
                self.user_bot_username,
                public_id,
                action,
            )
            for action in ("details", "photos", "book")
        }
        public_token = "ql" + hashlib.sha256(
            f"{listing_id}:{offer_id}:{canonical['facts_hash']}".encode("utf-8")
        ).hexdigest()[:14]
        snapshot = {
            "schema": "v3_publication_snapshot.v1",
            "listing_id": str(listing_id),
            "public_listing_id": public_id,
            "offer_id": str(offer_id),
            "canonical_record_id": str(canonical["canonical_record_id"]),
            "canonical_facts_hash": str(canonical["facts_hash"]),
            # Freeze the complete canonical evidence used to build this public
            # product. User Bot detail/adviser copy must never reach back into a
            # later canonical row and silently change what an existing channel
            # publication means.
            "canonical_facts": facts,
            "listing": {
                key: listing.get(key)
                for key in (
                    "project_name",
                    "project_alias",
                    "project_brand",
                    "property_type",
                    "property_subtype",
                    "public_location_key",
                    "public_location_display",
                    "canonical_area_key",
                    "canonical_area_display",
                    "publication_location_level",
                    "layout",
                    "bedrooms",
                    "bathrooms",
                    "size_sqm",
                    "floor",
                    "inventory_status",
                )
            },
            "offer": {
                key: offer.get(key)
                for key in (
                    "offer_type",
                    "currency",
                    "monthly_rent_usd",
                    "sale_price_usd",
                    "deposit_terms",
                    "payment_terms",
                    "contract_term",
                    "available_date",
                    "offer_status",
                    "publication_policy",
                )
            },
        }
        return self.store.create(
            listing_id=str(listing_id),
            offer_id=str(offer_id),
            canonical_record_id=str(canonical["canonical_record_id"]),
            canonical_facts_hash=str(canonical["facts_hash"]),
            cover_style=str(cover_style),
            cover_path=str(Path(cover_path)),
            gallery=[str(Path(path)) for path in gallery or []],
            post_text=caption,
            actions=actions,
            snapshot=snapshot,
            source_identity=dict(source_identity or {}),
            public_token=public_token,
        )


class PackageApprovalService:
    def __init__(
        self,
        *,
        reader: InventoryReader,
        inventory_repository: InventoryRepository,
        store: FrozenPackageStore,
    ):
        self.reader = reader
        self.inventory = inventory_repository
        self.store = store

    def approve(self, *, package_id: str, approved_by: str) -> FrozenPackage:
        package = self.store.verify_frozen(package_id)
        listing = self.reader.listing(package.listing_id)
        offer = self.reader.offer(package.offer_id)
        canonical = self.reader.canonical(package.canonical_record_id)

        if str(listing["canonical_record_id"]) != package.canonical_record_id:
            raise ValueError("package_canonical_record_stale")
        if str(canonical["facts_hash"]) != package.canonical_facts_hash:
            raise ValueError("package_canonical_facts_mismatch")
        if str(listing["canonical_facts_hash"]) != package.canonical_facts_hash:
            raise ValueError("package_listing_projection_stale")
        if str(offer["listing_id"]) != package.listing_id:
            raise ValueError("package_offer_listing_mismatch")

        review_approved = self.reader.review_approved(offer_id=package.offer_id)
        eligibility = evaluate_offer_eligibility(
            facts=dict(canonical["facts"]),
            offer=offer,
            review_approved=review_approved,
            media_count=len(package.gallery),
            cover_exists=Path(package.cover_path).is_file(),
        )
        if not eligibility.ok:
            raise ValueError(
                "package_publishability_blocked:" + ",".join(eligibility.blocking)
            )

        approved = self.store.approve(package.package_id, approved_by=approved_by)
        self.inventory.set_offer_publication(
            offer_id=package.offer_id,
            publishable=True,
            block_reason="",
        )
        return approved


__all__ = ["PackageApprovalService", "PackageBuildService"]
