"""CanonicalRecord -> V3 Listing + ListingOffer materialization.

Phase 3 deliberately does not perform cross-source property dedupe. A SourcePost
owns one stable Listing identity across revisions. Strong cross-source identity
(media overlap / explicit unit identity) belongs to Phase 4.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from qiaolian_v3.db.repositories.offers import ListingOfferRepository

from .public_id import assign_public_listing_id


@dataclass(frozen=True)
class MaterializedListing:
    listing_id: int
    public_listing_id: str
    canonical_record_id: int
    offer_ids: tuple[int, ...]


def _identity_key(source_post_id: int) -> str:
    """Stable within one SourcePost; never weakly merges different sources."""
    payload = f"source_post:{int(source_post_id)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class CanonicalListingMaterializer:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.offers = ListingOfferRepository(conn)

    def materialize(self, canonical_record_id: int) -> MaterializedListing:
        record = self.conn.execute(
            "SELECT * FROM canonical_records WHERE id=?", (int(canonical_record_id),)
        ).fetchone()
        if record is None:
            raise LookupError("canonical_record_not_found")

        facts = json.loads(str(record["facts_json"]))
        source_post_id = int(record["source_post_id"])
        deal_type = str(record["deal_type"] or facts.get("deal_type") or "unknown")

        # Unresolved deal conflicts stop before Listing/Offer writes. Phase 3 has
        # no Admin Bot resolution/routing_decision path. A later resolved
        # canonical/override may re-enter with an explicit rent or sale state.
        if deal_type == "unknown":
            raise ValueError("unresolved_deal_type")
        if deal_type not in {"rent", "sale"}:
            raise ValueError("invalid_deal_type")

        rent = facts.get("monthly_rent_usd")
        sale = facts.get("sale_price_usd")
        if deal_type == "rent" and not (isinstance(rent, (int, float)) and int(rent) > 0):
            raise ValueError("canonical_has_no_materializable_rent_offer")
        if deal_type == "sale" and not (isinstance(sale, (int, float)) and int(sale) > 0):
            raise ValueError("canonical_has_no_materializable_sale_offer")

        property_key = _identity_key(source_post_id)
        listing_id = self.offers.create_listing(
            property_identity_key=property_key,
            current_canonical_record_id=int(canonical_record_id),
            project_name=str(facts.get("project_name") or ""),
            project_alias=str(facts.get("project_alias") or ""),
            property_type=str(facts.get("property_type") or "未知"),
            property_subtype=str(facts.get("property_subtype") or ""),
            city_key=str(facts.get("city_key") or ""),
            project_key=str(facts.get("project_key") or ""),
            canonical_area_key=str(facts.get("canonical_area_key") or ""),
            public_location_key=str(facts.get("public_location_key") or ""),
            public_location_display=str(facts.get("public_location_display") or ""),
            layout=str(facts.get("layout") or ""),
            bedrooms=facts.get("bedrooms"),
            living_rooms=facts.get("living_rooms"),
            bathrooms=facts.get("bathrooms"),
            helper_rooms=facts.get("helper_rooms"),
            size_sqm=facts.get("size_sqm"),
            floor=str(facts.get("floor") or ""),
            listing_status="pending",
        )
        self.conn.execute(
            """UPDATE v3_listings SET
               current_canonical_record_id=?,project_name=?,project_alias=?,property_type=?,property_subtype=?,
               city_key=?,project_key=?,canonical_area_key=?,public_location_key=?,public_location_display=?,
               layout=?,bedrooms=?,living_rooms=?,bathrooms=?,helper_rooms=?,size_sqm=?,floor=?,updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (
                int(canonical_record_id), str(facts.get("project_name") or ""), str(facts.get("project_alias") or ""),
                str(facts.get("property_type") or "未知"), str(facts.get("property_subtype") or ""), str(facts.get("city_key") or ""),
                str(facts.get("project_key") or ""), str(facts.get("canonical_area_key") or ""), str(facts.get("public_location_key") or ""),
                str(facts.get("public_location_display") or ""), str(facts.get("layout") or ""), facts.get("bedrooms"),
                facts.get("living_rooms"), facts.get("bathrooms"), facts.get("helper_rooms"), facts.get("size_sqm"),
                str(facts.get("floor") or ""), int(listing_id),
            ),
        )
        self.offers.link_source(listing_id=listing_id, source_post_id=source_post_id)
        public_id = assign_public_listing_id(self.conn, listing_id)

        if deal_type == "rent":
            offer = self.offers.append_offer(
                listing_id=listing_id,
                canonical_record_id=int(canonical_record_id),
                offer_type="rent",
                monthly_rent_usd=int(rent),
                original_price_usd=int(facts["original_monthly_rent_usd"]) if facts.get("original_monthly_rent_usd") else None,
                deposit_terms=str(facts.get("deposit_payment_terms") or ""),
                payment_terms=str(facts.get("deposit_payment_terms") or ""),
                contract_term=str(facts.get("contract_term_display") or facts.get("rental", {}).get("lease") or ""),
                available_date=str(facts.get("available_date") or facts.get("rental", {}).get("available_date") or ""),
                publication_policy="store_only",
            )
        else:
            offer = self.offers.append_offer(
                listing_id=listing_id,
                canonical_record_id=int(canonical_record_id),
                offer_type="sale",
                sale_price_usd=int(sale),
                publication_policy="store_only",
                publish_block_reason="sale_not_enabled_for_rent_channel",
            )

        self.conn.execute(
            "UPDATE canonical_records SET processing_status='STORED' WHERE id=?",
            (int(canonical_record_id),),
        )
        return MaterializedListing(
            listing_id=listing_id,
            public_listing_id=public_id,
            canonical_record_id=int(canonical_record_id),
            offer_ids=(offer.id,),
        )


__all__ = ["CanonicalListingMaterializer", "MaterializedListing"]
