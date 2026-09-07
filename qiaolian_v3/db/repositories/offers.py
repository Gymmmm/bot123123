from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ListingOffer:
    id: int
    listing_id: int
    offer_type: str
    publication_policy: str
    is_current: bool


class ListingOfferRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def create_listing(
        self,
        *,
        property_identity_key: str | None = None,
        public_listing_id: str | None = None,
        current_canonical_record_id: int | None = None,
        project_name: str = '',
        project_alias: str = '',
        property_type: str = '',
        property_subtype: str = '',
        city_key: str = '',
        project_key: str = '',
        canonical_area_key: str = '',
        public_location_key: str = '',
        public_location_display: str = '',
        layout: str = '',
        bedrooms: int | None = None,
        living_rooms: int | None = None,
        bathrooms: int | None = None,
        helper_rooms: int | None = None,
        size_sqm: float | None = None,
        floor: str = '',
        listing_status: str = 'pending',
        semantic_key: str | None = None,
        status: str | None = None,
    ) -> int:
        """Create/get a formal V3 Listing.

        `semantic_key`/`status` are Phase-1 compatibility aliases only. The persisted
        contract is `property_identity_key` + `listing_status`.
        """
        resolved_key = property_identity_key if property_identity_key is not None else semantic_key
        if resolved_key is None:
            raise TypeError('property_identity_key is required')
        resolved_status = status if status is not None else listing_status
        self.conn.execute(
            '''
            INSERT INTO v3_listings(
                public_listing_id,current_canonical_record_id,property_identity_key,
                project_name,project_alias,property_type,property_subtype,city_key,project_key,
                canonical_area_key,public_location_key,public_location_display,layout,
                bedrooms,living_rooms,bathrooms,helper_rooms,size_sqm,floor,listing_status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(property_identity_key) DO NOTHING
            ''',
            (
                public_listing_id, current_canonical_record_id, str(resolved_key), str(project_name),
                str(project_alias), str(property_type), str(property_subtype), str(city_key), str(project_key),
                str(canonical_area_key), str(public_location_key), str(public_location_display), str(layout),
                bedrooms, living_rooms, bathrooms, helper_rooms, size_sqm, str(floor), str(resolved_status),
            ),
        )
        row = self.conn.execute(
            'SELECT id FROM v3_listings WHERE property_identity_key=?',
            (str(resolved_key),),
        ).fetchone()
        if row is None:
            raise RuntimeError('v3_listing_insert_failed')
        return int(row['id'])

    def link_source(
        self,
        *,
        listing_id: int,
        source_post_id: int | None = None,
        source_identity_id: int | None = None,
    ) -> None:
        resolved_source_post_id = source_post_id if source_post_id is not None else source_identity_id
        if resolved_source_post_id is None:
            raise TypeError('source_post_id is required')
        self.conn.execute(
            'INSERT OR IGNORE INTO listing_sources(listing_id,source_post_id) VALUES (?,?)',
            (int(listing_id), int(resolved_source_post_id)),
        )

    def append_offer(
        self,
        *,
        listing_id: int,
        canonical_record_id: int,
        offer_type: str,
        monthly_rent_usd: int | None = None,
        sale_price_usd: int | None = None,
        original_price_usd: int | None = None,
        currency: str = 'USD',
        deposit_terms: str = '',
        payment_terms: str = '',
        contract_term: str = '',
        available_date: str = '',
        offer_status: str = 'active',
        publication_policy: str = 'store_only',
        publish_block_reason: str = '',
    ) -> ListingOffer:
        previous = self.conn.execute(
            'SELECT id FROM listing_offers WHERE listing_id=? AND offer_type=? AND is_current=1',
            (int(listing_id), str(offer_type)),
        ).fetchone()
        if previous is not None:
            self.conn.execute(
                "UPDATE listing_offers SET is_current=0, offer_status=CASE WHEN offer_status='closed' THEN 'closed' ELSE 'paused' END, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (int(previous['id']),),
            )
        cur = self.conn.execute(
            '''
            INSERT INTO listing_offers(
                listing_id,canonical_record_id,offer_type,currency,monthly_rent_usd,sale_price_usd,
                original_price_usd,deposit_terms,payment_terms,contract_term,available_date,
                offer_status,publication_policy,publish_block_reason,supersedes_id,is_current
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
            ''',
            (
                int(listing_id), int(canonical_record_id), str(offer_type), str(currency),
                monthly_rent_usd, sale_price_usd, original_price_usd, str(deposit_terms),
                str(payment_terms), str(contract_term), str(available_date), str(offer_status),
                str(publication_policy), str(publish_block_reason),
                int(previous['id']) if previous is not None else None,
            ),
        )
        return ListingOffer(
            id=int(cur.lastrowid), listing_id=int(listing_id), offer_type=str(offer_type),
            publication_policy=str(publication_policy), is_current=True,
        )

    def current(self, *, listing_id: int, offer_type: str):
        return self.conn.execute(
            'SELECT * FROM listing_offers WHERE listing_id=? AND offer_type=? AND is_current=1',
            (int(listing_id), str(offer_type)),
        ).fetchone()
