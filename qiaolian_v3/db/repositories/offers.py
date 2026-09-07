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

    def create_listing(self, *, semantic_key: str, status: str = 'active') -> int:
        self.conn.execute(
            'INSERT INTO v3_listings(semantic_key,status) VALUES (?,?) ON CONFLICT(semantic_key) DO NOTHING',
            (str(semantic_key), str(status)),
        )
        row = self.conn.execute('SELECT id FROM v3_listings WHERE semantic_key=?', (str(semantic_key),)).fetchone()
        if row is None:
            raise RuntimeError('v3_listing_insert_failed')
        return int(row[0])

    def link_source(self, *, listing_id: int, source_identity_id: int) -> None:
        self.conn.execute(
            'INSERT OR IGNORE INTO listing_sources(listing_id,source_identity_id) VALUES (?,?)',
            (int(listing_id), int(source_identity_id)),
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
                (int(previous[0]),),
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
                int(previous[0]) if previous is not None else None,
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
