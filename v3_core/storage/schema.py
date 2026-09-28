"""Migration-safe V3 shadow schema.

These tables are intentionally additive and do not replace the current
``drafts``/``listings`` tables yet.  They allow dual-write validation on the
extraction branch before any production cut-over.
"""
from __future__ import annotations

import sqlite3


DDL = """
CREATE TABLE IF NOT EXISTS canonical_records (
    canonical_record_id TEXT PRIMARY KEY,
    source_post_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    parser_revision TEXT NOT NULL DEFAULT '',
    deal_type TEXT NOT NULL DEFAULT 'unknown',
    facts_json TEXT NOT NULL,
    facts_hash TEXT NOT NULL,
    quality_score INTEGER NOT NULL DEFAULT 0,
    quality_json TEXT NOT NULL DEFAULT '{}',
    review_status TEXT NOT NULL DEFAULT 'unreviewed',
    supersedes_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_post_id, facts_hash)
);
CREATE INDEX IF NOT EXISTS idx_canonical_records_source
ON canonical_records(source_post_id, created_at);

CREATE TABLE IF NOT EXISTS canonical_overrides (
    override_id TEXT PRIMARY KEY,
    canonical_record_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value_json TEXT NOT NULL DEFAULT 'null',
    new_value_json TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    operator_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_canonical_overrides_record
ON canonical_overrides(canonical_record_id, field_name, created_at);

CREATE TABLE IF NOT EXISTS listings_v3 (
    listing_id TEXT PRIMARY KEY,
    public_listing_id TEXT UNIQUE,
    canonical_record_id TEXT NOT NULL,
    project_name TEXT NOT NULL DEFAULT '',
    project_alias TEXT NOT NULL DEFAULT '',
    project_brand TEXT NOT NULL DEFAULT '',
    property_type TEXT NOT NULL DEFAULT '未知',
    property_subtype TEXT NOT NULL DEFAULT '',
    public_location_key TEXT NOT NULL DEFAULT '',
    public_location_display TEXT NOT NULL DEFAULT '',
    canonical_area_key TEXT NOT NULL DEFAULT '',
    canonical_area_display TEXT NOT NULL DEFAULT '',
    publication_location_level TEXT NOT NULL DEFAULT 'unknown',
    layout TEXT NOT NULL DEFAULT '',
    bedrooms INTEGER,
    bathrooms INTEGER,
    size_sqm REAL,
    floor TEXT NOT NULL DEFAULT '',
    display_title TEXT NOT NULL DEFAULT '',
    canonical_facts_hash TEXT NOT NULL,
    canonical_facts_schema TEXT NOT NULL,
    inventory_status TEXT NOT NULL DEFAULT 'active',
    data_status TEXT NOT NULL DEFAULT 'current',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_listings_v3_location
ON listings_v3(public_location_key, property_type, inventory_status);
CREATE INDEX IF NOT EXISTS idx_listings_v3_canonical
ON listings_v3(canonical_record_id, data_status);

CREATE TABLE IF NOT EXISTS listing_offers (
    offer_id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    offer_type TEXT NOT NULL CHECK(offer_type IN ('rent','sale')),
    currency TEXT NOT NULL DEFAULT 'USD',
    monthly_rent_usd INTEGER,
    sale_price_usd INTEGER,
    deposit_terms TEXT NOT NULL DEFAULT '',
    payment_terms TEXT NOT NULL DEFAULT '',
    contract_term TEXT NOT NULL DEFAULT '',
    available_date TEXT NOT NULL DEFAULT '',
    offer_status TEXT NOT NULL DEFAULT 'active',
    publication_policy TEXT NOT NULL DEFAULT 'store_only',
    publishable INTEGER NOT NULL DEFAULT 0 CHECK(publishable IN (0,1)),
    publish_block_reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK(
        (offer_type='rent' AND monthly_rent_usd IS NOT NULL AND monthly_rent_usd > 0 AND sale_price_usd IS NULL)
        OR
        (offer_type='sale' AND sale_price_usd IS NOT NULL AND sale_price_usd > 0 AND monthly_rent_usd IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_listing_offers_listing
ON listing_offers(listing_id, offer_type, offer_status);
CREATE INDEX IF NOT EXISTS idx_listing_offers_publish
ON listing_offers(offer_type, publication_policy, publishable, offer_status);

CREATE TABLE IF NOT EXISTS review_items (
    review_id TEXT PRIMARY KEY,
    canonical_record_id TEXT NOT NULL,
    listing_id TEXT NOT NULL DEFAULT '',
    offer_id TEXT NOT NULL DEFAULT '',
    review_status TEXT NOT NULL DEFAULT 'pending',
    review_score INTEGER NOT NULL DEFAULT 0,
    review_note TEXT NOT NULL DEFAULT '',
    operator_user_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_review_items_status
ON review_items(review_status, created_at);
"""


def ensure_v3_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


__all__ = ["DDL", "ensure_v3_schema"]
