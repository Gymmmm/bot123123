-- V3 Phase 1 additive schema.
-- Baseline: 8e4605cf5cc21dfec3ce30729654b09e39de9abf
-- Additive only: no legacy table/column is removed in Phase 1.

ALTER TABLE source_posts ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE source_posts ADD COLUMN content_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE source_posts ADD COLUMN ingest_origin TEXT NOT NULL DEFAULT 'collector';

CREATE TABLE canonical_records (
    id TEXT PRIMARY KEY,
    source_post_id INTEGER NOT NULL,
    source_revision INTEGER NOT NULL DEFAULT 1,
    schema_version TEXT NOT NULL,
    parser_revision TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    facts_hash TEXT NOT NULL,
    deal_type TEXT NOT NULL DEFAULT 'unknown',
    quality_score INTEGER NOT NULL DEFAULT 0,
    quality_status TEXT NOT NULL DEFAULT 'needs_review',
    hard_flags_json TEXT NOT NULL DEFAULT '[]',
    review_flags_json TEXT NOT NULL DEFAULT '[]',
    supersedes_id TEXT,
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(source_post_id) REFERENCES source_posts(id),
    FOREIGN KEY(supersedes_id) REFERENCES canonical_records(id),
    UNIQUE(source_post_id, source_revision, facts_hash)
);

CREATE INDEX idx_canonical_records_source_current
    ON canonical_records(source_post_id, is_current, created_at);
CREATE INDEX idx_canonical_records_deal_quality
    ON canonical_records(deal_type, quality_status);

CREATE TABLE listing_offers (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    canonical_record_id TEXT NOT NULL,
    offer_type TEXT NOT NULL CHECK(offer_type IN ('rent','sale')),
    currency TEXT NOT NULL DEFAULT 'USD',
    monthly_rent_usd INTEGER,
    sale_price_usd INTEGER,
    original_price_usd INTEGER,
    deposit_terms TEXT NOT NULL DEFAULT '',
    payment_terms TEXT NOT NULL DEFAULT '',
    contract_term TEXT NOT NULL DEFAULT '',
    available_date TEXT NOT NULL DEFAULT '',
    offer_status TEXT NOT NULL DEFAULT 'stored',
    publishable INTEGER NOT NULL DEFAULT 0,
    publish_block_reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(listing_id) REFERENCES listings(listing_id),
    FOREIGN KEY(canonical_record_id) REFERENCES canonical_records(id),
    UNIQUE(listing_id, canonical_record_id, offer_type)
);

CREATE INDEX idx_listing_offers_listing_type
    ON listing_offers(listing_id, offer_type, offer_status);
CREATE INDEX idx_listing_offers_publishable
    ON listing_offers(offer_type, publishable, offer_status);

CREATE TABLE review_items (
    id TEXT PRIMARY KEY,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    review_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    score INTEGER,
    note TEXT NOT NULL DEFAULT '',
    operator_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT
);

CREATE INDEX idx_review_items_subject
    ON review_items(subject_type, subject_id, status);

CREATE TABLE listing_media (
    id TEXT PRIMARY KEY,
    listing_id TEXT NOT NULL,
    source_media_asset_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'photo',
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_cover INTEGER NOT NULL DEFAULT 0,
    processing_state TEXT NOT NULL DEFAULT 'ready',
    derived_asset_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(listing_id) REFERENCES listings(listing_id),
    UNIQUE(listing_id, source_media_asset_id, role)
);

CREATE INDEX idx_listing_media_listing_order
    ON listing_media(listing_id, sort_order);
