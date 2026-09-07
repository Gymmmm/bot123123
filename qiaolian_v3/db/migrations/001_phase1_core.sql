-- V3 Phase 1 additive DB foundation.
-- This migration creates only isolated V3-owned tables. It does not alter
-- Parser/Collector/Publisher/User Bot behavior and does not write legacy listings.

CREATE TABLE IF NOT EXISTS source_post_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_identity_key TEXT NOT NULL UNIQUE,
    legacy_source_post_id INTEGER NOT NULL UNIQUE REFERENCES source_posts(id) ON DELETE RESTRICT,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    external_post_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, source_name, external_post_id)
);

CREATE TABLE IF NOT EXISTS source_post_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_identity_id INTEGER NOT NULL REFERENCES source_post_identities(id) ON DELETE RESTRICT,
    revision_no INTEGER NOT NULL CHECK(revision_no > 0),
    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
    raw_text TEXT NOT NULL DEFAULT '',
    sanitized_text TEXT NOT NULL DEFAULT '',
    raw_images_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_images_json)),
    raw_videos_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_videos_json)),
    raw_contact TEXT NOT NULL DEFAULT '',
    raw_meta_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_meta_json)),
    ingest_origin TEXT NOT NULL DEFAULT 'collector'
        CHECK(ingest_origin IN ('collector','admin','csv_import','other')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_identity_id, revision_no),
    UNIQUE(source_identity_id, content_hash)
);

CREATE TRIGGER IF NOT EXISTS trg_source_post_revisions_immutable_update
BEFORE UPDATE ON source_post_revisions
BEGIN
    SELECT RAISE(ABORT, 'source_post_revisions_are_immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_source_post_revisions_immutable_delete
BEFORE DELETE ON source_post_revisions
BEGIN
    SELECT RAISE(ABORT, 'source_post_revisions_are_immutable');
END;

CREATE TABLE IF NOT EXISTS source_post_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_post_revision_id INTEGER NOT NULL REFERENCES source_post_revisions(id) ON DELETE RESTRICT,
    media_asset_key TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0 CHECK(sort_order >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_post_revision_id, media_asset_key)
);

CREATE TABLE IF NOT EXISTS canonical_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_identity_id INTEGER NOT NULL REFERENCES source_post_identities(id) ON DELETE RESTRICT,
    source_post_revision_id INTEGER NOT NULL REFERENCES source_post_revisions(id) ON DELETE RESTRICT,
    schema_version TEXT NOT NULL,
    parser_revision TEXT NOT NULL,
    facts_json TEXT NOT NULL CHECK(json_valid(facts_json)),
    facts_hash TEXT NOT NULL CHECK(length(facts_hash) > 0),
    deal_type TEXT NOT NULL CHECK(deal_type IN ('rent','sale','unknown')),
    deal_type_candidates_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(deal_type_candidates_json)),
    quality_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(quality_json)),
    processing_status TEXT NOT NULL DEFAULT 'parsed'
        CHECK(processing_status IN ('parsed','needs_review','invalid','superseded')),
    supersedes_id INTEGER REFERENCES canonical_records(id) ON DELETE RESTRICT,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_post_revision_id, facts_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_canonical_one_current_per_source
    ON canonical_records(source_identity_id) WHERE is_current = 1;

CREATE TRIGGER IF NOT EXISTS trg_canonical_revision_identity_match_insert
BEFORE INSERT ON canonical_records
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM source_post_revisions r
        WHERE r.id = NEW.source_post_revision_id
          AND r.source_identity_id = NEW.source_identity_id
    ) THEN RAISE(ABORT, 'canonical_revision_identity_mismatch') END;
END;

CREATE TABLE IF NOT EXISTS canonical_overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_record_id INTEGER NOT NULL REFERENCES canonical_records(id) ON DELETE RESTRICT,
    field_name TEXT NOT NULL,
    old_value_json TEXT CHECK(old_value_json IS NULL OR json_valid(old_value_json)),
    new_value_json TEXT NOT NULL CHECK(json_valid(new_value_json)),
    reason TEXT NOT NULL DEFAULT '',
    operator_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS v3_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    semantic_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','inactive','closed','archived')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    source_identity_id INTEGER NOT NULL REFERENCES source_post_identities(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(listing_id, source_identity_id)
);

CREATE TABLE IF NOT EXISTS listing_offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    canonical_record_id INTEGER NOT NULL REFERENCES canonical_records(id) ON DELETE RESTRICT,
    offer_type TEXT NOT NULL CHECK(offer_type IN ('rent','sale')),
    currency TEXT NOT NULL DEFAULT 'USD',
    monthly_rent_usd INTEGER,
    sale_price_usd INTEGER,
    original_price_usd INTEGER,
    deposit_terms TEXT NOT NULL DEFAULT '',
    payment_terms TEXT NOT NULL DEFAULT '',
    contract_term TEXT NOT NULL DEFAULT '',
    available_date TEXT NOT NULL DEFAULT '',
    offer_status TEXT NOT NULL DEFAULT 'active' CHECK(offer_status IN ('active','paused','closed')),
    publication_policy TEXT NOT NULL DEFAULT 'store_only'
        CHECK(publication_policy IN ('store_only','telegram_rent')),
    publish_block_reason TEXT NOT NULL DEFAULT '',
    supersedes_id INTEGER REFERENCES listing_offers(id) ON DELETE RESTRICT,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK(
        (offer_type='rent' AND monthly_rent_usd IS NOT NULL AND monthly_rent_usd > 0 AND sale_price_usd IS NULL)
        OR
        (offer_type='sale' AND sale_price_usd IS NOT NULL AND sale_price_usd > 0 AND monthly_rent_usd IS NULL)
    ),
    CHECK(offer_type <> 'sale' OR publication_policy = 'store_only')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_listing_offer_one_current
    ON listing_offers(listing_id, offer_type) WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS review_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_type TEXT NOT NULL CHECK(subject_type IN ('canonical','listing','offer','publication')),
    subject_id INTEGER NOT NULL,
    review_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','resolved','dismissed')),
    note TEXT NOT NULL DEFAULT '',
    operator_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT
);

CREATE TABLE IF NOT EXISTS listing_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    source_post_media_id INTEGER NOT NULL REFERENCES source_post_media(id) ON DELETE RESTRICT,
    role TEXT NOT NULL DEFAULT 'photo' CHECK(role IN ('photo','floorplan','video','cover_candidate')),
    sort_order INTEGER NOT NULL DEFAULT 0 CHECK(sort_order >= 0),
    is_cover INTEGER NOT NULL DEFAULT 0 CHECK(is_cover IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(listing_id, source_post_media_id)
);

-- Phase 1 publication persistence contracts only. No publisher runtime wiring.
CREATE TABLE IF NOT EXISTS v3_publication_packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_key TEXT NOT NULL UNIQUE,
    listing_offer_id INTEGER NOT NULL REFERENCES listing_offers(id) ON DELETE RESTRICT,
    snapshot_json TEXT NOT NULL CHECK(json_valid(snapshot_json)),
    snapshot_hash TEXT NOT NULL CHECK(length(snapshot_hash) > 0),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared','approved','published','superseded')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER IF NOT EXISTS trg_v3_package_snapshot_immutable
BEFORE UPDATE OF package_key, listing_offer_id, snapshot_json, snapshot_hash ON v3_publication_packages
BEGIN
    SELECT RAISE(ABORT, 'publication_package_snapshot_is_immutable');
END;

CREATE TABLE IF NOT EXISTS v3_channel_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_package_id INTEGER NOT NULL UNIQUE REFERENCES v3_publication_packages(id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL UNIQUE,
    channel_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, message_id)
);
