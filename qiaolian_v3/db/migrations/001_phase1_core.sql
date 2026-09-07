-- V3 Phase 1 additive DB foundation.
-- This migration creates only isolated V3-owned tables. It does not alter
-- Parser/Collector/Publisher/User Bot behavior and does not write legacy business tables.

-- V3-owned source registry. A Source represents one intake origin (for example
-- a Telegram channel) and owns SourcePost.source_id.
CREATE TABLE IF NOT EXISTS v3_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    external_identity TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    collector_config_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(collector_config_json)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, external_identity)
);

-- V3 SourcePost is an independent logical-post identity/state record.
-- legacy_source_post_id is a nullable compatibility bridge only; it is not a FK
-- and is never required to create a V3 SourcePost.
CREATE TABLE IF NOT EXISTS v3_source_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES v3_sources(id) ON DELETE RESTRICT,
    source_identity_key TEXT NOT NULL UNIQUE,
    source_mode TEXT NOT NULL CHECK(source_mode IN ('collector','admin_import','migration')),
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    external_post_id TEXT NOT NULL,
    source_url TEXT NOT NULL DEFAULT '',
    source_author TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    ingest_status TEXT NOT NULL DEFAULT 'COLLECTED' CHECK(length(ingest_status) > 0),
    parse_status TEXT NOT NULL DEFAULT 'COLLECTED' CHECK(length(parse_status) > 0),
    current_revision_id INTEGER REFERENCES source_post_revisions(id) ON DELETE RESTRICT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'active' CHECK(length(status) > 0),
    legacy_source_post_id INTEGER UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_type, source_name, external_post_id)
);

CREATE TABLE IF NOT EXISTS source_post_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_post_id INTEGER NOT NULL REFERENCES v3_source_posts(id) ON DELETE RESTRICT,
    revision_no INTEGER NOT NULL CHECK(revision_no > 0),
    source_content_hash TEXT NOT NULL CHECK(length(source_content_hash) > 0),
    raw_text TEXT NOT NULL DEFAULT '',
    sanitized_text TEXT NOT NULL DEFAULT '',
    raw_payload_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_payload_json)),
    raw_images_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_images_json)),
    raw_videos_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_videos_json)),
    raw_contact TEXT NOT NULL DEFAULT '',
    raw_meta_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(raw_meta_json)),
    source_created_at TEXT,
    fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_post_id, revision_no),
    UNIQUE(source_post_id, source_content_hash)
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

CREATE TRIGGER IF NOT EXISTS trg_v3_source_current_revision_match_update
BEFORE UPDATE OF current_revision_id ON v3_source_posts
WHEN NEW.current_revision_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM source_post_revisions r
        WHERE r.id = NEW.current_revision_id
          AND r.source_post_id = NEW.id
    ) THEN RAISE(ABORT, 'source_current_revision_mismatch') END;
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
    source_post_id INTEGER NOT NULL REFERENCES v3_source_posts(id) ON DELETE RESTRICT,
    source_post_revision_id INTEGER NOT NULL REFERENCES source_post_revisions(id) ON DELETE RESTRICT,
    schema_version TEXT NOT NULL,
    parser_revision TEXT NOT NULL,
    facts_json TEXT NOT NULL CHECK(json_valid(facts_json)),
    facts_hash TEXT NOT NULL CHECK(length(facts_hash) > 0),
    deal_type TEXT NOT NULL CHECK(deal_type IN ('rent','sale','unknown')),
    deal_type_candidates_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(deal_type_candidates_json)),
    quality_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(quality_json)),
    processing_status TEXT NOT NULL DEFAULT 'PARSED'
        CHECK(processing_status IN (
            'COLLECTED','PARSING','PARSED','STORED','NEEDS_REVIEW',
            'READY_TO_PUBLISH','PUBLISHING','PUBLISHED','REJECTED','FAILED'
        )),
    supersedes_id INTEGER REFERENCES canonical_records(id) ON DELETE RESTRICT,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_post_revision_id, facts_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_canonical_one_current_per_source
    ON canonical_records(source_post_id) WHERE is_current = 1;

CREATE TRIGGER IF NOT EXISTS trg_canonical_revision_source_match_insert
BEFORE INSERT ON canonical_records
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM source_post_revisions r
        WHERE r.id = NEW.source_post_revision_id
          AND r.source_post_id = NEW.source_post_id
    ) THEN RAISE(ABORT, 'canonical_revision_source_mismatch') END;
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

-- Physical/semantic listing identity. Rent/sale commercial terms belong to listing_offers.
CREATE TABLE IF NOT EXISTS v3_listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_listing_id TEXT UNIQUE,
    current_canonical_record_id INTEGER REFERENCES canonical_records(id) ON DELETE RESTRICT,
    property_identity_key TEXT NOT NULL UNIQUE,
    project_name TEXT NOT NULL DEFAULT '',
    project_alias TEXT NOT NULL DEFAULT '',
    property_type TEXT NOT NULL DEFAULT '',
    property_subtype TEXT NOT NULL DEFAULT '',
    city_key TEXT NOT NULL DEFAULT '',
    project_key TEXT NOT NULL DEFAULT '',
    canonical_area_key TEXT NOT NULL DEFAULT '',
    public_location_key TEXT NOT NULL DEFAULT '',
    public_location_display TEXT NOT NULL DEFAULT '',
    layout TEXT NOT NULL DEFAULT '',
    bedrooms INTEGER CHECK(bedrooms IS NULL OR bedrooms >= 0),
    living_rooms INTEGER CHECK(living_rooms IS NULL OR living_rooms >= 0),
    bathrooms INTEGER CHECK(bathrooms IS NULL OR bathrooms >= 0),
    helper_rooms INTEGER CHECK(helper_rooms IS NULL OR helper_rooms >= 0),
    size_sqm REAL CHECK(size_sqm IS NULL OR size_sqm > 0),
    floor TEXT NOT NULL DEFAULT '',
    listing_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(listing_status IN ('active','reserved','pending','rented','inactive')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS listing_sources (
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    source_post_id INTEGER NOT NULL REFERENCES v3_source_posts(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(listing_id, source_post_id)
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
    review_type TEXT NOT NULL,
    canonical_record_id INTEGER REFERENCES canonical_records(id) ON DELETE RESTRICT,
    listing_id INTEGER REFERENCES v3_listings(id) ON DELETE RESTRICT,
    offer_id INTEGER REFERENCES listing_offers(id) ON DELETE RESTRICT,
    reason_codes_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(reason_codes_json)),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','approved','rejected','resolved')),
    source_mode TEXT NOT NULL CHECK(source_mode IN ('collector','admin_import','migration')),
    operator_id TEXT NOT NULL DEFAULT '',
    resolution_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(resolution_json)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TEXT,
    CHECK(canonical_record_id IS NOT NULL OR listing_id IS NOT NULL OR offer_id IS NOT NULL)
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

-- Final publication persistence contract only. There is no Publisher runtime wiring in Phase 1.
CREATE TABLE IF NOT EXISTS v3_publication_packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    offer_id INTEGER NOT NULL REFERENCES listing_offers(id) ON DELETE RESTRICT,
    canonical_record_id INTEGER NOT NULL REFERENCES canonical_records(id) ON DELETE RESTRICT,
    package_version INTEGER NOT NULL CHECK(package_version > 0),
    target_kind TEXT NOT NULL DEFAULT 'telegram_rent' CHECK(target_kind = 'telegram_rent'),
    target_channel_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'prepared'
        CHECK(status IN ('prepared','frozen','publishing','published','superseded','failed')),
    approval_mode TEXT NOT NULL CHECK(approval_mode IN ('auto','admin')),
    approved_by TEXT NOT NULL DEFAULT '',
    approved_at TEXT,
    cover_path TEXT NOT NULL DEFAULT '',
    cover_hash TEXT NOT NULL DEFAULT '',
    gallery_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(gallery_json)),
    gallery_hash TEXT NOT NULL DEFAULT '',
    caption_html TEXT NOT NULL DEFAULT '',
    keyboard_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(keyboard_json)),
    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
    canonical_hash TEXT NOT NULL CHECK(length(canonical_hash) > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TEXT,
    UNIQUE(listing_id, offer_id, package_version, target_kind, target_channel_id)
);

CREATE TRIGGER IF NOT EXISTS trg_v3_package_frozen_content_immutable
BEFORE UPDATE OF
    package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,
    target_kind,target_channel_id,cover_path,cover_hash,gallery_json,gallery_hash,
    caption_html,keyboard_json,content_hash,canonical_hash
ON v3_publication_packages
WHEN OLD.status <> 'prepared'
BEGIN
    SELECT RAISE(ABORT, 'publication_package_frozen_content_is_immutable');
END;

CREATE TABLE IF NOT EXISTS v3_channel_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL UNIQUE,
    channel_id TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    listing_id INTEGER NOT NULL REFERENCES v3_listings(id) ON DELETE RESTRICT,
    offer_id INTEGER NOT NULL REFERENCES listing_offers(id) ON DELETE RESTRICT,
    current_package_id INTEGER NOT NULL REFERENCES v3_publication_packages(id) ON DELETE RESTRICT,
    publication_kind TEXT NOT NULL DEFAULT 'telegram_rent' CHECK(publication_kind = 'telegram_rent'),
    content_hash TEXT NOT NULL CHECK(length(content_hash) > 0),
    post_status TEXT NOT NULL DEFAULT 'published' CHECK(length(post_status) > 0),
    last_synced_at TEXT,
    published_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(channel_id, message_id),
    UNIQUE(channel_id, listing_id, publication_kind)
);

CREATE TRIGGER IF NOT EXISTS trg_v3_channel_post_package_match_insert
BEFORE INSERT ON v3_channel_posts
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM v3_publication_packages p
        WHERE p.id = NEW.current_package_id
          AND p.listing_id = NEW.listing_id
          AND p.offer_id = NEW.offer_id
          AND p.target_channel_id = NEW.channel_id
          AND p.target_kind = NEW.publication_kind
    ) THEN RAISE(ABORT, 'channel_post_package_mismatch') END;
END;

CREATE TRIGGER IF NOT EXISTS trg_v3_channel_post_package_match_update
BEFORE UPDATE OF channel_id,listing_id,offer_id,current_package_id,publication_kind ON v3_channel_posts
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM v3_publication_packages p
        WHERE p.id = NEW.current_package_id
          AND p.listing_id = NEW.listing_id
          AND p.offer_id = NEW.offer_id
          AND p.target_channel_id = NEW.channel_id
          AND p.target_kind = NEW.publication_kind
    ) THEN RAISE(ABORT, 'channel_post_package_mismatch') END;
END;
