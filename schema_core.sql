-- Fresh-database compatibility schema for the production runtime.
--
-- This file owns only the legacy tables that are still written/read by the
-- locked production collector/parser/publisher compatibility path. V3-owned
-- tables remain exclusively owned by qiaolian_v3/db/migrations/*.sql.
-- Every statement is additive/idempotent; no existing production data is
-- dropped or rewritten.

PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS collect_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_url TEXT,
    fetch_mode TEXT DEFAULT 'manual',
    fetch_rule_json TEXT,
    is_enabled INTEGER DEFAULT 1,
    last_fetched_at TEXT,
    remark TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_post_id TEXT,
    source_url TEXT,
    source_author TEXT,
    raw_text TEXT,
    raw_images_json TEXT DEFAULT '[]',
    raw_videos_json TEXT DEFAULT '[]',
    raw_contact TEXT,
    raw_meta_json TEXT,
    dedupe_hash TEXT,
    parse_status TEXT DEFAULT 'pending',
    parse_error TEXT,
    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id TEXT NOT NULL UNIQUE,
    source_post_id INTEGER,
    listing_id TEXT,
    title TEXT,
    project TEXT,
    community TEXT,
    area TEXT,
    property_type TEXT,
    price INTEGER,
    layout TEXT,
    size TEXT,
    floor TEXT,
    deposit TEXT,
    available_date TEXT,
    highlights TEXT DEFAULT '[]',
    drawbacks TEXT DEFAULT '[]',
    advisor_comment TEXT,
    cost_notes TEXT,
    extracted_data TEXT,
    normalized_data TEXT,
    review_status TEXT DEFAULT 'pending',
    review_note TEXT,
    operator_user_id TEXT,
    cover_asset_id INTEGER,
    approved_at TEXT,
    published_at TEXT,
    queue_score REAL DEFAULT 0,
    preview_msg_chat_id TEXT,
    preview_msg_id TEXT,
    water_rate TEXT,
    electric_rate TEXT,
    is_real_photo INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL UNIQUE,
    owner_type TEXT NOT NULL,
    owner_ref_id INTEGER,
    owner_ref_key TEXT,
    asset_type TEXT NOT NULL,
    source_type TEXT,
    source_url TEXT,
    source_file_id TEXT,
    local_path TEXT,
    file_url TEXT,
    file_hash TEXT,
    telegram_file_id TEXT,
    telegram_file_unique_id TEXT,
    media_type TEXT,
    is_watermarked INTEGER DEFAULT 0,
    is_cover INTEGER DEFAULT 0,
    sort_order INTEGER DEFAULT 0,
    width INTEGER,
    height INTEGER,
    duration INTEGER,
    file_size INTEGER,
    mime_type TEXT,
    meta_json TEXT,
    status TEXT DEFAULT 'active',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id TEXT NOT NULL UNIQUE,
    listing_id TEXT NOT NULL,
    draft_id TEXT,
    platform TEXT NOT NULL,
    channel_chat_id TEXT,
    channel_message_id TEXT,
    media_group_id TEXT,
    caption_message_id TEXT,
    button_message_id TEXT,
    discuss_chat_id TEXT,
    discuss_thread_id TEXT,
    discuss_message_id TEXT,
    notion_page_id TEXT,
    platform_post_id TEXT,
    post_url TEXT,
    publish_version INTEGER DEFAULT 1,
    publish_status TEXT DEFAULT 'published',
    post_text TEXT,
    comment_text TEXT,
    published_by TEXT,
    published_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS publish_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_id TEXT NOT NULL UNIQUE,
    post_id TEXT,
    draft_id TEXT,
    listing_id TEXT,
    target_type TEXT NOT NULL,
    target_ref TEXT,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_no INTEGER DEFAULT 1,
    request_payload TEXT,
    response_payload TEXT,
    error_code TEXT,
    error_message TEXT,
    log_message TEXT,
    log_level TEXT DEFAULT 'INFO',
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_collect_sources_source_key
    ON collect_sources (source_key);
CREATE INDEX IF NOT EXISTS idx_collect_sources_type_enabled
    ON collect_sources (source_type, is_enabled);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_posts_unique
    ON source_posts (source_type, source_name, source_post_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_dedupe_hash
    ON source_posts (dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_source_posts_parse_status
    ON source_posts (parse_status);
CREATE INDEX IF NOT EXISTS idx_drafts_source_post_id
    ON drafts (source_post_id);
CREATE INDEX IF NOT EXISTS idx_drafts_review_status
    ON drafts (review_status);
CREATE INDEX IF NOT EXISTS idx_drafts_listing_id
    ON drafts (listing_id);
CREATE INDEX IF NOT EXISTS idx_drafts_ready_queue
    ON drafts (review_status, queue_score, id) WHERE review_status = 'ready';
CREATE INDEX IF NOT EXISTS idx_drafts_pending_preview
    ON drafts (review_status, id) WHERE review_status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_assets_asset_id
    ON media_assets (asset_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_owner
    ON media_assets (owner_type, owner_ref_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_hash
    ON media_assets (file_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_post_id
    ON posts (post_id);
CREATE INDEX IF NOT EXISTS idx_posts_listing_id
    ON posts (listing_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_logs_log_id
    ON publish_logs (log_id);
