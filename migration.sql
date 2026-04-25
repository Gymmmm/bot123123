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
CREATE TABLE IF NOT EXISTS bot_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    setting_key TEXT NOT NULL UNIQUE,
    setting_value TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS admin_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operator_id TEXT,
    action TEXT,
    target_type TEXT,
    target_id TEXT,
    payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_collect_sources_source_key ON collect_sources (source_key);
CREATE INDEX IF NOT EXISTS idx_collect_sources_type_enabled ON collect_sources (source_type, is_enabled);

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_posts_unique ON source_posts (source_type, source_name, source_post_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_dedupe_hash ON source_posts (dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_source_posts_parse_status ON source_posts (parse_status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_drafts_draft_id ON drafts (draft_id);
CREATE INDEX IF NOT EXISTS idx_drafts_source_post_id ON drafts (source_post_id);
CREATE INDEX IF NOT EXISTS idx_drafts_review_status ON drafts (review_status);
CREATE INDEX IF NOT EXISTS idx_drafts_listing_id ON drafts (listing_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_assets_asset_id ON media_assets (asset_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_owner ON media_assets (owner_type, owner_ref_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_hash ON media_assets (file_hash);
CREATE INDEX IF NOT EXISTS idx_media_assets_tg_unique ON media_assets (telegram_file_unique_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_post_id ON posts (post_id);
CREATE INDEX IF NOT EXISTS idx_posts_listing_id ON posts (listing_id);
CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts (platform);
CREATE INDEX IF NOT EXISTS idx_posts_channel_message ON posts (channel_chat_id, channel_message_id);
CREATE INDEX IF NOT EXISTS idx_posts_media_group ON posts (media_group_id);
CREATE INDEX IF NOT EXISTS idx_posts_notion_page ON posts (notion_page_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_logs_log_id ON publish_logs (log_id);
CREATE INDEX IF NOT EXISTS idx_publish_logs_post_id ON publish_logs (post_id);
CREATE INDEX IF NOT EXISTS idx_publish_logs_listing_id ON publish_logs (listing_id);
CREATE INDEX IF NOT EXISTS idx_publish_logs_target_status ON publish_logs (target_type, status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_settings_key ON bot_settings (setting_key);
