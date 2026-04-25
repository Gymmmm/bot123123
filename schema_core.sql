-- 采集 + 解析 + 发帖 核心表（CREATE IF NOT EXISTS，可重复执行）
-- drafts 含 autopilot 用的 queue_score / preview 字段

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

-- Excel 采集 → 封面预处理 → 审核发布 新链路（与现有 source_posts/drafts 并行）
CREATE TABLE IF NOT EXISTS excel_intake_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL DEFAULT '',
    source_file TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT 'excel_intake',
    imported_rows INTEGER NOT NULL DEFAULT 0,
    valid_rows INTEGER NOT NULL DEFAULT 0,
    invalid_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'imported',
    operator_user_id TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS excel_listing_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id TEXT NOT NULL UNIQUE,
    batch_id TEXT NOT NULL,
    source_row_no INTEGER NOT NULL DEFAULT 0,
    listing_id TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    area TEXT NOT NULL DEFAULT '',
    property_type TEXT NOT NULL DEFAULT '',
    layout TEXT NOT NULL DEFAULT '',
    monthly_rent INTEGER,
    payment_terms TEXT NOT NULL DEFAULT '',
    contract_term TEXT NOT NULL DEFAULT '',
    contact TEXT NOT NULL DEFAULT '',
    raw_row_json TEXT NOT NULL DEFAULT '{}',
    image_cover TEXT NOT NULL DEFAULT '',
    image2 TEXT NOT NULL DEFAULT '',
    image3 TEXT NOT NULL DEFAULT '',
    image4 TEXT NOT NULL DEFAULT '',
    desired_cover_w INTEGER NOT NULL DEFAULT 800,
    desired_cover_h INTEGER NOT NULL DEFAULT 600,
    desired_cover_kind TEXT NOT NULL DEFAULT 'right_price',
    ingestion_status TEXT NOT NULL DEFAULT 'pending',
    validation_errors TEXT NOT NULL DEFAULT '',
    normalized_data TEXT NOT NULL DEFAULT '{}',
    source_post_id INTEGER,
    draft_id TEXT NOT NULL DEFAULT '',
    publish_status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cover_render_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    row_id TEXT NOT NULL,
    draft_id TEXT NOT NULL DEFAULT '',
    desired_w INTEGER NOT NULL DEFAULT 800,
    desired_h INTEGER NOT NULL DEFAULT 600,
    desired_kind TEXT NOT NULL DEFAULT 'right_price',
    render_status TEXT NOT NULL DEFAULT 'pending',
    output_path TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS publish_queue_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id TEXT NOT NULL UNIQUE,
    row_id TEXT NOT NULL,
    draft_id TEXT NOT NULL DEFAULT '',
    listing_id TEXT NOT NULL DEFAULT '',
    channel_id TEXT NOT NULL DEFAULT '',
    caption_variant TEXT NOT NULL DEFAULT 'a',
    queue_status TEXT NOT NULL DEFAULT 'pending',
    scheduled_at TEXT NOT NULL DEFAULT '',
    dequeued_at TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    publish_result_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE IF NOT EXISTS discussion_map (
    channel_post_id INTEGER PRIMARY KEY,
    discussion_msg_id INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_collect_sources_source_key ON collect_sources (source_key);
CREATE INDEX IF NOT EXISTS idx_collect_sources_type_enabled ON collect_sources (source_type, is_enabled);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_posts_unique ON source_posts (source_type, source_name, source_post_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_dedupe_hash ON source_posts (dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_source_posts_parse_status ON source_posts (parse_status);
CREATE INDEX IF NOT EXISTS idx_drafts_source_post_id ON drafts (source_post_id);
CREATE INDEX IF NOT EXISTS idx_drafts_review_status ON drafts (review_status);
CREATE INDEX IF NOT EXISTS idx_drafts_listing_id ON drafts (listing_id);
CREATE INDEX IF NOT EXISTS idx_drafts_ready_queue ON drafts (review_status, queue_score, id) WHERE review_status = 'ready';
CREATE INDEX IF NOT EXISTS idx_drafts_pending_preview ON drafts (review_status, id) WHERE review_status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_assets_asset_id ON media_assets (asset_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_owner ON media_assets (owner_type, owner_ref_id);
CREATE INDEX IF NOT EXISTS idx_media_assets_hash ON media_assets (file_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_post_id ON posts (post_id);
CREATE INDEX IF NOT EXISTS idx_posts_listing_id ON posts (listing_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_logs_log_id ON publish_logs (log_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_excel_intake_batches_batch_id ON excel_intake_batches (batch_id);
CREATE INDEX IF NOT EXISTS idx_excel_rows_batch_status ON excel_listing_rows (batch_id, ingestion_status);
CREATE INDEX IF NOT EXISTS idx_excel_rows_publish_status ON excel_listing_rows (publish_status, id);
CREATE INDEX IF NOT EXISTS idx_cover_render_jobs_status ON cover_render_jobs (render_status, id);
CREATE INDEX IF NOT EXISTS idx_publish_queue_v2_status ON publish_queue_v2 (queue_status, id);
CREATE INDEX IF NOT EXISTS idx_discussion_map_msg ON discussion_map (discussion_msg_id);
