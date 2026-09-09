from __future__ import annotations

import sqlite3
from pathlib import Path

from db import SCHEMA as USER_LISTING_SCHEMA


PIPELINE_SCHEMA = r'''
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
    water_rate TEXT,
    electric_rate TEXT,
    queue_score REAL DEFAULT 0,
    is_real_photo INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS publication_packages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL,
    draft_id TEXT NOT NULL,
    property_id TEXT NOT NULL,
    package_version INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    listing_type TEXT NOT NULL,
    media_type TEXT NOT NULL,
    cover_template TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'package_ready',
    cover_path TEXT NOT NULL,
    main_images_json TEXT NOT NULL,
    discussion_images_json TEXT NOT NULL,
    post_text TEXT NOT NULL,
    discussion_text TEXT NOT NULL DEFAULT '',
    fee_text TEXT NOT NULL DEFAULT '',
    advice_text TEXT NOT NULL DEFAULT '',
    snapshot_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    source_identity_json TEXT,
    source_identity_hash TEXT,
    source_identity_migrated_at TEXT,
    public_token TEXT,
    canonical_facts_hash TEXT,
    canonical_facts_schema TEXT,
    publication_location_level TEXT,
    canonical_projection_hash TEXT,
    canonical_provenance_json TEXT,
    quality_json TEXT,
    approved_by TEXT,
    approved_at TEXT,
    published_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(draft_id, package_version)
);

CREATE TABLE IF NOT EXISTS publication_delivery_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL UNIQUE,
    package_id TEXT NOT NULL,
    draft_id TEXT NOT NULL,
    listing_id TEXT NOT NULL,
    channel_chat_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'prepared',
    telegram_result_json TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    prepared_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sending_at TEXT,
    sent_at TEXT,
    committed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(package_id, channel_chat_id)
);

CREATE TABLE IF NOT EXISTS bot_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    setting_key TEXT NOT NULL UNIQUE,
    setting_value TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_posts_unique
ON source_posts(source_type, source_name, source_post_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_parse_status ON source_posts(parse_status);
CREATE INDEX IF NOT EXISTS idx_drafts_source_post_id ON drafts(source_post_id);
CREATE INDEX IF NOT EXISTS idx_drafts_review_status ON drafts(review_status);
CREATE INDEX IF NOT EXISTS idx_media_assets_owner ON media_assets(owner_type, owner_ref_id);
CREATE INDEX IF NOT EXISTS idx_posts_listing_id ON posts(listing_id);
CREATE INDEX IF NOT EXISTS idx_publication_packages_status ON publication_packages(status, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_publication_packages_package_id ON publication_packages(package_id);
CREATE INDEX IF NOT EXISTS idx_publication_delivery_state ON publication_delivery_attempts(state, updated_at);
'''

REQUIRED_RUNTIME_TABLES = frozenset({
    "source_posts",
    "drafts",
    "media_assets",
    "listings",
    "posts",
    "publish_logs",
    "publication_packages",
    "publication_delivery_attempts",
    "bot_settings",
})


def initialize_runtime_storage(db_path: str | Path) -> Path:
    """Idempotently recover an empty candidate DB using this branch's legacy data model."""
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.executescript(USER_LISTING_SCHEMA)
        conn.executescript(PIPELINE_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    return path


def existing_tables(db_path: str | Path) -> frozenset[str]:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        return frozenset()
    conn = sqlite3.connect(str(path), timeout=5)
    try:
        return frozenset(
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        )
    finally:
        conn.close()


def missing_runtime_tables(db_path: str | Path) -> frozenset[str]:
    return REQUIRED_RUNTIME_TABLES - existing_tables(db_path)


__all__ = [
    "PIPELINE_SCHEMA",
    "REQUIRED_RUNTIME_TABLES",
    "existing_tables",
    "initialize_runtime_storage",
    "missing_runtime_tables",
]
