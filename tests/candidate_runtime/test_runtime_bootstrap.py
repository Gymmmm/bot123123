from __future__ import annotations

import sqlite3

from candidate_runtime.bootstrap import REQUIRED_RUNTIME_TABLES, initialize_runtime_storage, missing_runtime_tables
from candidate_runtime.runtime_env import configure_candidate_environment


def test_candidate_runtime_isolates_mutable_state(monkeypatch, tmp_path):
    runtime_root = tmp_path / "candidate"
    monkeypatch.setenv("QIAOLIAN_RUNTIME_ROOT", str(runtime_root))
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("SQLITE_PATH", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.delenv("MEDIA_ROOT", raising=False)
    monkeypatch.delenv("COLLECTOR_DOWNLOAD_DIR", raising=False)
    monkeypatch.delenv("TELETHON_SESSION_PATH", raising=False)

    paths = configure_candidate_environment()

    assert paths.runtime_root == runtime_root.resolve()
    assert paths.db_path == (runtime_root / "data" / "qiaolian_dual_bot.db").resolve()
    assert paths.download_dir == (runtime_root / "media" / "collector_downloads").resolve()
    assert paths.session_path == (runtime_root / "telethon_sessions" / "qiaolian_collector").resolve()


def test_empty_database_recovers_drafts_listings_posts(monkeypatch, tmp_path):
    db_path = tmp_path / "empty.db"
    assert missing_runtime_tables(db_path) == REQUIRED_RUNTIME_TABLES

    initialize_runtime_storage(db_path)
    assert missing_runtime_tables(db_path) == frozenset()

    with sqlite3.connect(db_path) as conn:
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"source_posts", "drafts", "media_assets", "listings", "posts"} <= tables

        draft_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(drafts)")}
        listing_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(listings)")}
        post_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(posts)")}
        assert {"draft_id", "source_post_id", "listing_id", "review_status"} <= draft_columns
        assert {"listing_id", "status", "price", "area"} <= listing_columns
        assert {"post_id", "listing_id", "draft_id", "channel_message_id"} <= post_columns


def test_bootstrap_is_idempotent_and_preserves_existing_rows(tmp_path):
    db_path = tmp_path / "runtime.db"
    initialize_runtime_storage(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text)
               VALUES ('telegram','test','1','hello')"""
        )
        conn.commit()

    initialize_runtime_storage(db_path)

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0]
    assert count == 1
