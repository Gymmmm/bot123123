from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _tables(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


def test_fresh_production_bootstrap_creates_legacy_user_and_v3_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh-production.db"
    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)

    result = subprocess.run(
        [sys.executable, str(ROOT / "bootstrap_db.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    tables = _tables(db_path)
    assert {
        # Locked production compatibility runtime.
        "source_posts",
        "drafts",
        "media_assets",
        "posts",
        "publish_logs",
        # User/admin runtime.
        "listings",
        "users",
        "appointments",
        # V3 runtime.
        "v3_sources",
        "v3_source_posts",
        "source_post_revisions",
        "canonical_records",
        "v3_listings",
        "listing_offers",
        "v3_publication_packages",
        "v3_channel_posts",
        "v3_sync_leases",
        "schema_migrations",
    }.issubset(tables)

    with sqlite3.connect(db_path) as conn:
        versions = [
            str(row[0])
            for row in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        ]
    assert versions == ["001", "002"]


def test_fresh_bootstrap_accepts_first_real_collector_source_write(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh-collector.db"
    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)

    result = subprocess.run(
        [sys.executable, str(ROOT / "bootstrap_db.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    # Reproduce the exact production boundary that previously failed with
    # sqlite3.OperationalError: no such table: source_posts.
    from collector_db_compat import DatabaseManager

    manager = DatabaseManager(str(db_path))
    row_id = manager.save_source_post(
        source_id=1,
        source_type="telegram",
        source_name="fresh-bootstrap-test",
        source_post_id="1",
        source_url="https://t.me/example/1",
        source_author="test",
        raw_text="出租测试房源 $500/月",
        raw_images_json=["/tmp/photo-1.jpg"],
        raw_videos_json=[],
        raw_contact="",
        raw_meta_json={"test": True},
        dedupe_hash="fresh-bootstrap-test-hash",
    )
    assert row_id == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT source_type,source_name,source_post_id,parse_status "
            "FROM source_posts WHERE id=?",
            (row_id,),
        ).fetchone()
    assert row == ("telegram", "fresh-bootstrap-test", "1", "pending")


def test_bootstrap_is_idempotent_and_preserves_existing_source_data(tmp_path: Path) -> None:
    db_path = tmp_path / "idempotent.db"
    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)

    first = subprocess.run(
        [sys.executable, str(ROOT / "bootstrap_db.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stderr

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO source_posts "
            "(source_type,source_name,source_post_id,raw_text,dedupe_hash) "
            "VALUES ('telegram','keep-me','99','existing data','keep-hash')"
        )
        conn.commit()

    second = subprocess.run(
        [sys.executable, str(ROOT / "bootstrap_db.py")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert second.returncode == 0, second.stderr

    with sqlite3.connect(db_path) as conn:
        kept = conn.execute(
            "SELECT raw_text FROM source_posts WHERE source_name='keep-me'"
        ).fetchone()
        versions = conn.execute(
            "SELECT version,COUNT(*) FROM schema_migrations GROUP BY version ORDER BY version"
        ).fetchall()
    assert kept == ("existing data",)
    assert versions == [("001", 1), ("002", 1)]
