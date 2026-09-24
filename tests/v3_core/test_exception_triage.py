from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.publishing.autopilot import AutoPublishRepository
from v3_core.publishing.autopilot_anomalies import FinalAutoPublishRepository


def _seed(db: Path) -> None:
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE listings_v3 (
                listing_id TEXT PRIMARY KEY,
                public_listing_id TEXT,
                display_title TEXT,
                project_name TEXT,
                inventory_status TEXT
            );
            CREATE TABLE listing_offers (
                offer_id TEXT PRIMARY KEY,
                listing_id TEXT,
                offer_type TEXT,
                offer_status TEXT,
                monthly_rent_usd REAL,
                publication_policy TEXT,
                publishable INTEGER
            );
            CREATE TABLE publisher_auto_items_v3 (
                offer_id TEXT PRIMARY KEY,
                listing_id TEXT NOT NULL,
                review_id TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'queued',
                reason_code TEXT NOT NULL DEFAULT '',
                reason_text TEXT NOT NULL DEFAULT '',
                package_id TEXT NOT NULL DEFAULT '',
                channel_message_id TEXT NOT NULL DEFAULT '',
                origin TEXT NOT NULL DEFAULT 'collector',
                ignored INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                published_at TEXT
            );
            INSERT INTO listings_v3 VALUES
              ('l_sale','QL-S1','出售A','项目A','pending'),
              ('l_loc','QL-L1','缺位置B','','pending'),
              ('l_can','QL-C1','规范错C','项目C','pending'),
              ('l_media','QL-M1','图片D','项目D','pending');
            INSERT INTO listing_offers VALUES
              ('off_sale','l_sale','sale','active',NULL,'sale_store',0),
              ('off_sale2','l_sale','sale','active',NULL,'sale_store',0),
              ('off_loc','l_loc','rent','active',800,'telegram_rent',1),
              ('off_can','l_can','rent','active',900,'telegram_rent',1),
              ('off_media','l_media','rent','active',700,'telegram_rent',1);
            INSERT INTO publisher_auto_items_v3
              (offer_id, listing_id, review_id, state, reason_code, reason_text, origin, ignored)
            VALUES
              ('off_sale','l_sale','','exception','sale_store_only','出售房源仅保存','collector',0),
              ('off_sale2','l_sale','','exception','sale_store_only','出售房源仅保存','collector',0),
              ('off_loc','l_loc','','exception','missing_location','缺少项目或位置','collector',0),
              ('off_can','l_can','','exception','canonical_error','房源信息存在关键错误','collector',0),
              ('off_media','l_media','','exception','unreadable_media','图片无法读取','collector',0);
            """
        )


def test_exception_counts_and_filter(tmp_path: Path):
    db = tmp_path / "q.db"
    _seed(db)
    repo = AutoPublishRepository(db)
    counts = repo.exception_counts_by_reason()
    assert counts["all"] == 5
    assert counts["sale_store_only"] == 2
    assert counts["missing_location"] == 1
    assert counts["canonical_error"] == 1
    assert counts["other"] == 1
    sale_rows = repo.exception_rows(reason_code="sale_store_only")
    assert {row["offer_id"] for row in sale_rows} == {"off_sale", "off_sale2"}
    other_rows = repo.exception_rows(reason_code="other")
    assert [row["offer_id"] for row in other_rows] == ["off_media"]


def test_ignore_by_reason_sale_only(tmp_path: Path):
    db = tmp_path / "q.db"
    _seed(db)
    repo = AutoPublishRepository(db)
    ignored = repo.ignore_by_reason("sale_store_only")
    assert ignored == 2
    counts = repo.exception_counts_by_reason()
    assert counts.get("sale_store_only", 0) == 0
    assert counts["all"] == 3
    try:
        repo.ignore_by_reason("missing_location")
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "bulk_ignore_not_allowed" in str(exc)


def test_final_repo_counts_include_review_exceptions(tmp_path: Path):
    db = tmp_path / "q.db"
    _seed(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """CREATE TABLE publisher_review_exceptions_v3 (
                review_id TEXT PRIMARY KEY,
                listing_id TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                reason_text TEXT NOT NULL,
                ignored INTEGER NOT NULL DEFAULT 0,
                resolved INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        conn.execute(
            """INSERT INTO publisher_review_exceptions_v3
               (review_id, listing_id, reason_code, reason_text, ignored, resolved)
               VALUES ('rv1','l_loc','missing_location','缺少项目或位置',0,0)"""
        )
        conn.commit()
    repo = FinalAutoPublishRepository(db)
    # Avoid sync_review_exceptions requiring wider schema
    repo.sync_review_exceptions = lambda: 0  # type: ignore[method-assign]
    counts = repo.exception_counts_by_reason()
    assert counts["missing_location"] == 2
    rows = repo.exception_rows(reason_code="missing_location")
    offer_ids = {row["offer_id"] for row in rows}
    assert "off_loc" in offer_ids
    assert "review:rv1" in offer_ids
