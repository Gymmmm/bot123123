from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.publishing.autopilot import AutoPublishRepository
from v3_core.publishing.autopilot_anomalies import FinalAutoPublishRepository
from v3_core.publishing.simple_admin import SimplePublisherAdminController


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
            CREATE TABLE publisher_review_exceptions_v3 (
                review_id TEXT PRIMARY KEY,
                listing_id TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                reason_text TEXT NOT NULL,
                ignored INTEGER NOT NULL DEFAULT 0 CHECK(ignored IN (0,1)),
                resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0,1)),
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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


def test_exception_category_mapping_and_filter_clause():
    """The 6 operator-facing tabs collapse existing reason_codes without
    touching the underlying DB or autopilot semantics.
    """
    from v3_core.publishing.simple_admin import SimplePublisherAdminController

    assert SimplePublisherAdminController.category_for_reason("missing_location") == "missing_location"
    assert SimplePublisherAdminController.category_for_reason("missing_rent") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("canonical_error") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("unreadable_media") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("insufficient_media") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("listing_not_publishable") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("admin_hold") == "data_issue"
    assert SimplePublisherAdminController.category_for_reason("duplicate_listing") == "duplicate"
    assert SimplePublisherAdminController.category_for_reason("already_published") == "duplicate"
    assert SimplePublisherAdminController.category_for_reason("telegram_unknown") == "delivery_pending"
    assert SimplePublisherAdminController.category_for_reason("telegram_failed") == "delivery_pending"
    assert SimplePublisherAdminController.category_for_reason("sale_store_only") == "sale_store_only"
    assert SimplePublisherAdminController.category_for_reason("legacy_backlog") == "other"
    assert SimplePublisherAdminController.category_for_reason("") == "other"

    # Plain-language labels for the confusing codes.
    assert SimplePublisherAdminController.human_reason("duplicate_listing") == "疑似重复房源"
    assert SimplePublisherAdminController.human_reason("already_published") == "已经发布"
    assert SimplePublisherAdminController.human_reason("missing_rent") == "缺少租金"

    # Tab labels stay operator-friendly.
    tabs = dict(SimplePublisherAdminController._EXCEPTION_TABS)
    assert tabs["missing_location"].startswith("📍")
    assert tabs["data_issue"].startswith("📝")
    assert tabs["duplicate"].startswith("🔁")
    assert tabs["delivery_pending"].startswith("📤")
    assert tabs["sale_store_only"].startswith("🏷")
    assert tabs["other"].startswith("⚠️")

    # Filter clause only emits known reasons; "all" returns empty.
    stub = object.__new__(SimplePublisherAdminController)
    assert stub._exception_filter_clause("all") == ("", [])
    clause, params = stub._exception_filter_clause("missing_location")
    assert clause == "reason_code IN (?)"
    assert params == ["missing_location"]
    other_clause, other_params = stub._exception_filter_clause("other")
    assert other_clause.startswith("reason_code NOT IN (")
    assert "missing_location" in other_params
    assert "sale_store_only" in other_params


def test_exception_rows_filtered_returns_other_category(tmp_path: Path):
    db = tmp_path / "q.db"
    _seed(db)
    repo = FinalAutoPublishRepository(db)
    repo.sync_review_exceptions = lambda: 0  # type: ignore[method-assign]
    stub = object.__new__(SimplePublisherAdminController)
    # The seed includes an `unreadable_media` exception.  That belongs to the
    # 📝 资料问题 tab, so it must NOT appear under ⚠️ 其他.
    clause, params = stub._exception_filter_clause("other")
    rows = repo.exception_rows_filtered(clause, params, limit=20)
    offer_ids = {row["offer_id"] for row in rows}
    assert "off_media" not in offer_ids
    # And it must appear under 📝 资料问题.
    clause, params = stub._exception_filter_clause("data_issue")
    rows = repo.exception_rows_filtered(clause, params, limit=20)
    assert any(row["offer_id"] == "off_media" for row in rows)
