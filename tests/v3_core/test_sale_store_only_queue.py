from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.publishing.autopilot_anomalies import FinalAutoPublishRepository
from v3_core.publishing.autopilot_policy import ProductionAutoPublishRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_sale_offer(db: Path, *, offer_id: str = "OFF_SALE_NEW") -> str:
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        conn.executescript(
            f"""
            INSERT INTO canonical_records
              (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash,
               quality_score,quality_json,review_status,created_at)
            VALUES ('c_sale','src_sale','v3','sale','{{"deal_type":"sale","sale_price_usd":120000}}','h_sale',90,'{{}}','unreviewed','2026-09-25 01:00:00');
            INSERT INTO listings_v3
              (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
               public_location_display,layout,display_title,canonical_facts_hash,
               canonical_facts_schema,inventory_status,created_at,updated_at)
            VALUES ('l_sale','QL-SALE-1','c_sale','出售验收盘','公寓','BKK1','2房','出售验收盘 2房','h_sale','v3','pending','2026-09-25 01:00:00','2026-09-25 01:00:00');
            INSERT INTO listing_offers
              (offer_id,listing_id,offer_type,currency,monthly_rent_usd,sale_price_usd,
               offer_status,publication_policy,publishable,publish_block_reason,created_at,updated_at)
            VALUES ('{offer_id}','l_sale','sale','USD',NULL,120000,'active','store_only',0,'sale_not_enabled_for_telegram','2026-09-25 01:00:00','2026-09-25 01:00:00');
            INSERT INTO review_items
              (review_id,canonical_record_id,listing_id,offer_id,review_status,review_score,
               review_note,created_at,updated_at)
            VALUES ('rv_sale','c_sale','l_sale','{offer_id}','pending',90,'','2026-09-25 01:00:00','2026-09-25 01:00:00');
            """
        )
    return offer_id


def test_new_sale_offer_is_archived_not_ops_exception(tmp_path: Path):
    db = tmp_path / "sale.db"
    offer_id = _seed_sale_offer(db)
    repo = ProductionAutoPublishRepository(db)
    repo.ensure_defaults()
    before = repo.exception_count()
    repo.sync_candidates()
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT state,ignored,reason_code FROM publisher_auto_items_v3 WHERE offer_id=?",
            (offer_id,),
        ).fetchone()
        offer = conn.execute(
            "SELECT offer_type,publication_policy,sale_price_usd,publishable FROM listing_offers WHERE offer_id=?",
            (offer_id,),
        ).fetchone()
    assert row == ("ignored", 1, "sale_store_only")
    assert offer == ("sale", "store_only", 120000, 0)
    assert repo.exception_count() == before
    assert repo.exception_rows(reason_code="sale_store_only") == []


def test_historical_sale_exception_preserved_for_one_click_ignore(tmp_path: Path):
    db = tmp_path / "sale_hist.db"
    offer_id = _seed_sale_offer(db, offer_id="OFF_SALE_OLD")
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO publisher_auto_items_v3
               (offer_id,listing_id,review_id,state,reason_code,reason_text,ignored,origin)
               VALUES (?,?,?,?,?,?,0,'collector')""",
            (
                offer_id,
                "l_sale",
                "rv_sale",
                "exception",
                "sale_store_only",
                "出售房源仅保存",
            ),
        )
        conn.commit()
    repo = ProductionAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    rows = repo.exception_rows(reason_code="sale_store_only")
    assert len(rows) == 1
    assert rows[0]["offer_id"] == offer_id
    assert rows[0]["state"] == "exception"
    ignored = repo.ignore_by_reason("sale_store_only")
    assert ignored == 1
    assert repo.exception_rows(reason_code="sale_store_only") == []


def test_sale_review_without_offer_is_auto_archived(tmp_path: Path):
    db = tmp_path / "sale_review.db"
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            INSERT INTO canonical_records
              (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash,
               quality_score,quality_json,review_status,created_at)
            VALUES ('c1','s1','v3','sale','{"deal_type":"sale"}','h1',90,'{}','unreviewed','2026-09-25 01:00:00');
            INSERT INTO listings_v3
              (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
               public_location_display,layout,display_title,canonical_facts_hash,
               canonical_facts_schema,inventory_status,created_at,updated_at)
            VALUES ('l1','QL1','c1','盘','公寓','BKK1','2房','盘','h1','v3','pending','2026-09-25 01:00:00','2026-09-25 01:00:00');
            INSERT INTO review_items
              (review_id,canonical_record_id,listing_id,offer_id,review_status,review_score,
               review_note,created_at,updated_at)
            VALUES ('rv1','c1','l1','','pending',90,'','2026-09-25 01:00:00','2026-09-25 01:00:00');
            """
        )
    repo = FinalAutoPublishRepository(db)
    repo.ensure_defaults()
    repo.sync_candidates()
    assert repo.exception_rows(reason_code="sale_store_only") == []
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT ignored,reason_code FROM publisher_review_exceptions_v3 WHERE review_id='rv1'"
        ).fetchone()
    assert row == (1, "sale_store_only")
