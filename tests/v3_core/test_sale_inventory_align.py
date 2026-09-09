from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from v3_core.parser.worker import CanonicalWorker
from v3_core.sale.align import SaleInventoryAligner, due_for_daily_scan
from v3_core.sale.catalog import SaleCatalogRepository
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_repository import InventoryRepository


def _facts(**overrides):
    facts = {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": "hash-sale-align",
        "deal_type": "rent",
        "project_name": "钻石岛",
        "project_alias": "",
        "project_brand": "",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "钻石岛",
        "public_location_display": "钻石岛",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "钻石岛",
        "canonical_area_display": "钻石岛",
        "layout": "2房2卫",
        "bedrooms": 2,
        "bathrooms": 2,
        "size_sqm": 88,
        "floor": "12",
        "display_title": "钻石岛 2房公寓",
        "monthly_rent_usd": 900,
        "sale_price_usd": None,
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "available_date": "",
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }
    facts.update(overrides)
    return facts


def test_aligner_adds_sale_offer_without_resetting_rent_publishable(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    repo = InventoryRepository(db)
    facts = _facts()
    canonical = repo.store_canonical(source_post_id=11, facts=facts)
    repo.upsert_listing(
        listing_id="l_11",
        public_listing_id="QL-DD-A2B3",
        canonical_record_id=canonical["canonical_record_id"],
        facts=facts,
    )
    rent = repo.sync_offers(listing_id="l_11", facts=facts)[0]
    repo.set_offer_publication(offer_id=str(rent["offer_id"]), publishable=True, block_reason="")

    later = _facts(
        deal_type="mixed",
        sale_price_usd=188000,
        canonical_facts_hash="hash-sale-align-2",
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE canonical_records SET facts_json=?,facts_hash=?,deal_type='mixed' WHERE canonical_record_id=?",
            (
                json.dumps(later, ensure_ascii=False),
                later["canonical_facts_hash"],
                canonical["canonical_record_id"],
            ),
        )
        conn.commit()

    stats = SaleInventoryAligner(db).align()
    assert stats["sale_upserted"] == 1

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        rent_row = conn.execute(
            "SELECT publishable,publication_policy FROM listing_offers WHERE offer_id=?",
            (str(rent["offer_id"]),),
        ).fetchone()
        sale_row = conn.execute(
            """SELECT sale_price_usd,publication_policy,publishable,publish_block_reason
               FROM listing_offers WHERE listing_id='l_11' AND offer_type='sale' AND offer_status='active'"""
        ).fetchone()
    assert int(rent_row["publishable"]) == 1
    assert rent_row["publication_policy"] == "telegram_rent"
    assert sale_row["sale_price_usd"] == 188000
    assert sale_row["publication_policy"] == "store_only"
    assert int(sale_row["publishable"]) == 0
    catalog = SaleCatalogRepository(db).list_sale_listings()
    assert catalog["total"] == 1
    assert catalog["items"][0]["public_id"] == "QL-DD-A2B3"


def test_aligner_repairs_sale_policy_and_materializes_orphan_sale_record(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    repo = InventoryRepository(db)
    facts = _facts(deal_type="sale", monthly_rent_usd=None, sale_price_usd=210000)
    repo.store_canonical(source_post_id=22, facts=facts)

    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,sale_price_usd,offer_status,
                publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_BAD','missing','sale',210000,'active','telegram_rent',1,'')"""
        )
        conn.commit()

    stats = SaleInventoryAligner(db).align()
    assert stats["orphans_materialized"] == 1
    assert stats["policy_repaired"] >= 1

    catalog = SaleCatalogRepository(db).list_sale_listings()
    assert catalog["total"] == 1
    assert catalog["items"][0]["sale_price_usd"] == 210000
    with sqlite3.connect(db) as conn:
        bad = conn.execute(
            "SELECT publication_policy,publishable,publish_block_reason FROM listing_offers WHERE offer_id='OFF_BAD'"
        ).fetchone()
        sale_count = conn.execute(
            "SELECT COUNT(*) FROM listing_offers WHERE offer_type='sale' AND publication_policy<>'store_only'"
        ).fetchone()[0]
        publishable_sale = conn.execute(
            "SELECT COUNT(*) FROM listing_offers WHERE offer_type='sale' AND publishable<>0"
        ).fetchone()[0]
    assert bad[0] == "store_only"
    assert bad[1] == 0
    assert bad[2] == "sale_not_enabled_for_telegram"
    assert sale_count == 0
    assert publishable_sale == 0


def test_aligner_inactivates_sale_when_canonical_price_removed(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    repo = InventoryRepository(db)
    facts = _facts(deal_type="mixed", sale_price_usd=150000)
    canonical = repo.store_canonical(source_post_id=33, facts=facts)
    repo.upsert_listing(
        listing_id="l_33",
        public_listing_id="QL-PP-C4D5",
        canonical_record_id=canonical["canonical_record_id"],
        facts=facts,
    )
    repo.sync_offers(listing_id="l_33", facts=facts)

    later = _facts(deal_type="rent", sale_price_usd=None, canonical_facts_hash="hash-no-sale")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE canonical_records SET facts_json=?,facts_hash=?,deal_type='rent' WHERE canonical_record_id=?",
            (
                json.dumps(later, ensure_ascii=False),
                later["canonical_facts_hash"],
                canonical["canonical_record_id"],
            ),
        )
        conn.commit()

    stats = SaleInventoryAligner(db).align()
    assert stats["sale_inactivated"] == 1
    assert SaleCatalogRepository(db).list_sale_listings()["total"] == 0


def test_daily_scan_runs_once_per_phnom_penh_day(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    aligner = SaleInventoryAligner(db)
    day = datetime(2026, 9, 10, 8, 0, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    first = aligner.align_if_due(now=day)
    second = aligner.align_if_due(now=day)
    assert first["skipped"] == 0
    assert second["skipped"] == 1
    assert due_for_daily_scan(aligner.runtime, now=day) is False
    next_day = aligner.align_if_due(
        now=datetime(2026, 9, 11, 0, 5, tzinfo=ZoneInfo("Asia/Phnom_Penh"))
    )
    assert next_day["skipped"] == 0


def test_canonical_worker_does_daily_sale_scan(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "worker.sqlite")
    repo = InventoryRepository(db)
    facts = _facts(deal_type="sale", monthly_rent_usd=None, sale_price_usd=199000)
    repo.store_canonical(source_post_id=44, facts=facts)

    stats = CanonicalWorker(str(db)).run_once()
    assert stats["sale_align_skipped"] == 0
    catalog = SaleCatalogRepository(db).list_sale_listings()
    assert catalog["total"] == 1
    assert catalog["items"][0]["sale_price_usd"] == 199000

    again = CanonicalWorker(str(db)).run_once()
    assert again["sale_align_skipped"] == 1


def test_sale_align_entrypoint_imports_without_running():
    import run_v3_sale_align

    assert callable(run_v3_sale_align.main)
    assert callable(run_v3_sale_align.run_daily_loop)
