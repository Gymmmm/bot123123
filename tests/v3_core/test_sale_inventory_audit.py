from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from v3_core.sale.audit import SaleInventoryAudit
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed(
    db: Path,
    tmp_path: Path,
    *,
    suffix: str,
    public_id: str,
    policy: str = "store_only",
    publishable: int = 0,
    inventory_status: str = "active",
    data_status: str = "current",
    location: str = "BKK1",
    media: bool = True,
    rent: bool = False,
    duplicate_sale: bool = False,
) -> None:
    image = tmp_path / f"{suffix}.jpg"
    image.write_bytes(b"img")
    with sqlite3.connect(db) as conn:
        source_pk = int(conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text,raw_images_json,raw_videos_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram_channel','sale-audit',?,'','[]','[]','{}',?,'parsed')""",
            (f"post-{suffix}", f"hash-{suffix}"),
        ).lastrowid)
        canonical_id = f"CAN_{suffix}"
        listing_id = f"L_{suffix}"
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash)
               VALUES (?,?, 'canonical_facts.v1','sale','{}',?)""",
            (canonical_id, str(source_pk), f"facts-{suffix}"),
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
                public_location_display,canonical_area_display,layout,display_title,
                canonical_facts_hash,canonical_facts_schema,inventory_status,data_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                listing_id, public_id, canonical_id, f"项目{suffix}" if location else "",
                "公寓", location, location, "2房", f"项目{suffix} 2房",
                f"facts-{suffix}", "canonical_facts.v1", inventory_status, data_status,
            ),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,sale_price_usd,offer_status,publication_policy,publishable,publish_block_reason)
               VALUES (?,?, 'sale',180000,'active',?,?,?)""",
            (f"SALE_{suffix}", listing_id, policy, publishable, "" if policy != "store_only" else "sale_not_enabled_for_telegram"),
        )
        if duplicate_sale:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,sale_price_usd,offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?, 'sale',185000,'active','store_only',0,'sale_not_enabled_for_telegram')""",
                (f"SALE2_{suffix}", listing_id),
            )
        if rent:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?, 'rent',850,'active','telegram_rent',0,'review_required')""",
                (f"RENT_{suffix}", listing_id),
            )
        if media:
            conn.execute(
                """INSERT INTO media_assets
                   (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,source_type,local_path,media_type,status)
                   VALUES (?,'source_post',?,?, 'photo','telegram',?,'photo','active')""",
                (f"AST_{suffix}", source_pk, str(source_pk), str(image)),
            )
        conn.commit()


def test_sale_audit_reports_clean_ready_and_dual_offer_inventory(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed(db, tmp_path, suffix="READY", public_id="QL-SALE-1", rent=True)
    report = SaleInventoryAudit(db).report()
    assert report["mode"] == "read_only"
    assert report["active_sale_offers"] == 1
    assert report["basic_catalog_ready"] == 1
    assert report["dual_rent_sale_listings"] == 1
    assert report["issues"] == {
        "sale_policy_violations": 0,
        "duplicate_active_sale_listings": 0,
        "missing_public_id": 0,
        "noncurrent_listing": 0,
        "unavailable_status": 0,
        "missing_project_or_location": 0,
        "missing_media": 0,
    }


def test_sale_audit_detects_policy_duplicate_and_incomplete_inventory(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed(
        db,
        tmp_path,
        suffix="BAD",
        public_id="",
        policy="telegram_rent",
        publishable=1,
        inventory_status="sold",
        data_status="superseded",
        location="",
        media=False,
        duplicate_sale=True,
    )
    report = SaleInventoryAudit(db).report(sample_limit=5)
    assert report["active_sale_offers"] == 2
    assert report["basic_catalog_ready"] == 0
    assert report["issues"]["sale_policy_violations"] == 1
    assert report["issues"]["duplicate_active_sale_listings"] == 1
    assert report["issues"]["missing_public_id"] == 1
    assert report["issues"]["noncurrent_listing"] == 1
    assert report["issues"]["unavailable_status"] == 1
    assert report["issues"]["missing_project_or_location"] == 1
    assert report["issues"]["missing_media"] == 1
    assert report["samples"]["sale_policy_violations"] == ["（缺少公开编号）"]
    assert report["samples"]["incomplete_sale_listings"] == ["（缺少公开编号）"]


def test_sale_audit_is_read_only_and_does_not_create_missing_database(tmp_path: Path):
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(FileNotFoundError):
        SaleInventoryAudit(missing).report()
    assert not missing.exists()

    db = initialize_v3_storage(tmp_path / "real.sqlite")
    _seed(db, tmp_path, suffix="RO", public_id="QL-SALE-RO")
    with sqlite3.connect(db) as conn:
        before = {
            "listings": conn.execute("SELECT COUNT(*) FROM listings_v3").fetchone()[0],
            "offers": conn.execute("SELECT COUNT(*) FROM listing_offers").fetchone()[0],
            "canonicals": conn.execute("SELECT COUNT(*) FROM canonical_records").fetchone()[0],
        }
    SaleInventoryAudit(db).report()
    with sqlite3.connect(db) as conn:
        after = {
            "listings": conn.execute("SELECT COUNT(*) FROM listings_v3").fetchone()[0],
            "offers": conn.execute("SELECT COUNT(*) FROM listing_offers").fetchone()[0],
            "canonicals": conn.execute("SELECT COUNT(*) FROM canonical_records").fetchone()[0],
        }
    assert after == before


def test_sale_audit_entrypoint_imports_without_running():
    import run_v3_sale_audit
    assert callable(run_v3_sale_audit.main)
    source = Path(run_v3_sale_audit.__file__).read_text(encoding="utf-8")
    assert "SaleInventoryAligner" not in source
    assert "initialize_v3_storage" not in source
    assert "SaleInventoryAudit" in source
