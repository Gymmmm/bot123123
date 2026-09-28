from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.sale.audit import SaleInventoryAudit
from v3_core.sale.catalog import SaleCatalogRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_sale(
    db: Path,
    tmp_path: Path,
    *,
    suffix: str,
    public_id: str,
    publication_policy: str,
    publishable: int,
) -> str:
    image = tmp_path / f"{suffix}.jpg"
    image.write_bytes(b"sale-image")
    with sqlite3.connect(db) as conn:
        source_pk = int(conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text,raw_images_json,
                raw_videos_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram_channel','sale-policy',?,'','[]','[]','{}',?,'parsed')""",
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
                public_location_display,canonical_area_display,layout,size_sqm,floor,
                display_title,canonical_facts_hash,canonical_facts_schema,inventory_status,data_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'active','current')""",
            (
                listing_id, public_id, canonical_id, f"项目{suffix}", "公寓",
                "BKK1", "BKK1", "2房2卫", 95, "19", f"项目{suffix} 2房2卫",
                f"facts-{suffix}", "canonical_facts.v1",
            ),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,sale_price_usd,offer_status,
                publication_policy,publishable,publish_block_reason)
               VALUES (?,?, 'sale',180000,'active',?,?,?)""",
            (
                f"SALE_{suffix}", listing_id, publication_policy, publishable,
                "sale_not_enabled_for_telegram" if publication_policy == "store_only" and publishable == 0 else "",
            ),
        )
        conn.execute(
            """INSERT INTO media_assets
               (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,source_type,
                local_path,media_type,mime_type,status)
               VALUES (?,'source_post',?,?, 'photo','telegram',?,'photo','image/jpeg','active')""",
            (f"AST_{suffix}", source_pk, str(source_pk), str(image)),
        )
        conn.commit()
    return f"AST_{suffix}"


def test_public_sale_catalog_only_exposes_store_only_nonpublishable_offers(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    good_asset = _seed_sale(
        db, tmp_path, suffix="GOOD", public_id="QL-SALE-GOOD",
        publication_policy="store_only", publishable=0,
    )
    bad_asset = _seed_sale(
        db, tmp_path, suffix="BAD", public_id="QL-SALE-BAD",
        publication_policy="telegram_rent", publishable=1,
    )

    repo = SaleCatalogRepository(db)
    payload = repo.list_sale_listings()

    assert payload["total"] == 1
    assert [item["public_id"] for item in payload["items"]] == ["QL-SALE-GOOD"]
    assert repo.get_sale_listing("QL-SALE-GOOD") is not None
    assert repo.get_sale_listing("QL-SALE-BAD") is None
    assert repo.media_asset(good_asset) is not None
    assert repo.media_asset(bad_asset) is None
    meta = repo.catalog_meta()
    assert meta["total"] == 1
    assert meta["available"] == 1


def test_sale_audit_flags_rows_hidden_by_public_policy_guard(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_sale(
        db, tmp_path, suffix="BAD", public_id="QL-SALE-BAD",
        publication_policy="telegram_rent", publishable=1,
    )

    report = SaleInventoryAudit(db).report()

    assert report["active_sale_offers"] == 1
    assert report["basic_catalog_ready"] == 0
    assert report["issues"]["sale_policy_violations"] == 1
    assert report["samples"]["sale_policy_violations"] == ["QL-SALE-BAD"]
