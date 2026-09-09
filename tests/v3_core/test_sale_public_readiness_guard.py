from __future__ import annotations

from pathlib import Path
import sqlite3

from v3_core.sale.audit import SaleInventoryAudit
from v3_core.sale.catalog import SaleCatalogRepository
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed(
    db: Path,
    tmp_path: Path,
    *,
    suffix: str,
    public_id: str,
    location: str = "BKK1",
    with_media: bool = True,
) -> None:
    image = tmp_path / f"{suffix}.jpg"
    image.write_bytes(b"sale-image")
    with sqlite3.connect(db) as conn:
        source_pk = int(conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,raw_text,raw_images_json,
                raw_videos_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram_channel','sale-ready',?,'','[]','[]','{}',?,'parsed')""",
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
                listing_id,
                public_id,
                canonical_id,
                f"项目{suffix}" if location else "",
                "公寓",
                location,
                location,
                "2房2卫",
                95,
                "19",
                f"项目{suffix} 2房2卫",
                f"facts-{suffix}",
                "canonical_facts.v1",
            ),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,sale_price_usd,offer_status,
                publication_policy,publishable,publish_block_reason)
               VALUES (?,?, 'sale',180000,'active','store_only',0,'sale_not_enabled_for_telegram')""",
            (f"SALE_{suffix}", listing_id),
        )
        if with_media:
            conn.execute(
                """INSERT INTO media_assets
                   (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,source_type,
                    local_path,media_type,mime_type,status)
                   VALUES (?,'source_post',?,?, 'photo','telegram',?,'photo','image/jpeg','active')""",
                (f"AST_{suffix}", source_pk, str(source_pk), str(image)),
            )
        conn.commit()


def test_incomplete_sale_inventory_stays_stored_but_is_hidden_from_public_catalog(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed(db, tmp_path, suffix="READY", public_id="QL-READY")
    _seed(db, tmp_path, suffix="NOLOC", public_id="QL-NOLOC", location="")
    _seed(db, tmp_path, suffix="NOMEDIA", public_id="QL-NOMEDIA", with_media=False)

    repo = SaleCatalogRepository(db)
    payload = repo.list_sale_listings()

    assert payload["total"] == 1
    assert [item["public_id"] for item in payload["items"]] == ["QL-READY"]
    assert repo.get_sale_listing("QL-READY") is not None
    assert repo.get_sale_listing("QL-NOLOC") is None
    assert repo.get_sale_listing("QL-NOMEDIA") is None

    # The incomplete rows were not deleted or rewritten; they remain sale inventory.
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM listing_offers WHERE offer_type='sale' AND offer_status='active'"
        ).fetchone()[0] == 3


def test_sale_audit_keeps_incomplete_stored_inventory_visible_to_operators(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed(db, tmp_path, suffix="NOLOC", public_id="QL-NOLOC", location="")
    _seed(db, tmp_path, suffix="NOMEDIA", public_id="QL-NOMEDIA", with_media=False)

    report = SaleInventoryAudit(db).report()

    assert report["active_sale_offers"] == 2
    assert report["basic_catalog_ready"] == 0
    assert report["issues"]["missing_project_or_location"] == 1
    assert report["issues"]["missing_media"] == 1
    assert set(report["samples"]["incomplete_sale_listings"]) == {"QL-NOLOC", "QL-NOMEDIA"}
