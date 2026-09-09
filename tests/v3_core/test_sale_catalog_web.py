from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import threading
from urllib.request import urlopen

from v3_core.inventory.materializer_v3 import offer_projections_v3
from v3_core.sale.catalog import SaleCatalogRepository
from v3_core.sale.web import make_handler
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_listing(
    db: Path,
    tmp_path: Path,
    *,
    suffix: str,
    public_id: str,
    offer_type: str = "sale",
    offer_status: str = "active",
    inventory_status: str = "active",
    sale_price: int = 180000,
    monthly_rent: int = 800,
    with_media: bool = True,
) -> str:
    image = tmp_path / f"{suffix}.jpg"
    image.write_bytes(b"jpeg-test")
    with sqlite3.connect(db) as conn:
        cur = conn.execute(
            """INSERT INTO source_posts
               (source_type,source_name,source_post_id,source_url,source_author,
                raw_text,raw_images_json,raw_videos_json,raw_meta_json,dedupe_hash,parse_status)
               VALUES ('telegram_channel','sale-test',?, '', 'channel','', '[]','[]','{}',?,'parsed')""",
            (f"post-{suffix}", f"hash-{suffix}"),
        )
        source_pk = int(cur.lastrowid)
        canonical_id = f"CAN_{suffix}"
        listing_id = f"l_{suffix}"
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,facts_hash)
               VALUES (?,?, 'canonical_facts.v1', ?, '{}', ?)""",
            (canonical_id, str(source_pk), offer_type, f"facts-{suffix}"),
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,property_type,
                public_location_display,canonical_area_display,layout,bedrooms,bathrooms,size_sqm,floor,
                display_title,canonical_facts_hash,canonical_facts_schema,inventory_status,data_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'current')""",
            (
                listing_id,
                public_id,
                canonical_id,
                f"项目{suffix}",
                "公寓",
                "BKK1",
                "BKK1",
                "2房2卫",
                2,
                2,
                95,
                "19",
                f"项目{suffix} 2房公寓",
                f"facts-{suffix}",
                "canonical_facts.v1",
                inventory_status,
            ),
        )
        if offer_type == "sale":
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,sale_price_usd,offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?, 'sale', ?, ?, 'store_only',0,'sale_not_enabled_for_telegram')""",
                (f"OFFER_{suffix}", listing_id, sale_price, offer_status),
            )
        else:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?, 'rent', ?, ?, 'telegram_rent',0,'review_required')""",
                (f"OFFER_{suffix}", listing_id, monthly_rent, offer_status),
            )
        if with_media:
            conn.execute(
                """INSERT INTO media_assets
                   (asset_id,owner_type,owner_ref_id,owner_ref_key,asset_type,source_type,local_path,
                    media_type,is_cover,sort_order,mime_type,status)
                   VALUES (?,'source_post',?,?, 'photo','telegram',?,'photo',1,0,'image/jpeg','active')""",
                (f"AST_{suffix}", source_pk, str(source_pk), str(image)),
            )
        conn.commit()
    return listing_id


def test_sale_catalog_reads_shared_v3_inventory_and_hides_internal_ids(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="SALE1", public_id="QL-1001")
    _seed_listing(db, tmp_path, suffix="RENT1", public_id="QL-2001", offer_type="rent")

    payload = SaleCatalogRepository(db).list_sale_listings()

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["public_id"] == "QL-1001"
    assert item["sale_price_usd"] == 180000
    assert item["cover_url"] == "/api/v3/sale/media/AST_SALE1"
    serialized = json.dumps(item, ensure_ascii=False)
    assert "listing_id" not in item
    assert "offer_id" not in item
    assert "canonical_record_id" not in item
    assert str(tmp_path) not in serialized


def test_sale_catalog_filters_inactive_and_missing_public_identity(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="OK", public_id="QL-3001")
    _seed_listing(db, tmp_path, suffix="OFF", public_id="QL-3002", offer_status="inactive")
    _seed_listing(db, tmp_path, suffix="DOWN", public_id="QL-3003", inventory_status="off_market")
    _seed_listing(db, tmp_path, suffix="NOID", public_id="")

    payload = SaleCatalogRepository(db).list_sale_listings()
    assert [item["public_id"] for item in payload["items"]] == ["QL-3001"]


def test_dual_offer_listing_appears_once_in_sale_catalog(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    listing_id = _seed_listing(db, tmp_path, suffix="DUAL", public_id="QL-4001")
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFFER_DUAL_RENT',?,'rent',900,'active','telegram_rent',0,'review_required')""",
            (listing_id,),
        )
        conn.commit()

    payload = SaleCatalogRepository(db).list_sale_listings()
    assert payload["total"] == 1
    assert payload["items"][0]["public_id"] == "QL-4001"


def test_sale_media_resolver_cannot_expose_rent_only_asset(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="SALEMEDIA", public_id="QL-5001")
    _seed_listing(db, tmp_path, suffix="RENTMEDIA", public_id="QL-5002", offer_type="rent")
    repo = SaleCatalogRepository(db)

    assert repo.media_asset("AST_SALEMEDIA") is not None
    assert repo.media_asset("AST_RENTMEDIA") is None


def test_sale_offer_remains_store_only_and_not_rent_publishable():
    offers = offer_projections_v3({"sale_price_usd": 250000})
    assert offers == [
        {
            "offer_type": "sale",
            "currency": "USD",
            "monthly_rent_usd": None,
            "sale_price_usd": 250000,
            "deposit_terms": "",
            "payment_terms": "",
            "contract_term": "",
            "available_date": "",
            "offer_status": "active",
            "publication_policy": "store_only",
            "publishable": False,
            "publish_block_reason": "sale_not_enabled_for_telegram",
        }
    ]


def test_sale_http_api_and_static_frontend(tmp_path: Path):
    from http.server import ThreadingHTTPServer

    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="WEB", public_id="QL-6001")
    index = tmp_path / "index.html"
    index.write_text("<html><body>sale frontend</body></html>", encoding="utf-8")
    handler = make_handler(SaleCatalogRepository(db), index)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        with urlopen(f"http://{host}:{port}/healthz", timeout=3) as response:
            health = json.loads(response.read().decode("utf-8"))
        assert health == {"ok": True, "component": "v3-sale-web", "mode": "read_only"}
        with urlopen(f"http://{host}:{port}/api/v3/sale/listings", timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["ok"] is True
        assert payload["items"][0]["public_id"] == "QL-6001"
        with urlopen(f"http://{host}:{port}/", timeout=3) as response:
            assert b"sale frontend" in response.read()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_sale_frontend_uses_api_not_hardcoded_demo_inventory():
    root = Path(__file__).resolve().parents[2]
    html = (root / "web" / "sale" / "index.html").read_text(encoding="utf-8")
    assert "/api/v3/sale/listings" in html
    assert "正式 <span>出售房源</span>" in html
    assert "仅出售" in html
    assert "本页只放出售库存" in html
    assert "?text=" in html
    assert "searchParams.set('id'" in html
    assert "The Bridge 2房公寓" not in html
    assert "炳发城精装㽓公寓" not in html
    assert "钻石岛江景公寓" not in html
    assert "listing_id" not in html
    assert "offer_id" not in html


def test_sale_web_entrypoint_imports_without_starting_server():
    import run_v3_sale_web

    assert callable(run_v3_sale_web.run)
