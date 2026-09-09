from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import threading
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

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
    mime_type: str = "image/jpeg",
    property_type: str = "公寓",
    location: str = "BKK1",
    layout: str = "2房2卫",
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
                listing_id, public_id, canonical_id, f"项目{suffix}", property_type,
                location, location, layout, 2, 2, 95, "19", f"项目{suffix} {layout}",
                f"facts-{suffix}", "canonical_facts.v1", inventory_status,
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
                   VALUES (?,'source_post',?,?, 'photo','telegram',?,'photo',1,0,?,'active')""",
                (f"AST_{suffix}", source_pk, str(source_pk), str(image), mime_type),
            )
        conn.commit()
    return listing_id


def test_sale_catalog_reads_shared_v3_inventory_and_hides_internal_ids(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="SALE1", public_id="QL-1001")
    _seed_listing(db, tmp_path, suffix="RENT1", public_id="QL-2001", offer_type="rent")
    payload = SaleCatalogRepository(db).list_sale_listings()
    assert payload["total"] == 1
    assert payload["has_more"] is False
    item = payload["items"][0]
    assert item["public_id"] == "QL-1001"
    assert item["sale_price_usd"] == 180000
    assert item["cover_url"] == "/api/v3/sale/media/AST_SALE1"
    serialized = json.dumps(item, ensure_ascii=False)
    for internal_key in ("listing_id", "offer_id", "canonical_record_id", "source_post_id", "local_path"):
        assert internal_key not in item
    assert str(tmp_path) not in serialized
    assert "$800" not in serialized


def test_sale_catalog_filters_non_public_inventory(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="OK", public_id="QL-3001")
    _seed_listing(db, tmp_path, suffix="HOLD", public_id="QL-3002", inventory_status="reserved")
    _seed_listing(db, tmp_path, suffix="OFF", public_id="QL-3003", offer_status="inactive")
    _seed_listing(db, tmp_path, suffix="DOWN", public_id="QL-3004", inventory_status="off_market")
    _seed_listing(db, tmp_path, suffix="SOLD", public_id="QL-3005", inventory_status="sold")
    _seed_listing(db, tmp_path, suffix="NOID", public_id="")
    payload = SaleCatalogRepository(db).list_sale_listings()
    items = {item["public_id"]: item for item in payload["items"]}
    assert set(items) == {"QL-3001", "QL-3002"}
    assert items["QL-3001"]["status"] == "可咨询"
    assert items["QL-3002"]["status"] == "已预留"


def test_dual_offer_listing_appears_once_and_rent_price_is_never_exposed(tmp_path: Path):
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
    serialized = json.dumps(payload, ensure_ascii=False)
    assert payload["items"][0]["public_id"] == "QL-4001"
    assert "monthly_rent_usd" not in serialized
    assert '900' not in serialized


def test_accidental_multiple_active_sale_offers_do_not_duplicate_listing(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    listing_id = _seed_listing(db, tmp_path, suffix="MULTI", public_id="QL-4101", sale_price=150000)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,sale_price_usd,offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('ZZZ_MULTI',?,'sale',160000,'active','store_only',0,'sale_not_enabled_for_telegram')""",
            (listing_id,),
        )
        conn.commit()
    payload = SaleCatalogRepository(db).list_sale_listings()
    assert payload["total"] == 1
    assert len(payload["items"]) == 1


def test_sale_catalog_search_status_price_sort_and_pagination(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="A", public_id="QL-7001", sale_price=100000, location="BKK1")
    _seed_listing(db, tmp_path, suffix="B", public_id="QL-7002", sale_price=300000, location="钻石岛", property_type="别墅")
    _seed_listing(db, tmp_path, suffix="C", public_id="QL-7003", sale_price=200000, location="BKK1", inventory_status="reserved")
    repo = SaleCatalogRepository(db)
    assert [x["public_id"] for x in repo.list_sale_listings(q="QL-7002")["items"]] == ["QL-7002"]
    assert repo.list_sale_listings(area="BKK1")["total"] == 2
    assert repo.list_sale_listings(property_type="别墅")["total"] == 1
    assert repo.list_sale_listings(status="reserved")["items"][0]["public_id"] == "QL-7003"
    assert repo.list_sale_listings(max_price=150000)["items"][0]["public_id"] == "QL-7001"
    priced = repo.list_sale_listings(sort="price_asc", limit=2, offset=0)
    assert [x["sale_price_usd"] for x in priced["items"]] == [100000, 200000]
    assert priced["has_more"] is True
    second = repo.list_sale_listings(sort="price_asc", limit=2, offset=2)
    assert [x["sale_price_usd"] for x in second["items"]] == [300000]
    assert second["has_more"] is False


def test_sale_catalog_meta_is_public_safe(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="M1", public_id="QL-8001", sale_price=120000, location="BKK1")
    _seed_listing(db, tmp_path, suffix="M2", public_id="QL-8002", sale_price=280000, location="钻石岛", property_type="别墅", inventory_status="reserved")
    assert SaleCatalogRepository(db).catalog_meta() == {
        "total": 2,
        "available": 1,
        "reserved": 1,
        "min_price_usd": 120000,
        "max_price_usd": 280000,
        "areas": ["BKK1", "钻石岛"],
        "property_types": ["公寓", "别墅"],
    }


def test_sale_media_resolver_cannot_expose_rent_or_non_image_asset(tmp_path: Path):
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="SALEMEDIA", public_id="QL-5001")
    _seed_listing(db, tmp_path, suffix="RENTMEDIA", public_id="QL-5002", offer_type="rent")
    _seed_listing(db, tmp_path, suffix="SVG", public_id="QL-5003", mime_type="image/svg+xml")
    repo = SaleCatalogRepository(db)
    assert repo.media_asset("AST_SALEMEDIA") is not None
    assert repo.media_asset("AST_RENTMEDIA") is None
    assert repo.media_asset("AST_SVG") is None
    assert repo.get_sale_listing("QL-5003") is None
    assert "QL-5003" not in {item["public_id"] for item in repo.list_sale_listings()["items"]}


def test_sale_offer_remains_store_only_and_not_rent_publishable():
    assert offer_projections_v3({"sale_price_usd": 250000}) == [{
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
    }]


def test_sale_repository_never_creates_missing_database(tmp_path: Path):
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(FileNotFoundError):
        SaleCatalogRepository(missing).list_sale_listings()
    assert not missing.exists()


def test_sale_http_api_meta_detail_media_and_static_frontend(tmp_path: Path):
    from http.server import ThreadingHTTPServer
    db = initialize_v3_storage(tmp_path / "v3.sqlite")
    _seed_listing(db, tmp_path, suffix="WEB", public_id="QL-6001")
    index = tmp_path / "index.html"
    index.write_text("<html><body>sale frontend</body></html>", encoding="utf-8")
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(SaleCatalogRepository(db), index))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base = f"http://{host}:{port}"
        with urlopen(base + "/healthz", timeout=3) as response:
            health = json.loads(response.read().decode("utf-8"))
            assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert health == {"ok": True, "component": "v3-sale-web", "mode": "read_only"}
        with urlopen(base + "/api/v3/sale/meta", timeout=3) as response:
            assert json.loads(response.read().decode("utf-8"))["total"] == 1
        with urlopen(base + "/api/v3/sale/listings?limit=1&sort=price_asc", timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["items"][0]["public_id"] == "QL-6001"
        with urlopen(base + "/api/v3/sale/listings/ql-6001", timeout=3) as response:
            detail = json.loads(response.read().decode("utf-8"))
        assert detail["item"]["public_id"] == "QL-6001"
        with urlopen(base + "/api/v3/sale/media/AST_WEB", timeout=3) as response:
            assert response.read() == b"jpeg-test"
            assert response.headers["Content-Type"] == "image/jpeg"
        with urlopen(base + "/", timeout=3) as response:
            assert b"sale frontend" in response.read()
        with pytest.raises(HTTPError) as exc:
            urlopen(base + "/api/v3/sale/listings/QL-NOTFOUND", timeout=3)
        assert exc.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_sale_frontend_uses_api_pagination_and_no_demo_inventory():
    root = Path(__file__).resolve().parents[2]
    html = (root / "web" / "sale" / "index.html").read_text(encoding="utf-8")
    assert "/api/v3/sale/listings" in html
    assert "/api/v3/sale/meta" in html
    assert "正式 <span>出售房源</span>" in html
    assert "仅出售" in html
    assert "PAGE_SIZE=24" in html
    assert "price_asc" in html and "price_desc" in html
    assert "searchParams.set('id'" in html
    for demo in ("The Bridge 2房公寓", "炳发城精装3房公寓", "钻石岛江景公寓"):
        assert demo not in html
    for internal in ("listing_id", "offer_id", "monthly_rent_usd"):
        assert internal not in html


def test_sale_web_entrypoint_is_read_only_and_has_no_alignment_side_effect():
    import run_v3_sale_web
    assert callable(run_v3_sale_web.run)
    source = Path(run_v3_sale_web.__file__).read_text(encoding="utf-8")
    assert "SaleInventoryAligner" not in source
    assert "align_if_due" not in source
    assert "serve(" in source
    root = Path(__file__).resolve().parents[2]
    assert not (root / "v3_core" / "sale" / "align.py").exists()
    assert not (root / "run_v3_sale_align.py").exists()
