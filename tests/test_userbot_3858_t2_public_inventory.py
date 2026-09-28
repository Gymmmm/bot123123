from __future__ import annotations

import json
import sqlite3

from v3_core.storage.bootstrap import initialize_v3_storage

from qiaolian_dual.adapters.public_inventory import PublicInventoryAdapter
from qiaolian_dual import listing as dual_listing
from qiaolian_dual import search as dual_search


PUBLIC_ID = "QL-BK-A2B3"


def _seed(db, *, published=True, status="active", offer_status="active"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": PUBLIC_ID,
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "public_location_key": "BKK1",
            "public_location_display": "BKK1",
            "layout": "2房1厅",
            "size_sqm": 88,
            "floor": "19",
            "inventory_status": "active",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "deposit_terms": "押1付1",
            "contract_term": "1年",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        "canonical_facts": {
            "display_title": "富力城｜2房1厅｜公寓",
            "project_name": "富力城",
            "community_name": "富力城",
            "property_type": "公寓",
            "public_location_display": "BKK1",
            "layout": "2房1厅",
            "highlights": ["带泳池"],
        },
        "adviser_copy": "这套采光不错。",
    }
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,parser_revision,
                deal_type,facts_json,facts_hash,quality_score,quality_json,review_status)
               VALUES ('CAN_1','SRC_1','canonical_facts.v1','v1.2','rent','{}','hash-1',100,'{}','approved')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,
                property_type,public_location_key,public_location_display,
                layout,canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES ('LST_1',?,'CAN_1','后台后来改名','公寓','BKK1','BKK1',
                       '2房1厅','hash-1','canonical_facts.v1',?)""",
            (PUBLIC_ID, status),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_1','LST_1','rent','USD',850,?,'telegram_rent',1,'')""",
            (offer_status,),
        )
        if published:
            conn.execute(
                """INSERT INTO publication_packages_v3
                   (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                    cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                    frozen_file_hashes_json,source_identity_json,public_token,
                    canonical_facts_hash,content_hash)
                   VALUES ('PKG_1','LST_1','OFF_1','CAN_1',1,'published',
                           'classic_blue','/frozen/cover.jpg',
                           '["/frozen/01.jpg","/frozen/02.jpg"]','caption','{}',?,
                           '{}','{}','token-1','hash-1','content-1')""",
                (json.dumps(snapshot, ensure_ascii=False),),
            )
            conn.execute(
                """INSERT INTO publication_instances
                   (instance_id,package_id,listing_id,offer_id,platform,
                    channel_chat_id,channel_message_id,publish_status,post_text)
                   VALUES ('PUB_1','PKG_1','LST_1','OFF_1','telegram',
                           '-100123','777','published','caption')"""
            )
        conn.commit()


def _db(tmp_path, **kwargs):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed(db, **kwargs)
    return db


def test_adapter_maps_public_identity_and_3858_facts(tmp_path):
    db = _db(tmp_path)
    item = PublicInventoryAdapter(str(db)).get_listing(PUBLIC_ID)

    assert item is not None
    assert item["listing_id"] == PUBLIC_ID
    assert item["internal_listing_id"] == "LST_1"
    assert item["project"] == "富力城"
    assert item["area"] == "BKK1"
    assert item["layout"] == "2房1厅"
    assert item["price"] == 800
    assert item["deposit"] == "押1付1"
    assert item["contract_term"] == "1年"
    assert item["media_files"] == ["/frozen/01.jpg", "/frozen/02.jpg"]
    assert PublicInventoryAdapter(str(db)).resolve_internal_id(PUBLIC_ID) == "LST_1"


def test_3858_search_reads_modern_published_inventory(tmp_path, monkeypatch):
    db = _db(tmp_path)
    monkeypatch.setattr(dual_search, "DB_PATH", str(db))

    matches, mode = dual_search.search_listings_with_fallback(
        property_type="公寓",
        area="BKK1",
        budget_min=700,
        budget_max=900,
        limit=3,
    )

    assert mode == "strict"
    assert [item["listing_id"] for item in matches] == [PUBLIC_ID]
    assert matches[0]["internal_listing_id"] == "LST_1"


def test_unpublished_public_listing_never_falls_back_to_legacy(tmp_path, monkeypatch):
    db = _db(tmp_path, published=False)
    monkeypatch.setattr(dual_listing, "DB_PATH", str(db))

    assert dual_listing.listing_context(PUBLIC_ID) == {}
    assert dual_listing.listing_is_available(PUBLIC_ID) == (False, "missing")


def test_listing_context_uses_public_inventory_snapshot(tmp_path, monkeypatch):
    db = _db(tmp_path)
    monkeypatch.setattr(dual_listing, "DB_PATH", str(db))

    item = dual_listing.listing_context(PUBLIC_ID)

    assert item["listing_id"] == PUBLIC_ID
    assert item["project"] == "富力城"
    assert item["price"] == 800
    assert item["floor"] == "19"
    assert item["normalized_data"]["display_title"] == "富力城｜2房1厅｜公寓"
    assert dual_listing.listing_is_available(PUBLIC_ID) == (True, "active")


def test_public_similar_target_resolves_area_from_public_inventory(tmp_path, monkeypatch):
    db = _db(tmp_path)
    monkeypatch.setattr(dual_listing, "DB_PATH", str(db))

    assert dual_listing._resolve_area_from_target(PUBLIC_ID) == ("BKK1", PUBLIC_ID)


def test_rented_public_listing_keeps_detail_but_blocks_booking(tmp_path, monkeypatch):
    db = _db(tmp_path, status="rented", offer_status="inactive")
    monkeypatch.setattr(dual_listing, "DB_PATH", str(db))

    assert dual_listing.listing_is_available(PUBLIC_ID) == (False, "rented")
    assert dual_listing.listing_action_allowed(PUBLIC_ID, "detail") == (True, "rented")
    assert dual_listing.listing_action_allowed(PUBLIC_ID, "photos") == (True, "rented")
    assert dual_listing.listing_action_allowed(PUBLIC_ID, "consult") == (True, "rented")


def test_t2_read_paths_no_longer_require_legacy_listing_rows():
    callback_source = open("qiaolian_dual/callback_listing.py", encoding="utf-8").read()
    navigation_source = open("qiaolian_dual/callback_navigation.py", encoding="utf-8").read()
    search_callback_source = open("qiaolian_dual/callback_search.py", encoding="utf-8").read()
    start_source = open("qiaolian_dual/start_routes.py", encoding="utf-8").read()

    assert "if not item or not db.get_listing(lid)" not in callback_source
    assert "item = db.get_listing(lid) if lid else None" not in callback_source
    assert "db.list_recent_listings(10)" not in navigation_source
    assert "matches_found = db.search_listings" not in search_callback_source
    assert "matches_found = [item for item in db.search_listings" not in start_source
    assert "if action in {'appoint', 'consult', 'photos', 'details', 'book'" not in start_source
