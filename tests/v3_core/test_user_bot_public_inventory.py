from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.public_inventory import PublicInventoryReader


PUBLIC_ID = "QL-RF-A2B3"


def _seed_published_rent(db, *, listing_status="active", offer_status="active", gallery=True):
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
                property_type,canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES ('LST_1',?,'CAN_1','富力城','公寓','hash-1','canonical_facts.v1',?)""",
            (PUBLIC_ID, listing_status),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_1','LST_1','rent','USD',800,?,'telegram_rent',1,'')""",
            (offer_status,),
        )
        gallery_json = '["/frozen/01.jpg","/frozen/02.jpg"]' if gallery else '[]'
        conn.execute(
            """INSERT INTO publication_packages_v3
               (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                frozen_file_hashes_json,source_identity_json,public_token,
                canonical_facts_hash,content_hash)
               VALUES ('PKG_1','LST_1','OFF_1','CAN_1',1,'published',
                       'classic_blue','/frozen/cover.jpg',?,'caption',
                       '{"details":"u1","photos":"u2","book":"u3"}','{}','{}','{}',
                       'qltoken','hash-1','content-hash')""",
            (gallery_json,),
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,
                channel_chat_id,channel_message_id,publish_status,post_text)
               VALUES ('PUB_1','PKG_1','LST_1','OFF_1','telegram',
                       '-100123','777','published','caption')"""
        )
        conn.commit()


def test_public_inventory_requires_durable_published_instance(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,facts_json,
                facts_hash,quality_json)
               VALUES ('CAN_X','SRC_X','canonical_facts.v1','rent','{}','hash-x','{}')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,canonical_facts_hash,
                canonical_facts_schema,inventory_status)
               VALUES ('LST_X',?,'CAN_X','hash-x','canonical_facts.v1','active')""",
            (PUBLIC_ID,),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,monthly_rent_usd,offer_status,
                publication_policy,publishable)
               VALUES ('OFF_X','LST_X','rent',800,'active','telegram_rent',1)"""
        )
        conn.commit()

    assert PublicInventoryReader(db).resolve(PUBLIC_ID) is None


def test_published_rent_view_exposes_frozen_gallery_and_bookability(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db)

    view = PublicInventoryReader(db).resolve(PUBLIC_ID)

    assert view is not None
    assert view.listing_id == "LST_1"
    assert view.public_listing_id == PUBLIC_ID
    assert view.gallery == ("/frozen/01.jpg", "/frozen/02.jpg")
    assert view.action_allowed("details")
    assert view.action_allowed("photos")
    assert view.action_allowed("book")
    assert view.action_allowed("consult")


def test_rented_listing_keeps_details_photos_and_consult_but_blocks_book(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db, listing_status="rented", offer_status="inactive")

    view = PublicInventoryReader(db).resolve(PUBLIC_ID)

    assert view is not None
    assert view.action_allowed("details")
    assert view.action_allowed("photos")
    assert view.action_allowed("consult")
    assert not view.action_allowed("book")


def test_photos_requires_frozen_package_gallery(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_published_rent(db, gallery=False)

    view = PublicInventoryReader(db).resolve(PUBLIC_ID)

    assert view is not None
    assert view.action_allowed("details")
    assert not view.action_allowed("photos")
    assert view.action_allowed("book")
    assert view.action_allowed("consult")


def test_reader_is_strictly_read_only_and_does_not_create_missing_db(tmp_path):
    db = tmp_path / "missing.db"
    reader = PublicInventoryReader(db)

    with pytest.raises(FileNotFoundError):
        reader.resolve(PUBLIC_ID)

    assert not db.exists()


def test_non_public_id_is_rejected_before_database_access(tmp_path):
    missing = tmp_path / "missing.db"
    reader = PublicInventoryReader(missing)

    assert reader.resolve("QC0350") is None
    assert not missing.exists()
