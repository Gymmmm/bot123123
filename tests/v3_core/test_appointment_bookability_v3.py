from __future__ import annotations

import sqlite3

from v3_core.storage.appointment_repository import V3ListingBookabilityChecker
from v3_core.storage.bootstrap import initialize_v3_storage


def _seed_inventory(
    db,
    *,
    listing_status="active",
    offer_type="rent",
    offer_status="active",
    publication_policy="telegram_rent",
    package_status="published",
    publication_status="published",
    with_package=True,
    with_publication=True,
):
    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,deal_type,
                facts_json,facts_hash,quality_json)
               VALUES ('CAN_1','SRC_1','canonical_facts.v1',?,'{}','hash-1','{}')""",
            (offer_type,),
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,
                canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES ('LST_1','QL-RF-A2B3','CAN_1','hash-1','canonical_facts.v1',?)""",
            (listing_status,),
        )
        if offer_type == "rent":
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                    offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES ('OFF_1','LST_1','rent','USD',800,?,?,1,'')""",
                (offer_status, publication_policy),
            )
        else:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,sale_price_usd,
                    offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES ('OFF_1','LST_1','sale','USD',120000,?,?,0,'sale_not_enabled_for_telegram')""",
                (offer_status, publication_policy),
            )

        if with_package:
            conn.execute(
                """INSERT INTO publication_packages_v3
                   (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                    cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                    frozen_file_hashes_json,source_identity_json,public_token,
                    canonical_facts_hash,content_hash)
                   VALUES ('PKG_1','LST_1','OFF_1','CAN_1',1,?,
                           'classic_blue','/frozen/cover.jpg','[]','caption','{}','{}','{}','{}',
                           'qltoken','hash-1','content-hash')""",
                (package_status,),
            )
        if with_publication and with_package:
            conn.execute(
                """INSERT INTO publication_instances
                   (instance_id,package_id,listing_id,offer_id,platform,
                    channel_chat_id,channel_message_id,publish_status,post_text)
                   VALUES ('PUB_1','PKG_1','LST_1','OFF_1','telegram',
                           '-100123','777',?,'caption')""",
                (publication_status,),
            )
        conn.commit()


def _checker(tmp_path, **seed):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_inventory(db, **seed)
    return V3ListingBookabilityChecker(db)


def test_published_active_rent_is_bookable(tmp_path):
    checker = _checker(tmp_path)

    assert checker.is_bookable("LST_1")


def test_reserved_published_active_rent_remains_bookable(tmp_path):
    checker = _checker(tmp_path, listing_status="reserved")

    assert checker.is_bookable("LST_1")


def test_unpublished_inventory_is_never_bookable(tmp_path):
    assert not _checker(tmp_path, with_publication=False).is_bookable("LST_1")


def test_non_published_publication_state_is_not_bookable(tmp_path):
    checker = _checker(tmp_path, publication_status="prepared")

    assert not checker.is_bookable("LST_1")


def test_inactive_rent_offer_is_not_bookable(tmp_path):
    checker = _checker(tmp_path, offer_status="inactive")

    assert not checker.is_bookable("LST_1")


def test_store_only_policy_is_not_bookable_even_for_rent(tmp_path):
    checker = _checker(tmp_path, publication_policy="store_only")

    assert not checker.is_bookable("LST_1")


def test_sale_offer_is_never_bookable_in_rent_user_bot(tmp_path):
    checker = _checker(
        tmp_path,
        offer_type="sale",
        publication_policy="store_only",
    )

    assert not checker.is_bookable("LST_1")


def test_rented_offline_or_pending_listing_is_not_bookable(tmp_path):
    for status in ("rented", "offline", "pending"):
        path = tmp_path / status
        path.mkdir()
        checker = _checker(path, listing_status=status)
        assert not checker.is_bookable("LST_1")


def test_package_must_be_approved_or_published(tmp_path):
    checker = _checker(tmp_path, package_status="draft")

    assert not checker.is_bookable("LST_1")


def test_checker_is_read_only_and_missing_db_is_not_created(tmp_path):
    db = tmp_path / "missing.db"
    checker = V3ListingBookabilityChecker(db)

    try:
        checker.is_bookable("LST_1")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("missing database must fail instead of being created")

    assert not db.exists()
