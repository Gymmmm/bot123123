from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.public_search import PublicSearchReader, PublicSearchService
from v3_core.user_bot.search_query import SearchCriteria, parse_search_criteria


def _seed_listing(
    db,
    *,
    suffix: str,
    public_id: str,
    property_type: str = "公寓",
    location_key: str = "BKK1",
    rent: int | None = 800,
    sale: int | None = None,
    listing_status: str = "active",
    offer_status: str = "active",
    published: bool = True,
    layout: str = "2房1厅",
):
    listing_id = f"LST_{suffix}"
    canonical_id = f"CAN_{suffix}"
    offer_id = f"OFF_{suffix}"
    package_id = f"PKG_{suffix}"
    instance_id = f"PUB_{suffix}"
    deal_type = "sale" if sale is not None and rent is None else "rent"
    offer_type = deal_type

    with sqlite3.connect(db) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,parser_revision,
                deal_type,facts_json,facts_hash,quality_score,quality_json,review_status)
               VALUES (?,?, 'canonical_facts.v1','v1.2',?,'{}',?,100,'{}','approved')""",
            (canonical_id, f"SRC_{suffix}", deal_type, f"hash-{suffix}"),
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,
                property_type,public_location_key,public_location_display,
                canonical_area_key,layout,canonical_facts_hash,
                canonical_facts_schema,inventory_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                listing_id,
                public_id,
                canonical_id,
                f"项目{suffix}",
                property_type,
                location_key,
                location_key,
                location_key if location_key.startswith("BKK") else "",
                layout,
                f"hash-{suffix}",
                "canonical_facts.v1",
                listing_status,
            ),
        )
        if offer_type == "rent":
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                    offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?,'rent','USD',?,?,'telegram_rent',1,'')""",
                (offer_id, listing_id, rent, offer_status),
            )
        else:
            conn.execute(
                """INSERT INTO listing_offers
                   (offer_id,listing_id,offer_type,currency,sale_price_usd,
                    offer_status,publication_policy,publishable,publish_block_reason)
                   VALUES (?,?,'sale','USD',?,?,'store_only',0,'sale_not_enabled_for_telegram')""",
                (offer_id, listing_id, sale, offer_status),
            )

        if published:
            conn.execute(
                """INSERT INTO publication_packages_v3
                   (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                    cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                    frozen_file_hashes_json,source_identity_json,public_token,
                    canonical_facts_hash,content_hash)
                   VALUES (?,?,?,?,1,'published','classic_blue','/cover.jpg','[]','caption',
                           '{}','{}','{}','{}',?,?,?)""",
                (
                    package_id,
                    listing_id,
                    offer_id,
                    canonical_id,
                    f"token-{suffix}",
                    f"hash-{suffix}",
                    f"content-{suffix}",
                ),
            )
            conn.execute(
                """INSERT INTO publication_instances
                   (instance_id,package_id,listing_id,offer_id,platform,
                    channel_chat_id,channel_message_id,publish_status,post_text)
                   VALUES (?,?,?,?, 'telegram','-100123',?,'published','caption')""",
                (instance_id, package_id, listing_id, offer_id, str(1000 + ord(suffix))),
            )
        conn.commit()


def _db(tmp_path):
    db = tmp_path / "v3.db"
    initialize_v3_storage(db)
    _seed_listing(db, suffix="A", public_id="QL-BK-A2B3", rent=800, layout="2房1厅")
    _seed_listing(
        db,
        suffix="B",
        public_id="QL-BK-C4D5",
        property_type="别墅",
        location_key="BKK1",
        rent=1200,
    )
    _seed_listing(
        db,
        suffix="C",
        public_id="QL-BK-E6F7",
        property_type="公寓",
        location_key="BKK2",
        rent=750,
        listing_status="reserved",
    )
    _seed_listing(
        db,
        suffix="D",
        public_id="QL-BK-G8H9",
        property_type="公寓",
        location_key="BKK1",
        rent=700,
        listing_status="rented",
        offer_status="inactive",
    )
    _seed_listing(
        db,
        suffix="E",
        public_id="QL-BK-J2K3",
        property_type="公寓",
        location_key="BKK1",
        rent=650,
        published=False,
    )
    _seed_listing(
        db,
        suffix="F",
        public_id="QL-BK-L4M5",
        property_type="公寓",
        location_key="BKK1",
        rent=None,
        sale=160000,
    )
    return db


def test_search_returns_only_current_durably_published_rent_inventory(tmp_path):
    db = _db(tmp_path)
    reader = PublicSearchReader(db)

    items = reader.search(location_keys=("BKK1",), budget_max=900, limit=10)

    assert [item.public_listing_id for item in items] == ["QL-BK-A2B3"]
    assert all(item.bookable for item in items)


def test_strict_search_uses_type_location_and_budget_but_not_room_hint(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = parse_search_criteria("BKK1 公寓 800以内 一房")

    result = service.strict(criteria, limit=5)

    assert result.mode == "strict"
    assert criteria.room_type == "1房"
    # Fixed-SHA parity: room hint is recorded but not yet a DB filter. The
    # published BKK1 apartment is a 2-room listing and still matches.
    assert [item.public_listing_id for item in result.items] == ["QL-BK-A2B3"]


def test_strict_search_never_auto_relaxes_conditions(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = SearchCriteria(
        property_type="别墅",
        location_keys=("BKK2",),
        budget_min=700,
        budget_max=900,
    )

    result = service.strict(criteria)

    assert result.mode == "no_match"
    assert result.items == ()


def test_similar_search_relaxes_type_before_area(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = SearchCriteria(
        property_type="别墅",
        location_keys=("BKK2",),
        budget_min=700,
        budget_max=900,
    )

    result = service.similar(criteria)

    assert result.mode == "no_type"
    assert [item.public_listing_id for item in result.items] == ["QL-BK-E6F7"]


def test_similar_search_relaxes_area_second(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = SearchCriteria(
        property_type="别墅",
        location_keys=("BKK2",),
        budget_min=1100,
        budget_max=1300,
    )

    result = service.similar(criteria)

    assert result.mode == "no_area"
    assert [item.public_listing_id for item in result.items] == ["QL-BK-C4D5"]


def test_similar_search_falls_back_to_budget_only_last(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = SearchCriteria(
        property_type="办公室",
        location_keys=("钻石岛",),
        budget_min=700,
        budget_max=900,
    )

    result = service.similar(criteria, limit=5)

    assert result.mode == "budget_only"
    assert {item.public_listing_id for item in result.items} == {
        "QL-BK-A2B3",
        "QL-BK-E6F7",
    }


def test_strict_single_result_augments_only_with_real_published_similar(tmp_path):
    db = _db(tmp_path)
    _seed_listing(
        db,
        suffix="G",
        public_id="QL-BK-N6P7",
        property_type="别墅",
        location_key="BKK1",
        rent=780,
    )
    service = PublicSearchService(PublicSearchReader(db))
    criteria = parse_search_criteria("BKK1 公寓 800以内")

    result = service.strict_for_results(criteria, limit=5)

    assert result.mode == "strict"
    assert result.has_similar
    assert [item.public_listing_id for item in result.items] == [
        "QL-BK-A2B3",
        "QL-BK-N6P7",
    ]


def test_strict_no_match_is_never_converted_into_automatic_similar(tmp_path):
    db = _db(tmp_path)
    service = PublicSearchService(PublicSearchReader(db))
    criteria = SearchCriteria(
        property_type="办公室",
        location_keys=("钻石岛",),
        budget_min=700,
        budget_max=900,
    )

    result = service.strict_for_results(criteria, limit=5)

    assert result.mode == "no_match"
    assert result.items == ()
    assert not result.has_similar


def test_search_reader_is_read_only_and_does_not_create_missing_db(tmp_path):
    missing = tmp_path / "missing.db"
    reader = PublicSearchReader(missing)

    with pytest.raises(FileNotFoundError):
        reader.search()

    assert not missing.exists()
