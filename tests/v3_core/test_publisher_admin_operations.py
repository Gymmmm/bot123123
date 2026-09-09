import sqlite3
from pathlib import Path

import pytest

from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.service import InventoryMaterializationService
from v3_core.publishing.admin_operations import PublisherAdminOperations
from v3_core.publishing.delivery_state import PublicationDeliveryStateRepository
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _facts(*, rent=800, sale=None):
    return {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "test",
        "canonical_facts_hash": "seed-hash",
        "project_name": "富力城",
        "project_alias": "",
        "property_type": "公寓",
        "public_location_key": "rf_city",
        "public_location_display": "富力城",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "",
        "canonical_area_display": "",
        "layout": "2房1厅1卫",
        "bedrooms": 2,
        "bathrooms": 1,
        "size_sqm": 95,
        "floor": "19",
        "display_title": "富力城｜2房1厅1卫｜公寓",
        "monthly_rent_usd": rent,
        "sale_price_usd": sale,
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "available_date": "",
        "highlights": ["采光好"],
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }


def _setup(tmp_path: Path, *, rent=800, sale=None):
    db = tmp_path / "admin.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(str(db))
    inventory = InventoryRepository(str(db))
    reader = InventoryReader(str(db))
    FrozenPackageStore(str(db))
    PublicationDeliveryStateRepository(str(db))
    source_post_id = sources.save_source_post(
        source_id=None,
        source_type="telegram_channel",
        source_name="collector",
        source_post_id="source-1",
        source_url="https://t.me/c/1/1",
        source_author="channel",
        raw_text="test",
        raw_images=[],
        raw_videos=[],
        raw_contact="",
        raw_meta={},
        dedupe_hash="source-hash",
    )
    canonical = inventory.store_canonical(
        source_post_id=source_post_id,
        facts=_facts(rent=rent, sale=sale),
    )
    materialized = InventoryMaterializationService(inventory).materialize(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id="l_1",
        public_listing_id="QL-RF-A2B3",
        create_review=True,
    )
    ops = PublisherAdminOperations(db_path=str(db), reader=reader, inventory=inventory)
    return db, inventory, reader, ops, materialized


def _review_types(reader, materialized):
    return {
        str(reader.offer(str(reader.review(review_id)["offer_id"]))["offer_type"]): review_id
        for review_id in materialized.review_ids
    }


def test_admin_edit_records_override_rematerializes_and_returns_review_to_pending(tmp_path):
    db, inventory, reader, ops, materialized = _setup(tmp_path)
    review_id = materialized.review_ids[0]
    offer_id = materialized.offer_ids[0]
    inventory.approve_review(review_id=review_id, operator_user_id="admin")
    old_hash = str(reader.canonical(str(reader.review(review_id)["canonical_record_id"]))["facts_hash"])

    updated = ops.edit_review_field(
        review_id=review_id,
        field_name="monthly_rent_usd",
        raw_value="$950 / month",
        operator_user_id="admin",
    )

    assert updated["review_status"] == "pending"
    canonical = reader.canonical(str(updated["canonical_record_id"]))
    assert canonical["facts"]["monthly_rent_usd"] == 950
    assert canonical["facts_hash"] != old_hash
    assert reader.offer(offer_id)["monthly_rent_usd"] == 950
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT field_name,new_value_json,operator_id FROM canonical_overrides"
        ).fetchone()
    assert row[0] == "monthly_rent_usd"
    assert row[1] == "950"
    assert row[2] == "admin"


def test_hold_reject_and_reopen_are_explicit_non_publishable_states(tmp_path):
    _, inventory, reader, ops, materialized = _setup(tmp_path)
    review_id = materialized.review_ids[0]
    offer_id = materialized.offer_ids[0]
    inventory.approve_review(review_id=review_id, operator_user_id="admin")

    held = ops.set_review_status(
        review_id=review_id,
        status="hold",
        operator_user_id="admin",
        note="later",
    )
    assert held["review_status"] == "hold"
    offer = reader.offer(offer_id)
    assert int(offer["publishable"]) == 0
    assert offer["publish_block_reason"] == "review_hold"

    rejected = ops.set_review_status(
        review_id=review_id,
        status="rejected",
        operator_user_id="admin",
    )
    assert rejected["review_status"] == "rejected"
    assert reader.offer(offer_id)["publish_block_reason"] == "review_rejected"

    reopened = ops.set_review_status(
        review_id=review_id,
        status="pending",
        operator_user_id="admin",
    )
    assert reopened["review_status"] == "pending"
    assert reader.offer(offer_id)["publish_block_reason"] == "review_required"


def test_shared_canonical_edit_resets_both_rent_and_sale_reviews_without_cross_binding(tmp_path):
    _, inventory, reader, ops, materialized = _setup(tmp_path, rent=800, sale=120000)
    assert len(materialized.offer_ids) == 2
    assert len(materialized.review_ids) == 2
    types_before = _review_types(reader, materialized)
    for review_id in materialized.review_ids:
        inventory.approve_review(review_id=review_id, operator_user_id="admin")

    sale_review_id = types_before["sale"]
    ops.edit_review_field(
        review_id=sale_review_id,
        field_name="project_name",
        raw_value="富力城二期",
        operator_user_id="admin",
    )

    assert len(reader.reviews_by_status("pending")) == 2
    for offer_type, review_id in types_before.items():
        review = reader.review(review_id)
        assert review["review_status"] == "pending"
        assert reader.offer(str(review["offer_id"]))["offer_type"] == offer_type
    canonical = reader.canonical(str(reader.review(sale_review_id)["canonical_record_id"]))
    assert canonical["facts"]["project_name"] == "富力城二期"


def test_adding_second_explicit_price_creates_second_review(tmp_path):
    _, _, reader, ops, materialized = _setup(tmp_path, rent=800, sale=None)
    review_id = materialized.review_ids[0]

    ops.edit_review_field(
        review_id=review_id,
        field_name="sale_price_usd",
        raw_value="120000",
        operator_user_id="admin",
    )

    pending = reader.reviews_by_status("pending")
    assert len(pending) == 2
    assert {reader.offer(str(row["offer_id"]))["offer_type"] for row in pending} == {"rent", "sale"}


def test_any_frozen_package_on_listing_blocks_shared_fact_edits(tmp_path):
    _, _, reader, ops, materialized = _setup(tmp_path, rent=800, sale=120000)
    types = _review_types(reader, materialized)
    rent_review = reader.review(types["rent"])
    rent_offer = reader.offer(str(rent_review["offer_id"]))
    canonical = reader.canonical(str(rent_review["canonical_record_id"]))
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = tmp_path / "gallery.jpg"
    gallery.write_bytes(b"gallery")
    store = FrozenPackageStore(str(tmp_path / "admin.sqlite3"))
    package = store.create(
        listing_id=str(rent_review["listing_id"]),
        offer_id=str(rent_offer["offer_id"]),
        canonical_record_id=str(rent_review["canonical_record_id"]),
        canonical_facts_hash=str(canonical["facts_hash"]),
        cover_style="classic_blue",
        cover_path=str(cover),
        gallery=[str(gallery)],
        post_text="test",
        actions={
            "details": "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_details",
            "photos": "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_photos",
            "book": "https://t.me/QiaolianBot?start=property_QL-RF-A2B3_book",
        },
        snapshot={},
        source_identity={},
        public_token="QL-RF-A2B3",
    )
    store.approve(package.package_id, approved_by="admin")

    with pytest.raises(ValueError, match="publisher_listing_has_frozen_package"):
        ops.edit_review_field(
            review_id=types["sale"],
            field_name="layout",
            raw_value="3房2厅2卫",
            operator_user_id="admin",
        )
