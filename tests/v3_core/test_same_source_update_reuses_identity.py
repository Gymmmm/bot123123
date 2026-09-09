from __future__ import annotations

import sqlite3

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_repository import SourceRepository
from v3_core.parser.worker import CanonicalWorker
from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.storage.inventory_repository import InventoryRepository


def _images():
    return [
        {
            "local_path": f"/tmp/source-update-{index}.jpg",
            "file_hash": f"same-source-image-{index}",
            "telegram_file_id": str(100 + index),
            "telegram_file_unique_id": f"unique-{index}",
        }
        for index in range(4)
    ]


def _source(price: int) -> SourceIntake:
    return SourceIntake(
        source_type="telegram_channel",
        source_name="same-source-contract",
        source_post_id="777",
        source_url="https://t.me/c/1/777",
        raw_text=f"富力城 公寓 2房1厅 月租 ${price}/月 押1付1 1年",
        raw_images=_images(),
    )


def test_same_source_edit_updates_in_place_and_reuses_listing_offer_review(tmp_path):
    db = tmp_path / "same-source.sqlite3"
    initialize_v3_storage(db)
    sources = SourceRepository(db)
    intake = IntakeService(sources, min_listing_images=4)
    worker = CanonicalWorker(str(db))
    inventory = InventoryRepository(db)

    first = intake.persist(_source(800))
    assert first.status == "inserted"
    first_materialized = worker.process_one(int(first.source_post_pk))
    assert first_materialized.status == "materialized"

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        listing_before = conn.execute("SELECT * FROM listings_v3").fetchone()
        offer_before = conn.execute("SELECT * FROM listing_offers").fetchone()
        review_before = conn.execute("SELECT * FROM review_items").fetchone()
    assert listing_before is not None
    assert offer_before is not None
    assert review_before is not None
    listing_id = str(listing_before["listing_id"])
    public_id = str(listing_before["public_listing_id"])
    offer_id = str(offer_before["offer_id"])
    review_id = str(review_before["review_id"])
    first_canonical = str(listing_before["canonical_record_id"])

    inventory.approve_review(review_id=review_id, operator_user_id="admin")
    inventory.set_offer_publication(offer_id=offer_id, publishable=True)

    changed = intake.persist(_source(950))
    assert changed.status == "updated"
    assert changed.reason == "source_changed"
    assert changed.source_post_pk == first.source_post_pk
    assert sources.get_source_post(int(first.source_post_pk))["parse_status"] == "pending"

    exact_repeat = intake.persist(_source(950))
    assert exact_repeat.status == "duplicate"
    assert exact_repeat.source_post_pk == first.source_post_pk

    second_materialized = worker.process_one(int(changed.source_post_pk))
    assert second_materialized.status == "materialized"
    assert second_materialized.listing_id == listing_id

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        listings = conn.execute("SELECT * FROM listings_v3").fetchall()
        offers = conn.execute("SELECT * FROM listing_offers").fetchall()
        reviews = conn.execute("SELECT * FROM review_items").fetchall()
        sources_rows = conn.execute("SELECT * FROM source_posts").fetchall()
        canonicals = conn.execute(
            "SELECT * FROM canonical_records WHERE source_post_id=? ORDER BY created_at,id",
            (str(first.source_post_pk),),
        ).fetchall()

    assert len(sources_rows) == 1
    assert len(listings) == 1
    assert len(offers) == 1
    assert len(reviews) == 1
    assert len(canonicals) == 2

    listing_after = listings[0]
    offer_after = offers[0]
    review_after = reviews[0]
    assert listing_after["listing_id"] == listing_id
    assert listing_after["public_listing_id"] == public_id
    assert listing_after["canonical_record_id"] != first_canonical
    assert offer_after["offer_id"] == offer_id
    assert offer_after["monthly_rent_usd"] == 950
    assert offer_after["publishable"] == 0
    assert offer_after["publish_block_reason"] == "review_required"
    assert review_after["review_id"] == review_id
    assert review_after["canonical_record_id"] == listing_after["canonical_record_id"]
    assert review_after["review_status"] == "pending"
    assert review_after["approved_at"] is None
