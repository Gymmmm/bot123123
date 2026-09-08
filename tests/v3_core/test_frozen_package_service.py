import json
import sqlite3

import pytest

from v3_core.publishing.package_service import (
    PackageApprovalService,
    PackageBuildService,
)
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _facts(*, rent=800, sale=None, deal_type="rent"):
    return {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": f"hash-{rent}-{sale}-{deal_type}",
        "deal_type": deal_type,
        "project_name": "富力城",
        "project_alias": "",
        "project_brand": "",
        "project_key": "rf_city",
        "property_type": "公寓",
        "property_subtype": "",
        "public_location_key": "rf_city",
        "public_location_display": "富力城",
        "publication_location_level": "level_1_project_confirmed",
        "canonical_area_key": "",
        "canonical_area_display": "",
        "layout": "2房1厅",
        "bedrooms": 2,
        "bathrooms": 1,
        "size_sqm": 95,
        "floor": "19",
        "display_title": "富力城 2房1厅",
        "monthly_rent_usd": rent,
        "sale_price_usd": sale,
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "available_date": "",
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }


def _files(tmp_path):
    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover-v1")
    gallery = []
    for i in range(4):
        path = tmp_path / f"gallery-{i}.jpg"
        path.write_bytes(f"gallery-{i}".encode())
        gallery.append(str(path))
    return str(cover), gallery


def _inventory(tmp_path, facts, *, listing_id="l_1", public_id="QL-RF-A2B3"):
    db = tmp_path / "v3.sqlite3"
    inventory = InventoryRepository(str(db))
    canonical = inventory.store_canonical(source_post_id="source-1", facts=facts)
    inventory.upsert_listing(
        listing_id=listing_id,
        public_listing_id=public_id,
        canonical_record_id=str(canonical["canonical_record_id"]),
        facts=facts,
    )
    offers = inventory.sync_offers(listing_id=listing_id, facts=facts)
    reader = InventoryReader(str(db))
    store = FrozenPackageStore(str(db))
    builder = PackageBuildService(
        reader=reader,
        store=store,
        user_bot_username="QiaolianBot",
    )
    approver = PackageApprovalService(
        reader=reader,
        inventory_repository=inventory,
        store=store,
    )
    return db, inventory, reader, store, builder, approver, canonical, offers


def test_rent_package_freezes_caption_actions_and_file_hashes(tmp_path):
    facts = _facts()
    _, inventory, _, store, builder, _, canonical, offers = _inventory(tmp_path, facts)
    rent = offers[0]
    cover, gallery = _files(tmp_path)

    package = builder.build(
        listing_id="l_1",
        offer_id=str(rent["offer_id"]),
        cover_style="classic_blue",
        cover_path=cover,
        gallery=gallery,
        source_identity={"source_post_id": "source-1"},
    )

    assert package.status == "package_ready"
    assert package.canonical_record_id == canonical["canonical_record_id"]
    assert package.gallery == tuple(gallery)
    assert tuple(package.actions) == ("details", "photos", "book")
    assert "QL-RF-A2B3" in package.post_text
    assert len(package.post_text) <= 1024
    assert len(package.frozen_file_hashes) == 5
    assert package.snapshot["canonical_facts"] == facts
    assert store.verify_frozen(package.package_id).content_hash == package.content_hash


def test_frozen_canonical_facts_do_not_drift_with_later_canonical_row_changes(tmp_path):
    facts = _facts()
    db, _, _, store, builder, _, _, offers = _inventory(tmp_path, facts)
    cover, gallery = _files(tmp_path)
    package = builder.build(
        listing_id="l_1",
        offer_id=str(offers[0]["offer_id"]),
        cover_style="classic_blue",
        cover_path=cover,
        gallery=gallery,
    )

    changed = dict(facts)
    changed["project_name"] = "后台后来改名"
    changed["monthly_rent_usd"] = 9999
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE canonical_records SET facts_json=? WHERE canonical_record_id=?",
            (
                json.dumps(changed, ensure_ascii=False, sort_keys=True),
                package.canonical_record_id,
            ),
        )
        conn.commit()

    frozen = store.get(package.package_id)
    assert frozen is not None
    assert frozen.snapshot["canonical_facts"]["project_name"] == "富力城"
    assert frozen.snapshot["canonical_facts"]["monthly_rent_usd"] == 800


def test_package_cannot_approve_until_offer_review_is_approved(tmp_path):
    facts = _facts()
    _, inventory, _, _, builder, approver, canonical, offers = _inventory(tmp_path, facts)
    rent = offers[0]
    cover, gallery = _files(tmp_path)
    package = builder.build(
        listing_id="l_1",
        offer_id=str(rent["offer_id"]),
        cover_style="classic_blue",
        cover_path=cover,
        gallery=gallery,
    )

    with pytest.raises(ValueError, match="review_required"):
        approver.approve(package_id=package.package_id, approved_by="admin")

    review = inventory.create_review_item(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id="l_1",
        offer_id=str(rent["offer_id"]),
    )
    inventory.approve_review(review_id=str(review["review_id"]), operator_user_id="admin")

    approved = approver.approve(package_id=package.package_id, approved_by="admin")
    assert approved.status == "approved"
    offer = approver.reader.offer(str(rent["offer_id"]))
    assert offer["publishable"] == 1
    assert offer["publish_block_reason"] == ""


def test_frozen_file_mutation_invalidates_package(tmp_path):
    facts = _facts()
    _, _, _, store, builder, _, _, offers = _inventory(tmp_path, facts)
    cover, gallery = _files(tmp_path)
    package = builder.build(
        listing_id="l_1",
        offer_id=str(offers[0]["offer_id"]),
        cover_style="classic_blue",
        cover_path=cover,
        gallery=gallery,
    )

    with open(cover, "wb") as handle:
        handle.write(b"mutated-cover")

    with pytest.raises(ValueError, match="frozen_media_hash_mismatch"):
        store.verify_frozen(package.package_id)


def test_sale_offer_never_enters_publication_packages(tmp_path):
    facts = _facts(rent=None, sale=160000, deal_type="sale")
    _, _, _, store, builder, _, _, offers = _inventory(tmp_path, facts)
    sale = offers[0]
    cover, gallery = _files(tmp_path)

    with pytest.raises(ValueError, match="requires_rent_offer"):
        builder.build(
            listing_id="l_1",
            offer_id=str(sale["offer_id"]),
            cover_style="classic_blue",
            cover_path=cover,
            gallery=gallery,
        )

    with store._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM publication_packages_v3").fetchone()[0]
    assert count == 0
