import pytest

from v3_core.publishing.delivery_coordinator import PublicationDeliveryCoordinator
from v3_core.publishing.delivery_state import (
    DeliveryBlocked,
    PublicationDeliveryStateRepository,
)
from v3_core.publishing.package_service import (
    PackageApprovalService,
    PackageBuildService,
)
from v3_core.publishing.package_store import FrozenPackageStore
from v3_core.publishing.publication_instances import PublicationInstanceRepository
from v3_core.storage.inventory_reader import InventoryReader
from v3_core.storage.inventory_repository import InventoryRepository


def _facts():
    return {
        "schema_version": "canonical_facts.v1",
        "parser_revision": "v1.2",
        "canonical_facts_hash": "coordinator-hash",
        "deal_type": "rent",
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
        "monthly_rent_usd": 800,
        "sale_price_usd": None,
        "deposit_payment_terms": "押1付1",
        "contract_term_display": "1年",
        "available_date": "",
        "quality": {"score": 100, "all_flags": [], "blocking_flags": []},
    }


def _setup(tmp_path):
    db = tmp_path / "delivery.sqlite3"
    inventory = InventoryRepository(str(db))
    reader = InventoryReader(str(db))
    packages = FrozenPackageStore(str(db))
    deliveries = PublicationDeliveryStateRepository(str(db))
    publications = PublicationInstanceRepository(str(db))

    facts = _facts()
    canonical = inventory.store_canonical(source_post_id="source-1", facts=facts)
    inventory.upsert_listing(
        listing_id="l_1",
        public_listing_id="QL-RF-A2B3",
        canonical_record_id=str(canonical["canonical_record_id"]),
        facts=facts,
    )
    rent = inventory.sync_offers(listing_id="l_1", facts=facts)[0]
    review = inventory.create_review_item(
        canonical_record_id=str(canonical["canonical_record_id"]),
        listing_id="l_1",
        offer_id=str(rent["offer_id"]),
    )
    inventory.approve_review(
        review_id=str(review["review_id"]), operator_user_id="admin"
    )

    cover = tmp_path / "cover.jpg"
    cover.write_bytes(b"cover")
    gallery = []
    for i in range(4):
        path = tmp_path / f"photo-{i}.jpg"
        path.write_bytes(f"photo-{i}".encode())
        gallery.append(str(path))

    builder = PackageBuildService(
        reader=reader,
        store=packages,
        user_bot_username="QiaolianBot",
    )
    package = builder.build(
        listing_id="l_1",
        offer_id=str(rent["offer_id"]),
        cover_style="classic_blue",
        cover_path=str(cover),
        gallery=gallery,
    )
    approver = PackageApprovalService(
        reader=reader,
        inventory_repository=inventory,
        store=packages,
    )
    package = approver.approve(package_id=package.package_id, approved_by="admin")

    coordinator = PublicationDeliveryCoordinator(
        reader=reader,
        packages=packages,
        deliveries=deliveries,
        publications=publications,
    )
    return package, coordinator, packages, deliveries, publications


def _receipt(message_id=777):
    return {
        "media_message_ids": [message_id],
        "caption_message_id": message_id,
        "button_message_id": "",
        "caption": "frozen caption",
    }


def test_prepare_send_returns_only_frozen_single_cover_command(tmp_path):
    package, coordinator, _, _, _ = _setup(tmp_path)

    command = coordinator.prepare_send(
        package_id=package.package_id,
        channel_chat_id="-100123",
    )

    assert command.cover_path == package.cover_path
    assert command.caption == package.post_text
    assert command.actions == package.actions
    assert tuple(command.actions) == ("details", "photos", "book")


def test_receipt_commits_delivery_package_and_exact_publication_identity(tmp_path):
    package, coordinator, packages, deliveries, publications = _setup(tmp_path)
    command = coordinator.prepare_send(
        package_id=package.package_id,
        channel_chat_id="-100123",
    )
    coordinator.mark_sending(command.attempt_id)

    committed = coordinator.record_sent(
        attempt_id=command.attempt_id,
        telegram_result=_receipt(777),
    )

    assert committed.attempt.state == "committed"
    assert committed.publication.channel_message_id == "777"
    assert committed.publication.package_id == package.package_id
    assert packages.get(package.package_id).status == "published"
    assert deliveries.get(command.attempt_id).state == "committed"
    stored = publications.get_for_package(
        package.package_id,
        platform="telegram",
        channel_chat_id="-100123",
    )
    assert stored.channel_message_id == "777"

    with pytest.raises(DeliveryBlocked, match="already published"):
        coordinator.prepare_send(
            package_id=package.package_id,
            channel_chat_id="-100123",
        )


def test_saved_sent_receipt_can_finalize_without_resending(tmp_path):
    package, coordinator, packages, deliveries, publications = _setup(tmp_path)
    command = coordinator.prepare_send(
        package_id=package.package_id,
        channel_chat_id="-100123",
    )
    coordinator.mark_sending(command.attempt_id)
    deliveries.mark_sent(command.attempt_id, _receipt(888))

    with pytest.raises(DeliveryBlocked, match="finalize without resending"):
        coordinator.prepare_send(
            package_id=package.package_id,
            channel_chat_id="-100123",
        )

    result = coordinator.finalize_saved_receipt(attempt_id=command.attempt_id)
    assert result.attempt.state == "committed"
    assert result.publication.channel_message_id == "888"
    assert packages.get(package.package_id).status == "published"
    assert publications.get(result.publication.instance_id).channel_message_id == "888"
