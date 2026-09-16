from __future__ import annotations

from v3_core.user_bot.consult import ConsultService
from v3_core.user_bot.public_inventory import PublishedListingView


class MemoryInventory:
    def __init__(self, view=None):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(str(public_listing_id))
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def _view(*, listing_status="active", offer_status="active"):
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": listing_status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={
            "instance_id": "PUB_1",
            "platform": "telegram",
            "publish_status": "published",
        },
        package={"snapshot_json": "{}", "gallery_json": "[]"},
    )


def test_active_published_listing_builds_consult_intent_without_side_effects():
    inventory = MemoryInventory(_view())
    result = ConsultService(inventory).resolve(
        "QL-RF-A2B3",
        source="listing_callback",
    )

    assert result.ok
    assert result.intent is not None
    assert result.intent.listing_id == "LST_1"
    assert result.intent.public_listing_id == "QL-RF-A2B3"
    assert result.intent.source == "listing_callback"
    assert result.intent.inventory_status == "active"
    assert result.intent.offer_status == "active"
    assert result.intent.publication_instance_id == "PUB_1"
    assert inventory.calls == ["QL-RF-A2B3"]


def test_rented_pending_and_offline_published_listings_remain_consultable():
    for listing_status, offer_status in (
        ("rented", "inactive"),
        ("pending", "active"),
        ("offline", "inactive"),
    ):
        result = ConsultService(
            MemoryInventory(
                _view(
                    listing_status=listing_status,
                    offer_status=offer_status,
                )
            )
        ).resolve("QL-RF-A2B3")

        assert result.ok
        assert result.intent is not None
        assert result.intent.inventory_status == listing_status
        assert result.intent.offer_status == offer_status


def test_unpublished_public_id_returns_not_found_and_no_intent():
    inventory = MemoryInventory(None)
    result = ConsultService(inventory).resolve("QL-RF-A2B3")

    assert result.status == "not_found"
    assert result.reason == "listing_not_publicly_published"
    assert result.intent is None
    assert inventory.calls == ["QL-RF-A2B3"]


def test_invalid_non_public_id_is_rejected_before_inventory_access():
    inventory = MemoryInventory(_view())
    result = ConsultService(inventory).resolve("QC0350")

    assert result.status == "invalid_link"
    assert result.reason == "invalid_public_listing_id"
    assert result.intent is None
    assert inventory.calls == []
