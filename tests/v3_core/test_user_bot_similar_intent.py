from __future__ import annotations

import json

from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.similar_intent import SimilarIntentService


class MemoryInventory:
    def __init__(self, view=None):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(str(public_listing_id))
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def _view(*, frozen_listing=None, live_location="BKK2", status="rented"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing": dict(
            frozen_listing
            or {
                "public_location_key": "BKK1",
                "public_location_display": "BKK1",
                "canonical_area_key": "BKK1",
                "canonical_area_display": "BKK1",
                "property_type": "公寓",
            }
        ),
        "offer": {"offer_type": "rent"},
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": status,
            "public_location_key": live_location,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": "inactive" if status == "rented" else "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1", "publish_status": "published"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
        },
    )


def test_similar_intent_uses_frozen_area_and_restarts_at_budget_step():
    inventory = MemoryInventory(_view())
    result = SimilarIntentService(inventory).resolve("QL-RF-A2B3")

    assert result.ok
    assert result.intent is not None
    assert result.intent.listing_id == "LST_1"
    assert result.intent.public_listing_id == "QL-RF-A2B3"
    assert result.intent.source == "similar_listing"
    assert result.intent.goal == "any"
    assert result.intent.location_keys == ("BKK1",)
    assert result.intent.area_display == "BKK1"
    assert result.intent.next_step == "budget"
    assert not hasattr(result.intent, "property_type")
    assert inventory.calls == ["QL-RF-A2B3"]


def test_live_location_change_does_not_rewrite_published_similar_area():
    result = SimilarIntentService(
        MemoryInventory(_view(live_location="钻石岛"))
    ).resolve("QL-RF-A2B3")

    assert result.ok
    assert result.intent is not None
    assert result.intent.location_keys == ("BKK1",)
    assert result.intent.area_display == "BKK1"


def test_published_rented_or_offline_listing_can_still_start_similar_flow():
    for status in ("rented", "offline", "pending"):
        result = SimilarIntentService(
            MemoryInventory(_view(status=status))
        ).resolve("QL-RF-A2B3")
        assert result.ok
        assert result.intent is not None
        assert result.intent.next_step == "budget"


def test_missing_frozen_area_still_produces_unscoped_budget_intent():
    result = SimilarIntentService(
        MemoryInventory(_view(frozen_listing={"property_type": "公寓"}))
    ).resolve("QL-RF-A2B3")

    assert result.ok
    assert result.intent is not None
    assert result.intent.location_keys == ()
    assert result.intent.area_display == ""
    assert result.intent.goal == "any"


def test_unpublished_listing_returns_not_found_without_intent():
    inventory = MemoryInventory(None)
    result = SimilarIntentService(inventory).resolve("QL-RF-A2B3")

    assert result.status == "not_found"
    assert result.reason == "listing_not_publicly_published"
    assert result.intent is None
    assert inventory.calls == ["QL-RF-A2B3"]


def test_invalid_id_is_rejected_before_inventory_access():
    inventory = MemoryInventory(_view())
    result = SimilarIntentService(inventory).resolve("QC0350")

    assert result.status == "invalid_link"
    assert result.reason == "invalid_public_listing_id"
    assert result.intent is None
    assert inventory.calls == []
