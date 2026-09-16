from __future__ import annotations

from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService


PUBLIC_ID = "QL-RF-A2B3"


def _view(*, listing_status="active", offer_status="active", gallery=True):
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": PUBLIC_ID,
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
            "publish_status": "published",
        },
        package={
            "package_id": "PKG_1",
            "gallery_json": '["/frozen/01.jpg"]' if gallery else "[]",
        },
    )


class FakeInventory:
    def __init__(self, view=None):
        self.view = view
        self.calls = []

    def resolve(self, public_listing_id):
        self.calls.append(public_listing_id)
        return self.view


def test_invalid_public_payload_is_rejected_before_inventory_lookup():
    inventory = FakeInventory(_view())
    service = PublicRouteService(inventory)

    decision = service.resolve("property_QL-RF-A2B3_consult")

    assert decision.status == "invalid_link"
    assert decision.reason == "unsupported_public_payload"
    assert inventory.calls == []


def test_unpublished_or_unknown_listing_is_not_found():
    service = PublicRouteService(FakeInventory(None))

    decision = service.resolve(f"property_{PUBLIC_ID}_details")

    assert decision.status == "not_found"
    assert decision.route is not None
    assert decision.route.public_listing_id == PUBLIC_ID
    assert decision.reason == "listing_not_publicly_published"


def test_active_published_listing_allows_all_three_public_actions():
    service = PublicRouteService(FakeInventory(_view()))

    for action in ("details", "photos", "book"):
        decision = service.resolve(f"property_{PUBLIC_ID}_{action}")
        assert decision.ok
        assert decision.view is not None
        assert decision.route is not None
        assert decision.route.action == action


def test_rented_listing_blocks_book_but_keeps_details_and_photos():
    service = PublicRouteService(
        FakeInventory(_view(listing_status="rented", offer_status="inactive"))
    )

    assert service.resolve(f"property_{PUBLIC_ID}_details").ok
    assert service.resolve(f"property_{PUBLIC_ID}_photos").ok
    book = service.resolve(f"property_{PUBLIC_ID}_book")
    assert book.status == "blocked"
    assert book.reason == "listing_not_bookable"


def test_missing_frozen_gallery_blocks_photos_without_blocking_details_or_book():
    service = PublicRouteService(FakeInventory(_view(gallery=False)))

    photos = service.resolve(f"property_{PUBLIC_ID}_photos")

    assert photos.status == "blocked"
    assert photos.reason == "frozen_gallery_unavailable"
    assert service.resolve(f"property_{PUBLIC_ID}_details").ok
    assert service.resolve(f"property_{PUBLIC_ID}_book").ok
