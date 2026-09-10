from __future__ import annotations

import json

from v3_core.user_bot.public_flow import PublicListingFlowService
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.route_service import PublicRouteService


class MemoryPublishedInventory:
    def __init__(self, view):
        self.view = view

    def resolve(self, public_listing_id):
        if self.view and self.view.public_listing_id == str(public_listing_id):
            return self.view
        return None


def _view(*, status="active", offer_status="active", gallery=()):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_record_id": "CAN_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": {"highlights": ["采光好"]},
        "listing": {
            "project_name": "富力城",
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "富力城",
            "size_sqm": 95,
            "floor": "19",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "payment_terms": "押1付1",
            "contract_term": "1年",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": json.dumps(list(gallery), ensure_ascii=False),
        },
    )


def _service(view):
    return PublicListingFlowService(
        PublicRouteService(MemoryPublishedInventory(view))
    )


def test_details_payload_resolves_all_the_way_to_frozen_public_response():
    result = _service(_view()).resolve("property_QL-RF-A2B3_details")

    assert result.ok
    assert result.action == "details"
    assert result.public_listing_id == "QL-RF-A2B3"
    assert result.details is not None
    assert "🏠 <b>租赁详情</b>" in result.details.text
    assert "富力城｜2房1厅" in result.details.text
    assert result.photos is None
    assert result.book is None


def test_photos_payload_returns_frozen_existing_gallery_response(tmp_path):
    photo = tmp_path / "room.jpg"
    photo.write_bytes(b"room")
    result = _service(_view(gallery=[str(photo)])).resolve(
        "property_QL-RF-A2B3_photos"
    )

    assert result.ok
    assert result.action == "photos"
    assert result.photos is not None
    assert result.photos.media_groups == ((str(photo),),)
    assert result.details is None
    assert result.book is None


def test_book_payload_returns_intent_not_a_telegram_side_effect():
    payload = "property_QL-RF-A2B3_book"
    result = _service(_view()).resolve(payload)

    assert result.ok
    assert result.action == "book"
    assert result.book is not None
    assert result.book.listing_id == "LST_1"
    assert result.book.public_listing_id == "QL-RF-A2B3"
    assert result.book.source == "channel_deeplink"
    assert result.book.start_payload == payload
    assert result.details is None
    assert result.photos is None


def test_direct_details_uses_the_same_published_gate_and_frozen_response():
    service = _service(_view())

    deep_link = service.resolve("property_QL-RF-A2B3_details")
    callback = service.resolve_action("QL-RF-A2B3", "details")

    assert deep_link.ok and callback.ok
    assert callback.action == "details"
    assert callback.public_listing_id == "QL-RF-A2B3"
    assert callback.details is not None
    assert deep_link.details is not None
    assert callback.details.text == deep_link.details.text


def test_direct_book_preserves_listing_callback_source_in_intent():
    result = _service(_view()).resolve_action(
        "QL-RF-A2B3",
        "book",
        source="listing_callback",
    )

    assert result.ok
    assert result.book is not None
    assert result.book.listing_id == "LST_1"
    assert result.book.public_listing_id == "QL-RF-A2B3"
    assert result.book.source == "listing_callback"
    assert result.book.start_payload == ""


def test_rented_book_is_blocked_before_appointment_flow_is_created():
    service = _service(_view(status="rented", offer_status="inactive"))

    deep_link = service.resolve("property_QL-RF-A2B3_book")
    callback = service.resolve_action("QL-RF-A2B3", "book")

    for result in (deep_link, callback):
        assert not result.ok
        assert result.status == "blocked"
        assert result.action == "book"
        assert result.public_listing_id == "QL-RF-A2B3"
        assert result.reason == "listing_not_bookable"
        assert result.book is None
        assert result.details is not None
        assert "🔴 房态：已租出" in result.details.text
        actions = [action for row in result.details.action_rows for action in row]
        labels = [action.label for action in actions]
        assert "📅 预约看房" not in labels
        assert "🏘 看相近房源" in labels
        similar = next(action for action in actions if action.label == "🏘 看相近房源")
        assert similar.action == "similar"
        assert similar.target_public_listing_id == "QL-RF-A2B3"


def test_direct_action_rejects_invalid_identity_and_non_public_action():
    service = _service(_view())

    invalid_id = service.resolve_action("LST_1", "details")
    unsupported = service.resolve_action("QL-RF-A2B3", "consult")

    assert invalid_id.status == "invalid_link"
    assert invalid_id.reason == "invalid_public_listing_id"
    assert unsupported.status == "invalid_link"
    assert unsupported.reason == "unsupported_public_action"


def test_invalid_or_unpublished_payload_never_builds_a_response():
    service = _service(_view())

    invalid = service.resolve("detail_l_1001")
    assert invalid.status == "invalid_link"
    assert invalid.details is None
    assert invalid.photos is None
    assert invalid.book is None

    missing = service.resolve("property_QL-BK-C4D5_details")
    assert missing.status == "not_found"
    assert missing.reason == "listing_not_publicly_published"
    assert missing.details is None
