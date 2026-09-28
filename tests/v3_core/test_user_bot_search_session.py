from __future__ import annotations

import json

from v3_core.user_bot.callbacks import parse_callback
from v3_core.user_bot.public_inventory import PublishedListingView
from v3_core.user_bot.search_session import SearchSessionService


def _view(*, listing_id: str, public_id: str, status: str = "active"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": listing_id,
        "public_listing_id": public_id,
        "offer_id": f"OFF_{listing_id}",
        "canonical_record_id": f"CAN_{listing_id}",
        "canonical_facts_hash": f"hash-{listing_id}",
        "canonical_facts": {},
        "listing": {
            "project_name": listing_id,
            "property_type": "公寓",
            "layout": "2房1厅",
            "public_location_display": "BKK1",
            "size_sqm": 90,
            "floor": "12",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": listing_id,
            "public_listing_id": public_id,
            "inventory_status": status,
        },
        offer={
            "offer_id": f"OFF_{listing_id}",
            "offer_type": "rent",
            "offer_status": "active" if status in {"active", "reserved"} else "inactive",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": f"PUB_{listing_id}"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": "[]",
            "cover_path": "",
        },
    )


class InventoryStub:
    def __init__(self, mapping):
        self.mapping = dict(mapping)
        self.calls = []

    def resolve(self, public_id):
        self.calls.append(public_id)
        return self.mapping.get(public_id)


def _ids():
    return ("QL-BK-A2B3", "QL-BK-C4D5", "QL-B2-E6F7")


def _inventory(*, statuses=("active", "active", "active")):
    ids = _ids()
    return InventoryStub(
        {
            public_id: _view(
                listing_id=f"LST_{index + 1}",
                public_id=public_id,
                status=statuses[index],
            )
            for index, public_id in enumerate(ids)
        }
    )


def test_navigation_validates_original_position_then_renders_live_target():
    inventory = _inventory()
    service = SearchSessionService(inventory)
    callback = parse_callback("v3u:card:1:QL-BK-C4D5")
    assert callback is not None

    result = service.navigate(callback, _ids())

    assert result.ok
    assert result.status == "ok"
    assert result.public_listing_ids == _ids()
    assert result.requested_public_listing_id == "QL-BK-C4D5"
    assert not result.requested_removed
    assert result.card is not None
    assert result.card.public_listing_id == "QL-BK-C4D5"
    assert result.card.index == 1
    assert inventory.calls == list(_ids())


def test_requested_listing_that_became_unavailable_falls_to_nearest_valid_index():
    inventory = _inventory(statuses=("active", "rented", "active"))
    service = SearchSessionService(inventory)
    callback = parse_callback("v3u:card:1:QL-BK-C4D5")
    assert callback is not None

    result = service.navigate(callback, _ids())

    assert result.ok
    assert result.public_listing_ids == ("QL-BK-A2B3", "QL-B2-E6F7")
    assert result.requested_public_listing_id == "QL-BK-C4D5"
    assert result.requested_removed
    assert result.card is not None
    # Fixed-SHA parity: min(old_index=1, len(valid)-1=1) -> second valid card.
    assert result.card.public_listing_id == "QL-B2-E6F7"
    assert result.card.index == 1


def test_earlier_listing_can_disappear_without_changing_requested_identity():
    inventory = _inventory(statuses=("rented", "active", "active"))
    service = SearchSessionService(inventory)
    callback = parse_callback("v3u:card:1:QL-BK-C4D5")
    assert callback is not None

    result = service.navigate(callback, _ids())

    assert result.ok
    assert result.public_listing_ids == ("QL-BK-C4D5", "QL-B2-E6F7")
    assert not result.requested_removed
    assert result.card is not None
    assert result.card.public_listing_id == "QL-BK-C4D5"
    assert result.card.index == 0


def test_entire_result_set_becoming_unavailable_expires_session():
    inventory = _inventory(statuses=("rented", "offline", "inactive"))
    service = SearchSessionService(inventory)
    callback = parse_callback("v3u:card:2:QL-B2-E6F7")
    assert callback is not None

    result = service.navigate(callback, _ids())

    assert not result.ok
    assert result.status == "expired"
    assert result.card is None
    assert result.public_listing_ids == ()
    assert result.requested_public_listing_id == "QL-B2-E6F7"


def test_tampered_callback_is_rejected_before_inventory_reads():
    inventory = _inventory()
    service = SearchSessionService(inventory)
    callback = parse_callback("v3u:card:0:QL-BK-C4D5")
    assert callback is not None

    result = service.navigate(callback, _ids())

    assert result.status == "invalid_callback"
    assert result.card is None
    assert inventory.calls == []


def test_invalid_or_duplicate_session_is_rejected_fail_closed():
    inventory = _inventory()
    service = SearchSessionService(inventory)

    invalid_callback = parse_callback("v3u:card:1:QL-BK-C4D5")
    duplicate_callback = parse_callback("v3u:card:1:QL-BK-A2B3")
    assert invalid_callback is not None
    assert duplicate_callback is not None

    invalid = service.navigate(
        invalid_callback,
        ("not-a-public-id", "QL-BK-C4D5"),
    )
    duplicate = service.navigate(
        duplicate_callback,
        ("QL-BK-A2B3", "QL-BK-A2B3"),
    )

    assert invalid.status == "invalid_callback"
    assert duplicate.status == "invalid_callback"
    assert inventory.calls == []


def test_refresh_drops_missing_or_non_bookable_public_views():
    ids = _ids()
    inventory = InventoryStub(
        {
            ids[0]: _view(listing_id="LST_1", public_id=ids[0], status="active"),
            ids[1]: None,
            ids[2]: _view(listing_id="LST_3", public_id=ids[2], status="rented"),
        }
    )
    service = SearchSessionService(inventory)

    views = service.refresh(ids)

    assert [view.public_listing_id for view in views] == ["QL-BK-A2B3"]
