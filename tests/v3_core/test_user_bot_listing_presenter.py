from __future__ import annotations

import json

import pytest

from v3_core.user_bot.listing_presenter import build_public_listing_details
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(*, live_status="active", live_offer_status="active"):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "listing": {
            "project_name": "富力城",
            "layout": "2房1厅",
            "public_location_display": "金边·富力城",
            "size_sqm": 95,
            "floor": "19",
            "inventory_status": "active",
        },
        "offer": {
            "offer_type": "rent",
            "monthly_rent_usd": 800,
            "deposit_terms": "押1付1",
            "contract_term": "1年",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": live_status,
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": live_offer_status,
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": '["/frozen/01.jpg","/frozen/02.jpg"]',
        },
    )


def test_presenter_uses_frozen_snapshot_for_public_facts():
    view = _view()
    # Simulate later live-row drift. Public facts must still come from snapshot.
    view.listing["project_name"] = "后台后来改名"
    view.offer["monthly_rent_usd"] = 9999

    details = build_public_listing_details(view)

    assert details.project_name == "富力城"
    assert details.layout == "2房1厅"
    assert details.subject == "富力城｜2房1厅"
    assert details.location == "金边·富力城"
    assert details.monthly_rent_usd == 800
    assert details.size_sqm == 95.0
    assert details.floor == "19"
    assert details.lease_summary == "押1付1 · 1年"
    assert details.gallery == ("/frozen/01.jpg", "/frozen/02.jpg")


def test_presenter_uses_live_status_only_for_current_availability():
    details = build_public_listing_details(_view(live_status="rented", live_offer_status="inactive"))

    assert details.inventory_status == "rented"
    assert details.status_icon == "🔴"
    assert details.status_label == "已租出"
    assert not details.bookable
    # Frozen public rent remains the historical published fact.
    assert details.monthly_rent_usd == 800


def test_reserved_remains_bookable_and_uses_current_status_label():
    details = build_public_listing_details(_view(live_status="reserved"))

    assert details.bookable
    assert details.status_icon == "🟡"
    assert details.status_label == "已有预约 · 仍可预约"


def test_missing_or_wrong_snapshot_schema_is_blocked():
    view = _view()
    view.package["snapshot_json"] = "{}"

    with pytest.raises(ValueError, match="frozen_public_snapshot_missing"):
        build_public_listing_details(view)
