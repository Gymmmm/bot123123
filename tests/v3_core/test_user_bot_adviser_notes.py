from __future__ import annotations

import json

import pytest

from v3_core.user_bot.adviser_notes import (
    adviser_notes_for_view,
    frozen_adviser_evidence,
    generate_adviser_notes,
)
from v3_core.user_bot.public_inventory import PublishedListingView


def _view(*, canonical_facts=None, listing=None, offer=None):
    canonical = {} if canonical_facts is None else dict(canonical_facts)
    frozen_listing = {
        "project_name": "富力城",
        "public_location_display": "富力城",
        "property_type": "公寓",
        **dict(listing or {}),
    }
    frozen_offer = {
        "offer_type": "rent",
        "monthly_rent_usd": 800,
        "publication_policy": "telegram_rent",
        **dict(offer or {}),
    }
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "offer_id": "OFF_1",
        "canonical_facts_hash": "hash-1",
        "canonical_facts": canonical,
        "listing": frozen_listing,
        "offer": frozen_offer,
    }
    return PublishedListingView(
        listing={
            "listing_id": "LST_1",
            "public_listing_id": "QL-RF-A2B3",
            "inventory_status": "active",
        },
        offer={
            "offer_id": "OFF_1",
            "offer_type": "rent",
            "offer_status": "active",
            "publication_policy": "telegram_rent",
        },
        publication={"instance_id": "PUB_1"},
        package={
            "snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "gallery_json": '["/frozen/01.jpg"]',
        },
    )


def test_locked_production_location_building_and_value_wording_is_preserved():
    evidence = {
        "area": "BKK1",
        "project": "富力城",
        "property_type": "公寓",
        "size_sqm": "95",
        "floor": "19",
        "price": "800",
        "management_fee": "包含",
    }

    assert generate_adviser_notes(evidence, max_points=3) == ""


def test_locked_production_highlights_take_precedence_over_generic_building_line():
    evidence = {
        "property_type": "公寓",
        "size_sqm": "95",
        "floor": "19",
        "highlights": ["一周两次保洁", "灭虫"],
    }

    assert generate_adviser_notes(evidence) == ""


def test_frozen_safe_included_list_projects_only_explicit_fee_inclusion():
    view = _view(canonical_facts={"included": ["物业费", "Wi-Fi"]})

    evidence = frozen_adviser_evidence(view)
    assert evidence["canonical_facts"]["included"] == ["物业费", "Wi-Fi"]
    assert adviser_notes_for_view(view) == ""


def test_amenity_presence_never_becomes_fee_inclusion():
    view = _view(canonical_facts={"amenities": ["游泳池", "健身房"]})

    evidence = frozen_adviser_evidence(view)
    assert "management_fee" not in evidence
    assert "internet_fee" not in evidence
    assert adviser_notes_for_view(view) == ""


def test_frozen_highlights_without_supported_signal_stay_silent():
    view = _view(
        canonical_facts={"highlights": ["采光好", "钥匙已备"]},
        listing={"public_location_display": "BKK1"},
    )

    assert adviser_notes_for_view(view) == ""


def test_older_package_with_frozen_signals_uses_shared_adviser_engine():
    view = _view(
        canonical_facts={
            "adviser_signals": ["cleaning_2x", "management_wifi"],
        }
    )

    text = adviser_notes_for_view(view, max_points=2)

    assert "两次" in text
    assert "物业" not in text
    assert "网费" not in text
    assert "位置标注" not in text


def test_missing_frozen_canonical_facts_is_blocked_instead_of_falling_back_live():
    view = _view(canonical_facts={})
    snapshot = view.snapshot
    snapshot.pop("canonical_facts")
    view.package["snapshot_json"] = json.dumps(snapshot, ensure_ascii=False)

    with pytest.raises(ValueError, match="frozen_canonical_facts_missing"):
        adviser_notes_for_view(view)
