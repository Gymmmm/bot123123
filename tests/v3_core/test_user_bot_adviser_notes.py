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

    assert generate_adviser_notes(evidence, max_points=3) == (
        "这套标注在BKK1，项目是富力城，可以按实际通勤路线再判断。\n"
        "资料里写的是公寓、95㎡、19，实际空间和楼层以现场为准。\n"
        "月租为$800；物业费已包含。"
    )


def test_locked_production_highlights_take_precedence_over_generic_building_line():
    evidence = {
        "property_type": "公寓",
        "size_sqm": "95",
        "floor": "19",
        "highlights": ["一周两次保洁", "灭虫"],
    }

    assert generate_adviser_notes(evidence) == (
        "资料明确标注：一周两次保洁、灭虫；具体状态可以结合实拍确认。"
    )


def test_frozen_safe_included_list_projects_only_explicit_fee_inclusion():
    view = _view(canonical_facts={"included": ["物业费", "Wi-Fi"]})

    evidence = frozen_adviser_evidence(view)
    assert evidence["management_fee"] == "包含"
    assert evidence["internet_fee"] == "包含"
    assert adviser_notes_for_view(view) == (
        "这套位置标注为富力城，看房前可以先核对具体定位。\n"
        "月租为$800；物业费已包含；网络费已包含。"
    )


def test_amenity_presence_never_becomes_fee_inclusion():
    view = _view(canonical_facts={"amenities": ["游泳池", "健身房"]})

    evidence = frozen_adviser_evidence(view)
    assert "management_fee" not in evidence
    assert "internet_fee" not in evidence
    assert adviser_notes_for_view(view) == (
        "这套位置标注为富力城，看房前可以先核对具体定位。"
    )


def test_frozen_highlights_are_used_without_live_database_lookup():
    view = _view(
        canonical_facts={"highlights": ["采光好", "钥匙已备"]},
        listing={"public_location_display": "BKK1"},
    )

    assert adviser_notes_for_view(view) == (
        "这套标注在BKK1，项目是富力城，可以按实际通勤路线再判断。\n"
        "资料明确标注：采光好、钥匙已备；具体状态可以结合实拍确认。"
    )


def test_missing_frozen_canonical_facts_is_blocked_instead_of_falling_back_live():
    view = _view(canonical_facts={})
    snapshot = view.snapshot
    snapshot.pop("canonical_facts")
    view.package["snapshot_json"] = json.dumps(snapshot, ensure_ascii=False)

    with pytest.raises(ValueError, match="frozen_canonical_facts_missing"):
        adviser_notes_for_view(view)
