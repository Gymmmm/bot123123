from __future__ import annotations

import json

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


def test_value_line_prefers_fee_inclusion_and_stays_one_sentence():
    evidence = {
        "area": "BKK1",
        "project": "富力城",
        "property_type": "公寓",
        "size_sqm": "95",
        "floor": "19",
        "price": "800",
        "management_fee": "包含",
    }
    assert generate_adviser_notes(evidence, max_points=1) == "月租 $800，物业费包了。"


def test_highlights_take_precedence_when_no_fee_signal():
    evidence = {
        "property_type": "公寓",
        "size_sqm": "95",
        "floor": "19",
        "highlights": ["一周两次保洁", "灭虫"],
    }
    assert generate_adviser_notes(evidence) == "资料写了一周两次保洁、灭虫，实拍再确认就行。"


def test_location_line_is_colloquial_when_only_place_facts_exist():
    evidence = {"area": "BKK1", "project": "富力城"}
    assert generate_adviser_notes(evidence) == "富力城在BKK1这边，通勤自己看着办。"


def test_frozen_safe_included_list_projects_only_explicit_fee_inclusion():
    view = _view(canonical_facts={"included": ["物业费", "Wi-Fi"]})
    evidence = frozen_adviser_evidence(view)
    assert evidence["management_fee"] == "包含"
    assert evidence["internet_fee"] == "包含"
    assert adviser_notes_for_view(view) == "月租 $800，物业费包了、网费包了。"


def test_amenity_presence_never_becomes_fee_inclusion():
    view = _view(canonical_facts={"amenities": ["游泳池", "健身房"]})
    evidence = frozen_adviser_evidence(view)
    assert "management_fee" not in evidence
    assert "internet_fee" not in evidence
    assert adviser_notes_for_view(view) == "在富力城，建议先翻实拍再约看。"


def test_frozen_highlights_are_used_without_live_database_lookup():
    view = _view(
        canonical_facts={"highlights": ["采光好", "钥匙已备"]},
        listing={"public_location_display": "BKK1"},
    )
    assert adviser_notes_for_view(view) == "资料写了采光好、钥匙已备，实拍再确认就行。"


def test_allow_empty_returns_blank_when_no_useful_signal():
    assert generate_adviser_notes({}, allow_empty=True) == ""
    assert generate_adviser_notes({}, allow_empty=False) == "条件以资料和现场核对为准。"
