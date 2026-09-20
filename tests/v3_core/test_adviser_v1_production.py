import json

import pytest

from v3_core.adviser_copy import adviser_tags_from_facts, generate_adviser_lines
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.user_bot.adviser_notes import adviser_notes_for_view
from v3_core.user_bot.listing_responses import build_details_response
from v3_core.user_bot.public_inventory import PublishedListingView


@pytest.mark.parametrize("facts,expected", [
    (None, []),
    ({}, []),
    ({"amenities": ["泳池"]}, ["pool"]),
    ({"amenities": ["泳池", "健身房"]}, ["pool_gym"]),
    ({"services": {"cleaning": "每周2次"}}, ["cleaning_2x"]),
    ({"services": {"cleaning": "包含"}}, ["cleaning_included"]),
    ({"adviser_signals": ["never_lived", "new_condition"]}, ["never_lived"]),
    ({"amenities": ["私人泳池", "泳池"]}, ["private_pool"]),
    ({"house": {"features": ["河景", "市景"]}}, ["river_view"]),
    ({"adviser_signals": ["management_included", "garbage"]}, ["management_included"]),
    ({"adviser_signals": ["wifi_included"]}, ["wifi_included"]),
    ({"adviser_signals": ["wifi_ready"]}, ["wifi_ready"]),
    ({"adviser_signals": ["management_wifi"]}, ["management_wifi"]),
    ({"adviser_signals": ["management_wifi_ready"]}, ["management_wifi_ready"]),
])
def test_v1_required_boundaries(facts, expected):
    tags = adviser_tags_from_facts(facts)
    lines = generate_adviser_lines(facts, seed="QC0001", allow_fallback=True)
    assert tags == expected
    assert len(lines) == len(expected)


def view(facts):
    snapshot = {"schema": "v3_publication_snapshot.v1", "listing_id": "LST_1",
                "public_listing_id": "QL-RF-A2B3", "canonical_facts": facts,
                "listing": {"project_name": "富力城", "property_type": "公寓"},
                "offer": {"offer_type": "rent", "monthly_rent_usd": 800},
                "adviser_copy": "旧的物业网费兜底话术", "canonical_facts_hash": "old"}
    return PublishedListingView(
        listing={"listing_id": "LST_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={}, package={"snapshot_json": json.dumps(snapshot)})


def test_rental_details_prefers_frozen_adviser_copy_plain_heading():
    """Merged caption: plain 💬 侨联说 + frozen adviser_copy (not bold / not generated bullets)."""
    listing = view({"adviser_signals": ["pet_allowed", "cleaning_2x"]})
    text = build_details_response(listing).text
    assert "💬 侨联说" in text
    assert "💬 <b>侨联说</b>" not in text
    assert "旧的物业网费兜底话术" in text
    assert "暂无建议" not in text
    # generated signal bullets are not injected when frozen copy wins
    assert "• " not in text


def test_rental_details_falls_back_to_generated_notes_without_frozen_copy():
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "canonical_facts": {"adviser_signals": ["pet_allowed"]},
        "listing": {"project_name": "富力城", "property_type": "公寓"},
        "offer": {"offer_type": "rent", "monthly_rent_usd": 800},
        "adviser_copy": "",
        "canonical_facts_hash": "old",
    }
    listing = PublishedListingView(
        listing={"listing_id": "LST_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={},
        package={"snapshot_json": json.dumps(snapshot)},
    )
    text = build_details_response(listing).text
    assert "💬 侨联说" in text
    assert "💬 <b>侨联说</b>" not in text
    assert "旧的物业" not in text
    assert adviser_notes_for_view(listing) in text


def test_rental_details_omits_adviser_section_when_empty():
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": "LST_1",
        "public_listing_id": "QL-RF-A2B3",
        "canonical_facts": {"adviser_signals": []},
        "listing": {"project_name": "富力城", "property_type": "公寓"},
        "offer": {"offer_type": "rent", "monthly_rent_usd": 800},
        "adviser_copy": "",
        "canonical_facts_hash": "old",
    }
    listing = PublishedListingView(
        listing={"listing_id": "LST_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={},
        package={"snapshot_json": json.dumps(snapshot)},
    )
    text = build_details_response(listing).text
    assert "💬 侨联说" not in text
    assert "暂无建议" not in text


def test_stable_seed_100_repetitions_and_frozen_hash_independence():
    facts = {"adviser_signals": ["never_lived", "cleaning_2x", "river_view"]}
    expected = generate_adviser_lines(facts, seed="QL-RF-A2B3", max_points=99)
    assert len(expected) == 2
    assert all(generate_adviser_lines(facts, seed="QL-RF-A2B3") == expected for _ in range(100))
    listing = view(facts)
    assert adviser_notes_for_view(listing) == "\n".join(expected)
    snapshot = listing.snapshot
    snapshot["canonical_facts_hash"] = "changed"
    listing.package["snapshot_json"] = json.dumps(snapshot)
    assert adviser_notes_for_view(listing) == "\n".join(expected)


def test_same_category_and_false_booleans_and_negative_pets():
    assert len(generate_adviser_lines({"adviser_signals": ["never_lived", "furnished"]})) == 1
    assert generate_adviser_lines({"services": {"pest_control": "false"}, "house": {"furnished": "yes"}}) == []
    assert generate_adviser_lines({"house": {"pets": "不允许养宠物"}, "adviser_signals": ["pet_allowed"]}) == []


def test_numeric_floor_never_becomes_adviser_evidence():
    raw = "BKK1 公寓出租 租金$800/月 2房1厅 面积95㎡ 39楼"
    facts = enrich_authoritative_facts(raw, canonicalize_source(raw))
    assert "high_floor" not in facts.get("adviser_signals", [])
    assert generate_adviser_lines({"floor": "39"}) == []
    assert adviser_notes_for_view(view({"floor": "39", "adviser_signals": ["high_floor"]})) == ""
    assert adviser_notes_for_view(view({"adviser_signals": ["high_floor"], "adviser_signals_version": "explicit-v1"}))


def test_villa_type_alone_is_silent_and_never_emits_villa_wording():
    facts = {"property_type": "别墅"}
    assert "villa" not in adviser_tags_from_facts(facts)
    assert generate_adviser_lines(facts, seed="villa") == []
