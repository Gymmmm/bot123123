import json

import pytest

from v3_core.adviser_copy import adviser_tags_from_facts, generate_adviser_lines
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.user_bot.adviser_notes import adviser_notes_for_view
from v3_core.user_bot.listing_responses import build_details_response
from v3_core.user_bot.public_inventory import PublishedListingView


@pytest.mark.parametrize("facts,expected_tags,has_copy", [
    (None, [], False),
    ({}, [], False),
    ({"amenities": ["泳池"]}, ["pool"], False),
    ({"amenities": ["泳池", "健身房"]}, ["pool_gym"], False),
    ({"services": {"cleaning": "每周2次"}}, ["cleaning_2x"], True),
    ({"services": {"cleaning": "包含"}}, ["cleaning_included"], True),
    ({"adviser_signals": ["never_lived", "new_condition"]}, ["never_lived"], True),
    ({"amenities": ["私人泳池", "泳池"]}, ["private_pool"], True),
    ({"house": {"features": ["河景", "市景"]}}, ["river_view"], True),
    ({"adviser_signals": ["management_included", "garbage"]}, ["management_included"], True),
    ({"adviser_signals": ["wifi_included"]}, ["wifi_included"], True),
    ({"adviser_signals": ["wifi_ready"]}, ["wifi_ready"], True),
    ({"adviser_signals": ["management_wifi"]}, ["management_wifi"], True),
    ({"adviser_signals": ["management_wifi_ready"]}, ["management_wifi_ready"], True),
])
def test_v2_required_boundaries(facts, expected_tags, has_copy):
    tags = adviser_tags_from_facts(facts)
    lines = generate_adviser_lines(facts, seed="QC0001", allow_fallback=True)
    assert tags == expected_tags
    assert bool(lines) is has_copy
    assert len(lines) <= 1


def view(facts, *, adviser_copy=None, adviser_copy_source=None):
    snapshot = {"schema": "v3_publication_snapshot.v1", "listing_id": "LST_1",
                "public_listing_id": "QL-RF-A2B3", "canonical_facts": facts,
                "listing": {"project_name": "富力城", "property_type": "公寓"},
                "offer": {"offer_type": "rent", "monthly_rent_usd": 800},
                "canonical_facts_hash": "old"}
    if adviser_copy is not None:
        snapshot["adviser_copy"] = adviser_copy
    if adviser_copy_source is not None:
        snapshot["adviser_copy_source"] = adviser_copy_source
    return PublishedListingView(
        listing={"listing_id": "LST_1", "public_listing_id": "QL-RF-A2B3", "inventory_status": "active"},
        offer={"offer_type": "rent", "offer_status": "active", "publication_policy": "telegram_rent"},
        publication={}, package={"snapshot_json": json.dumps(snapshot)})


@pytest.mark.parametrize("source", ["auto", "manual"])
def test_rental_details_use_publisher_frozen_copy_only(source):
    listing = view(
        {"adviser_signals": ["pet_allowed", "cleaning_2x"]},
        adviser_copy="Publisher frozen text.",
        adviser_copy_source=source,
    )
    text = build_details_response(listing).text
    assert "💬 <b>侨联说</b>\nPublisher frozen text." in text
    assert "如果带宠物入住" not in text


def test_no_frozen_copy_means_user_bot_does_not_regenerate():
    listing = view({"layout": "2房1厅", "bedrooms": 2, "adviser_signals": ["never_lived", "pet_allowed"]})
    assert adviser_notes_for_view(listing) == ""
    assert "侨联说" not in build_details_response(listing).text


def test_stable_seed_100_repetitions_and_frozen_hash_independence():
    facts = {"layout": "2房1厅", "bedrooms": 2, "adviser_signals": ["never_lived", "cleaning_2x", "river_view"]}
    expected = generate_adviser_lines(facts, seed="QL-RF-A2B3", max_points=99)
    assert len(expected) == 1
    assert all(generate_adviser_lines(facts, seed="QL-RF-A2B3") == expected for _ in range(100))
    listing = view(facts)
    assert adviser_notes_for_view(listing) == ""
    snapshot = listing.snapshot
    snapshot["canonical_facts_hash"] = "changed"
    listing.package["snapshot_json"] = json.dumps(snapshot)
    assert adviser_notes_for_view(listing) == ""


def test_same_category_false_booleans_and_negative_pets():
    assert len(generate_adviser_lines({"adviser_signals": ["never_lived", "furnished"]})) == 1
    assert generate_adviser_lines({"services": {"pest_control": "false"}, "house": {"furnished": "yes"}}) == []
    assert generate_adviser_lines({"house": {"pets": "不允许养宠物"}, "adviser_signals": ["pet_allowed"]}) == []


def test_numeric_floor_is_allowed_for_publisher_but_user_bot_still_requires_frozen_copy():
    raw = "BKK1 公寓出租 租金$800/月 2房1厅 面积95㎡ 39楼"
    facts = enrich_authoritative_facts(raw, canonicalize_source(raw))
    assert "high_floor" not in facts.get("adviser_signals", [])
    assert len(generate_adviser_lines({"floor": "39"})) == 1
    assert adviser_notes_for_view(view({"floor": "39"})) == ""


def test_villa_type_alone_is_silent_and_never_emits_villa_wording():
    facts = {"property_type": "别墅"}
    assert "villa" not in adviser_tags_from_facts(facts)
    assert generate_adviser_lines(facts, seed="villa") == []
