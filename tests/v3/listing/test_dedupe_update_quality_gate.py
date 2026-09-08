from __future__ import annotations

from qiaolian_v3.listing.dedupe import DedupeDecision, classify_listing_candidate
from qiaolian_v3.listing.update_policy import UpdateKind, classify_update
from qiaolian_v3.parser.quality_gate import RoutingDecision, evaluate_quality


def test_same_source_price_change_is_update_not_duplicate():
    decision = classify_listing_candidate(
        source_post_id=10,
        existing_source_post_id=10,
        canonical_hash='new',
        existing_canonical_hash='old',
        media_identities=('a', 'b', 'c', 'd'),
        existing_media_identities=('a', 'b', 'c', 'd'),
    )
    assert decision == DedupeDecision.UPDATE


def test_new_photo_same_source_is_update():
    assert classify_update(
        previous_facts={'monthly_rent_usd': 800},
        current_facts={'monthly_rent_usd': 800},
        previous_media=('a', 'b', 'c', 'd'),
        current_media=('a', 'b', 'c', 'd', 'e'),
    ) == UpdateKind.MEDIA_UPDATE


def test_cross_source_weak_similarity_never_auto_merges():
    decision = classify_listing_candidate(
        source_post_id=11,
        existing_source_post_id=10,
        canonical_hash='same',
        existing_canonical_hash='same',
        media_identities=(),
        existing_media_identities=(),
        unit_identity=None,
        existing_unit_identity=None,
    )
    assert decision == DedupeDecision.REVIEW


def test_cross_source_strong_media_overlap_can_be_duplicate():
    decision = classify_listing_candidate(
        source_post_id=11,
        existing_source_post_id=10,
        canonical_hash='same',
        existing_canonical_hash='same',
        media_identities=('a', 'b', 'c', 'd'),
        existing_media_identities=('a', 'b', 'c', 'd'),
    )
    assert decision == DedupeDecision.DUPLICATE


def test_quality_gate_is_routing_authority_for_rent_and_sale():
    rent = evaluate_quality(
        canonical={'deal_type': 'rent', 'monthly_rent_usd': 800, 'quality': {'blocking_flags': []}},
        usable_photo_count=4,
    )
    assert rent.routing_decision == RoutingDecision.READY

    sale = evaluate_quality(
        canonical={'deal_type': 'sale', 'sale_price_usd': 200000, 'quality': {'blocking_flags': []}},
        usable_photo_count=8,
    )
    assert sale.routing_decision == RoutingDecision.STORE_ONLY


def test_quality_gate_reviews_unknown_or_insufficient_media():
    unknown = evaluate_quality(
        canonical={'deal_type': 'unknown', 'quality': {'blocking_flags': ['conflicting_deal_type']}},
        usable_photo_count=8,
    )
    assert unknown.routing_decision == RoutingDecision.REVIEW

    few = evaluate_quality(
        canonical={'deal_type': 'rent', 'monthly_rent_usd': 800, 'quality': {'blocking_flags': []}},
        usable_photo_count=3,
    )
    assert few.routing_decision == RoutingDecision.REVIEW
    assert 'insufficient_usable_photos' in few.blocking_flags
