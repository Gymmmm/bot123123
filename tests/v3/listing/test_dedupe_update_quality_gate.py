from __future__ import annotations

from qiaolian_v3.listing.dedupe import DedupeDecision, classify_listing_candidate
from qiaolian_v3.listing.update_policy import UpdateKind, classify_update
from qiaolian_v3.parser.quality_gate import RoutingDecision, evaluate_quality_gate


def _rent_facts():
    return {
        'deal_type': 'rent',
        'monthly_rent_usd': 800,
        'property_type': 'apartment',
        'layout': '2房1厅',
        'project_name': 'Test Project',
        'schema_version': 'canonical_facts.v3',
        'canonical_facts_hash': 'abc123',
        'deposit_payment_terms': '押1付1',
        'contract_term_months': 12,
        'quality': {'hard_flags': [], 'review_flags': [], 'warning_flags': []},
    }


def _media(count=4):
    return {
        'usable_count': count,
        'cover_candidates': ['a'] if count else [],
        'source_identity_complete': True,
    }


def test_same_source_price_change_is_update_not_duplicate():
    decision = classify_listing_candidate(
        source_post_id=10,
        existing_source_post_id=10,
        canonical_hash='new',
        existing_canonical_hash='old',
        media_identities=('a', 'b', 'c', 'd'),
        existing_media_identities=('a', 'b', 'c', 'd'),
    )
    assert decision == DedupeDecision.UPDATE_EXISTING


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
    )
    assert decision == DedupeDecision.NEEDS_REVIEW


def test_cross_source_strong_media_overlap_can_be_duplicate():
    decision = classify_listing_candidate(
        source_post_id=11,
        existing_source_post_id=10,
        canonical_hash='same',
        existing_canonical_hash='same',
        media_identities=('a', 'b', 'c', 'd'),
        existing_media_identities=('a', 'b', 'c', 'd'),
    )
    assert decision == DedupeDecision.DUPLICATE_IGNORE


def test_quality_gate_is_routing_authority_for_rent_and_sale():
    rent = evaluate_quality_gate(
        _rent_facts(), source_mode='collector', media_summary=_media(4), dedupe_result=DedupeDecision.NEW,
    )
    assert rent.routing_decision == RoutingDecision.AUTO_PUBLISH
    assert rent.publication_policy == 'telegram_rent'

    sale = evaluate_quality_gate(
        {'deal_type': 'sale', 'sale_price_usd': 200000, 'quality': {'warning_flags': []}},
        source_mode='collector', media_summary=_media(8), dedupe_result=DedupeDecision.NEW,
    )
    assert sale.routing_decision is None
    assert sale.publication_policy == 'store_only'


def test_quality_gate_reviews_unknown_or_insufficient_media():
    unknown = evaluate_quality_gate(
        {'deal_type': 'unknown', 'quality': {'review_flags': ['conflicting_deal_type']}},
        source_mode='collector', media_summary=_media(8), dedupe_result=DedupeDecision.NEW,
    )
    assert unknown.routing_decision == RoutingDecision.NEEDS_REVIEW

    few = evaluate_quality_gate(
        _rent_facts(), source_mode='collector', media_summary=_media(3), dedupe_result=DedupeDecision.NEW,
    )
    assert few.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'insufficient_usable_photos' in few.blocking_reasons
