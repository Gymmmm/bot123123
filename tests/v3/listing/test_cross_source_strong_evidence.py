from __future__ import annotations

from qiaolian_v3.listing.dedupe import DedupeDecision, classify_listing_candidate


def test_same_known_unit_identity_can_update_existing_listing():
    result = classify_listing_candidate(
        source_post_id=22,
        existing_source_post_id=11,
        canonical_hash='new-facts',
        existing_canonical_hash='old-facts',
        media_identities=('new-a', 'new-b'),
        existing_media_identities=('old-a', 'old-b'),
        unit_identity='tower-a:1908',
        existing_unit_identity='tower-a:1908',
    )
    assert result == DedupeDecision.UPDATE_EXISTING


def test_media_overlap_with_changed_business_facts_stays_review_without_unit_identity():
    result = classify_listing_candidate(
        source_post_id=22,
        existing_source_post_id=11,
        canonical_hash='new-facts',
        existing_canonical_hash='old-facts',
        media_identities=('a', 'b', 'c', 'd'),
        existing_media_identities=('a', 'b', 'c', 'd'),
    )
    assert result == DedupeDecision.NEEDS_REVIEW
