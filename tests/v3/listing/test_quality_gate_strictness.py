from __future__ import annotations

import pytest

from qiaolian_v3.listing.dedupe import DedupeDecision
from qiaolian_v3.parser.quality_gate import RoutingDecision, evaluate_quality_gate


def _valid_rent():
    return {
        'deal_type': 'rent',
        'monthly_rent_usd': 800,
        'property_type': 'apartment',
        'layout': '2房1厅',
        'project_name': 'Test Project',
        'schema_version': 'canonical_facts.v3',
        'canonical_facts_hash': 'facts-hash',
        'deposit_payment_terms': '押1付1',
        'contract_term_months': 12,
        'quality': {'hard_flags': [], 'review_flags': [], 'blocking_flags': [], 'warning_flags': []},
    }


def _media():
    return {'usable_count': 4, 'cover_candidates': ['cover-a'], 'source_identity_complete': True}


def test_collector_auto_requires_payment_and_contract_terms():
    facts = _valid_rent()
    facts.pop('deposit_payment_terms')
    facts.pop('contract_term_months')
    result = evaluate_quality_gate(facts, source_mode='collector', media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'missing_payment_terms' in result.blocking_reasons
    assert 'missing_contract_term' in result.blocking_reasons


def test_admin_import_never_auto_publishes():
    result = evaluate_quality_gate(_valid_rent(), source_mode='admin_import', media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'admin_import_requires_review' in result.blocking_reasons


@pytest.mark.parametrize('source_mode', ['manual', 'legacy', 'anything', ''])
def test_unknown_source_modes_fail_closed(source_mode):
    result = evaluate_quality_gate(_valid_rent(), source_mode=source_mode, media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'invalid_source_mode' in result.blocking_reasons


def test_exact_duplicate_does_not_enter_publication_route():
    result = evaluate_quality_gate(_valid_rent(), source_mode='collector', media_summary=_media(), dedupe_result=DedupeDecision.DUPLICATE_IGNORE)
    assert result.routing_decision is None
    assert result.publication_policy == 'store_only'
    assert 'duplicate_ignore' in result.warnings


def test_missing_identity_or_cover_blocks_collector_auto():
    media = {'usable_count': 4, 'cover_candidates': [], 'cover_path': '', 'source_identity_complete': False}
    result = evaluate_quality_gate(_valid_rent(), source_mode='collector', media_summary=media, dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'missing_cover_candidate' in result.blocking_reasons
    assert 'incomplete_source_identity' in result.blocking_reasons


def test_missing_property_layout_and_location_are_blocking():
    facts = _valid_rent()
    for key in ('property_type', 'layout', 'project_name'):
        facts.pop(key)
    result = evaluate_quality_gate(facts, source_mode='collector', media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert {'missing_property_type', 'missing_layout', 'missing_public_location'} <= set(result.blocking_reasons)


def test_unknown_property_hard_flag_never_auto_publishes():
    facts = _valid_rent()
    facts['property_type'] = '未知'
    facts['quality']['hard_flags'] = ['unknown_property_type']
    facts['quality']['blocking_flags'] = ['unknown_property_type']
    result = evaluate_quality_gate(facts, source_mode='collector', media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert 'unknown_property_type' in result.blocking_reasons


@pytest.mark.parametrize('flag', ['ambiguous_property_type', 'conflicting_rental_price', 'conflicting_deal_type'])
def test_non_garbage_hard_flags_block_auto_publish(flag):
    facts = _valid_rent()
    facts['quality']['hard_flags'] = [flag]
    result = evaluate_quality_gate(facts, source_mode='collector', media_summary=_media(), dedupe_result=DedupeDecision.NEW)
    assert result.routing_decision == RoutingDecision.NEEDS_REVIEW
    assert flag in result.blocking_reasons
