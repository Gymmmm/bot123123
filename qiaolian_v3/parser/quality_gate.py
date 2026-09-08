from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from qiaolian_v3.listing.dedupe import DedupeDecision


class RoutingDecision(str, Enum):
    AUTO_PUBLISH = 'AUTO_PUBLISH'
    NEEDS_REVIEW = 'NEEDS_REVIEW'
    REJECT = 'REJECT'


@dataclass(frozen=True)
class QualityDecision:
    routing_decision: RoutingDecision | None
    blocking_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    publication_policy: str


def _present(value: Any) -> bool:
    return value not in (None, '', [], {}, ())


def evaluate_quality_gate(
    canonical_record: Mapping[str, Any],
    *,
    source_mode: str,
    media_summary: Mapping[str, Any],
    dedupe_result: DedupeDecision | str,
) -> QualityDecision:
    """Single Phase-4 routing authority.

    routing_decision is only AUTO_PUBLISH / NEEDS_REVIEW / REJECT / None.
    Sale is storage-only with a null routing decision. Media and collector code
    supply evidence but never duplicate this policy.
    """
    facts = dict(canonical_record)
    media = dict(media_summary)
    dedupe = DedupeDecision(str(getattr(dedupe_result, 'value', dedupe_result)))
    deal_type = str(facts.get('deal_type') or 'unknown')
    quality = dict(facts.get('quality') or {})
    hard = {str(v) for v in quality.get('hard_flags') or []}
    review = {str(v) for v in quality.get('review_flags') or []}
    blocking: list[str] = []
    warnings: list[str] = []

    if dedupe == DedupeDecision.DUPLICATE_IGNORE:
        return QualityDecision(None, (), ('duplicate_ignore',), 'store_only')

    if deal_type == 'sale':
        return QualityDecision(None, (), tuple(sorted(set(quality.get('warning_flags') or []))), 'store_only')

    reject_flags = {'tenant_request', 'seeking_rental', 'non_property_ad', 'spam', 'garbage_source'}
    if hard & reject_flags:
        return QualityDecision(RoutingDecision.REJECT, tuple(sorted(hard & reject_flags)), (), 'store_only')

    if deal_type != 'rent':
        blocking.append('unresolved_deal_type')
    if not _present(facts.get('monthly_rent_usd')):
        blocking.append('missing_price')
    if not _present(facts.get('property_type')):
        blocking.append('missing_property_type')
    if not _present(facts.get('layout')):
        blocking.append('missing_layout')
    if not any(_present(facts.get(key)) for key in ('project_name', 'public_location_key', 'public_location_display', 'canonical_area_key')):
        blocking.append('missing_public_location')
    if not _present(facts.get('schema_version')):
        blocking.append('invalid_canonical_schema')
    if not _present(facts.get('canonical_facts_hash') or facts.get('facts_hash')):
        blocking.append('missing_canonical_hash')

    blocking.extend(sorted(review))

    if source_mode == 'collector':
        if not (_present(facts.get('deposit_payment_terms')) or (_present(facts.get('deposit_months')) and _present(facts.get('prepay_months')))):
            blocking.append('missing_payment_terms')
        if not (_present(facts.get('contract_term_months')) or _present(facts.get('contract_term_display'))):
            blocking.append('missing_contract_term')
    elif source_mode == 'admin_import':
        blocking.append('admin_import_requires_review')

    usable_count = int(media.get('usable_count') or 0)
    if usable_count < 4:
        blocking.append('insufficient_usable_photos')
    if not media.get('cover_candidates') and not media.get('cover_path'):
        blocking.append('missing_cover_candidate')
    if media.get('source_identity_complete') is not True:
        blocking.append('incomplete_source_identity')

    if dedupe == DedupeDecision.NEEDS_REVIEW:
        blocking.append('dedupe_needs_review')

    if not _present(facts.get('size_sqm')):
        warnings.append('missing_size')
    if not any(_present(facts.get(key)) for key in ('management_fee', 'internet_fee', 'water_rate', 'electric_rate', 'parking_fee')):
        warnings.append('missing_recurring_costs')
    if not _present(facts.get('highlights')):
        warnings.append('missing_highlights')
    if not _present(facts.get('available_date')):
        warnings.append('missing_available_date')

    blocking = list(dict.fromkeys(blocking))
    warnings.extend(str(v) for v in quality.get('warning_flags') or [])
    warnings = list(dict.fromkeys(warnings))
    decision = RoutingDecision.NEEDS_REVIEW if blocking else RoutingDecision.AUTO_PUBLISH
    return QualityDecision(decision, tuple(blocking), tuple(warnings), 'telegram_rent')


__all__ = ['RoutingDecision', 'QualityDecision', 'evaluate_quality_gate']
