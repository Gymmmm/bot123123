from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from qiaolian_v3.listing.dedupe import DedupeDecision, classify_listing_candidate
from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.parser.quality_gate import RoutingDecision, evaluate_quality_gate

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests' / 'v3' / 'fixtures' / 'phase5_channel_house_groups.tsv'
REAL_HISTORY_EVIDENCE = ROOT / 'tests' / 'v3' / 'fixtures' / 'phase5_real_history_evidence.json'

REQUIRED_RESULT_FIELDS = {
    'source_identity', 'provenance', 'canonical', 'deal_type', 'dedupe_result',
    'quality_result', 'publication_policy', 'listing_offer_projection',
    'blocking_reasons', 'warnings', 'media_evidence_status',
}


def load_samples(path: str | Path = FIXTURE, limit: int | None = None) -> list[dict[str, str]]:
    with Path(path).open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle, delimiter='\t'))
    return rows[: int(limit)] if limit is not None else rows


def load_real_history_evidence(path: str | Path = REAL_HISTORY_EVIDENCE) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding='utf-8'))


def source_text(row: dict[str, str]) -> str:
    """Use only evidence present in the historical production-derived export."""
    parts = [str(row.get('房源') or '').strip()]
    area = str(row.get('区域') or '').strip()
    layout = str(row.get('户型') or '').strip()
    price = str(row.get('价格') or '').strip()
    if area:
        parts.append(f'区域：{area}')
    if layout:
        parts.append(f'户型：{layout}')
    if price:
        parts.append(f'历史导出价格字段：{price}')
    return '\n'.join(part for part in parts if part)


def _listing_offer_projection(facts: dict[str, Any], quality: Any) -> dict[str, Any]:
    deal = str(facts.get('deal_type') or 'unknown')
    if deal == 'rent':
        return {'listing_materializable': True, 'offer_type': 'rent', 'publication_policy': quality.publication_policy}
    if deal == 'sale':
        return {'listing_materializable': True, 'offer_type': 'sale', 'publication_policy': 'store_only'}
    return {'listing_materializable': False, 'offer_type': None, 'publication_policy': 'store_only'}


def evaluate_sample(row: dict[str, str], row_no: int) -> dict[str, Any]:
    text = source_text(row)
    source_name = str(row.get('频道') or '').strip()
    source_identity = {
        'source_type': 'historical_production_group_export',
        'source_name': source_name,
        'external_post_id': f'group-row-{row_no}',
        'provenance': 'historic_commit_7f9865a_channel_house_group_tsv',
    }
    media = {
        'usable_count': 0, 'cover_candidates': [], 'cover_path': '',
        'source_identity_complete': bool(source_name),
        'evidence_status': 'historical_export_has_no_media_evidence',
    }
    facts = canonicalize_source(raw_text=text, sanitized_text=text, source_identity=source_identity, media_summary=media)
    dedupe = classify_listing_candidate(
        source_post_id=row_no, existing_source_post_id=None,
        canonical_hash=str(facts.get('canonical_facts_hash') or ''), existing_canonical_hash=None,
    )
    quality = evaluate_quality_gate(facts, source_mode='collector', media_summary=media, dedupe_result=dedupe)
    projection = _listing_offer_projection(facts, quality)
    return {
        'row_no': row_no,
        'provenance': source_identity['provenance'],
        'source_identity': source_identity,
        'source_text': text,
        'historical_group_count': int(row.get('总条数') or 0),
        'canonical': facts,
        'deal_type': facts.get('deal_type'),
        'dedupe_result': dedupe.value,
        'quality_result': quality.routing_decision.value if quality.routing_decision else None,
        'publication_policy': projection['publication_policy'],
        'blocking_reasons': list(quality.blocking_reasons),
        'warnings': list(quality.warnings),
        'listing_offer_projection': projection,
        'media_evidence_status': media['evidence_status'],
    }


def _scenario_metrics(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Synthetic unit regression only; never counted as real-history evidence."""
    anchor = results[0]
    canonical_hash = str(anchor['canonical'].get('canonical_facts_hash') or '')
    exact = classify_listing_candidate(1, 1, canonical_hash, canonical_hash)
    updated_facts = canonicalize_source(
        raw_text=anchor['source_text'] + '\n月租：$999/月',
        sanitized_text=anchor['source_text'] + '\n月租：$999/月',
        source_identity=anchor['source_identity'], media_summary={'usable_count': 0},
    )
    update = classify_listing_candidate(
        1, 1, str(updated_facts.get('canonical_facts_hash') or ''), canonical_hash,
    )
    return {
        'evidence_class': 'synthetic_unit_regression_only',
        'exact_same_source': exact.value,
        'changed_same_source': update.value,
        'exact_duplicate_second_listing_target': exact != DedupeDecision.DUPLICATE_IGNORE,
        'source_update_duplicate_skipped': update == DedupeDecision.DUPLICATE_IGNORE,
    }


def _real_history_metrics(evidence: dict[str, Any]) -> dict[str, Any]:
    duplicate = evidence['exact_duplicate']
    identity = duplicate['source_identity']
    old_text = duplicate['snapshot_a']['raw_historical_text']
    new_text = duplicate['snapshot_b']['raw_historical_text']
    old_facts = canonicalize_source(old_text, source_identity=identity, media_summary={'usable_count': 0})
    new_facts = canonicalize_source(new_text, source_identity=identity, media_summary={'usable_count': 0})
    exact = classify_listing_candidate(
        source_post_id=int(identity['source_post_id']), existing_source_post_id=int(identity['source_post_id']),
        canonical_hash=str(new_facts.get('canonical_facts_hash') or ''),
        existing_canonical_hash=str(old_facts.get('canonical_facts_hash') or ''),
    )
    changed = evidence['changed_revision']
    return {
        'exact_duplicate': {
            'evidence_status': duplicate['status'], 'source_identity': identity,
            'snapshot_a': duplicate['snapshot_a'], 'snapshot_b': duplicate['snapshot_b'],
            'dedupe_result': exact.value,
            'second_publication_listing_target': exact != DedupeDecision.DUPLICATE_IGNORE,
        },
        'changed_revision': {
            'evidence_status': changed['status'],
            'dedupe_result': None,
            'source_update_duplicate_skipped': None,
            'reason': changed['reason'],
            'searched_existing_history': changed['searched_existing_history'],
        },
    }


def _anomaly(row: dict[str, Any], anomaly_type: str, why: str, disposition: str) -> dict[str, Any]:
    return {
        'row_no': row.get('row_no'), 'source_identity': row.get('source_identity'),
        'anomaly_type': anomaly_type,
        'actual_classification': {'deal_type': row.get('deal_type'), 'quality_result': row.get('quality_result'), 'publication_policy': row.get('publication_policy')},
        'why_anomaly': why, 'reviewed_disposition': disposition,
    }


def build_anomaly_review(results: list[dict[str, Any]], real_history: dict[str, Any]) -> list[dict[str, Any]]:
    review: list[dict[str, Any]] = []
    for row in results:
        text = row['source_text']
        deal = row['deal_type']
        quality = row['quality_result']
        if deal == 'sale':
            review.append(_anomaly(row, 'sale', 'Sale records require explicit publication safety review.', 'ACCEPT_STORE_ONLY: sale is retained as business data and blocked from rental auto-publication.'))
        if deal == 'unknown' and ('租售' in text or ('出售' in text and '出租' in text)):
            review.append(_anomaly(row, 'mixed_rent_sale_conflict', 'Source contains both rent and sale intent.', 'ACCEPT_REVIEW_BLOCK: unresolved mixed intent remains unknown and is not materialized/published.'))
        elif deal == 'unknown':
            review.append(_anomaly(row, 'unknown', 'Canonical deal type is unresolved.', 'ACCEPT_REVIEW_BLOCK: insufficient deal evidence remains fail-closed.'))
        if quality == RoutingDecision.REJECT.value:
            review.append(_anomaly(row, 'reject_non_property', 'Quality gate rejected the historical row.', 'ACCEPT_REJECT: rejected/non-property evidence is not publication eligible.'))
        if row['media_evidence_status'] == 'historical_export_has_no_media_evidence' or row['blocking_reasons']:
            review.append(_anomaly(row, 'incomplete_insufficient_evidence', 'Historical export lacks media evidence and/or has blocking facts.', 'ACCEPT_FAIL_CLOSED: retain evidence for audit; no AUTO_PUBLISH.'))
        if ('出售' in text or '售价' in text) and deal == 'rent' and not ('出租' in text or '租售' in text):
            review.append(_anomaly(row, 'unexpected_parser_classification', 'Explicit sale evidence was classified as rent.', 'BLOCKER: parser classification requires Phase 2-4 bug review before acceptance.'))
    duplicate = real_history['exact_duplicate']
    review.append({
        'row_no': None, 'source_identity': duplicate['source_identity'], 'anomaly_type': 'duplicate_evidence',
        'actual_classification': {'dedupe_result': duplicate['dedupe_result']},
        'why_anomaly': 'Same traceable source row exists unchanged in two repository snapshots.',
        'reviewed_disposition': 'ACCEPT_DUPLICATE_IGNORE: exact duplicate must not create a second listing/publication target.',
    })
    changed = real_history['changed_revision']
    review.append({
        'row_no': None, 'source_identity': None, 'anomaly_type': 'update_evidence',
        'actual_classification': {'evidence_status': changed['evidence_status'], 'dedupe_result': changed['dedupe_result']},
        'why_anomaly': 'A real changed revision pair was required but no traceable pair exists in available repository history/exports/fixtures.',
        'reviewed_disposition': 'REAL_REVISION_EVIDENCE_UNAVAILABLE: do not fabricate an update; synthetic scenario remains unit regression only.',
    })
    return review


def run_dry_run(rows: Iterable[dict[str, str]]) -> dict[str, Any]:
    results = [evaluate_sample(row, index) for index, row in enumerate(rows, start=1)]
    deal_counts = Counter(str(item['deal_type']) for item in results)
    routing_counts = Counter(str(item['quality_result']) for item in results)
    explicit_sale = [item for item in results if '出售' in item['source_text'] or '售价' in item['source_text']]
    explicit_mixed = [item for item in results if '出售' in item['source_text'] and ('出租' in item['source_text'] or '租售' in item['source_text'])]
    detected_sale = [item for item in results if item['deal_type'] == 'sale']
    sale_auto = [item for item in detected_sale if item['quality_result'] == RoutingDecision.AUTO_PUBLISH.value]
    sale_not_store_only = [item for item in detected_sale if item['publication_policy'] != 'store_only']
    obvious_false_rent = [item for item in explicit_sale if item['deal_type'] == 'rent' and item not in explicit_mixed]
    real_history = _real_history_metrics(load_real_history_evidence())
    anomalies = build_anomaly_review(results, real_history)
    return {
        'report_schema': 'phase5_dry_run_v2',
        'sample_count': len(results),
        'provenance': 'historic production-derived group rows; aggregate counts are not expanded into fake messages',
        'deal_type_counts': dict(deal_counts), 'routing_counts': dict(routing_counts),
        'explicit_sale_intent_count': len(explicit_sale), 'detected_sale_count': len(detected_sale),
        'sale_auto_publish_count': len(sale_auto), 'sale_not_store_only_count': len(sale_not_store_only),
        'obvious_false_rent_count': len(obvious_false_rent),
        'real_history_evidence': real_history,
        'scenario_metrics': _scenario_metrics(results) if results else {},
        'anomaly_review': anomalies,
        'results': results,
    }


def main() -> int:
    # Formal CLI output is the complete deterministic report, including every row and anomaly review.
    print(json.dumps(run_dry_run(load_samples()), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
