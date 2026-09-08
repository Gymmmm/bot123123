from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from qiaolian_v3.listing.dedupe import DedupeDecision, classify_listing_candidate
from qiaolian_v3.parser.canonical import canonicalize_source
from qiaolian_v3.parser.quality_gate import RoutingDecision, evaluate_quality_gate

FIXTURE = Path(__file__).resolve().parents[1] / 'tests' / 'v3' / 'fixtures' / 'phase5_channel_house_groups.tsv'


def load_samples(path: str | Path = FIXTURE, limit: int | None = None) -> list[dict[str, str]]:
    with Path(path).open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle, delimiter='\t'))
    if limit is not None:
        rows = rows[: int(limit)]
    return rows


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
        # Do not invent currency or rent/sale semantics for the legacy exported number.
        parts.append(f'历史导出价格字段：{price}')
    return '\n'.join(part for part in parts if part)


def _listing_offer_projection(facts: dict[str, Any], quality: Any) -> dict[str, Any]:
    deal = str(facts.get('deal_type') or 'unknown')
    if deal == 'rent':
        return {
            'listing_materializable': True,
            'offer_type': 'rent',
            'publication_policy': quality.publication_policy,
        }
    if deal == 'sale':
        return {
            'listing_materializable': True,
            'offer_type': 'sale',
            'publication_policy': 'store_only',
        }
    return {
        'listing_materializable': False,
        'offer_type': None,
        'publication_policy': 'store_only',
    }


def evaluate_sample(row: dict[str, str], row_no: int) -> dict[str, Any]:
    text = source_text(row)
    source_name = str(row.get('频道') or '').strip()
    source_identity = {
        'source_type': 'historical_production_group_export',
        'source_name': source_name,
        # This is a fixture row identity, explicitly not a Telegram message id.
        'external_post_id': f'group-row-{row_no}',
        'provenance': 'historic_commit_7f9865a_channel_house_group_tsv',
    }
    media = {
        'usable_count': 0,
        'cover_candidates': [],
        'cover_path': '',
        'source_identity_complete': bool(source_name),
        'evidence_status': 'historical_export_has_no_media_evidence',
    }
    facts = canonicalize_source(
        raw_text=text,
        sanitized_text=text,
        source_identity=source_identity,
        media_summary=media,
    )
    dedupe = classify_listing_candidate(
        source_post_id=row_no,
        existing_source_post_id=None,
        canonical_hash=str(facts.get('canonical_facts_hash') or ''),
        existing_canonical_hash=None,
    )
    quality = evaluate_quality_gate(
        facts,
        source_mode='collector',
        media_summary=media,
        dedupe_result=dedupe,
    )
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
        'publication_policy': quality.publication_policy,
        'blocking_reasons': list(quality.blocking_reasons),
        'warnings': list(quality.warnings),
        'listing_offer_projection': _listing_offer_projection(facts, quality),
        'media_evidence_status': media['evidence_status'],
    }


def _scenario_metrics(results: list[dict[str, Any]]) -> dict[str, Any]:
    anchor = results[0]
    facts = anchor['canonical']
    canonical_hash = str(facts.get('canonical_facts_hash') or '')
    exact = classify_listing_candidate(
        source_post_id=1,
        existing_source_post_id=1,
        canonical_hash=canonical_hash,
        existing_canonical_hash=canonical_hash,
    )
    updated_facts = canonicalize_source(
        raw_text=anchor['source_text'] + '\n月租：$999/月',
        sanitized_text=anchor['source_text'] + '\n月租：$999/月',
        source_identity=anchor['source_identity'],
        media_summary={'usable_count': 0},
    )
    update = classify_listing_candidate(
        source_post_id=1,
        existing_source_post_id=1,
        canonical_hash=str(updated_facts.get('canonical_facts_hash') or ''),
        existing_canonical_hash=canonical_hash,
    )
    return {
        'exact_same_source': exact.value,
        'changed_same_source': update.value,
        'exact_duplicate_second_listing_target': exact != DedupeDecision.DUPLICATE_IGNORE,
        'source_update_duplicate_skipped': update == DedupeDecision.DUPLICATE_IGNORE,
    }


def run_dry_run(rows: Iterable[dict[str, str]]) -> dict[str, Any]:
    results = [evaluate_sample(row, index) for index, row in enumerate(rows, start=1)]
    deal_counts = Counter(str(item['deal_type']) for item in results)
    routing_counts = Counter(str(item['quality_result']) for item in results)
    explicit_sale = [item for item in results if '出售' in item['source_text'] or '售价' in item['source_text']]
    explicit_mixed = [item for item in results if '出售' in item['source_text'] and ('出租' in item['source_text'] or '租售' in item['source_text'])]
    detected_sale = [item for item in results if item['deal_type'] == 'sale']
    sale_auto = [item for item in detected_sale if item['quality_result'] == RoutingDecision.AUTO_PUBLISH.value]
    sale_not_store_only = [item for item in detected_sale if item['publication_policy'] != 'store_only']
    obvious_false_rent = [
        item for item in explicit_sale
        if item['deal_type'] == 'rent' and item not in explicit_mixed
    ]
    scenarios = _scenario_metrics(results) if results else {}
    return {
        'sample_count': len(results),
        'provenance': 'historic production-derived group rows; aggregate counts are not expanded into fake messages',
        'deal_type_counts': dict(deal_counts),
        'routing_counts': dict(routing_counts),
        'explicit_sale_intent_count': len(explicit_sale),
        'detected_sale_count': len(detected_sale),
        'sale_auto_publish_count': len(sale_auto),
        'sale_not_store_only_count': len(sale_not_store_only),
        'obvious_false_rent_count': len(obvious_false_rent),
        'scenario_metrics': scenarios,
        'results': results,
    }


def main() -> int:
    report = run_dry_run(load_samples())
    summary = {key: value for key, value in report.items() if key != 'results'}
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
