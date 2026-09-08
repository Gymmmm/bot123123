from __future__ import annotations

import json

from tools.v3_phase5_dry_run import REQUIRED_RESULT_FIELDS, load_samples, main, run_dry_run


def test_phase5_keeps_at_least_100_production_derived_real_rows_without_expanding_counts():
    rows = load_samples()
    assert len(rows) >= 100
    assert all(row.get('频道') and row.get('房源') for row in rows)
    assert len(rows) == len(list(rows))


def test_every_real_row_has_complete_formal_output_and_anomalies_are_disposed():
    report = run_dry_run(load_samples())
    assert report['sample_count'] >= 100
    assert len(report['results']) == report['sample_count']
    assert all(REQUIRED_RESULT_FIELDS <= set(item) for item in report['results'])
    assert report['anomaly_review']
    assert all(entry.get('reviewed_disposition') for entry in report['anomaly_review'])
    covered = {entry['anomaly_type'] for entry in report['anomaly_review']}
    assert {'sale', 'unknown', 'incomplete_insufficient_evidence', 'duplicate_evidence', 'update_evidence'} <= covered


def test_formal_cli_emits_complete_results_and_anomaly_review(capsys):
    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload['results']) == payload['sample_count']
    assert payload['anomaly_review']


def test_phase5_sale_and_real_history_evidence_contracts():
    report = run_dry_run(load_samples())
    assert report['explicit_sale_intent_count'] > 0
    assert report['detected_sale_count'] > 0
    assert report['sale_auto_publish_count'] == 0
    assert report['sale_not_store_only_count'] == 0
    assert report['obvious_false_rent_count'] == 0

    real = report['real_history_evidence']
    duplicate = real['exact_duplicate']
    assert duplicate['evidence_status'] == 'AVAILABLE'
    assert duplicate['snapshot_a']['raw_historical_text'] == duplicate['snapshot_b']['raw_historical_text']
    assert duplicate['dedupe_result'] == 'DUPLICATE_IGNORE'
    assert duplicate['second_publication_listing_target'] is False

    changed = real['changed_revision']
    # No traceable changed-revision pair exists in the available repository history.
    # Phase 5 must report that fact rather than manufacture a price edit.
    assert changed['evidence_status'] == 'REAL_REVISION_EVIDENCE_UNAVAILABLE'
    assert changed['dedupe_result'] is None
    assert changed['source_update_duplicate_skipped'] is None
    assert changed['searched_existing_history']

    # Synthetic scenario remains only a unit regression and is not real-history evidence.
    scenarios = report['scenario_metrics']
    assert scenarios['evidence_class'] == 'synthetic_unit_regression_only'
    assert scenarios['changed_same_source'] == 'UPDATE_EXISTING'
    assert scenarios['source_update_duplicate_skipped'] is False


def test_historical_media_absence_is_fail_closed_not_invented():
    report = run_dry_run(load_samples())
    assert all(item['media_evidence_status'] == 'historical_export_has_no_media_evidence' for item in report['results'])
    assert all(item['quality_result'] != 'AUTO_PUBLISH' for item in report['results'])


def test_price_like_sale_deposit_and_utility_evidence_does_not_create_false_rent():
    from qiaolian_v3.parser.canonical import canonicalize_source

    cases = [
        '金边公寓出售 1居室 52㎡ 售价 $78000',
        '押金 $800，水费 $5，电费 $0.25，公寓可咨询',
        '售价 $78000，押金 $800，电费 $0.25，金边公寓出售',
    ]
    deals = [canonicalize_source(text)['deal_type'] for text in cases]
    assert all(deal != 'rent' for deal in deals)
