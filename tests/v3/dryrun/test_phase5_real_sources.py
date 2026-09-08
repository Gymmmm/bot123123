from __future__ import annotations

from tools.v3_phase5_dry_run import load_samples, run_dry_run


def test_phase5_uses_at_least_100_production_derived_real_rows():
    rows = load_samples()
    assert len(rows) >= 100
    assert all(row.get('频道') and row.get('房源') for row in rows)


def test_phase5_sale_and_dedupe_acceptance_contracts():
    report = run_dry_run(load_samples())

    assert report['sample_count'] >= 100
    assert report['explicit_sale_intent_count'] > 0
    assert report['detected_sale_count'] > 0
    assert report['sale_auto_publish_count'] == 0
    assert report['sale_not_store_only_count'] == 0
    assert report['obvious_false_rent_count'] == 0

    scenarios = report['scenario_metrics']
    assert scenarios['exact_same_source'] == 'DUPLICATE_IGNORE'
    assert scenarios['changed_same_source'] == 'UPDATE_EXISTING'
    assert scenarios['exact_duplicate_second_listing_target'] is False
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
    assert deals[0] != 'rent'
    assert deals[1] != 'rent'
    assert deals[2] != 'rent'
