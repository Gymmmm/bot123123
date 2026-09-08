from __future__ import annotations

from qiaolian_v3.parser.canonical import canonical_business_projection, canonicalize_source


BASE_TEXT = '区域：BKK1\n公寓出租\n2房2卫\n面积：88㎡\n租金：$800/月\n押1付1'


def test_hash_ignores_source_revision_and_media_transport_metadata():
    first = canonicalize_source(
        BASE_TEXT,
        source_identity={
            'source_post_id': 1,
            'source_post_revision_id': 10,
            'source_identity_key': 'telegram:a:1',
        },
        media_summary={
            'images': [{'local_path': '/tmp/a.jpg', 'telegram_file_id': 'AAA', 'message_id': 1}],
            'videos': [],
        },
    )
    second = canonicalize_source(
        BASE_TEXT,
        source_identity={
            'source_post_id': 999,
            'source_post_revision_id': 777,
            'source_identity_key': 'telegram:b:9',
        },
        media_summary={
            'images': [{'local_path': '/srv/other/b.jpg', 'telegram_file_id': 'BBB', 'message_id': 900}],
            'videos': [{'local_path': '/tmp/v.mp4', 'telegram_unique_id': 'VID'}],
        },
    )

    assert first['source_identity'] != second['source_identity']
    assert first['media_summary'] != second['media_summary']
    assert first['canonical_facts_hash'] == second['canonical_facts_hash']
    assert canonical_business_projection(first) == canonical_business_projection(second)


def test_hash_changes_when_business_fact_changes():
    first = canonicalize_source(BASE_TEXT)
    second = canonicalize_source(BASE_TEXT.replace('$800/月', '$850/月'))
    assert first['monthly_rent_usd'] == 800
    assert second['monthly_rent_usd'] == 850
    assert first['canonical_facts_hash'] != second['canonical_facts_hash']


def test_hash_projection_excludes_operational_and_audit_fields():
    facts = canonicalize_source(
        BASE_TEXT,
        source_identity={'source_post_id': 1, 'source_post_revision_id': 2},
        media_summary={'images': [{'local_path': '/tmp/a.jpg'}]},
    )
    projection = canonical_business_projection(facts)
    forbidden = {
        'source_identity', 'raw_text_sha256', 'sanitized_text_sha256', 'media_summary',
        'evidence', 'manual_overrides', 'quality', 'hard_flags', 'review_flags',
        'warning_flags', 'info_flags', 'schema_version', 'parser_revision',
        'canonical_facts_hash', 'display_title',
    }
    assert forbidden.isdisjoint(projection)
