from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.publishing.package import BUTTON_TEXTS
from tools.migrations import rebuild_channel
from tools.migrations.rebuild_channel import (
    PreviewInvalid, RebuildCandidate, generate_preview, load_candidates_from_db, validate_preview,
)
from tools.migrations.workpack_c_acceptance import REAL_RENT_SOURCES, build_official_preview


FORMAL_IDS = ('QL-RF-A2B3', 'QL-BK-C3D4', 'QL-PP-H4K7', 'QL-PP-M5N8', 'QL-PP-T7V5')


def buttons(ql: str):
    assert normalize_public_id(ql) == ql
    return tuple(
        (text, f'https://t.me/QiaoLianBot?start={action}_{ql}')
        for text, action in zip(BUTTON_TEXTS, ('details','photos','book'))
    )


def candidate(n: int, **overrides) -> RebuildCandidate:
    ql = FORMAL_IDS[(n - 1) % len(FORMAL_IDS)]
    data = dict(
        listing_id=n,
        public_listing_id=ql,
        offer_id=100 + n,
        canonical_record_id=200 + n,
        source_identity=f'unit-test|{n}|source-{n}',
        deal_type='rent',
        offer_type='rent',
        publication_policy='telegram_rent',
        quality_result='AUTO_PUBLISH',
        frozen=True,
        package_id=f'PKG_RENT_{n}',
        package_content_hash=f'after-hash-{n}',
        channel_id='-100123',
        message_id=4900 + n,
        current_content_hash=f'before-hash-{4900+n}',
        cover=f'covers/{ql}.jpg',
        caption=f'🏠 房源 {ql}\n💰 $800/月',
        buttons=buttons(ql),
    )
    data.update(overrides)
    return RebuildCandidate(**data)


def synthetic_negative_control_candidates():
    return (
        candidate(1, message_id=5001, current_content_hash='before-hash-5001'),
        candidate(2, message_id=5002, current_content_hash='before-hash-5002', quality_result='ADMIN_APPROVED'),
        candidate(3, message_id=5003, current_content_hash='before-hash-5003', deal_type='sale', offer_type='sale', publication_policy='store_only', quality_result='ADMIN_APPROVED', package_id=''),
        candidate(4, message_id=5004, current_content_hash='before-hash-5004', deal_type='unknown', offer_type='', publication_policy='store_only', quality_result='NEEDS_REVIEW', package_id=''),
        candidate(5, message_id=5005, current_content_hash='before-hash-5005', frozen=False),
    )


def test_unit_preview_generation_has_zero_writes_and_expected_blocks():
    preview = generate_preview(synthetic_negative_control_candidates())
    assert len(preview.manifest) == 2
    reasons = [failure.reason for failure in preview.failures]
    assert reasons.count('sale_target_blocked') == 1
    assert reasons.count('unknown_target_blocked') == 1
    assert reasons.count('unfrozen_package_blocked') == 1
    assert preview.telegram_writes == 0
    assert preview.publication_status_mutations == 0
    assert preview.preview_hash != 'c2beed370a354ba09ab9a62a35633556e1290be4928970b73acc2ae275480172'


def test_each_manifest_record_is_formal_ql_human_inspectable_and_exactly_mapped():
    preview = generate_preview(synthetic_negative_control_candidates())
    for record in preview.manifest:
        assert normalize_public_id(record.public_ql_id) == record.public_ql_id
        assert record.source_identity and record.canonical_record_id > 0
        assert record.cover and record.caption
        assert len(record.buttons) == 3
        assert tuple(text for text, _ in record.buttons) == BUTTON_TEXTS
        assert record.before_content_hash and record.after_content_hash
        assert record.channel_id == '-100123'
        assert record.message_id in {5001, 5002}
        assert record.target_key == f'{record.channel_id}:{record.message_id}'


def test_target_set_is_frozen_and_any_data_or_mapping_drift_invalidates_preview():
    original = synthetic_negative_control_candidates()
    preview = generate_preview(original)
    with pytest.raises(FrozenInstanceError):
        preview.preview_hash = 'changed'  # type: ignore[misc]
    assert isinstance(preview.target_set, tuple)

    drift_cases = (
        replace(original[0], listing_id=999),
        replace(original[0], offer_id=999),
        replace(original[0], package_content_hash='changed-package-hash'),
        replace(original[0], current_content_hash='changed-current-hash'),
        replace(original[0], message_id=9001),
        replace(original[0], channel_id='-100999'),
    )
    for changed in drift_cases:
        with pytest.raises(PreviewInvalid, match='PREVIEW_INVALID'):
            validate_preview(preview, (changed,) + original[1:])
    with pytest.raises(PreviewInvalid, match='PREVIEW_INVALID'):
        validate_preview(preview, original[:-1])


def test_no_preview_no_apply_and_phase11_apply_is_permanently_blocked_here():
    assert not hasattr(rebuild_channel, 'apply')
    assert not hasattr(rebuild_channel, 'apply_preview')
    with pytest.raises(SystemExit, match='PHASE_11_NOT_STARTED'):
        rebuild_channel.main(['--apply'])


def test_official_cli_is_db_backed_and_generic_json_mode_is_not_an_acceptance_path(tmp_path):
    with pytest.raises(SystemExit):
        rebuild_channel.main(['--input', str(tmp_path / 'synthetic.json'), '--output', str(tmp_path / 'out.json')])


def test_duplicate_or_missing_explicit_slot_mapping_is_blocked():
    same_slot = candidate(1, message_id=7001)
    with pytest.raises(PreviewInvalid, match='duplicate_target_slot'):
        generate_preview((same_slot, replace(candidate(2), message_id=7001)))
    missing = candidate(3, message_id=0)
    preview = generate_preview((missing,))
    assert preview.manifest == ()
    assert preview.failures[0].reason == 'explicit_message_mapping_required'


def test_read_only_db_loader_uses_formal_ql_and_preserves_publication_status():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    conn.execute("INSERT INTO v3_sources(id,source_type,source_name,external_identity) VALUES (1,'telegram','s','s')")
    conn.execute("INSERT INTO v3_source_posts(id,source_id,source_identity_key,source_mode,source_type,source_name,external_post_id) VALUES (1,1,'sp','collector','telegram','s','1')")
    conn.execute("INSERT INTO source_post_revisions(id,source_post_id,revision_no,source_content_hash,raw_text,sanitized_text) VALUES (1,1,1,'rch','x','x')")
    facts = {'deal_type':'rent'}
    conn.execute("INSERT INTO canonical_records(id,source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,facts_hash,deal_type) VALUES (1,1,1,'v3','p',?,'canon','rent')", (json.dumps(facts),))
    conn.execute("INSERT INTO v3_listings(id,public_listing_id,current_canonical_record_id,property_identity_key,listing_status) VALUES (1,'QL-RF-A2B3',1,'prop-1','active')")
    conn.execute("INSERT INTO listing_offers(id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy) VALUES (1,1,1,'rent',800,'telegram_rent')")
    keyboard = [{'text': text, 'url': url} for text, url in buttons('QL-RF-A2B3')]
    conn.execute('''INSERT INTO v3_publication_packages(
        id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,target_channel_id,status,
        approval_mode,cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash
    ) VALUES (1,'PKG_DB_201','PKG_DB_201',1,1,1,1,'-100123','frozen','auto','cover.jpg','[]','caption',?,'after-db','canon')''', (json.dumps(keyboard),))
    conn.commit()
    before = conn.execute("SELECT status FROM v3_publication_packages WHERE id=1").fetchone()[0]
    changes = conn.total_changes
    loaded = load_candidates_from_db(conn, ({'package_id':'PKG_DB_201','channel_id':'-100123','message_id':8201,'current_content_hash':'before-db'},))
    preview = generate_preview(loaded)
    after = conn.execute("SELECT status FROM v3_publication_packages WHERE id=1").fetchone()[0]
    assert before == after == 'frozen' and conn.total_changes == changes
    assert preview.publication_status_mutations == 0 and preview.telegram_writes == 0
    assert preview.manifest[0].public_ql_id == 'QL-RF-A2B3'
    assert preview.manifest[0].source_identity.startswith('s|1|sp')


def test_db_loader_rejects_old_numeric_ql_as_official_rent_identity():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    conn.execute("INSERT INTO v3_sources(id,source_type,source_name,external_identity) VALUES (1,'telegram','s','s')")
    conn.execute("INSERT INTO v3_source_posts(id,source_id,source_identity_key,source_mode,source_type,source_name,external_post_id) VALUES (1,1,'sp','collector','telegram','s','1')")
    conn.execute("INSERT INTO source_post_revisions(id,source_post_id,revision_no,source_content_hash,raw_text,sanitized_text) VALUES (1,1,1,'rch','x','x')")
    conn.execute("INSERT INTO canonical_records(id,source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,facts_hash,deal_type) VALUES (1,1,1,'v3','p','{}','canon','rent')")
    conn.execute("INSERT INTO v3_listings(id,public_listing_id,current_canonical_record_id,property_identity_key) VALUES (1,'QL000101',1,'prop')")
    conn.execute("INSERT INTO listing_offers(id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy) VALUES (1,1,1,'rent',800,'telegram_rent')")
    conn.execute("INSERT INTO v3_publication_packages(id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,target_channel_id,status,approval_mode,cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash) VALUES (1,'PKG_BAD','PKG_BAD',1,1,1,'-100123','frozen','auto','c','[]','c','[]','h','canon')")
    conn.commit()
    with pytest.raises(PreviewInvalid, match='invalid_public_ql_id'):
        load_candidates_from_db(conn, ({'package_id':'PKG_BAD','channel_id':'-100123','message_id':1,'current_content_hash':'before'},))


def test_official_acceptance_preview_uses_real_production_derived_pipeline_and_db_loader(tmp_path):
    preview = build_official_preview(tmp_path)
    assert len(REAL_RENT_SOURCES) == 2
    assert len(preview.manifest) == 2
    assert all(record.source_identity for record in preview.manifest)
    assert all(normalize_public_id(record.public_ql_id) == record.public_ql_id for record in preview.manifest)
    assert all(record.canonical_record_id > 0 and record.listing_id > 0 and record.offer_id > 0 for record in preview.manifest)
    assert all(record.frozen_package_id.startswith('PKG_') for record in preview.manifest)
    assert all(Path(record.cover).exists() for record in preview.manifest)
    assert all(tuple(text for text, _ in record.buttons) == BUTTON_TEXTS for record in preview.manifest)
    reasons = [failure.reason for failure in preview.failures]
    assert reasons.count('sale_target_blocked') == 1
    assert reasons.count('unknown_target_blocked') == 1
    assert reasons.count('unfrozen_package_blocked') == 1
    assert preview.telegram_writes == 0 and preview.publication_status_mutations == 0
    assert preview.preview_hash != 'c2beed370a354ba09ab9a62a35633556e1290be4928970b73acc2ae275480172'
