from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, SourceRepository
from tests.v3.db._helpers import insert_source, migrated_connection


def _source(conn, external_id='100'):
    legacy_id = insert_source(conn, external_id=external_id)
    sources = SourceRepository(conn)
    identity_id = sources.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id=external_id,
    )
    revision = sources.append_revision(
        source_identity_id=identity_id, content_hash=f'h-{external_id}', raw_text='evidence'
    )
    return identity_id, revision.id


def test_deal_type_db_contract_rejects_mixed():
    conn = migrated_connection()
    identity_id, revision_id = _source(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_identity_id,source_post_revision_id,schema_version,parser_revision,
               facts_json,facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status
               ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (identity_id, revision_id, 'v3', 'test', '{}', 'hash', 'mixed', '["rent","sale"]', '{}', 'needs_review'),
        )


def test_repository_supersedes_and_keeps_exactly_one_current_record():
    conn = migrated_connection()
    identity_id, revision_id = _source(conn)
    repo = CanonicalRecordRepository(conn)
    first = repo.append(
        source_identity_id=identity_id, source_post_revision_id=revision_id,
        schema_version='v3', parser_revision='r1', facts={'a': 1}, facts_hash='f1', deal_type='rent',
    )
    sources = SourceRepository(conn)
    revision2 = sources.append_revision(
        source_identity_id=identity_id, content_hash='h2', raw_text='changed evidence'
    )
    second = repo.append(
        source_identity_id=identity_id, source_post_revision_id=revision2.id,
        schema_version='v3', parser_revision='r1', facts={'a': 2}, facts_hash='f2', deal_type='unknown',
        deal_type_candidates=['rent','sale'], processing_status='needs_review',
    )
    rows = conn.execute(
        'SELECT id,is_current,supersedes_id,deal_type FROM canonical_records WHERE source_identity_id=? ORDER BY id',
        (identity_id,),
    ).fetchall()
    assert [(r['id'], r['is_current']) for r in rows] == [(first.id, 0), (second.id, 1)]
    assert rows[1]['supersedes_id'] == first.id
    assert rows[1]['deal_type'] == 'unknown'
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_identity_id,source_post_revision_id,schema_version,parser_revision,facts_json,
               facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status,is_current
               ) VALUES (?,?,?,?,?,?,?,?,?,?,1)''',
            (identity_id, revision2.id, 'v3', 'r1', '{}', 'another', 'rent', '[]', '{}', 'parsed'),
        )


def test_canonical_revision_must_belong_to_same_source_identity():
    conn = migrated_connection()
    identity_a, _ = _source(conn, '100')
    _, revision_b = _source(conn, '200')
    with pytest.raises(sqlite3.IntegrityError, match='mismatch'):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_identity_id,source_post_revision_id,schema_version,parser_revision,facts_json,
               facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status
               ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (identity_a, revision_b, 'v3', 'r1', '{}', 'x', 'unknown', '[]', '{}', 'needs_review'),
        )
