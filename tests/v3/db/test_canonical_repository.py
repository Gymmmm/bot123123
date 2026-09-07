from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import CanonicalRecordRepository, SourceRepository
from tests.v3.db._helpers import migrated_connection


def _source(conn, external_id='100'):
    sources = SourceRepository(conn)
    source_post_id = sources.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='fixture', external_post_id=external_id,
    )
    revision = sources.append_revision(
        source_post_id=source_post_id, source_content_hash=f'h-{external_id}', raw_text='evidence'
    )
    return source_post_id, revision.id


def test_deal_type_db_contract_rejects_mixed():
    conn = migrated_connection()
    source_post_id, revision_id = _source(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_post_id,source_post_revision_id,schema_version,parser_revision,
               facts_json,facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status
               ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (source_post_id, revision_id, 'v3', 'test', '{}', 'hash', 'mixed', '["rent","sale"]', '{}', 'NEEDS_REVIEW'),
        )


@pytest.mark.parametrize('status', [
    'COLLECTED','PARSING','PARSED','STORED','NEEDS_REVIEW',
    'READY_TO_PUBLISH','PUBLISHING','PUBLISHED','REJECTED','FAILED',
])
def test_processing_status_supports_formal_lifecycle(status: str):
    conn = migrated_connection()
    source_post_id, revision_id = _source(conn, external_id=status)
    row_id = conn.execute(
        '''INSERT INTO canonical_records(
           source_post_id,source_post_revision_id,schema_version,parser_revision,
           facts_json,facts_hash,deal_type,processing_status
           ) VALUES (?,?,?,?,?,?,?,?)''',
        (source_post_id, revision_id, 'v3', 'test', '{}', f'hash-{status}', 'unknown', status),
    ).lastrowid
    assert conn.execute('SELECT processing_status FROM canonical_records WHERE id=?', (row_id,)).fetchone()[0] == status


def test_superseded_is_not_a_processing_lifecycle_state():
    conn = migrated_connection()
    source_post_id, revision_id = _source(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_post_id,source_post_revision_id,schema_version,parser_revision,
               facts_json,facts_hash,deal_type,processing_status
               ) VALUES (?,?,?,?,?,?,?,?)''',
            (source_post_id, revision_id, 'v3', 'test', '{}', 'hash', 'unknown', 'superseded'),
        )


def test_repository_supersedes_by_lineage_only_and_keeps_prior_lifecycle_state():
    conn = migrated_connection()
    source_post_id, revision_id = _source(conn)
    repo = CanonicalRecordRepository(conn)
    first = repo.append(
        source_post_id=source_post_id, source_post_revision_id=revision_id,
        schema_version='v3', parser_revision='r1', facts={'a': 1}, facts_hash='f1',
        deal_type='rent', processing_status='STORED',
    )
    sources = SourceRepository(conn)
    revision2 = sources.append_revision(
        source_post_id=source_post_id, source_content_hash='h2', raw_text='changed evidence'
    )
    second = repo.append(
        source_post_id=source_post_id, source_post_revision_id=revision2.id,
        schema_version='v3', parser_revision='r1', facts={'a': 2}, facts_hash='f2', deal_type='unknown',
        deal_type_candidates=['rent','sale'], processing_status='NEEDS_REVIEW',
    )
    rows = conn.execute(
        '''SELECT id,is_current,supersedes_id,deal_type,processing_status
           FROM canonical_records WHERE source_post_id=? ORDER BY id''',
        (source_post_id,),
    ).fetchall()
    assert [(r['id'], r['is_current']) for r in rows] == [(first.id, 0), (second.id, 1)]
    assert rows[0]['processing_status'] == 'STORED'
    assert rows[1]['supersedes_id'] == first.id
    assert rows[1]['deal_type'] == 'unknown'
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,
               facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status,is_current
               ) VALUES (?,?,?,?,?,?,?,?,?,?,1)''',
            (source_post_id, revision2.id, 'v3', 'r1', '{}', 'another', 'rent', '[]', '{}', 'PARSED'),
        )


def test_canonical_revision_must_belong_to_same_source_post():
    conn = migrated_connection()
    source_a, _ = _source(conn, '100')
    _, revision_b = _source(conn, '200')
    with pytest.raises(sqlite3.IntegrityError, match='mismatch'):
        conn.execute(
            '''INSERT INTO canonical_records(
               source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,
               facts_hash,deal_type,deal_type_candidates_json,quality_json,processing_status
               ) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (source_a, revision_b, 'v3', 'r1', '{}', 'x', 'unknown', '[]', '{}', 'NEEDS_REVIEW'),
        )
