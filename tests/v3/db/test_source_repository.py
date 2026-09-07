from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import SourceRepository
from tests.v3.db._helpers import migrated_connection


def test_v3_source_registry_owns_source_post_fk_and_is_independent_from_legacy():
    conn = migrated_connection()
    repo = SourceRepository(conn)

    source_id = repo.register_source(
        source_type='telegram_channel',
        source_name='fixture channel',
        external_identity='-100123456',
        enabled=True,
        collector_config={'min_images': 4},
    )
    source_post_id = repo.register_source_post(
        source_id=source_id,
        source_mode='collector',
        source_type='telegram_channel',
        source_name='fixture channel',
        external_post_id='100',
    )

    source = repo.get_source(source_id)
    post = repo.get_source_post(source_post_id)
    assert source['source_type'] == 'telegram_channel'
    assert source['external_identity'] == '-100123456'
    assert source['enabled'] == 1
    assert source['collector_config_json'] == '{"min_images": 4}'
    assert post['source_id'] == source_id
    assert conn.execute('SELECT COUNT(*) FROM source_posts').fetchone()[0] == 0

    fk_targets = {
        r['table'] for r in conn.execute("PRAGMA foreign_key_list('v3_source_posts')").fetchall()
    }
    assert 'v3_sources' in fk_targets
    assert 'source_posts' not in fk_targets

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_source_posts(
               source_id,source_identity_key,source_mode,source_type,source_name,external_post_id
               ) VALUES (999999,'missing-source','collector','telegram_channel','missing','1')'''
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute('DELETE FROM v3_sources WHERE id=?', (source_id,))


def test_source_post_identity_is_stable_and_tracks_first_last_seen_and_status():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_id = repo.register_source(
        source_type='telegram_channel', source_name='fixture', external_identity='fixture-channel'
    )

    first = repo.register_source_post(
        source_id=source_id,
        source_mode='collector',
        source_type='telegram_channel',
        source_name='fixture',
        external_post_id='100',
        first_seen_at='2026-09-01T10:00:00Z',
        last_seen_at='2026-09-01T10:00:00Z',
        status='active',
    )
    again = repo.register_source_post(
        source_id=source_id,
        source_mode='collector',
        source_type='telegram_channel',
        source_name='fixture',
        external_post_id='100',
        first_seen_at='2026-09-07T11:00:00Z',
        last_seen_at='2026-09-07T11:00:00Z',
        status='seen_again',
    )
    assert first == again
    row = repo.get_source_post(first)
    assert row['source_id'] == source_id
    assert row['first_seen_at'] == '2026-09-01T10:00:00Z'
    assert row['last_seen_at'] == '2026-09-07T11:00:00Z'
    assert row['status'] == 'seen_again'


def test_source_mode_is_db_locked_to_formal_values():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_id = repo.register_source(
        source_type='fixture', source_name='mode', external_identity='mode-source'
    )
    for idx, mode in enumerate(('collector', 'admin_import', 'migration'), start=1):
        repo.register_source_post(
            source_id=source_id,
            source_mode=mode,
            source_type='fixture', source_name='mode', external_post_id=str(idx),
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_source_posts(
               source_id,source_identity_key,source_mode,source_type,source_name,external_post_id
               ) VALUES (?,'bad','csv_import','fixture','mode','bad')''',
            (source_id,),
        )


def test_nullable_legacy_bridge_is_optional_not_creation_precondition():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_post_id = repo.register_source_post(
        source_mode='migration', source_type='legacy_bridge', source_name='fixture', external_post_id='200',
        legacy_source_post_id=999999,
    )
    row = repo.get_source_post(source_post_id)
    assert row['legacy_source_post_id'] == 999999
    assert row['source_id'] is not None
    assert repo.get_source(row['source_id']) is not None
    assert conn.execute('SELECT COUNT(*) FROM source_posts').fetchone()[0] == 0


def test_revision_persists_source_created_and_fetched_times_and_same_hash_is_noop():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_post_id = repo.register_source_post(
        source_mode='collector', source_type='telegram_channel', source_name='fixture', external_post_id='100'
    )
    r1 = repo.append_revision(
        source_post_id=source_post_id,
        source_content_hash='h1',
        raw_text='old',
        raw_payload={'v': 1},
        source_created_at='2026-09-01T09:30:00Z',
        fetched_at='2026-09-01T10:00:00Z',
    )
    same = repo.append_revision(
        source_post_id=source_post_id,
        source_content_hash='h1',
        raw_text='ignored',
        source_created_at='2099-01-01T00:00:00Z',
        fetched_at='2099-01-01T00:00:01Z',
    )
    assert same.id == r1.id
    assert same.source_created_at == '2026-09-01T09:30:00Z'
    assert same.fetched_at == '2026-09-01T10:00:00Z'
    stored = repo.get_revision(r1.id)
    assert stored['source_created_at'] == '2026-09-01T09:30:00Z'
    assert stored['fetched_at'] == '2026-09-01T10:00:00Z'
    assert stored['raw_text'] == 'old'


def test_new_hash_creates_next_immutable_revision_and_moves_current_pointer():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_post_id = repo.register_source_post(
        source_mode='collector', source_type='telegram_channel', source_name='fixture', external_post_id='100'
    )
    r1 = repo.append_revision(
        source_post_id=source_post_id,
        source_content_hash='h1',
        raw_text='old',
        fetched_at='2026-09-01T10:00:00Z',
    )
    r2 = repo.append_revision(
        source_post_id=source_post_id,
        source_content_hash='h2',
        raw_text='new',
        source_created_at='2026-09-07T10:30:00Z',
        fetched_at='2026-09-07T11:00:00Z',
    )
    assert (r1.revision_no, r2.revision_no) == (1, 2)
    assert repo.get_revision(r2.id)['source_created_at'] == '2026-09-07T10:30:00Z'
    assert repo.get_revision(r2.id)['fetched_at'] == '2026-09-07T11:00:00Z'
    assert repo.get_source_post(source_post_id)['current_revision_id'] == r2.id
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('UPDATE source_post_revisions SET fetched_at=? WHERE id=?', ('tamper', r1.id))
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('DELETE FROM source_post_revisions WHERE id=?', (r1.id,))
