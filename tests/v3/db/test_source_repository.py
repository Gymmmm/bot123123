from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import SourceRepository
from tests.v3.db._helpers import migrated_connection


def test_v3_source_post_is_independent_and_identity_is_stable():
    conn = migrated_connection()
    repo = SourceRepository(conn)

    # No row is created in legacy source_posts. V3 SourcePost creation must still work.
    first = repo.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='fixture', external_post_id='100',
    )
    again = repo.register_source_post(
        source_mode='collector', source_type='telegram_channel',
        source_name='fixture', external_post_id='100',
    )
    assert first == again
    row = repo.get_source_post(first)
    assert row['legacy_source_post_id'] is None
    assert row['source_mode'] == 'collector'
    assert conn.execute('SELECT COUNT(*) FROM source_posts').fetchone()[0] == 0

    # There is deliberately no FK from the V3 identity to legacy source_posts.
    legacy_targets = {
        r['table'] for r in conn.execute("PRAGMA foreign_key_list('v3_source_posts')").fetchall()
    }
    assert 'source_posts' not in legacy_targets


def test_source_mode_is_db_locked_to_formal_values():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    for idx, mode in enumerate(('collector', 'admin_import', 'migration'), start=1):
        repo.register_source_post(
            source_mode=mode, source_type='fixture', source_name='mode', external_post_id=str(idx)
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO v3_source_posts(
               source_identity_key,source_mode,source_type,source_name,external_post_id
               ) VALUES ('bad','csv_import','fixture','mode','bad')'''
        )


def test_nullable_legacy_bridge_is_optional_not_creation_precondition():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_post_id = repo.register_source_post(
        source_mode='migration', source_type='legacy_bridge', source_name='fixture', external_post_id='200',
        legacy_source_post_id=999999,
    )
    # The compatibility id does not need to resolve to a legacy row.
    assert repo.get_source_post(source_post_id)['legacy_source_post_id'] == 999999
    assert conn.execute('SELECT COUNT(*) FROM source_posts').fetchone()[0] == 0


def test_same_hash_is_noop_new_hash_creates_next_immutable_revision_and_moves_current_pointer():
    conn = migrated_connection()
    repo = SourceRepository(conn)
    source_post_id = repo.register_source_post(
        source_mode='collector', source_type='telegram_channel', source_name='fixture', external_post_id='100'
    )
    r1 = repo.append_revision(
        source_post_id=source_post_id, source_content_hash='h1', raw_text='old', raw_payload={'v': 1}
    )
    same = repo.append_revision(
        source_post_id=source_post_id, source_content_hash='h1', raw_text='ignored'
    )
    r2 = repo.append_revision(
        source_post_id=source_post_id, source_content_hash='h2', raw_text='new', raw_payload={'v': 2}
    )
    assert same.id == r1.id
    assert (r1.revision_no, r2.revision_no) == (1, 2)
    assert repo.get_revision(r1.id)['raw_text'] == 'old'
    assert repo.get_revision(r2.id)['raw_text'] == 'new'
    assert repo.get_source_post(source_post_id)['current_revision_id'] == r2.id
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('UPDATE source_post_revisions SET raw_text=? WHERE id=?', ('tamper', r1.id))
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('DELETE FROM source_post_revisions WHERE id=?', (r1.id,))
