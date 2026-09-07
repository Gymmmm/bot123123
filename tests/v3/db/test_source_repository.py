from __future__ import annotations

import sqlite3

import pytest

from qiaolian_v3.db.repositories import SourceRepository
from tests.v3.db._helpers import insert_source, migrated_connection


def test_source_identity_is_unique_and_same_identity_is_stable():
    conn = migrated_connection()
    legacy_id = insert_source(conn)
    repo = SourceRepository(conn)
    first = repo.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id='100',
    )
    again = repo.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id='100',
    )
    assert first == again
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            '''INSERT INTO source_post_identities(
               source_identity_key,legacy_source_post_id,source_type,source_name,external_post_id
               ) VALUES ('different-key',?,?,?,?)''',
            (legacy_id, 'telegram_channel', 'fixture', '100'),
        )


def test_same_hash_is_noop_new_hash_creates_next_immutable_revision():
    conn = migrated_connection()
    legacy_id = insert_source(conn)
    repo = SourceRepository(conn)
    identity_id = repo.register_identity(
        legacy_source_post_id=legacy_id,
        source_type='telegram_channel', source_name='fixture', external_post_id='100',
    )
    r1 = repo.append_revision(source_identity_id=identity_id, content_hash='h1', raw_text='old')
    same = repo.append_revision(source_identity_id=identity_id, content_hash='h1', raw_text='ignored')
    r2 = repo.append_revision(source_identity_id=identity_id, content_hash='h2', raw_text='new')
    assert same.id == r1.id
    assert (r1.revision_no, r2.revision_no) == (1, 2)
    assert repo.get_revision(r1.id)['raw_text'] == 'old'
    assert repo.get_revision(r2.id)['raw_text'] == 'new'
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('UPDATE source_post_revisions SET raw_text=? WHERE id=?', ('tamper', r1.id))
    with pytest.raises(sqlite3.IntegrityError, match='immutable'):
        conn.execute('DELETE FROM source_post_revisions WHERE id=?', (r1.id,))
