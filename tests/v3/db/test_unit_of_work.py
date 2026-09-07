from __future__ import annotations

from qiaolian_v3.db.unit_of_work import UnitOfWork
from tests.v3.db._helpers import migrated_connection


def test_unit_of_work_rolls_back_without_explicit_commit():
    conn = migrated_connection()
    with UnitOfWork(conn) as uow:
        uow.sources.register_source_post(
            source_mode='collector', source_type='telegram_channel',
            source_name='fixture', external_post_id='100',
        )
    assert conn.execute('SELECT COUNT(*) FROM v3_source_posts').fetchone()[0] == 0


def test_unit_of_work_commits_repository_changes_explicitly():
    conn = migrated_connection()
    with UnitOfWork(conn) as uow:
        uow.sources.register_source_post(
            source_mode='collector', source_type='telegram_channel',
            source_name='fixture', external_post_id='100',
        )
        uow.commit()
    assert conn.execute('SELECT COUNT(*) FROM v3_source_posts').fetchone()[0] == 1
