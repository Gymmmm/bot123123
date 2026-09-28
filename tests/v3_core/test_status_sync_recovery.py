import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, NetworkError

from v3_core.publishing.manual_status_sync import PublisherManualStatusSynchronizer


def setup(tmp_path):
    db = tmp_path / 'status.sqlite3'
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE listings_v3(listing_id TEXT PRIMARY KEY,public_listing_id TEXT,inventory_status TEXT);
            CREATE TABLE publication_packages_v3(package_id TEXT PRIMARY KEY,listing_id TEXT,snapshot_json TEXT,actions_json TEXT);
            CREATE TABLE publication_instances(id INTEGER PRIMARY KEY,package_id TEXT,listing_id TEXT,platform TEXT,
                publish_status TEXT,channel_chat_id TEXT,channel_message_id TEXT,post_text TEXT,updated_at TEXT);
            INSERT INTO listings_v3 VALUES('l1','QL-RF-A2B3','active');
        """)
        snapshot = json.dumps({"listing_id":"l1","public_listing_id":"QL-RF-A2B3"})
        actions = json.dumps({
            "details":"https://t.me/test_bot?start=property_QL-RF-A2B3_details",
            "photos":"https://t.me/test_bot?start=property_QL-RF-A2B3_photos",
            "book":"https://t.me/test_bot?start=property_QL-RF-A2B3_book",
        })
        conn.execute("INSERT INTO publication_packages_v3 VALUES('pkg1','l1',?,?)",(snapshot,actions))
        conn.execute("INSERT INTO publication_packages_v3 VALUES('pkg2','l1',?,?)",(snapshot,actions))
        conn.execute("INSERT INTO publication_instances VALUES(1,'pkg1','l1','telegram','published','-1001','10','🟢 当前可预约　QL-RF-A2B3','')")
        conn.execute("INSERT INTO publication_instances VALUES(2,'pkg2','l1','telegram','published','-1001','11','🟢 当前可预约　QL-RF-A2B3','')")
    return db, PublisherManualStatusSynchronizer(db, user_bot_username='test_bot')


@pytest.mark.asyncio
async def test_status_change_is_durable_and_syncs_every_bound_post(tmp_path):
    db, sync = setup(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE listings_v3 SET inventory_status='rented'")
    bot = SimpleNamespace(edit_message_caption=AsyncMock(side_effect=NetworkError('offline')))
    await sync.scheduled_tick(SimpleNamespace(bot=bot))
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT attempts FROM publisher_status_sync_outbox_v3').fetchone()[0] == 1
        conn.execute("UPDATE listings_v3 SET inventory_status='pending'")
    restarted = PublisherManualStatusSynchronizer(db, user_bot_username='test_bot')
    bot.edit_message_caption = AsyncMock()
    await restarted.scheduled_tick(SimpleNamespace(bot=bot))
    assert [c.kwargs['message_id'] for c in bot.edit_message_caption.call_args_list] == [10, 11]
    assert all('房态确认中' in c.kwargs['caption'] for c in bot.edit_message_caption.call_args_list)
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT count(*) FROM publisher_status_sync_outbox_v3').fetchone()[0] == 0


@pytest.mark.asyncio
async def test_not_modified_acknowledges_retry(tmp_path):
    db, sync = setup(tmp_path)
    bot = SimpleNamespace(edit_message_caption=AsyncMock(side_effect=BadRequest('Message is not modified')))
    assert (await sync.sync(bot, listing_id='l1', status='rented')).synced
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT count(*) FROM publisher_status_sync_outbox_v3').fetchone()[0] == 0


@pytest.mark.asyncio
async def test_status_changed_during_send_keeps_new_revision(tmp_path):
    db, sync = setup(tmp_path)
    async def change(**kwargs):
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE listings_v3 SET inventory_status='rented'")
    bot = SimpleNamespace(edit_message_caption=AsyncMock(side_effect=change))
    await sync.sync(bot, listing_id='l1', status='active')
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT count(*) FROM publisher_status_sync_outbox_v3').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_manual_status_sync_blocks_frozen_identity_mismatch_before_edit(tmp_path):
    db, sync = setup(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE publication_packages_v3 SET actions_json=? WHERE package_id='pkg2'",
            (json.dumps({
                "details":"https://t.me/test_bot?start=property_QL-XX-Z9Z9_details",
                "photos":"https://t.me/test_bot?start=property_QL-XX-Z9Z9_photos",
                "book":"https://t.me/test_bot?start=property_QL-XX-Z9Z9_book",
            }),),
        )
        conn.commit()
    bot = SimpleNamespace(edit_message_caption=AsyncMock())
    result = await sync.sync(bot, listing_id='l1', status='rented')
    assert result.attempted
    assert not result.synced
    assert "channel_" in result.error
    assert bot.edit_message_caption.call_count == 0
