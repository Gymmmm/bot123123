from __future__ import annotations

import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage
from v3_core.user_bot.channel_status_sync import V3AppointmentChannelSynchronizer


class FakeBot:
    def __init__(self):
        self.calls = []

    async def edit_message_caption(self, **kwargs):
        self.calls.append(kwargs)


def _seed(db_path, *, appointment_count=5):
    initialize_v3_storage(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,canonical_facts_hash,
                canonical_facts_schema,inventory_status)
               VALUES ('l_1','QL-RF-A2B3','cr_1','hash','v3','reserved')"""
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,channel_chat_id,
                channel_message_id,publish_status,post_text,published_at,updated_at)
               VALUES
               ('PUB_OLD','PKG_OLD','l_1','OFF_1','telegram','-100123','111','published',
                '🏠 <b>富力城｜两房</b>\n\n🟡 已有预约 · 仍可预约　QL-RF-A2B3',
                '2026-09-08 10:00:00','2026-09-08 10:00:00'),
               ('PUB_NEW','PKG_NEW','l_1','OFF_1','telegram','-100123','222','published',
                '🏠 <b>富力城｜两房</b>\n\n🟡 已有预约 · 仍可预约　QL-RF-A2B3',
                '2026-09-08 11:00:00','2026-09-08 11:00:00')"""
        )
        for index in range(appointment_count):
            conn.execute(
                """INSERT INTO appointments_v3
                   (user_id,listing_id,appointment_date,appointment_time,status)
                   VALUES (?,?,?,?,?)""",
                (100 + index, "l_1", "09-10", "pm", "pending"),
            )
        conn.commit()


@pytest.mark.asyncio
async def test_sync_edits_only_latest_exact_publication_message_and_locks_at_five(tmp_path):
    db_path = tmp_path / "v3.db"
    _seed(db_path, appointment_count=5)
    fake = FakeBot()
    sync = V3AppointmentChannelSynchronizer(
        db_path,
        publisher_bot_token="publisher-token",
        user_bot_username="qiaolian_rent_bot",
        bot_factory=lambda token: fake,
    )

    result = await sync.sync("l_1")

    assert result.synced
    assert result.channel_chat_id == "-100123"
    assert result.channel_message_id == "222"
    assert result.next_status == "pending"
    assert result.active_appointment_count == 5
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["chat_id"] == "-100123"
    assert call["message_id"] == 222
    assert "🔵 房态确认中" in call["caption"]
    buttons = [button.text for row in call["reply_markup"].inline_keyboard for button in row]
    assert buttons == ["🏠 房源详情", "📸 更多实拍"]

    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            "SELECT inventory_status FROM listings_v3 WHERE listing_id='l_1'"
        ).fetchone()[0]
        newest = conn.execute(
            "SELECT post_text FROM publication_instances WHERE instance_id='PUB_NEW'"
        ).fetchone()[0]
        old = conn.execute(
            "SELECT post_text FROM publication_instances WHERE instance_id='PUB_OLD'"
        ).fetchone()[0]
    assert status == "pending"
    assert "🔵 房态确认中" in newest
    assert "🔵 房态确认中" not in old
