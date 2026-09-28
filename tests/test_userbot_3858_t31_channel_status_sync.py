from __future__ import annotations

import json
import sqlite3

import pytest

from v3_core.storage.bootstrap import initialize_v3_storage

from qiaolian_dual.db import Database
import qiaolian_dual.channel_status_sync as sync


PUBLIC_ID = "QL-PP-D2W2"
INTERNAL_ID = "l_15"


def _seed(path):
    snapshot = {
        "schema": "v3_publication_snapshot.v1",
        "listing_id": INTERNAL_ID,
        "public_listing_id": PUBLIC_ID,
        "listing": {"inventory_status": "active"},
        "offer": {"offer_type": "rent", "publication_policy": "telegram_rent"},
        "canonical_facts": {"display_title": "炳发城｜4房5卫｜别墅"},
    }
    with sqlite3.connect(path) as conn:
        conn.execute(
            """INSERT INTO canonical_records
               (canonical_record_id,source_post_id,schema_version,parser_revision,
                deal_type,facts_json,facts_hash,quality_score,quality_json,review_status)
               VALUES ('CAN_1','SRC_1','canonical_facts.v1','v1.3','rent','{}','hash-1',100,'{}','approved')"""
        )
        conn.execute(
            """INSERT INTO listings_v3
               (listing_id,public_listing_id,canonical_record_id,project_name,
                property_type,public_location_key,public_location_display,
                layout,canonical_facts_hash,canonical_facts_schema,inventory_status)
               VALUES (?,?,'CAN_1','炳发城','别墅','炳发城','炳发城',
                       '4房5卫','hash-1','canonical_facts.v1','active')""",
            (INTERNAL_ID, PUBLIC_ID),
        )
        conn.execute(
            """INSERT INTO listing_offers
               (offer_id,listing_id,offer_type,currency,monthly_rent_usd,
                offer_status,publication_policy,publishable,publish_block_reason)
               VALUES ('OFF_1',?,'rent','USD',1300,'active','telegram_rent',1,'')""",
            (INTERNAL_ID,),
        )
        conn.execute(
            """INSERT INTO publication_packages_v3
               (package_id,listing_id,offer_id,canonical_record_id,package_version,status,
                cover_style,cover_path,gallery_json,post_text,actions_json,snapshot_json,
                frozen_file_hashes_json,source_identity_json,public_token,
                canonical_facts_hash,content_hash)
               VALUES ('PKG_1',?,'OFF_1','CAN_1',1,'published',
                       'classic_blue','/frozen/cover.jpg','[]',?,'{}',?,
                       '{}','{}','token-1','hash-1','content-1')""",
            (
                INTERNAL_ID,
                "🏡 炳发城｜4房5卫\n\n🟢 当前可预约　QL-PP-D2W2",
                json.dumps(snapshot, ensure_ascii=False),
            ),
        )
        conn.execute(
            """INSERT INTO publication_instances
               (instance_id,package_id,listing_id,offer_id,platform,
                channel_chat_id,channel_message_id,publish_status,post_text)
               VALUES ('PUB_1','PKG_1',?,'OFF_1','telegram',
                       '@Jinbianzufanz','3246','published',?)""",
            (
                INTERNAL_ID,
                "🏡 炳发城｜4房5卫\n\n🟢 当前可预约　QL-PP-D2W2",
            ),
        )
        conn.execute(
            """INSERT INTO appointments
               (user_id,username,display_name,listing_id,viewing_mode,
                appointment_date,appointment_time,contact_value,note,status,created_at)
               VALUES (1001,'tester','Tester',?,'offline','09-23','pm',
                       '@tester','','pending','2026-09-22 20:00:00')""",
            (INTERNAL_ID,),
        )
        conn.commit()


class FakeBot:
    calls = []

    def __init__(self, token):
        self.token = token

    async def edit_message_caption(self, **kwargs):
        self.__class__.calls.append(kwargs)


@pytest.mark.asyncio
async def test_modern_channel_sync_uses_publication_instances_and_does_not_mutate_inventory(tmp_path, monkeypatch):
    path = tmp_path / "sync.db"
    initialize_v3_storage(path)
    Database(path)
    _seed(path)

    monkeypatch.setattr(sync, "DB_PATH", str(path))
    monkeypatch.setattr(sync, "USER_BOT_USERNAME", "XxxXiaopengbot")
    monkeypatch.setattr(sync, "Bot", FakeBot)
    monkeypatch.setenv("PUBLISHER_BOT_TOKEN", "fake-token")
    FakeBot.calls.clear()

    ok = await sync.sync_channel_listing_status(INTERNAL_ID)

    assert ok is True
    assert len(FakeBot.calls) == 1
    call = FakeBot.calls[0]
    assert call["chat_id"] == "@Jinbianzufanz"
    assert call["message_id"] == 3246
    assert "🟡 已有预约 · 仍可预约" in call["caption"]
    assert PUBLIC_ID in call["caption"]

    buttons = call["reply_markup"].inline_keyboard
    urls = [button.url for row in buttons for button in row if button.url]
    assert any(f"property_{PUBLIC_ID}_photos" in url for url in urls)
    assert any(f"property_{PUBLIC_ID}_book" in url for url in urls)
    assert any(f"property_{PUBLIC_ID}_contact" in url for url in urls)
    button_texts = [button.text for row in buttons for button in row]
    assert button_texts == ["📷 更多实拍", "📅 预约看房", "💬 中文顾问"]

    with sqlite3.connect(path) as conn:
        inventory_status = conn.execute(
            "SELECT inventory_status FROM listings_v3 WHERE listing_id=?",
            (INTERNAL_ID,),
        ).fetchone()[0]
        post_text = conn.execute(
            "SELECT post_text FROM publication_instances WHERE listing_id=?",
            (INTERNAL_ID,),
        ).fetchone()[0]

    assert inventory_status == "active"
    assert "🟡 已有预约 · 仍可预约" in post_text


def test_channel_status_sync_has_no_legacy_post_join_or_durable_status_update():
    source = open("qiaolian_dual/channel_status_sync.py", encoding="utf-8").read()

    assert "FROM posts p" not in source
    assert "publication_package_id" not in source
    assert "UPDATE listings SET status" not in source
    assert "UPDATE listings_v3 SET inventory_status" not in source
    assert "FROM publication_instances pi" in source
