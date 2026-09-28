from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

from v3_core.ingest.intake_service import IntakeService, SourceIntake
from v3_core.ingest.source_repository import SourceRepository
from v3_core.publishing.simple_admin import NEW_LISTING_STATE_KEY
from v3_core.publishing.simple_admin_production import ProductionSimplePublisherAdminController
from v3_core.storage.bootstrap import initialize_v3_storage


def _images(count: int):
    return [
        {
            "local_path": f"/tmp/source-{index}.jpg",
            "file_hash": f"hash-{index}",
            "telegram_file_id": f"file-{index}",
            "telegram_file_unique_id": f"unique-{index}",
        }
        for index in range(count)
    ]


def test_same_source_repair_updates_original_source_post_in_place(tmp_path):
    db = tmp_path / "qiaolian.db"
    initialize_v3_storage(db)
    service = IntakeService(SourceRepository(db), min_listing_images=4)
    first = service.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="source-A",
            source_post_id="12345",
            source_url="https://t.me/c/1/12345",
            source_author="channel",
            raw_text="BKK1 两房 $800",
            raw_images=_images(4),
        )
    )
    repaired = service.persist(
        SourceIntake(
            source_type="telegram_channel",
            source_name="source-A",
            source_post_id="12345",
            source_url="https://t.me/c/1/12345",
            source_author="channel",
            raw_text="BKK1 两房 $800 补充实拍",
            raw_images=_images(5),
        )
    )
    assert first.status == "inserted"
    assert repaired.status == "updated"
    assert repaired.source_post_pk == first.source_post_pk
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0] == 1
        row = conn.execute("SELECT source_type,source_name,source_post_id,raw_text FROM source_posts").fetchone()
    assert row == ("telegram_channel", "source-A", "12345", "BKK1 两房 $800 补充实拍")


def test_exception_add_media_session_keeps_original_source_identity():
    controller = object.__new__(ProductionSimplePublisherAdminController)
    controller._exception_detail = lambda token: {
        "source_post_id": 17,
        "review_id": "REV_17",
    }
    controller.source_reader = SimpleNamespace(
        source_post=lambda source_post_id: {
            "id": 17,
            "source_id": "SRC_DB_7",
            "source_type": "telegram_channel",
            "source_name": "金边房源源A",
            "source_post_id": "9988",
            "source_url": "https://t.me/c/123/9988",
            "source_author": "channel",
            "raw_text": "原始房源",
            "raw_images": _images(3),
            "raw_meta": {"chat_id": "-100123"},
        }
    )

    class Message:
        def __init__(self):
            self.text = ""

        async def reply_text(self, text, **kwargs):
            self.text = text

    message = Message()
    context = SimpleNamespace(user_data={})
    asyncio.run(controller.append_exception_media(message, context, "review:REV_17"))
    state = context.user_data[NEW_LISTING_STATE_KEY]
    identity = state["repair_source_identity"]
    assert state["source_post_pk"] == 17
    assert state["session_id"] == "9988"
    assert identity == {
        "source_id": "SRC_DB_7",
        "source_type": "telegram_channel",
        "source_name": "金边房源源A",
        "source_post_id": "9988",
        "source_url": "https://t.me/c/123/9988",
        "source_author": "channel",
        "meta": {"chat_id": "-100123"},
    }
    assert "原房源身份" in message.text
