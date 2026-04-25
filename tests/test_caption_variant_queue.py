import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import autopilot_publish_bot
from test_media_consistency import _init_db, _seed_draft


def _add_bot_settings(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE bot_settings (
             setting_key TEXT PRIMARY KEY,
             setting_value TEXT,
             updated_at TEXT
           )"""
    )
    conn.commit()
    conn.close()


def _review_note(db_path: str, draft_id: str = "DRF_TEST") -> str:
    conn = sqlite3.connect(db_path)
    note = conn.execute("SELECT review_note FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()[0]
    conn.close()
    return note or ""


def _review_status(db_path: str, draft_id: str = "DRF_TEST") -> str:
    conn = sqlite3.connect(db_path)
    status = conn.execute("SELECT review_status FROM drafts WHERE draft_id=?", (draft_id,)).fetchone()[0]
    conn.close()
    return status


class _FakeUser:
    id = 123


class _FakeChat:
    id = 123


class _FakeQuery:
    def __init__(self, data: str):
        self.data = data
        self.answer = AsyncMock()
        self.edit_message_text = AsyncMock()
        self.edit_message_reply_markup = AsyncMock()


class _FakeUpdate:
    def __init__(self, data: str):
        self.callback_query = _FakeQuery(data)
        self.effective_user = _FakeUser()
        self.effective_chat = _FakeChat()


class _FakeContext:
    def __init__(self):
        self.bot = AsyncMock()


class CaptionVariantQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.tmp = Path(self.td.name)
        self.db_path = str(self.tmp / "test.db")
        _init_db(self.db_path)
        _add_bot_settings(self.db_path)
        self.old_db_path = autopilot_publish_bot.DB_PATH
        self.old_admin_ids = autopilot_publish_bot.ADMIN_IDS
        autopilot_publish_bot.DB_PATH = self.db_path
        autopilot_publish_bot.ADMIN_IDS = {123}

    def tearDown(self) -> None:
        autopilot_publish_bot.DB_PATH = self.old_db_path
        autopilot_publish_bot.ADMIN_IDS = self.old_admin_ids
        self.td.cleanup()

    def test_scheduled_publish_uses_saved_b_variant(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready")
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE drafts SET review_note='caption_variant:b' WHERE draft_id='DRF_TEST'")
        conn.commit()
        conn.close()
        calls = []

        class FakePublisher:
            def __init__(self, db_path):
                self.db_path = db_path

            def publish_draft(self, draft_id, variant="a"):
                calls.append((draft_id, variant))
                return True

        with patch("meihua_publisher.MeihuaPublisher", FakePublisher), patch(
            "autopilot_publish_bot._scheduler_paused", return_value=False
        ):
            asyncio.run(autopilot_publish_bot.scheduled_publish(_FakeContext()))

        self.assertEqual(calls, [("DRF_TEST", "b")])

    def test_scheduled_publish_defaults_to_a_without_saved_variant(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready")
        calls = []

        class FakePublisher:
            def __init__(self, db_path):
                self.db_path = db_path

            def publish_draft(self, draft_id, variant="a"):
                calls.append((draft_id, variant))
                return True

        with patch("meihua_publisher.MeihuaPublisher", FakePublisher), patch(
            "autopilot_publish_bot._scheduler_paused", return_value=False
        ):
            asyncio.run(autopilot_publish_bot.scheduled_publish(_FakeContext()))

        self.assertEqual(calls, [("DRF_TEST", "a")])

    def test_preview_b_then_queue_saves_b_variant(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="pending")
        context = _FakeContext()
        with patch("autopilot_publish_bot._send_visual_preview", new=AsyncMock()), patch(
            "autopilot_publish_bot._log_action"
        ):
            asyncio.run(autopilot_publish_bot.on_preview_callback(_FakeUpdate("ap:vb:10"), context))
            asyncio.run(autopilot_publish_bot.on_preview_callback(_FakeUpdate("ap:q:10"), context))

        self.assertIn("caption_variant:b", _review_note(self.db_path))
        self.assertEqual(_review_status(self.db_path), "ready")


if __name__ == "__main__":
    unittest.main()
