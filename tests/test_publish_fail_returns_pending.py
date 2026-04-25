import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import autopilot_publish_bot
from test_caption_variant_queue import _add_bot_settings
from test_media_consistency import _init_db, _seed_draft


class _FakeContext:
    def __init__(self):
        self.bot = AsyncMock()


class PublishFailReturnsPendingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.tmp = Path(self.td.name)
        self.db_path = str(self.tmp / "test.db")
        _init_db(self.db_path)
        _add_bot_settings(self.db_path)
        self.old_db_path = autopilot_publish_bot.DB_PATH
        autopilot_publish_bot.DB_PATH = self.db_path

    def tearDown(self) -> None:
        autopilot_publish_bot.DB_PATH = self.old_db_path
        self.td.cleanup()

    def test_publish_fail_returns_pending(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready")

        class FakePublisher:
            def __init__(self, db_path):
                self.db_path = db_path

            def publish_draft(self, draft_id, variant="a"):
                return False

        with patch("meihua_publisher.MeihuaPublisher", FakePublisher), patch(
            "autopilot_publish_bot._scheduler_paused", return_value=False
        ):
            asyncio.run(autopilot_publish_bot.scheduled_publish(_FakeContext()))

        conn = sqlite3.connect(self.db_path)
        review_status, review_note = conn.execute(
            "SELECT review_status, review_note FROM drafts WHERE draft_id='DRF_TEST'"
        ).fetchone()
        conn.close()

        self.assertEqual(review_status, "pending")
        self.assertIn("publish_gate_blocked", review_note)


if __name__ == "__main__":
    unittest.main()
