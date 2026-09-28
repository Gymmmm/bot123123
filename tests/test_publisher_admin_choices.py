from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
V2_ROOT = ROOT / "v2"
for path in (str(ROOT), str(V2_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import autopilot_publish_bot as autopilot
import qiaolian_publisher_v2.bot as publisher_bot_module
from qiaolian_publisher_v2.bot import PublisherBot
from qiaolian_publisher_v2.daily_broadcast_patch import install_daily_broadcast_patch


class PublisherAdminChoiceTests(unittest.TestCase):
    def test_publish_options_are_independent(self) -> None:
        self.assertEqual(autopilot._publish_options_from_note(""), ("none", "cover"))
        self.assertEqual(
            autopilot._publish_options_from_note("publish_layout:buttons | publish_cover:cover"),
            ("buttons", "cover"),
        )
        self.assertEqual(
            autopilot._publish_options_from_note("publish_layout:links | publish_cover:none"),
            ("none", "none"),
        )
        self.assertEqual(
            autopilot._publish_options_from_note("publish_layout:buttons | publish_cover:none"),
            ("buttons", "none"),
        )

    def test_production_daily_patch_replaces_weekly_scheduler(self) -> None:
        install_daily_broadcast_patch()
        self.assertEqual(autopilot.scheduled_daily_broadcast.__module__, "qiaolian_publisher_v2.daily_broadcast_patch")
        self.assertEqual(autopilot.cmd_daily.__module__, "qiaolian_publisher_v2.daily_broadcast_patch")
        self.assertEqual(autopilot.on_daily_callback.__module__, "qiaolian_publisher_v2.daily_broadcast_patch")

    def test_daily_info_renders_weather_and_usd_cny_minus_point_two(self) -> None:
        class FakeResponse:
            def __init__(self, payload: bytes):
                self.payload = payload

            def read(self) -> bytes:
                return self.payload

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        weather = b'{"daily":{"weather_code":[95],"temperature_2m_max":[31.1],"temperature_2m_min":[26.1],"precipitation_probability_max":[97]}}'
        exchange = b'{"rates":{"CNY":7.10}}'
        with patch.object(autopilot, "urlopen", side_effect=[FakeResponse(weather), FakeResponse(exchange)]):
            body = autopilot._fetch_phnom_penh_daily_info()
        self.assertIn("雷雨", body)
        self.assertIn("26–31℃", body)
        self.assertIn("97%", body)
        self.assertIn("1 USD ≈ 6.90 CNY", body)
        self.assertNotIn("KHR", body)

    def test_preview_keyboard_exposes_four_plain_language_choices(self) -> None:
        with patch.object(autopilot, "_conn", side_effect=sqlite3.OperationalError("test")):
            keyboard = autopilot._kb_preview(1)
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertTrue(any("带行动按钮" in label for label in labels))
        self.assertTrue(any("不带行动按钮" in label for label in labels))
        self.assertTrue(any("使用封面" in label for label in labels))
        self.assertTrue(any("不使用封面" in label for label in labels))

    def test_new_forward_origin_maps_channel_post_without_publish_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "publisher.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE posts (channel_message_id INTEGER, discuss_chat_id TEXT, discuss_thread_id TEXT, discuss_message_id TEXT, updated_at TEXT)"
                )
            settings = SimpleNamespace(sqlite_path=db_path, channel_id=-100123, admin_ids=[])
            publisher = PublisherBot(settings)
            publisher._channel_chat_id = -100123
            msg = SimpleNamespace(
                is_automatic_forward=True,
                sender_chat=None,
                forward_origin=SimpleNamespace(chat=SimpleNamespace(id=-100123), message_id=777),
                forward_from_message_id=None,
                message_thread_id=None,
                media_group_id=None,
                message_id=888,
                chat_id=-100456,
            )
            update = SimpleNamespace(effective_message=msg)
            context = SimpleNamespace(bot=SimpleNamespace())
            saved_map = {}
            with patch.object(publisher_bot_module, "load_discussion_bridge", return_value={"publish_queue": [], "discuss_mgid": {}}), patch.object(
                publisher_bot_module, "save_discussion_bridge"
            ), patch.object(publisher_bot_module, "load_discuss_map", return_value={}), patch.object(
                publisher_bot_module, "save_discuss_map", side_effect=lambda value: saved_map.update(value)
            ):
                asyncio.run(publisher.capture_discussion_forward(update, context))
            self.assertEqual(saved_map, {"777": 888})


if __name__ == "__main__":
    unittest.main()
