import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from qiaolian_dual.user_bot import MAIN, handle_main_message, old_tenant_binding_text


class UserBotUxTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_keyword_text_is_routed_back_to_buttons(self):
        message = SimpleNamespace(text="我想租房", reply_text=AsyncMock())
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1001, username="u1", full_name="Test User"),
            effective_message=message,
        )
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None):
            state = await handle_main_message(update, context)

        self.assertEqual(state, MAIN)
        message.reply_text.assert_awaited()
        text = message.reply_text.await_args.args[0]
        self.assertIn("除「🎲 一句话找房」外", text)

    async def test_guided_pref_state_blocks_free_text(self):
        message = SimpleNamespace(text="BKK1", reply_text=AsyncMock())
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=1002, username="u2", full_name="Test User"),
            effective_message=message,
        )
        context = SimpleNamespace(user_data={"search_pref": {"goal": "住宅"}})

        with patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None):
            state = await handle_main_message(update, context)

        self.assertEqual(state, MAIN)
        message.reply_text.assert_awaited()
        text = message.reply_text.await_args.args[0]
        self.assertIn("按钮选择", text)

    def test_old_tenant_binding_text_reads_backend_binding(self):
        binding = {
            "property_name": "BKK1 The Peak 12A",
            "rent_day": 15,
            "lease_end_date": "2026-12-31",
        }
        with patch("qiaolian_dual.user_bot.db.get_active_binding", return_value=binding):
            text, row = old_tenant_binding_text(1003)

        self.assertIsNotNone(row)
        self.assertIn("BKK1 The Peak 12A", text)
        self.assertIn("每月 15 号", text)
        self.assertIn("2026-12-31", text)


if __name__ == "__main__":
    unittest.main()
