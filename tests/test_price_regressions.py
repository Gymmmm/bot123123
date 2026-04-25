import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from qiaolian_dual.user_bot import MAIN, _fmt_price, start
from v2_admin.house_cover_v2 import _norm_price as cover_norm_price


def _mk_user(uid: int = 9101):
    return SimpleNamespace(id=uid, username=f"u{uid}", full_name=f"User {uid}")


def _mk_message():
    return SimpleNamespace(reply_text=AsyncMock())


class UserBotPriceRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_fmt_price_uses_pending_copy_for_invalid_values(self):
        cases = [None, "", 0, "0", "咨询", "价格可咨询", "$0/月"]
        for raw in cases:
            with self.subTest(raw=raw):
                self.assertEqual(_fmt_price(raw), "价格待确认")

    def test_fmt_price_formats_positive_price_only(self):
        cases = [
            (680, "$680/月"),
            ("680", "$680/月"),
            ("$680/月", "$680/月"),
            ("USD 680", "$680/月"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(_fmt_price(raw), expected)

    async def test_start_with_binding_replies_once(self):
        user = _mk_user(9102)
        message = _mk_message()
        update = SimpleNamespace(effective_user=user, effective_message=message)
        context = SimpleNamespace(args=[], user_data={})
        binding = {
            "property_name": "BKK1 The Peak 12A",
            "lease_end_date": "2026-12-31",
        }

        with (
            patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None),
            patch("qiaolian_dual.user_bot.clear_session_for_fresh_entry", return_value=None),
            patch("qiaolian_dual.user_bot.db.get_active_binding", return_value=binding),
        ):
            state = await start(update, context)

        self.assertEqual(state, MAIN)
        self.assertEqual(message.reply_text.await_count, 1)

    async def test_start_for_new_user_replies_once(self):
        user = _mk_user(9103)
        message = _mk_message()
        update = SimpleNamespace(effective_user=user, effective_message=message)
        context = SimpleNamespace(args=[], user_data={})

        with (
            patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None),
            patch("qiaolian_dual.user_bot.clear_session_for_fresh_entry", return_value=None),
            patch("qiaolian_dual.user_bot.db.get_active_binding", return_value=None),
        ):
            state = await start(update, context)

        self.assertEqual(state, MAIN)
        self.assertEqual(message.reply_text.await_count, 1)


class HouseCoverPriceRegressionTests(unittest.TestCase):
    def test_cover_price_uses_pending_copy_for_zero_or_empty(self):
        cases = [None, "", 0, "0", "$0/月", "咨询", "价格可咨询"]
        for raw in cases:
            with self.subTest(raw=raw):
                self.assertEqual(cover_norm_price(raw), ("价格待确认", ""))

    def test_cover_price_formats_positive_values(self):
        cases = [
            (680, ("$680", "/月")),
            ("680", ("$680", "/月")),
            ("$680/月", ("$680", "/月")),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(cover_norm_price(raw), expected)


if __name__ == "__main__":
    unittest.main()
