import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from qiaolian_dual.user_bot import FIND_AREA, FIND_BUDGET, MAIN, handle_ui_callback, start


def _mk_user(uid: int = 9001):
    return SimpleNamespace(id=uid, username=f"u{uid}", full_name=f"User {uid}")


def _mk_message():
    return SimpleNamespace(reply_text=AsyncMock())


def _mk_query(data: str):
    return SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=_mk_message(),
    )


class UserJourneyRehearsalTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_want_home_deeplink_routes_to_button_filter(self):
        user = _mk_user(9002)
        message = _mk_message()
        update = SimpleNamespace(effective_user=user, effective_message=message)
        context = SimpleNamespace(args=["want_home"], user_data={})

        with (
            patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None),
            patch("qiaolian_dual.user_bot.create_lead", Mock()) as create_lead,
        ):
            state = await start(update, context)

        self.assertEqual(state, MAIN)
        message.reply_text.assert_awaited()
        text = message.reply_text.await_args.args[0]
        self.assertIn("条件筛选", text)
        self.assertIn("点击选择", text)
        self.assertEqual(context.user_data["pref_select"]["source"], "channel_want_home")
        create_lead.assert_called()
        self.assertEqual(create_lead.call_args.kwargs.get("action"), "want_home_click")

    async def test_guided_find_flow_is_fully_button_driven(self):
        user = _mk_user(9003)
        context = SimpleNamespace(user_data={})

        sample_match = {
            "listing_id": "l_100",
            "area": "富力城",
            "price": 750,
            "layout": "1房1卫",
            "size_sqm": 65,
        }

        with (
            patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None),
            patch("qiaolian_dual.user_bot.create_lead", Mock()) as create_lead,
            patch("qiaolian_dual.user_bot._notify_admins", AsyncMock()),
            patch("qiaolian_dual.user_bot.db.search_listings", return_value=[sample_match]),
        ):
            q1 = _mk_query("findmode:guided")
            state1 = await handle_ui_callback(SimpleNamespace(callback_query=q1, effective_user=user), context)
            self.assertEqual(state1, MAIN)
            self.assertIn("下面直接点按钮", q1.edit_message_text.await_args.args[0])

            q2 = _mk_query("findtype:住宅")
            state2 = await handle_ui_callback(SimpleNamespace(callback_query=q2, effective_user=user), context)
            self.assertEqual(state2, FIND_AREA)
            self.assertIn("不需要手动输入", q2.edit_message_text.await_args.args[0])

            q3 = _mk_query("findarea:a1")
            state3 = await handle_ui_callback(SimpleNamespace(callback_query=q3, effective_user=user), context)
            self.assertEqual(state3, FIND_BUDGET)
            self.assertIn("第三步：请选择预算区间", q3.edit_message_text.await_args.args[0])

            q4 = _mk_query("findbudget:r3")
            state4 = await handle_ui_callback(SimpleNamespace(callback_query=q4, effective_user=user), context)
            self.assertEqual(state4, MAIN)
            q4.edit_message_text.assert_awaited()
            self.assertIn("已为您筛出更匹配的房源", q4.edit_message_text.await_args.args[0])

        self.assertGreaterEqual(create_lead.call_count, 1)

    async def test_old_user_repeat_entry_shows_binding_card_and_buttons(self):
        user = _mk_user(9004)
        context = SimpleNamespace(user_data={})
        query = _mk_query("profile:repeat")
        binding = {
            "property_name": "BKK1 The Peak 12A",
            "rent_day": 15,
            "lease_end_date": "2026-12-31",
        }

        with (
            patch("qiaolian_dual.user_bot.upsert_user_profile", return_value=None),
            patch("qiaolian_dual.user_bot.create_lead", Mock()),
            patch("qiaolian_dual.user_bot._notify_admins", AsyncMock()),
            patch("qiaolian_dual.user_bot.db.get_active_binding", return_value=binding),
        ):
            state = await handle_ui_callback(SimpleNamespace(callback_query=query, effective_user=user), context)

        self.assertEqual(state, MAIN)
        query.edit_message_text.assert_awaited()
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("BKK1 The Peak 12A", text)
        self.assertIn("每月 15 号", text)
        self.assertIn("2026-12-31", text)

        markup = query.edit_message_text.await_args.kwargs.get("reply_markup")
        button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
        self.assertIn("🏠 我要换房", button_texts)
        self.assertIn("💬 联系顾问", button_texts)
        self.assertIn("📅 预约看房", button_texts)


if __name__ == "__main__":
    unittest.main()
