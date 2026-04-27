"""Tests for the 周边生活 → 富力周边 navigation callbacks and lead tracking."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from qiaolian_dual.user_bot import MAIN, handle_ui_callback


def _mk_user(uid: int = 8001):
    return SimpleNamespace(id=uid, username=f"u{uid}", full_name=f"User {uid}")


def _mk_query(data: str):
    return SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=SimpleNamespace(reply_text=AsyncMock()),
    )


class LocalLifeNavigationTests(unittest.IsolatedAsyncioTestCase):
    """Tests for service:local_life → local:rfcity → rfcity:* flow."""

    async def test_service_local_life_shows_local_life_page(self):
        """service:local_life callback renders 周边生活 text and keyboard."""
        user = _mk_user(8002)
        query = _mk_query("service:local_life")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        query.edit_message_text.assert_awaited_once()
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("周边生活", text)
        # Keyboard should contain 富力周边 button
        markup = query.edit_message_text.await_args.kwargs.get("reply_markup")
        button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
        self.assertIn("🏙 富力周边", button_texts)

    async def test_local_rfcity_shows_rfcity_overview(self):
        """local:rfcity callback renders R&F City overview text and category keyboard."""
        user = _mk_user(8003)
        query = _mk_query("local:rfcity")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        query.edit_message_text.assert_awaited_once()
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("R&amp;F City", text)
        # Verify lead is recorded with correct payload
        mock_lead.assert_called_once()
        call_kwargs = mock_lead.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "local_area_click")
        self.assertEqual(call_kwargs.get("area"), "rfcity")
        # Keyboard should have all 9 category buttons
        markup = query.edit_message_text.await_args.kwargs.get("reply_markup")
        button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
        self.assertIn("🍴 餐厅小吃", button_texts)
        self.assertIn("🛒 超市便利", button_texts)
        self.assertIn("🤝 商家入驻", button_texts)

    async def test_rfcity_restaurant_category_shows_merchant_list(self):
        """rfcity:restaurant callback renders restaurant merchant list and back keyboard."""
        user = _mk_user(8004)
        query = _mk_query("rfcity:restaurant")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        query.edit_message_text.assert_awaited_once()
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("餐厅", text)
        # Verify lead records category correctly
        call_kwargs = mock_lead.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "local_category_click")
        self.assertEqual(call_kwargs.get("area"), "rfcity")
        self.assertEqual(call_kwargs["payload"]["category"], "restaurant")
        # Keyboard should have back-to-rfcity button
        markup = query.edit_message_text.await_args.kwargs.get("reply_markup")
        button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
        self.assertIn("↩️ 返回富力周边", button_texts)

    async def test_rfcity_bbq_category_shows_bbq_text(self):
        """rfcity:bbq callback renders BBQ merchant list."""
        user = _mk_user(8005)
        query = _mk_query("rfcity:bbq")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("烧烤", text)
        self.assertEqual(mock_lead.call_args.kwargs["payload"]["category"], "bbq")

    async def test_rfcity_supermarket_category_shows_supermarket_text(self):
        """rfcity:supermarket callback renders supermarket merchant list."""
        user = _mk_user(8006)
        query = _mk_query("rfcity:supermarket")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("超市", text)
        self.assertEqual(mock_lead.call_args.kwargs["payload"]["category"], "supermarket")

    async def test_rfcity_join_shows_merchant_join_page(self):
        """rfcity:join callback renders 商家入驻 page and merchant join keyboard."""
        user = _mk_user(8007)
        query = _mk_query("rfcity:join")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        query.edit_message_text.assert_awaited_once()
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("商家合作", text)
        # Lead should record category=join
        call_kwargs = mock_lead.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "local_category_click")
        self.assertEqual(call_kwargs["payload"]["category"], "join")
        # Keyboard should have 返回富力周边 button
        markup = query.edit_message_text.await_args.kwargs.get("reply_markup")
        button_texts = [btn.text for row in markup.inline_keyboard for btn in row]
        self.assertIn("🏙 返回富力周边", button_texts)

    async def test_rfcity_unknown_category_falls_back_to_rfcity_overview(self):
        """rfcity:<unknown> callback falls back to the rfcity overview page."""
        user = _mk_user(8008)
        query = _mk_query("rfcity:nonexistent_cat")
        update = SimpleNamespace(callback_query=query, effective_user=user)
        context = SimpleNamespace(user_data={})

        with patch("qiaolian_dual.user_bot.create_lead", Mock()):
            state = await handle_ui_callback(update, context)

        self.assertEqual(state, MAIN)
        text = query.edit_message_text.await_args.args[0]
        self.assertIn("R&amp;F City", text)

    async def test_rfcity_lead_payload_contains_area_and_category(self):
        """Lead payload for all rfcity category clicks includes area and category fields."""
        user = _mk_user(8009)
        for category in ("restaurant", "bbq", "drinks", "supermarket", "hotel", "recreation", "logistics", "property"):
            query = _mk_query(f"rfcity:{category}")
            update = SimpleNamespace(callback_query=query, effective_user=user)
            context = SimpleNamespace(user_data={})

            with patch("qiaolian_dual.user_bot.create_lead", Mock()) as mock_lead:
                state = await handle_ui_callback(update, context)

            self.assertEqual(state, MAIN, msg=f"rfcity:{category} should return MAIN")
            payload = mock_lead.call_args.kwargs.get("payload", {})
            self.assertEqual(payload.get("area"), "rfcity", msg=f"area mismatch for {category}")
            self.assertEqual(payload.get("category"), category, msg=f"category mismatch for {category}")


if __name__ == "__main__":
    unittest.main()
