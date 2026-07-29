import unittest
from unittest.mock import patch

from qiaolian_dual.messages import lead_capture_text
from qiaolian_dual.user_bot import (
    lead_capture_keyboard,
    search_results_keyboard,
    welcome_text,
)


class InteractionUxTests(unittest.TestCase):
    def test_search_results_are_directly_openable(self):
        keyboard = search_results_keyboard(
            [
                {
                    "listing_id": "l_100",
                    "area": "BKK1",
                    "layout": "1房",
                    "price": 700,
                },
                {
                    "listing_id": "l_200",
                    "area": "钻石岛",
                    "layout": "2房",
                    "price": 1200,
                },
            ]
        )
        rows = keyboard.inline_keyboard
        self.assertEqual(rows[0][0].callback_data, "listing:open:l_100")
        self.assertEqual(rows[1][0].callback_data, "listing:open:l_200")
        self.assertIn("BKK1", rows[0][0].text)
        self.assertIn("$700/月", rows[0][0].text)

    def test_lead_handoff_does_not_request_phone_or_wechat(self):
        text = lead_capture_text()
        self.assertNotIn("手机号", text)
        self.assertNotIn("微信", text)

        with patch("qiaolian_dual.user_bot.ADVISOR_TG", "@advisor"):
            keyboard = lead_capture_keyboard()
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertNotIn("📱 发送手机号", labels)
        self.assertIn("💬 打开顾问对话", labels)

    def test_return_home_uses_same_product_identity(self):
        text = welcome_text()
        self.assertIn("侨联找房助手", text)
        self.assertNotIn("土地", text)


if __name__ == "__main__":
    unittest.main()
