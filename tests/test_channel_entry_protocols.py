import unittest
from unittest.mock import patch

import autopilot_publish_bot
from autopilot_publish_bot import build_channel_menu_keyboard, channel_index_html, default_pin_html
from scripts.ops.post_pinned import PINNED_TEXT
from v2_admin.publisher import build_detail_text, build_keyboard


class ChannelEntryProtocolsTests(unittest.TestCase):
    def test_channel_menu_uses_explicit_supported_index_actions(self):
        with patch.object(autopilot_publish_bot, "DEEPLINK_BOT_USERNAME", "TestDeepLinkBot"):
            keyboard = build_channel_menu_keyboard()

        rows = keyboard.inline_keyboard
        self.assertEqual([[button.text for button in row] for row in rows], [
            ["📍 区域找房", "💰 预算找房"],
            ["🆕 最新房源", "💬 中文顾问"],
        ])
        self.assertEqual([button.url for row in rows for button in row], [
            "https://t.me/TestDeepLinkBot?start=find_area",
            "https://t.me/TestDeepLinkBot?start=find_budget",
            "https://t.me/TestDeepLinkBot?start=latest",
            "https://t.me/TestDeepLinkBot?start=advisor",
        ])

    def test_default_pin_html_matches_channel_handoff_language(self):
        with patch.object(autopilot_publish_bot, "_get_setting", return_value=""), patch.object(
            autopilot_publish_bot, "BRAND_NAME", "侨联地产"
        ):
            text = default_pin_html()

        self.assertIn("实拍房源 · 费用先说 · 中文带看", text)
        self.assertIn("系统会自动带上房源编号", text)
        self.assertNotIn("找房路径（3 步）", text)
        self.assertNotIn("三项承诺", text)
        self.assertEqual(channel_index_html(), text)
        self.assertEqual(PINNED_TEXT, text)

    def test_v2_admin_keyboard_uses_short_alias_payloads(self):
        listing = {"listing_id": "l_1024", "area": "BKK1"}
        with patch("v2_admin.publisher.BOT_USERNAME", "TestDeepLinkBot"):
            keyboard = build_keyboard(listing)

        rows = keyboard.inline_keyboard
        self.assertIn("start=q_l_1024", rows[0][0].url)
        self.assertIn("start=a_l_1024", rows[0][1].url)
        self.assertIn("start=f_l_1024", rows[1][0].url)
        self.assertIn("start=m_BKK1", rows[1][1].url)

    def test_v2_admin_detail_text_matches_listing_handoff_language(self):
        text = build_detail_text({"area": "BKK1", "project": "The Peak", "price": "800"})
        self.assertIn("点下面按钮继续", text)
        self.assertIn("不用重新解释是哪套房", text)


if __name__ == "__main__":
    unittest.main()
