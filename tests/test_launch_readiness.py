"""上线口径测试：首页按钮结构、品牌文案、频道房源帖按钮、配置项。"""
from __future__ import annotations

import unittest
from unittest.mock import patch


class HomeKeyboardTests(unittest.TestCase):
    def test_main_keyboard_has_3x2_layout(self):
        """首页按钮应为 3 行 2 列。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        rows = kb.inline_keyboard
        self.assertEqual(len(rows), 3, f"Expected 3 rows, got {len(rows)}")
        for i, row in enumerate(rows):
            self.assertEqual(len(row), 2, f"Row {i} should have 2 buttons, got {len(row)}: {row}")

    def test_main_keyboard_callback_data(self):
        """首页按钮 callback_data 是否正确。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        flat = [btn for row in kb.inline_keyboard for btn in row]
        data_set = {btn.callback_data for btn in flat}
        for expected in ("home_smart_search", "home_brand", "home_appoint", "home_consult", "home_living", "home_nearby"):
            self.assertIn(expected, data_set, f"Missing callback_data: {expected}")

    def test_main_keyboard_no_url_buttons(self):
        """首页按钮不应包含 URL（全部内部回调）。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        for row in kb.inline_keyboard:
            for btn in row:
                self.assertIsNone(btn.url, f"Button '{btn.text}' should not have a URL on the home keyboard")


class BrandTextTests(unittest.TestCase):
    def test_brand_text_is_html(self):
        """brand_text() 应返回包含 HTML 标签的字符串。"""
        from qiaolian_dual.messages import brand_text
        text = brand_text()
        self.assertIn("<b>", text, "brand_text should contain HTML bold tags")
        self.assertIn("侨联地产", text)
        self.assertIn("服务承诺", text)

    def test_channel_welcome_text(self):
        """首页欢迎语含正确关键字。"""
        from qiaolian_dual.messages import channel_welcome_text
        text = channel_welcome_text()
        self.assertIn("侨联找房助手", text)
        self.assertNotIn("您", text, "Should use '你' not '您'")


class ChannelKeyboardTests(unittest.TestCase):
    def test_four_buttons_with_channel_message_id(self):
        """有 channel_message_id 时，发布键盘应有 4 个按钮。"""
        from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard
        kb = publish_post_keyboard(
            listing_id="l_1001",
            area="BKK1",
            user_bot_username="TestBot",
            channel_username="qiaolian_channel",
            channel_message_id=12345,
        )
        flat = [btn for row in kb.inline_keyboard for btn in row]
        self.assertEqual(len(flat), 4, f"Expected 4 buttons, got {len(flat)}: {[b.text for b in flat]}")

    def test_comment_url_uses_channel_message_id(self):
        """评论区链接应包含 channel_username/channel_message_id?comment=1。"""
        from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard
        kb = publish_post_keyboard(
            listing_id="l_1001",
            area="BKK1",
            user_bot_username="TestBot",
            channel_username="qiaolian_channel",
            channel_message_id=99,
        )
        urls = [btn.url for row in kb.inline_keyboard for btn in row if btn.url]
        comment_urls = [u for u in urls if "comment=1" in (u or "")]
        self.assertTrue(comment_urls, "Should have at least one comment URL")
        self.assertIn("qiaolian_channel/99", comment_urls[0])

    def test_fallback_to_discussion_group_link(self):
        """CHANNEL_USERNAME 缺失时，应降级使用 discussion_group_link。"""
        from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard
        kb = publish_post_keyboard(
            listing_id="l_1001",
            area="BKK1",
            user_bot_username="TestBot",
            channel_username="",
            channel_message_id=None,
            discussion_group_link="https://t.me/joinchat/group",
        )
        flat = [btn for row in kb.inline_keyboard for btn in row]
        # should still produce 4 buttons
        self.assertEqual(len(flat), 4)
        discussion_btn = next((b for b in flat if "group" in (b.url or "")), None)
        self.assertIsNotNone(discussion_btn, "Discussion group link button should exist")

    def test_four_buttons_always_including_fallback(self):
        """无 channel_username 且无 discussion_group_link 时，仍输出 4 按钮（🖼 降级为 similar 链接），并输出警告日志。"""
        import logging
        from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard
        with self.assertLogs("v2.qiaolian_publisher_v2.keyboards", level=logging.WARNING):
            kb = publish_post_keyboard(
                listing_id="l_1001",
                area="BKK1",
                user_bot_username="TestBot",
                channel_username="",
                channel_message_id=None,
                discussion_group_link="",
            )
        flat = [btn for row in kb.inline_keyboard for btn in row]
        self.assertEqual(len(flat), 4, f"Expected 4 buttons always, got {len(flat)}")
        media_btn = next((b for b in flat if b.text and "实拍" in b.text), None)
        similar_btn = next((b for b in flat if b.text and "类似" in b.text), None)
        self.assertIsNotNone(media_btn, "🖼 更多实拍/评论区 must always be present")
        self.assertIsNotNone(similar_btn, "🔍 找类似房源 must always be present")
        # 兜底时，🖼 按钮的链接应等于「找类似房源」deep link
        self.assertEqual(media_btn.url, similar_btn.url)

    def test_book_and_consult_deeplinks(self):
        """预约和咨询按钮应使用新格式 book_ / consult_。"""
        from v2.qiaolian_publisher_v2.keyboards import publish_post_keyboard
        kb = publish_post_keyboard(
            listing_id="l_1001",
            area="BKK1",
            user_bot_username="MyBot",
        )
        flat = [btn for row in kb.inline_keyboard for btn in row]
        book_btn = next((b for b in flat if b.text and "预约" in b.text), None)
        consult_btn = next((b for b in flat if b.text and "顾问" in b.text), None)
        self.assertIsNotNone(book_btn)
        self.assertIn("book_l_1001", book_btn.url)
        self.assertIsNotNone(consult_btn)
        self.assertIn("consult_l_1001", consult_btn.url)


class ConfigDerivationTests(unittest.TestCase):
    def test_channel_username_derived_from_url(self):
        """CHANNEL_USERNAME 未配置时从 CHANNEL_URL 推导。"""
        import importlib
        import os

        with patch.dict(os.environ, {"CHANNEL_URL": "https://t.me/my_channel", "CHANNEL_USERNAME": ""}):
            import qiaolian_dual.config as cfg
            importlib.reload(cfg)
            self.assertEqual(cfg.CHANNEL_USERNAME, "my_channel")

    def test_channel_username_explicit_wins(self):
        """CHANNEL_USERNAME 显式配置优先于 CHANNEL_URL 推导。"""
        import importlib
        import os

        with patch.dict(os.environ, {"CHANNEL_URL": "https://t.me/other_channel", "CHANNEL_USERNAME": "explicit_ch"}):
            import qiaolian_dual.config as cfg
            importlib.reload(cfg)
            self.assertEqual(cfg.CHANNEL_USERNAME, "explicit_ch")


if __name__ == "__main__":
    unittest.main()
