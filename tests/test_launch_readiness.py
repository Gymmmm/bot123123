"""上线口径测试：首页按钮结构、品牌文案、频道房源帖按钮、配置项。"""
from __future__ import annotations

import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock, patch


class HomeKeyboardTests(unittest.TestCase):
    def test_main_keyboard_keeps_one_primary_action_and_two_support_actions(self):
        """首页按钮结构匹配当前产品布局。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        rows = kb.inline_keyboard
        self.assertEqual([len(row) for row in rows], [2, 2, 2, 1])
        self.assertEqual(rows[0][0].text, "🔍 找房")

    def test_main_keyboard_callback_data(self):
        """首页按钮 callback_data 是否正确。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        flat = [btn for row in kb.inline_keyboard for btn in row]
        data_set = {btn.callback_data for btn in flat}
        for expected in ("home_smart_search", "hub:latest", "hub:appoint"):
            self.assertIn(expected, data_set, f"Missing callback_data: {expected}")
        self.assertEqual(len(data_set), 7)

    def test_main_keyboard_no_url_buttons(self):
        """首页按钮不应包含 URL（全部内部回调）。"""
        from qiaolian_dual.user_bot import main_keyboard
        kb = main_keyboard()
        for row in kb.inline_keyboard:
            for btn in row:
                self.assertIsNone(btn.url, f"Button '{btn.text}' should not have a URL on the home keyboard")


class ListingCallbackRoutingTests(unittest.TestCase):
    def test_listing_buttons_work_from_every_appointment_step(self):
        """旧预约流程未结束时，房源卡片按钮仍须由主回调接住。"""
        from unittest.mock import patch

        from qiaolian_dual import user_bot

        with patch.object(
            user_bot,
            "USER_BOT_TOKEN",
            "123456:abcdefghijklmnopqrstuvwxyzABCDEFGH",
        ):
            app = user_bot.build_application()

        conversation = app.handlers[0][0]
        appointment_states = (
            user_bot.APPT_MODE,
            user_bot.APPT_FOCUS,
            user_bot.APPT_DATE,
            user_bot.APPT_TIME,
            user_bot.APPT_CONFIRM,
        )
        for state in appointment_states:
            patterns = [getattr(handler, "pattern", None) for handler in conversation.states[state]]
            for callback_data in ("listing:open:l_100", "listing:appoint:l_100"):
                self.assertTrue(
                    any(pattern and pattern.match(callback_data) for pattern in patterns),
                    f"state {state} does not accept {callback_data}",
                )


class BrandTextTests(unittest.TestCase):
    def test_brand_text_is_html(self):
        """brand_text() 应返回包含 HTML 标签的字符串。"""
        from qiaolian_dual.messages import brand_text
        text = brand_text()
        self.assertIn("<b>", text, "brand_text should contain HTML bold tags")
        self.assertIn("侨联地产", text)
        self.assertIn("金边租房中介", text)
        self.assertNotIn("侨联地产测试", text)

    def test_channel_welcome_text(self):
        """首页欢迎语含正确关键字。"""
        from qiaolian_dual.messages import channel_welcome_text
        text = channel_welcome_text()
        self.assertIn("金边租房中介", text)
        self.assertIn("签约入住", text)
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
        self.assertEqual(
            [button.text for button in flat],
            ["📅 预约", "💬 问顾问", "🖼 更多实拍", "🔍 类似房源"],
        )

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
        # Degraded state: both buttons share the same URL.
        # Configure DISCUSSION_GROUP_LINK in .env to restore proper media/comments link.
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


class DeploymentReadinessTests(unittest.TestCase):
    def test_simple_mode_can_keep_scheduler_for_v2_runtime(self):
        import autopilot_publish_bot as ap

        job_queue = Mock()
        app = SimpleNamespace(job_queue=job_queue, add_handler=Mock())
        ap.register_autopilot_features(app, include_cancel=False, simple_mode=True, enable_scheduler=True)
        job_queue.run_repeating.assert_called_once()

    def test_v2_main_enables_scheduler_without_restoring_full_legacy_menu(self):
        import v2.qiaolian_publisher_v2.bot as bot_module

        fake_builder = Mock()
        fake_application = SimpleNamespace(
            add_handler=Mock(),
            add_error_handler=Mock(),
            bot=SimpleNamespace(set_my_commands=Mock()),
            run_polling=Mock(),
            post_init=None,
        )
        fake_builder.token.return_value = fake_builder
        fake_builder.build.return_value = fake_application
        fake_ptb_application = SimpleNamespace(builder=Mock(return_value=fake_builder))

        with patch.object(bot_module, "get_settings", return_value=SimpleNamespace(publisher_bot_token="123:abc", sqlite_path=":memory:", admin_ids=[])), patch.object(bot_module, "PublisherBot"), patch.object(bot_module, "Application", fake_ptb_application), patch("autopilot_publish_bot.register_autopilot_features") as register:
            bot_module.main()

        register.assert_called_once_with(
            fake_application,
            include_cancel=False,
            simple_mode=True,
            enable_scheduler=True,
        )

    def test_root_requirements_include_admin_web_runtime(self):
        requirements = Path(__file__).resolve().parents[1] / "requirements.txt"
        content = requirements.read_text(encoding="utf-8").lower()
        self.assertIn("flask==3.1.3", content)
        self.assertIn("waitress==3.0.1", content)

    def test_procfile_starts_admin_server_with_waitress(self):
        procfile = Path(__file__).resolve().parents[1] / "Procfile"
        content = procfile.read_text(encoding="utf-8").strip()
        self.assertIn("cd v2_admin", content)
        self.assertIn("python -m waitress", content)
        self.assertIn("admin_server:app", content)

    def test_admin_db_creates_parent_directory_for_default_like_path(self):
        import v2_admin.db as admin_db

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "data" / "qiaolian_dual_bot.db"
            original = admin_db.DB_PATH
            try:
                admin_db.DB_PATH = str(db_path)
                conn = admin_db.get_conn()
                conn.close()
            finally:
                admin_db.DB_PATH = original

            self.assertTrue(db_path.parent.is_dir())
            self.assertTrue(db_path.is_file())


if __name__ == "__main__":
    unittest.main()
