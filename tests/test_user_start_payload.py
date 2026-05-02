import unittest
from unittest.mock import patch

from qiaolian_dual.user_bot import listing_landing_keyboard, parse_start_arg_payload


class UserStartPayloadTests(unittest.TestCase):
    def test_static_start_args_are_parsed(self):
        for arg in ("brand", "about", "want_home", "ask"):
            payload = parse_start_arg_payload(arg)
            self.assertIsNotNone(payload)
            self.assertEqual(payload["action"], arg)
            self.assertEqual(payload["target"], "")
            self.assertEqual(payload["post_token"], "")
            self.assertIsNone(payload["channel_message_id"])

    def test_new_static_start_args_are_parsed(self):
        """新增静态深链参数：find_home, area_index, latest, cooperate, consult_general"""
        # find_home, cooperate, consult_general → action = arg itself
        for arg in ("find_home", "cooperate", "consult_general"):
            payload = parse_start_arg_payload(arg)
            self.assertIsNotNone(payload, f"parse_start_arg_payload({arg!r}) returned None")
            self.assertEqual(payload["action"], arg)
            self.assertEqual(payload["target"], "")

        # area_index → mapped via _channel_index_action to "index_area"
        payload = parse_start_arg_payload("area_index")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["target"], "")

        # latest → mapped via _channel_index_action to "index_latest"
        payload = parse_start_arg_payload("latest")
        self.assertIsNotNone(payload)
        self.assertIn("latest", payload["action"])

    def test_channel_payload_format_still_supported(self):
        payload = parse_start_arg_payload("consult__abc__l_1|cv=b")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "l_1|cv=b")

    def test_new_short_channel_payloads_are_supported(self):
        payload = parse_start_arg_payload("a__abc__l_1|cv=b")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "appoint")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "l_1|cv=b")

        payload = parse_start_arg_payload("q__abc__l_2|cv=c")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "l_2|cv=c")

        payload = parse_start_arg_payload("f__abc__l_3")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "fav")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "l_3")

        payload = parse_start_arg_payload("m__abc__BKK1|cv=a")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "more")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "BKK1|cv=a")

    def test_new_deeplink_formats(self):
        """新版深链：book_{id}, similar_{id}, video_{id}"""
        for action, prefix in (("book", "book_"), ("similar", "similar_"), ("video", "video_")):
            payload = parse_start_arg_payload(f"{prefix}l_123")
            self.assertIsNotNone(payload, f"parse_start_arg_payload({prefix}l_123) returned None")
            self.assertEqual(payload["action"], action)
            self.assertEqual(payload["target"], "l_123")

    def test_legacy_deeplink_compat_appoint(self):
        """兼容旧格式：appoint_{id} → action=appoint, {id}_appoint → action=book"""
        # appoint_123 → action appoint (via START_ACTIONS loop)
        payload = parse_start_arg_payload("appoint_123")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "appoint")
        self.assertEqual(payload["target"], "123")

        # 123_appoint → action book (via old suffix map)
        payload = parse_start_arg_payload("123_appoint")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "book")
        self.assertEqual(payload["target"], "123")

    def test_legacy_deeplink_compat_consult(self):
        """兼容旧格式：consult_{id} 和 {id}_consult"""
        payload = parse_start_arg_payload("consult_l_99")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["target"], "l_99")

        payload = parse_start_arg_payload("l_99_consult")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["target"], "l_99")

    def test_listing_landing_keyboard_has_three_buttons(self):
        """listing_landing_keyboard 应有 4 行按钮（预约/视频/顾问/详情）"""
        with patch("qiaolian_dual.user_bot.USER_BOT_USERNAME", "TestBot"):
            with patch("qiaolian_dual.user_bot.listing_context", return_value={}):
                keyboard = listing_landing_keyboard("l_1024", area="BKK1")

        rows = keyboard.inline_keyboard
        self.assertEqual(len(rows), 4, f"Expected 4 rows, got {len(rows)}: {rows}")
        # Row 0: 预约看房
        self.assertIn("book_l_1024", rows[0][0].url)
        # Row 1: 视频代看
        self.assertIn("video_l_1024", rows[1][0].url)
        # Row 2: callback, not URL (问顾问)
        self.assertIn("l_1024", rows[2][0].callback_data)
        # Row 3: 查看详情 callback
        self.assertIn("listing:detail:l_1024", rows[3][0].callback_data)

    def test_tenant_bind_and_channel_topic_payloads_are_supported(self):
        payload = parse_start_arg_payload("t_bind_ABC123")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "tenant_bind")
        self.assertEqual(payload["target"], "ABC123")
        self.assertEqual(payload["post_token"], "")

        payload = parse_start_arg_payload("ch__district_guide")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "channel_topic")
        self.assertEqual(payload["target"], "district_guide")
        self.assertEqual(payload["post_token"], "")

    def test_discussion_entry_deep_link_is_parsed(self):
        """讨论区入口深链：discussion_entry__<post_token>__<listing_id>"""
        payload = parse_start_arg_payload("discussion_entry__abc123__l_42")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "discussion_entry")
        self.assertEqual(payload["post_token"], "abc123")
        self.assertEqual(payload["target"], "l_42")

    def test_discussion_entry_deep_link_without_token(self):
        """讨论区入口深链：只有 prefix，无 token，返回 discussion_entry action。"""
        payload = parse_start_arg_payload("discussion_entry__")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "discussion_entry")


if __name__ == "__main__":
    unittest.main()
