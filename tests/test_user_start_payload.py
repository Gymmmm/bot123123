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
        for arg in ("find_home", "cooperate", "consult_general"):
            payload = parse_start_arg_payload(arg)
            self.assertIsNotNone(payload, f"parse_start_arg_payload({arg!r}) returned None")
            self.assertEqual(payload["action"], arg)
            self.assertEqual(payload["target"], "")

        payload = parse_start_arg_payload("area_index")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["target"], "")

        payload = parse_start_arg_payload("latest")
        self.assertIsNotNone(payload)
        self.assertIn("latest", payload["action"])

    def test_channel_payload_format_still_supported(self):
        payload = parse_start_arg_payload("consult__abc__l_1|cv=b")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["post_token"], "abc")
        self.assertEqual(payload["target"], "l_1|cv=b")

    def test_channel_detail_payload_maps_to_details_action(self):
        payload = parse_start_arg_payload("detail__qlabc123__l_42")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "details")
        self.assertEqual(payload["post_token"], "qlabc123")
        self.assertEqual(payload["opaque_token"], "qlabc123")
        self.assertEqual(payload["target"], "")

        legacy = parse_start_arg_payload("detail__abc__l_42")
        self.assertIsNotNone(legacy)
        self.assertEqual(legacy["action"], "details")
        self.assertEqual(legacy["post_token"], "abc")
        self.assertEqual(legacy["target"], "l_42")

        stable = parse_start_arg_payload("detail_l_42")
        self.assertIsNotNone(stable)
        self.assertEqual(stable["action"], "details")
        self.assertEqual(stable["target"], "l_42")

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
        cases = (
            ("details", "detail_"),
            ("book", "book_"),
            ("similar", "similar_"),
            ("video", "video_"),
        )
        for action, prefix in cases:
            payload = parse_start_arg_payload(f"{prefix}l_123")
            self.assertIsNotNone(payload, f"parse_start_arg_payload({prefix}l_123) returned None")
            self.assertEqual(payload["action"], action)
            self.assertEqual(payload["target"], "l_123")

    def test_legacy_deeplink_compat_appoint(self):
        payload = parse_start_arg_payload("appoint_123")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "appoint")
        self.assertEqual(payload["target"], "123")

        payload = parse_start_arg_payload("123_appoint")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "book")
        self.assertEqual(payload["target"], "123")

    def test_legacy_deeplink_compat_consult(self):
        payload = parse_start_arg_payload("consult_l_99")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["target"], "l_99")

        payload = parse_start_arg_payload("l_99_consult")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "consult")
        self.assertEqual(payload["target"], "l_99")

    def test_listing_landing_keyboard_has_locked_four_ctas(self):
        with patch("qiaolian_dual.user_bot.USER_BOT_USERNAME", "TestBot"):
            with patch("qiaolian_dual.listing.listing_context", return_value={"status": "active"}):
                keyboard = listing_landing_keyboard("l_1024", area="BKK1")

        rows = keyboard.inline_keyboard
        labels = [button.text for row in rows for button in row]
        callbacks = [button.callback_data for row in rows for button in row]
        self.assertEqual(labels[:4], ["🏠 房源详情", "📸 更多实拍", "📅 预约看房", "💬 联系我们"])
        self.assertIn("listing:photos:l_1024", callbacks)
        self.assertIn("listing:detail:l_1024", callbacks)
        self.assertIn("listing:appoint:l_1024", callbacks)
        self.assertIn("listing:consult:l_1024", callbacks)
        self.assertNotIn("listing:similar:l_1024", callbacks)
        self.assertNotIn("联系中文顾问", " ".join(labels))

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
        payload = parse_start_arg_payload("discussion_entry__abc123__l_42")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "discussion_entry")
        self.assertEqual(payload["post_token"], "abc123")
        self.assertEqual(payload["target"], "l_42")

    def test_discussion_entry_deep_link_without_token(self):
        payload = parse_start_arg_payload("discussion_entry__")
        self.assertIsNotNone(payload)
        self.assertEqual(payload["action"], "discussion_entry")


if __name__ == "__main__":
    unittest.main()
