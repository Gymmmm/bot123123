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

    def test_listing_landing_keyboard_uses_short_aliases(self):
        with patch("qiaolian_dual.user_bot.USER_BOT_USERNAME", "TestDeepLinkBot"):
            keyboard = listing_landing_keyboard("l_1024", area="BKK1")

        rows = keyboard.inline_keyboard
        self.assertIn("start=a_l_1024%7Cmode%3Doffline", rows[0][0].url)
        self.assertIn("start=a_l_1024%7Cmode%3Dvideo", rows[0][1].url)
        self.assertIn("start=f_l_1024", rows[1][0].url)
        self.assertIn("start=m_l_1024", rows[1][1].url)

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
