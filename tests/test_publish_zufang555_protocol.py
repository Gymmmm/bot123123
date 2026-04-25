import unittest

from tools.publish_zufang555 import build_start_payload, get_keyboard, parse_cli_args


class PublishZuFang555ProtocolTests(unittest.TestCase):
    def test_cli_defaults_to_one_post(self):
        dry_run, limit = parse_cli_args([])
        self.assertFalse(dry_run)
        self.assertEqual(limit, 1)

    def test_cli_limit_override_is_supported(self):
        dry_run, limit = parse_cli_args(["--dry-run", "--limit", "3"])
        self.assertTrue(dry_run)
        self.assertEqual(limit, 3)

    def test_build_start_payload_uses_short_aliases(self):
        self.assertEqual(build_start_payload("consult", "l_1024"), "q_l_1024")
        self.assertEqual(build_start_payload("fav", "l_1024"), "f_l_1024")

    def test_keyboard_uses_short_payloads(self):
        keyboard = get_keyboard("l_1024")
        row = keyboard.inline_keyboard[0]
        self.assertEqual(row[0].text, "💬 立即咨询")
        self.assertIn("start=q_l_1024", row[0].url)
        self.assertIn("start=f_l_1024", row[1].url)


if __name__ == "__main__":
    unittest.main()