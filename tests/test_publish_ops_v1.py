import unittest
from unittest.mock import patch

import meihua_publisher
from meihua_publisher import build_keyboard


class PublishOpsV1Tests(unittest.TestCase):
    def test_listing_keyboard_uses_two_clear_channel_actions(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "@TestDeepLinkBot"):
            keyboard = build_keyboard(
                "l_1024",
                area="BKK1",
                post_token="tk7f3a",
                caption_variant="b",
            )

        rows = keyboard.inline_keyboard
        self.assertEqual(len(rows), 1)
        self.assertEqual([len(row) for row in rows], [2])
        self.assertEqual([button.text for button in rows[0]], ["📅 预约这套", "💬 咨询这套"])
        self.assertIn("start=", rows[0][0].url)
        self.assertIn("start=", rows[0][1].url)
        self.assertIn("tk7f3a", rows[0][0].url)
        self.assertIn("tk7f3a", rows[0][1].url)
        self.assertIn("l_1024", rows[0][0].url)
        self.assertIn("l_1024", rows[0][1].url)


if __name__ == "__main__":
    unittest.main()
