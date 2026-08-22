import unittest
from unittest.mock import patch

import meihua_publisher

from meihua_publisher import build_keyboard


class PublishOpsV1Tests(unittest.TestCase):
    def test_listing_keyboard_uses_one_clear_channel_action(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "@TestDeepLinkBot"):
            keyboard = build_keyboard(
                "l_1024",
                area="BKK1",
                post_token="tk7f3a",
                caption_variant="b",
            )

        rows = keyboard.inline_keyboard
        self.assertEqual(len(rows), 1)
        self.assertEqual([len(row) for row in rows], [1])
        self.assertEqual(rows[0][0].text, "💬 咨询这套")
        self.assertIn("start=q__tk7f3a__l_1024|cv=b", rows[0][0].url)


if __name__ == "__main__":
    unittest.main()
