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
        self.assertEqual([len(row) for row in rows], [2])
        # Both action buttons must be present
        texts = [btn.text for btn in rows[0]]
        self.assertIn("💬 咨询这套", texts)
        self.assertIn("📅 预约看房", texts)


if __name__ == "__main__":
    unittest.main()
