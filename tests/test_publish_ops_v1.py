import unittest
from unittest.mock import patch

import meihua_publisher

from meihua_publisher import build_keyboard


class PublishOpsV1Tests(unittest.TestCase):
    def test_listing_keyboard_uses_all_four_short_actions(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "@TestDeepLinkBot"):
            keyboard = build_keyboard(
                "l_1024",
                area="BKK1",
                post_token="tk7f3a",
                caption_variant="b",
            )

        rows = keyboard.inline_keyboard
        self.assertEqual(len(rows), 2)
        self.assertEqual([len(row) for row in rows], [2, 2])
        self.assertEqual(rows[0][0].text, "💬 立即咨询")
        self.assertEqual(rows[0][1].text, "📅 预约看房")
        self.assertEqual(rows[1][0].text, "❤️ 收藏房源")
        self.assertEqual(rows[1][1].text, "🏠 同区域更多")
        self.assertIn("start=q__tk7f3a__l_1024|cv=b", rows[0][0].url)
        self.assertIn("start=a__tk7f3a__l_1024|cv=b", rows[0][1].url)
        self.assertIn("start=f__tk7f3a__l_1024|cv=b", rows[1][0].url)
        self.assertIn("start=m__tk7f3a__BKK1|cv=b", rows[1][1].url)


if __name__ == "__main__":
    unittest.main()
