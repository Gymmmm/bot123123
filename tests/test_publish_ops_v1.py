import unittest
from unittest.mock import patch

import meihua_publisher
from meihua_publisher import build_keyboard
from qiaolian_dual.public_listing_id import public_listing_id


class PublishOpsV1Tests(unittest.TestCase):
    def test_listing_keyboard_uses_public_qc_channel_actions(self):
        with patch.object(meihua_publisher, "BOT_USERNAME", "@TestDeepLinkBot"):
            keyboard = build_keyboard(
                "l_1024",
                area="BKK1",
                post_token="tk7f3a",
                caption_variant="b",
            )

        rows = keyboard.inline_keyboard
        self.assertEqual(
            [[button.text for button in row] for row in rows],
            [["🏠 房源详情", "📸 更多实拍"], ["📅 预约看房"]],
        )
        urls = [button.url for row in rows for button in row]
        public_id = public_listing_id("l_1024")
        self.assertTrue(any(f"start=property_{public_id}_details" in url for url in urls))
        self.assertTrue(any(f"start=property_{public_id}_photos" in url for url in urls))
        self.assertTrue(any(f"start=property_{public_id}_book" in url for url in urls))
        self.assertTrue(all("l_1024" not in url for url in urls))


if __name__ == "__main__":
    unittest.main()
