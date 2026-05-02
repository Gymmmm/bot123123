import unittest

from meihua_publisher import build_chinese_listing_post, build_listing_tags


class PublishTagsTests(unittest.TestCase):
    def test_tags_are_relevant_and_capped(self):
        draft = {
            "area": "BKK1",
            "layout": "2房1卫",
            "property_type": "公寓",
            "price": 800,
            "highlights": ["带泳池", "带健身房", "可养宠物", "拎包入住"],
        }
        tags = build_listing_tags(draft)
        self.assertLessEqual(len(tags), 8)
        self.assertIn("#金边租房", tags)
        self.assertIn("#金边华人租房", tags)
        self.assertIn("#侨联实拍", tags)
        self.assertIn("#BKK1", tags)

    def test_caption_contains_capped_tags(self):
        draft = {
            "area": "钻石岛",
            "layout": "1房1卫",
            "property_type": "服务式公寓",
            "price": 1200,
            "highlights": ["全家具", "24小时安保"],
        }
        caption = build_chinese_listing_post(draft)
        # Tags are on the last line
        last_line = caption.strip().splitlines()[-1]
        tags = [part for part in last_line.split() if part.startswith("#")]
        self.assertLessEqual(len(tags), 8)
        self.assertIn("#钻石岛", tags)
        # Core structure checks
        self.assertIn("QIAOLIAN VERIFIED LISTING", caption)
        self.assertIn("侨联地产", caption)
        self.assertIn("您在金边的自己人", caption)
        self.assertIn("提前说清：", caption)
        self.assertIn("侨联判断：", caption)

    def test_caption_keeps_new_channel_structure(self):
        draft = {
            "area": "Sen Sok",
            "layout": "1房",
            "price": 250,
        }
        caption = build_chinese_listing_post(draft)
        lines = caption.splitlines()
        # Bold compact title on line 0
        self.assertTrue(lines[0].startswith("<b>Sen Sok｜"))
        self.assertIn("$250/月", lines[0])
        # QIAOLIAN tag on line 1
        self.assertIn("QIAOLIAN VERIFIED LISTING", lines[1])
        # Separator on line 2
        self.assertIn("━", lines[2])
        # Required fields
        self.assertIn("房源编号：", caption)
        self.assertIn("位置：", caption)
        self.assertIn("户型：", caption)
        self.assertIn("租金：", caption)
        self.assertIn("押付：", caption)
        self.assertIn("提前说清：", caption)
        self.assertIn("侨联判断：", caption)
        # Brand signature
        self.assertIn("侨联地产", caption)
        self.assertIn("您在金边的自己人", caption)
        # SEO tags on last line
        last_line = caption.strip().splitlines()[-1]
        self.assertIn("#金边租房", last_line)
        self.assertIn("#侨联实拍", last_line)

    def test_caption_variants_use_unified_factual_structure(self):
        draft = {
            "area": "BKK1",
            "layout": "1房1卫",
            "property_type": "公寓",
            "price": 1300,
            "size": "85平",
            "floor": "14楼",
            "deposit": "押一付一",
            "highlights": ["BKK1核心地段", "14楼视野开阔"],
        }
        cap_a = build_chinese_listing_post(draft, caption_variant="a")
        cap_b = build_chinese_listing_post(draft, caption_variant="b")
        cap_c = build_chinese_listing_post(draft, caption_variant="c")

        # All variants produce identical output (no A/B splitting in v2)
        self.assertEqual(cap_a, cap_b)
        self.assertEqual(cap_b, cap_c)
        # Compact title format
        self.assertTrue(cap_a.splitlines()[0].startswith("<b>BKK1｜"))
        # Structure checks
        self.assertIn("QIAOLIAN VERIFIED LISTING", cap_a)
        self.assertIn("提前说清：", cap_a)
        self.assertIn("侨联判断：", cap_a)
        self.assertIn("侨联地产", cap_a)

    def test_caption_contains_payment_and_contract_line(self):
        draft = {
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 1300,
            "deposit": "押一付一",
            "normalized_data": '{"contract_term":"1年","payment_terms":"押1付1"}',
        }
        caption = build_chinese_listing_post(draft)
        self.assertIn("付款/合同：", caption)
        self.assertIn("押1付1", caption)
        self.assertIn("1年", caption)


if __name__ == "__main__":
    unittest.main()
