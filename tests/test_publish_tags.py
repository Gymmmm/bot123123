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
        self.assertLessEqual(len(tags), 5)
        self.assertIn("#金边租房", tags)
        self.assertIn("#侨联实拍", tags)
        self.assertIn("#BKK1", tags)
        self.assertIn("#两房一厅", tags)

    def test_caption_contains_capped_tags(self):
        draft = {
            "area": "钻石岛",
            "layout": "1房1卫",
            "property_type": "服务式公寓",
            "price": 1200,
            "highlights": ["全家具", "24小时安保"],
        }
        caption = build_chinese_listing_post(draft)
        last_line = caption.strip().splitlines()[-1]
        tags = [part for part in last_line.split() if part.startswith("#")]
        self.assertLessEqual(len(tags), 4)
        self.assertIn("#钻石岛", tags)
        self.assertNotIn("实拍图片", caption)
        self.assertNotIn("———", caption)
        self.assertIn("下方按钮：咨询这套 / 预约看房", caption)
        self.assertNotIn("人工顾问", caption)
        self.assertIn("侨联地产", caption)
        self.assertIn("实拍直发，编号可追溯", caption)

    def test_caption_keeps_new_channel_structure(self):
        draft = {
            "area": "Sen Sok",
            "layout": "1房",
            "price": 250,
            "furniture": "可咨询确认",
            "highlights": ["实拍房源", "中文顾问可约看房"],
        }
        caption = build_chinese_listing_post(draft)
        lines = caption.splitlines()
        self.assertEqual(len(lines), 7)
        self.assertTrue(lines[0].startswith("Sen Sok｜1房｜$250/月｜编号:"))
        self.assertTrue(lines[1].startswith("核心亮点："))
        self.assertTrue(lines[2].startswith("项目参数："))
        self.assertTrue(lines[3].startswith("费用提醒："))
        self.assertTrue(lines[4].startswith("看房方式："))
        self.assertEqual(lines[5], "下方按钮：咨询这套 / 预约看房")
        self.assertIn("侨联地产", lines[6])
        self.assertIn("#金边租房", lines[6])
        self.assertIn("#侨联实拍", lines[6])

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

        self.assertEqual(cap_a, cap_b)
        self.assertEqual(cap_b, cap_c)
        self.assertTrue(cap_a.splitlines()[0].startswith("BKK1｜1房1卫｜"))
        self.assertIn("｜编号:", cap_a.splitlines()[0])
        self.assertTrue(cap_a.splitlines()[1].startswith("核心亮点："))
        self.assertTrue(cap_a.splitlines()[3].startswith("费用提醒："))
        self.assertIn("下方按钮：咨询这套 / 预约看房", cap_a)
        self.assertIn("下方按钮：咨询这套 / 预约看房", cap_b)
        self.assertIn("下方按钮：咨询这套 / 预约看房", cap_c)

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
