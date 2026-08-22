import unittest

from meihua_publisher import (
    build_chinese_listing_post,
    build_discussion_detail_text,
    build_listing_tags,
    default_caption_variant_for_property,
)


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
        self.assertNotIn("发布前已核实", caption)
        self.assertIn("📸 实拍房源，可直接预约看房", caption)
        self.assertNotIn("您在金边的自己人", caption)

    def test_caption_keeps_new_channel_structure(self):
        draft = {
            "area": "Sen Sok",
            "layout": "1房",
            "price": 250,
        }
        caption = build_chinese_listing_post(draft)
        lines = caption.splitlines()
        # Bold compact title on line 0
        self.assertTrue(lines[0].startswith("🏠 <b>Sen Sok｜"))
        self.assertIn("💰 <b>$250/月</b>", caption)
        self.assertNotIn("发布前已核实", caption)
        self.assertIn("<code>QC", caption)
        self.assertNotIn("🧾 每月费用：", caption)
        last_line = caption.strip().splitlines()[-1]
        self.assertIn("#SenSok", last_line)

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

        # 所有版本包含完整公开事实。
        for caption in (cap_a, cap_b, cap_c):
            for fact in ("BKK1", "1房1卫", "$1,300/月", "85", "14楼", "QC"):
                self.assertIn(fact, caption)
            self.assertNotIn("待确认", caption)
        self.assertTrue(cap_a.splitlines()[0].startswith("🏠 <b>BKK1｜"))

    def test_property_type_selects_one_default_variant(self):
        self.assertEqual(default_caption_variant_for_property("公寓"), "a")
        self.assertEqual(default_caption_variant_for_property("排屋"), "b")
        self.assertEqual(default_caption_variant_for_property("别墅"), "b")
        self.assertEqual(default_caption_variant_for_property("办公室"), "c")
        self.assertEqual(default_caption_variant_for_property("商铺"), "c")

    def test_caption_contains_payment_and_contract_line(self):
        draft = {
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 1300,
            "deposit": "押一付一",
            "normalized_data": '{"contract_term":"1年","payment_terms":"押1付1"}',
        }
        caption = build_chinese_listing_post(draft)
        detail = build_discussion_detail_text(draft)
        self.assertNotIn("押付/合同：", caption)
        self.assertIn("押付｜押1付1", detail)
        self.assertIn("租期｜1年", detail)

    def test_caption_shows_verification_date_and_recurring_costs(self):
        draft = {
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 700,
            "approved_at": "2026-07-31 10:20:00",
            "normalized_data": (
                '{"management_fee":"包含","water_rate":"$5/人",'
                '"electric_rate":"$0.25/度","parking_fee":"汽车$50/月"}'
            ),
        }
        caption = build_chinese_listing_post(draft)
        self.assertNotIn("管理包含", caption)
        self.assertIn("#BKK1", caption)
        tags = caption.strip().splitlines()[-1].split()
        self.assertLessEqual(len(tags), 4)

    def test_caption_turns_street_noise_into_actionable_advice(self):
        draft = {
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 700,
            "drawbacks": ["临街，低楼层可能有车声"],
        }
        caption = build_chinese_listing_post(draft)
        self.assertNotIn("重点确认楼层和窗外环境", caption)
        self.assertIn("📸 实拍房源，可直接预约看房", caption)


if __name__ == "__main__":
    unittest.main()
