import unittest

from qiaolian_pipeline.parser import RuleBasedListingParser, is_non_rental_source, whitelist_quality_tags


class NonRentalFilterTests(unittest.TestCase):
    def test_sale_price_is_marked_non_rental(self):
        text = "BKK1 restaurant business for sale. Sale Price: $90,000. Land area: 590 sqm"
        parsed = RuleBasedListingParser().parse(text)
        self.assertTrue(is_non_rental_source(text))
        self.assertEqual(parsed["quality_score"], 0)
        self.assertIn("non_rental_source", parsed["quality_flags"])
        self.assertIn("commercial_waste", parsed["quality_flags"])
        self.assertIn("non_rental_blacklist_keyword", parsed["quality_flags"])
        self.assertIn("non_rental_price_over_15000", parsed["quality_flags"])

    def test_chinese_transfer_is_marked_non_rental(self):
        text = "金边BKK1餐厅转让，生意好，设备齐全，租约稳定"
        parsed = RuleBasedListingParser().parse(text)
        self.assertTrue(is_non_rental_source(text))
        self.assertEqual(parsed["quality_score"], 0)
        self.assertIn("non_rental_source", parsed["quality_flags"])

    def test_normal_rental_is_not_marked_non_rental(self):
        text = "BKK1 apartment for rent, 1 bedroom, $600/month, fully furnished, pool and gym"
        parsed = RuleBasedListingParser().parse(text)
        self.assertFalse(is_non_rental_source(text))
        self.assertGreater(parsed["quality_score"], 0)
        self.assertNotIn("non_rental_source", parsed["quality_flags"])

    def test_core_area_and_known_property_add_quality_tags(self):
        text = "The Bridge BKK1 apartment for rent, 1 bedroom, $800/month, pool and gym"
        parsed = RuleBasedListingParser().parse(text)
        self.assertIn("core_area", whitelist_quality_tags(text))
        self.assertIn("known_property", whitelist_quality_tags(text))
        self.assertIn("rental_intent", whitelist_quality_tags(text))
        self.assertIn("whitelist_core_area", parsed["quality_flags"])
        self.assertIn("whitelist_known_property", parsed["quality_flags"])

    def test_missing_rental_intent_stays_below_ready_threshold(self):
        text = "The Bridge BKK1 apartment, 1 bedroom, $800, pool and gym"
        parsed = RuleBasedListingParser().parse(text)
        self.assertIn("missing_rental_intent", parsed["quality_flags"])
        self.assertLess(parsed["quality_score"], 60)


if __name__ == "__main__":
    unittest.main()
