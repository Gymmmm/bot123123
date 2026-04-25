import unittest

from qiaolian_pipeline.parser import RuleBasedListingParser


class ParserPaymentContractTests(unittest.TestCase):
    def setUp(self):
        self.parser = RuleBasedListingParser()

    def test_parse_cn_payment_and_contract(self):
        raw = "BKK1 一房出租，租金$1300/月，押一付一，合同1年，可随时入住。"
        parsed = self.parser.parse(raw)
        self.assertEqual(parsed.get("price"), 1300)
        self.assertEqual(parsed.get("payment_terms"), "押1付1")
        self.assertEqual(parsed.get("contract_term"), "1年")
        self.assertEqual(parsed.get("deposit"), "押1付1")

    def test_parse_en_payment_and_contract(self):
        raw = (
            "BKK1 apartment for rent USD 900/month. "
            "Deposit: 1 month. Rent in advance: 1 month. Minimum lease 1 year."
        )
        parsed = self.parser.parse(raw)
        self.assertEqual(parsed.get("price"), 900)
        self.assertEqual(parsed.get("payment_terms"), "押1付1")
        self.assertEqual(parsed.get("contract_term"), "1年")


if __name__ == "__main__":
    unittest.main()
