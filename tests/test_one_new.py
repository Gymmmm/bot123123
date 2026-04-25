import unittest


class ManualOneNewScriptTests(unittest.TestCase):
    @unittest.skip("legacy manual wrapper removed from repository")
    def test_manual_script(self):
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
