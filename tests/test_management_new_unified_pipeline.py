import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from v2.qiaolian_publisher_v2.bot import Draft, PublisherBot


class ManagementNewUnifiedPipelineTests(unittest.TestCase):
    def test_new_defaults_to_pending_and_preserves_four_photos_and_fields(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "test.db"
            schema = Path(__file__).resolve().parent.parent / "schema_core.sql"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(schema.read_text(encoding="utf-8"))
            paths = []
            for index in range(4):
                path = root / f"photo-{index}.jpg"
                path.write_bytes(f"photo-{index}".encode())
                paths.append(str(path))

            bot = object.__new__(PublisherBot)
            bot.settings = SimpleNamespace(sqlite_path=str(db_path))
            draft = Draft(
                listing_id="l_test", property_type="apartment", area="BKK1",
                title="Urban Village", community="Urban Village", price="680",
                layout="2房1厅2卫", size_sqm="82㎡", fee_note="12楼",
                deposit_rule="押一付一", available_date="随时入住",
                highlights=["采光好", "家具齐全"], media_type="photo",
                media_file_id="f2", media_file_ids=["f1", "f2", "f3", "f4"],
                source_caption="Urban Village BKK1 2房，租金 $680/月",
            )
            pending_id = bot._persist_new_as_pending(draft, local_paths=paths, operator_user_id=7)

            with sqlite3.connect(db_path) as conn:
                source = conn.execute("SELECT raw_text, raw_images_json FROM source_posts").fetchone()
                saved = conn.execute(
                    "SELECT review_status, price, layout, area, source_post_id FROM drafts WHERE draft_id=?",
                    (pending_id,),
                ).fetchone()
                media = conn.execute(
                    "SELECT telegram_file_id, local_path, sort_order FROM media_assets ORDER BY sort_order"
                ).fetchall()
            self.assertIn("$680", source[0])
            self.assertEqual(saved[:4], ("pending", 680, "2房1厅2卫", "BKK1"))
            self.assertEqual([row[0] for row in media], ["f1", "f2", "f3", "f4"])
            self.assertEqual([row[1] for row in media], paths)
            self.assertEqual([row[2] for row in media], [0, 1, 2, 3])

    def test_legacy_direct_publish_is_off_by_default(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(PublisherBot._legacy_direct_new_enabled())
        with patch.dict("os.environ", {"PUBLISHER_NEW_DIRECT_LEGACY_ENABLED": "true"}):
            self.assertFalse(PublisherBot._legacy_direct_new_enabled())


if __name__ == "__main__":
    unittest.main()
