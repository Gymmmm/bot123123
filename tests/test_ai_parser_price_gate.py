import sqlite3
import tempfile
import unittest
from pathlib import Path

from ai_parser import AIParserModule


class AIParserPriceGateTests(unittest.TestCase):
    def test_source_without_price_is_skipped(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "test.db"
            schema = Path(__file__).resolve().parent / "schema_core.sql"
            conn = sqlite3.connect(str(db_path))
            try:
                conn.executescript(schema.read_text(encoding="utf-8"))
                conn.execute(
                    """
                    INSERT INTO source_posts (
                        source_id, source_type, source_name, source_post_id, source_url, source_author,
                        raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json, dedupe_hash,
                        parse_status, fetched_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        1,
                        "telegram",
                        "test_source",
                        "sp_1",
                        "",
                        "",
                        "BKK1 一房出租 押一付一 可随时入住",
                        "[]",
                        "[]",
                        "",
                        "{}",
                        "h1",
                        "pending",
                    ),
                )
                conn.commit()
            finally:
                conn.close()

            AIParserModule(str(db_path)).process_pending_source_posts()

            conn = sqlite3.connect(str(db_path))
            try:
                parse_row = conn.execute(
                    "SELECT parse_status, parse_error FROM source_posts WHERE source_post_id = 'sp_1'"
                ).fetchone()
                draft_count = conn.execute("SELECT COUNT(*) FROM drafts").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(parse_row[0], "skipped_no_price")
            self.assertEqual(parse_row[1], "missing_price")
            self.assertEqual(draft_count, 0)


if __name__ == "__main__":
    unittest.main()
