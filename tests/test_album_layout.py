import sqlite3
import tempfile
import unittest
from pathlib import Path

from meihua_publisher import (
    BASIC_PUBLISH_MIN_SCORE,
    evaluate_publish_gate,
    normalize_album_grid,
)


def _init_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE source_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_images_json TEXT DEFAULT '[]'
        );
        CREATE TABLE media_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_type TEXT,
            owner_ref_id INTEGER,
            asset_type TEXT,
            local_path TEXT,
            sort_order INTEGER DEFAULT 0
        );
        """
    )
    conn.commit()
    conn.close()


class AlbumLayoutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.tmp = Path(self.td.name)
        self.db_path = str(self.tmp / "album.db")
        _init_db(self.db_path)

    def tearDown(self) -> None:
        self.td.cleanup()

    def _seed_real_media(self, count: int) -> tuple[str, list[str]]:
        cover = self.tmp / "cover.jpg"
        cover.write_bytes(b"cover")
        real_paths = []
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO source_posts (id, raw_images_json) VALUES (1, '[]')")
        for i in range(count):
            path = self.tmp / f"real_{i}.jpg"
            path.write_bytes(f"real-{i}".encode("utf-8"))
            real_paths.append(str(path))
            conn.execute(
                """INSERT INTO media_assets (
                       owner_type, owner_ref_id, asset_type, local_path, sort_order
                   ) VALUES ('source_post', 1, 'photo', ?, ?)""",
                (str(path), i),
            )
        conn.commit()
        conn.close()
        return str(cover), real_paths

    def test_gate_returns_cover_first_and_keeps_extra_media_for_discussion(self):
        cover, real_paths = self._seed_real_media(5)
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 600,
            "queue_score": 90,
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)
        main_album = normalize_album_grid(gate["album_all"])
        extra_album = gate["album_all"][len(main_album) :]

        self.assertTrue(gate["is_publishable"])
        self.assertEqual(gate["mode"], "premium_4image")
        self.assertEqual(gate["album_all"][0], cover)
        self.assertEqual(gate["album_all"][1:], real_paths)
        self.assertEqual(main_album, [cover] + real_paths[:3])
        self.assertEqual(extra_album, real_paths[3:])

    def test_fallback_gate_keeps_same_album_order_without_relaxing_threshold(self):
        cover, real_paths = self._seed_real_media(4)
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 600,
            "queue_score": BASIC_PUBLISH_MIN_SCORE,
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)

        self.assertTrue(gate["is_publishable"])
        self.assertEqual(gate["mode"], "fallback_media")
        self.assertIn(f"score_below_premium:{BASIC_PUBLISH_MIN_SCORE}", gate["reasons"])
        self.assertEqual(gate["album_all"], [cover] + real_paths)

    def test_sale_post_with_monthly_price_is_blocked(self):
        cover, _ = self._seed_real_media(4)
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 4800,
            "property_type": "sale",
            "queue_score": 90,
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)

        self.assertFalse(gate["is_publishable"])
        self.assertIn("price_unit_ambiguous", gate["reasons"])

    def test_rent_post_with_sale_price_is_blocked(self):
        cover, _ = self._seed_real_media(4)
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "2房2卫",
            "price": 60000,
            "property_type": "rent",
            "queue_score": 90,
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)

        self.assertFalse(gate["is_publishable"])
        self.assertIn("suspicious_sale_price_in_rent", gate["reasons"])


if __name__ == "__main__":
    unittest.main()
