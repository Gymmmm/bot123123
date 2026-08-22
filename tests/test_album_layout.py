import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from meihua_publisher import (
    BASIC_PUBLISH_MIN_SCORE,
    evaluate_publish_gate,
    normalize_album_grid,
    split_album_for_channel,
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
        import json
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 600,
            "queue_score": 90,
            "normalized_data": json.dumps({
                "schema_version": "canonical_facts.v1",
                "deal_type": "rent",
                "publication_location_level": "level_2_physical_confirmed",
                "public_location_display": "BKK1",
                "public_location_key": "bkk1",
                "property_type": "公寓",
                "layout": "1房1卫",
                "monthly_rent_usd": 600,
                "canonical_facts_hash": "test_hash_001",
            }),
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)
        main_album = normalize_album_grid(gate["album_all"])
        main_album, extra_album = split_album_for_channel(gate["album_all"])

        self.assertTrue(gate["is_publishable"])
        self.assertEqual(gate["mode"], "premium_4image")
        self.assertIn("missing_deposit_details", gate["warnings"])
        self.assertIn("missing_recurring_cost_details", gate["warnings"])
        self.assertEqual(gate["album_all"][0], cover)
        self.assertEqual(gate["album_all"][1:], real_paths)
        self.assertEqual(main_album, [cover] + real_paths[:3])
        self.assertEqual(extra_album, real_paths[3:])

    def test_non_contiguous_selection_keeps_every_unselected_path_once(self):
        cover, real_paths = self._seed_real_media(6)
        all_paths = [cover, *real_paths]
        with patch(
            "meihua_publisher._select_diverse_detail_paths",
            return_value=[real_paths[0], real_paths[2], real_paths[4]],
        ):
            main_album, extra_album = split_album_for_channel(all_paths)

        self.assertEqual(main_album, [cover, real_paths[0], real_paths[2], real_paths[4]])
        self.assertEqual(extra_album, [real_paths[1], real_paths[3], real_paths[5]])
        self.assertEqual(set(main_album) | set(extra_album), set(all_paths))
        self.assertFalse(set(main_album) & set(extra_album))

    def test_fallback_gate_keeps_same_album_order_without_relaxing_threshold(self):
        cover, real_paths = self._seed_real_media(4)
        import json
        draft = {
            "source_post_id": 1,
            "area": "BKK1",
            "layout": "1房1卫",
            "price": 600,
            "queue_score": BASIC_PUBLISH_MIN_SCORE,
            "normalized_data": json.dumps({
                "schema_version": "canonical_facts.v1",
                "deal_type": "rent",
                "publication_location_level": "level_2_physical_confirmed",
                "public_location_display": "BKK1",
                "public_location_key": "bkk1",
                "property_type": "公寓",
                "layout": "1房1卫",
                "monthly_rent_usd": 600,
                "canonical_facts_hash": "test_hash_002",
            }),
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

    def test_listing_without_specific_area_or_project_is_blocked(self):
        cover, _ = self._seed_real_media(4)
        draft = {
            "source_post_id": 1,
            "area": "金边",
            "layout": "1房1卫",
            "price": 600,
            "queue_score": 90,
        }

        gate = evaluate_publish_gate(draft, cover, self.db_path)

        self.assertFalse(gate["is_publishable"])
        self.assertIn("invalid_area", gate["reasons"])


if __name__ == "__main__":
    unittest.main()
