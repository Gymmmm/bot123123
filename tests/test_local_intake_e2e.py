import asyncio
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

import collector_bot
from ai_parser import AIParserModule
from db import DatabaseManager
from meihua_publisher import (
    add_channel_listing_overlay,
    build_chinese_listing_post,
    build_cover_listing_data,
    evaluate_publish_gate,
    split_album_for_channel,
)


REALISTIC_LISTING_TEXT = """
Urban Village 洪森大道公寓出租
区域：洪森大道
户型：2房1厅2卫
面积：82㎡
楼层：12楼
租金：$680/月
押一付一，一年起租，随时入住
家具家电齐全，采光好，可视频看房
水费 $0.75/吨，电费 $0.25/度，物业费已含
""".strip()


class LocalIntakeEndToEndTests(unittest.TestCase):
    def test_four_photo_intake_parse_review_and_offline_publish_preview(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "exercise.db"
            schema = Path(__file__).resolve().parent.parent / "schema_core.sql"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(schema.read_text(encoding="utf-8"))

            image_paths = []
            colors = ((195, 210, 225), (220, 198, 175), (178, 208, 188), (205, 185, 215))
            for index, color in enumerate(colors, start=1):
                path = root / f"room-{index}.jpg"
                Image.new("RGB", (1200 + index * 20, 900), color).save(path, "JPEG")
                image_paths.append(path)

            raw_images = []
            for index, path in enumerate(image_paths, start=1):
                raw_images.append(
                    {
                        "local_path": str(path),
                        "file_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "telegram_file_id": f"local-photo-{index}",
                        "telegram_file_unique_id": f"local-unique-{index}",
                        "message_id": index,
                    }
                )

            manager = DatabaseManager(str(db_path))
            with patch.object(collector_bot, "db_manager", manager):
                result = asyncio.run(
                    collector_bot.persist_source_post(
                        object(),
                        {
                            "source_name": "management_bot_local_exercise",
                            "source_type": "telegram_admin_upload",
                            "source_db_id": None,
                        },
                        chat_id=-100999,
                        source_post_id="album_local_1",
                        anchor_message_id=1,
                        raw_text=REALISTIC_LISTING_TEXT,
                        raw_images=raw_images,
                        raw_videos=[],
                        grouped_id=12345,
                        source_author="local_admin",
                        ingest_kind="album",
                        message_count=4,
                    )
                )
            self.assertEqual(result["status"], "inserted")

            with sqlite3.connect(db_path) as conn:
                source = conn.execute(
                    "SELECT id, raw_text, raw_images_json, parse_status FROM source_posts"
                ).fetchone()
                media_before_parse = conn.execute(
                    "SELECT local_path, sort_order FROM media_assets ORDER BY sort_order"
                ).fetchall()
            self.assertEqual(source[1], REALISTIC_LISTING_TEXT)
            self.assertEqual(len(json.loads(source[2])), 4)
            self.assertEqual(source[3], "pending")
            self.assertEqual([row[1] for row in media_before_parse], [0, 1, 2, 3])

            AIParserModule(str(db_path)).process_pending_source_posts()
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                source_status = conn.execute(
                    "SELECT parse_status FROM source_posts WHERE id=?", (source[0],)
                ).fetchone()[0]
                draft = dict(conn.execute("SELECT * FROM drafts").fetchone())
                conn.execute(
                    "UPDATE drafts SET review_status='ready' WHERE draft_id=?",
                    (draft["draft_id"],),
                )
                conn.commit()
                ready_status = conn.execute(
                    "SELECT review_status FROM drafts WHERE draft_id=?", (draft["draft_id"],)
                ).fetchone()[0]

            self.assertEqual(source_status, "parsed")
            self.assertEqual(draft["price"], 680)
            self.assertIn("2房", draft["layout"])
            self.assertEqual(ready_status, "ready")

            first_bytes = image_paths[0].read_bytes()
            cover_bytes = add_channel_listing_overlay(
                first_bytes,
                build_cover_listing_data(draft),
                with_listing_footer=True,
            ).getvalue()
            cover_path = root / "cover-preview.jpg"
            cover_path.write_bytes(cover_bytes)

            draft["queue_score"] = max(int(draft.get("queue_score") or 0), 90)
            gate = evaluate_publish_gate(draft, str(cover_path), str(db_path))
            main_album, discussion_album = split_album_for_channel(gate["album_all"])
            caption = build_chinese_listing_post(draft)

            self.assertTrue(gate["is_publishable"], gate)
            self.assertEqual(len(gate["album_all"]), 5)
            self.assertEqual(len(main_album), 4)
            self.assertEqual(len(discussion_album), 1)
            self.assertIn("$680/月", caption)
            self.assertIn("洪森大道", caption)
            self.assertEqual(len(set(gate["album_all"])), 5)


if __name__ == "__main__":
    unittest.main()
