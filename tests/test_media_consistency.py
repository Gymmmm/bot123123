import os
import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import media_consistency
import run_pipeline_autopilot
from media_consistency import (
    assess_draft_media,
    mark_broken_ready,
    media_blocks_publish,
    media_blocks_ready,
    redownload_source_post_media,
)
from meihua_publisher import MeihuaPublisher


def _init_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE source_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            source_post_id TEXT,
            source_url TEXT,
            raw_images_json TEXT DEFAULT '[]',
            raw_meta_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_id TEXT UNIQUE,
            source_post_id INTEGER,
            listing_id TEXT,
            title TEXT,
            project TEXT,
            community TEXT,
            area TEXT,
            property_type TEXT,
            price INTEGER,
            layout TEXT,
            size TEXT,
            floor TEXT,
            deposit TEXT,
            available_date TEXT,
            highlights TEXT DEFAULT '[]',
            drawbacks TEXT DEFAULT '[]',
            advisor_comment TEXT,
            cost_notes TEXT,
            extracted_data TEXT,
            normalized_data TEXT,
            review_status TEXT DEFAULT 'pending',
            review_note TEXT,
            operator_user_id TEXT,
            cover_asset_id INTEGER,
            queue_score REAL DEFAULT 0,
            approved_at TEXT,
            published_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE media_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id TEXT UNIQUE,
            owner_type TEXT,
            owner_ref_id INTEGER,
            owner_ref_key TEXT,
            asset_type TEXT,
            source_type TEXT,
            source_file_id TEXT,
            local_path TEXT,
            file_url TEXT,
            file_hash TEXT,
            telegram_file_id TEXT,
            media_type TEXT,
            is_cover INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            status TEXT,
            updated_at TEXT
        );
        CREATE TABLE publish_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_id TEXT UNIQUE,
            post_id TEXT,
            draft_id TEXT,
            listing_id TEXT,
            target_type TEXT,
            target_ref TEXT,
            action TEXT,
            status TEXT,
            attempt_no INTEGER,
            request_payload TEXT,
            response_payload TEXT,
            error_message TEXT,
            log_message TEXT,
            log_level TEXT,
            created_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()


def _seed_draft(
    db_path: str,
    *,
    draft_id: str = "DRF_TEST",
    review_status: str = "pending",
    cover_exists: bool = True,
    real_exists: bool = True,
    raw_images: bool = True,
    tmp: Path,
) -> dict[str, str]:
    cover = tmp / f"cover_{draft_id}.jpg"
    real = tmp / f"real_{draft_id}.jpg"
    if cover_exists:
        cover.write_bytes(b"cover")
    if real_exists:
        real.write_bytes(b"real")
    raw = f'[{{"local_path": "{real}", "message_id": 1001}}]' if raw_images else "[]"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO source_posts (id, source_name, source_post_id, source_url, raw_images_json, raw_meta_json) VALUES (1, 'source_missing', '1001', 'https://t.me/source_missing/1001', ?, '{}')",
        (raw,),
    )
    cur.execute(
        """INSERT INTO drafts (
             id, draft_id, source_post_id, title, project, area, property_type,
             price, layout, highlights, drawbacks, extracted_data, normalized_data,
             review_status, review_note, cover_asset_id, queue_score
           ) VALUES (10, ?, 1, 'Title', 'Project', 'BKK1', '公寓',
                     500, '1房', '[]', '[]', '{}', '{"quality_score": 90}',
                     ?, '', 1, 90)""",
        (draft_id, review_status),
    )
    cur.execute(
        """INSERT INTO media_assets (
             id, asset_id, owner_type, owner_ref_id, owner_ref_key,
             asset_type, local_path, media_type, is_cover, sort_order, status
           ) VALUES (1, 'COVER', 'draft', 10, ?, 'image', ?, 'photo', 1, 0, 'active')""",
        (draft_id, str(cover)),
    )
    if raw_images:
        cur.execute(
            """INSERT INTO media_assets (
                 asset_id, owner_type, owner_ref_id, owner_ref_key,
                 asset_type, local_path, media_type, is_cover, sort_order, status
               ) VALUES ('REAL', 'source_post', 1, '1', 'photo', ?, 'photo', 1, 0, 'active')""",
            (str(real),),
        )
    conn.commit()
    conn.close()
    return {"cover": str(cover), "real": str(real)}


class MediaConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.tmp = Path(self.td.name)
        self.db_path = str(self.tmp / "test.db")
        _init_db(self.db_path)

    def tearDown(self) -> None:
        self.td.cleanup()

    def test_auto_approve_refuses_missing_original_media(self):
        _seed_draft(self.db_path, tmp=self.tmp, real_exists=False)
        old_db_path = run_pipeline_autopilot.DB_PATH
        old_auto = run_pipeline_autopilot.AUTO_APPROVE
        old_min = run_pipeline_autopilot.AUTO_READY_MIN_SCORE
        try:
            run_pipeline_autopilot.DB_PATH = self.db_path
            run_pipeline_autopilot.AUTO_APPROVE = True
            run_pipeline_autopilot.AUTO_READY_MIN_SCORE = 60
            self.assertEqual(run_pipeline_autopilot.step_auto_ready(), 0)
        finally:
            run_pipeline_autopilot.DB_PATH = old_db_path
            run_pipeline_autopilot.AUTO_APPROVE = old_auto
            run_pipeline_autopilot.AUTO_READY_MIN_SCORE = old_min
        conn = sqlite3.connect(self.db_path)
        status, note = conn.execute("SELECT review_status, review_note FROM drafts WHERE draft_id='DRF_TEST'").fetchone()
        conn.close()
        self.assertEqual(status, "pending")
        self.assertIn("missing_real_media", note)

    def test_manual_queue_guard_refuses_missing_original_media(self):
        _seed_draft(self.db_path, tmp=self.tmp, real_exists=False)
        status = assess_draft_media("DRF_TEST", self.db_path)
        self.assertTrue(media_blocks_ready(status))
        self.assertIn("missing_real_media", status.issue_codes)

    def test_publish_precheck_returns_broken_media_to_pending(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready", real_exists=False)
        ok = MeihuaPublisher(self.db_path).publish_draft("DRF_TEST")
        self.assertFalse(ok)
        conn = sqlite3.connect(self.db_path)
        review_status, note = conn.execute("SELECT review_status, review_note FROM drafts WHERE draft_id='DRF_TEST'").fetchone()
        log = conn.execute("SELECT target_type, error_message FROM publish_logs ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        self.assertEqual(review_status, "pending")
        self.assertIn("missing_real_media", note)
        self.assertEqual(log[0], "media_consistency")
        self.assertIn("missing_real_media", log[1])

    def test_missing_cover_only_can_enter_queue_and_publish_precheck(self):
        _seed_draft(self.db_path, tmp=self.tmp, cover_exists=False, real_exists=True)
        status = assess_draft_media("DRF_TEST", self.db_path)
        self.assertIn("missing_cover", status.issue_codes)
        self.assertNotIn("missing_real_media", status.issue_codes)
        self.assertFalse(media_blocks_ready(status))
        self.assertFalse(media_blocks_publish(status))

    def test_unrecoverable_source_reports_clear_error(self):
        _seed_draft(self.db_path, tmp=self.tmp, real_exists=False, raw_images=False)
        result = __import__("asyncio").run(redownload_source_post_media(1, self.db_path, dry_run=True))
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "no_message_ids")

    def test_report_shows_unrecoverable_hint_and_gate_fields(self):
        _seed_draft(self.db_path, tmp=self.tmp, real_exists=False, raw_images=False)
        status = assess_draft_media("DRF_TEST", self.db_path)
        buf = io.StringIO()
        with redirect_stdout(buf):
            media_consistency._print_status(status, self.db_path)
        out = buf.getvalue()
        self.assertIn("blocks_ready: yes", out)
        self.assertIn("blocks_publish: yes", out)
        self.assertIn("recovery: source_unrecoverable:no_message_ids", out)

    def test_report_shows_cover_only_as_non_blocking(self):
        _seed_draft(self.db_path, tmp=self.tmp, cover_exists=False, real_exists=True)
        status = assess_draft_media("DRF_TEST", self.db_path)
        buf = io.StringIO()
        with redirect_stdout(buf):
            media_consistency._print_status(status, self.db_path)
        out = buf.getvalue()
        self.assertIn("issues: missing_cover", out)
        self.assertIn("blocks_ready: no", out)
        self.assertIn("blocks_publish: no", out)

    def test_mark_broken_ready_dry_run_does_not_modify(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready", real_exists=False)
        result = mark_broken_ready(self.db_path, dry_run=True)
        conn = sqlite3.connect(self.db_path)
        review_status, note = conn.execute("SELECT review_status, review_note FROM drafts WHERE draft_id='DRF_TEST'").fetchone()
        conn.close()
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["hits"], 1)
        self.assertEqual(result["reverted"], 0)
        self.assertEqual(review_status, "ready")
        self.assertEqual(note, "")

    def test_mark_broken_ready_moves_blocked_ready_to_pending(self):
        _seed_draft(self.db_path, tmp=self.tmp, review_status="ready", real_exists=False)
        result = mark_broken_ready(self.db_path, dry_run=False)
        conn = sqlite3.connect(self.db_path)
        review_status, note = conn.execute("SELECT review_status, review_note FROM drafts WHERE draft_id='DRF_TEST'").fetchone()
        conn.close()
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["hits"], 1)
        self.assertEqual(result["reverted"], 1)
        self.assertEqual(review_status, "pending")
        self.assertIn("missing_real_media", note)


if __name__ == "__main__":
    unittest.main()
