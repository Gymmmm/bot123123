
import sqlite3
import os
import json
import threading
from datetime import datetime

DATABASE_PATH = os.getenv("DB_PATH", "qiaolian_dual_bot.db")

# Per-thread connection cache: {db_path: sqlite3.Connection}
_thread_local = threading.local()


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._create_tables()
        self._ensure_optional_columns()

    def _get_connection(self):
        """Return a thread-local persistent connection, creating it on first use."""
        cache = getattr(_thread_local, "db_conns", None)
        if cache is None:
            _thread_local.db_conns = {}
            cache = _thread_local.db_conns
        conn = cache.get(self.db_path)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            # SQLite 不接受 PRAGMA busy_timeout 的 ? 占位符，需内联非负整数
            busy_ms = max(0, int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "30000")))
            conn.execute(f"PRAGMA busy_timeout={busy_ms}")
            cache[self.db_path] = conn
        return conn

    def _create_tables(self):
        # This method is for initial table creation if the DB is empty.
        # For migration, we use separate SQL scripts.
        pass

    def _ensure_optional_columns(self):
        """Best-effort schema evolution for additive fields used by newer parsers."""
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(drafts)")
        cols = {str(r[1]) for r in (cur.fetchall() or [])}
        alters = []
        if "water_rate" not in cols:
            alters.append("ALTER TABLE drafts ADD COLUMN water_rate TEXT")
        if "electric_rate" not in cols:
            alters.append("ALTER TABLE drafts ADD COLUMN electric_rate TEXT")
        if "queue_score" not in cols:
            alters.append("ALTER TABLE drafts ADD COLUMN queue_score INTEGER")
        if "review_note" not in cols:
            alters.append("ALTER TABLE drafts ADD COLUMN review_note TEXT")
        for sql in alters:
            cur.execute(sql)
        if alters:
            conn.commit()

    def _execute_query(self, query, params=()):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            conn.rollback()
            raise

    def _fetch_one(self, query, params=()):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()

    def _fetch_all(self, query, params=()):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    # --- Operation-specific functions ---

    def save_source_post(self, source_id, source_type, source_name, source_post_id, source_url, source_author, raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json, dedupe_hash, parse_status='pending'):
        query = """
        INSERT INTO source_posts (
            source_id, source_type, source_name, source_post_id, source_url, source_author,
            raw_text, raw_images_json, raw_videos_json, raw_contact, raw_meta_json, dedupe_hash,
            parse_status, fetched_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            query,
            (
                source_id,
                source_type,
                source_name,
                source_post_id,
                source_url,
                source_author,
                raw_text,
                json.dumps(raw_images_json),
                json.dumps(raw_videos_json),
                raw_contact,
                json.dumps(raw_meta_json),
                dedupe_hash,
                parse_status,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def create_draft(self, draft_id, source_post_id, title, project, community, area, property_type, price, layout, size, floor, deposit, available_date, highlights, drawbacks, advisor_comment, cost_notes, extracted_data, normalized_data, review_status='pending', operator_user_id=None, cover_asset_id=None, water_rate=None, electric_rate=None, queue_score=None, review_note=None):
        query = """
        INSERT INTO drafts (
            draft_id, source_post_id, listing_id, title, project, community, area, property_type, price, layout, size, floor, deposit, available_date,
            highlights, drawbacks, advisor_comment, cost_notes, extracted_data, normalized_data, review_status, operator_user_id, cover_asset_id,
            water_rate, electric_rate, queue_score, review_note,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            query,
            (
                draft_id,
                source_post_id,
                None,
                title,
                project,
                community,
                area,
                property_type,
                price,
                layout,
                size,
                floor,
                deposit,
                available_date,
                json.dumps(highlights),
                json.dumps(drawbacks),
                advisor_comment,
                cost_notes,
                extracted_data,
                normalized_data,
                review_status,
                operator_user_id,
                cover_asset_id,
                water_rate,
                electric_rate,
                queue_score,
                review_note,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def update_draft(self, draft_id, **kwargs):
        set_clauses = []
        params = []
        for key, value in kwargs.items():
            if key in ['highlights', 'drawbacks'] and isinstance(value, list):
                value = json.dumps(value)
            set_clauses.append(f"{key} = ?")
            params.append(value)
        params.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) # updated_at
        params.append(draft_id)

        query = f"UPDATE drafts SET {', '.join(set_clauses)}, updated_at = ? WHERE draft_id = ?"
        self._execute_query(query, tuple(params))

    def approve_draft(self, draft_id, operator_user_id):
        query = "UPDATE drafts SET review_status = ?, operator_user_id = ?, approved_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE draft_id = ?"
        self._execute_query(query, (
            'approved', operator_user_id, draft_id
        ))

    def create_listing_from_draft(self, draft_id, listing_data):
        # This function would typically interact with the existing 'listings' table.
        # As per instructions, we are not modifying existing tables, so this is a placeholder.
        # In a real scenario, 'listing_data' would be inserted into the 'listings' table.
        # For now, we'll just update the draft's listing_id.
        listing_id = f"LST_{datetime.now().strftime('%Y%m%d%H%M%S%f')}" # Generate a dummy listing_id
        self.update_draft(draft_id, listing_id=listing_id, published_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return listing_id

    def save_media_asset(self, asset_id, owner_type, owner_ref_id, owner_ref_key, asset_type, source_type, source_url, source_file_id, local_path, file_url, file_hash, telegram_file_id, telegram_file_unique_id, media_type, is_watermarked=0, is_cover=0, sort_order=0, width=None, height=None, duration=None, file_size=None, mime_type=None, meta_json=None, status='active'):
        query = """
        INSERT INTO media_assets (
            asset_id, owner_type, owner_ref_id, owner_ref_key, asset_type, source_type, source_url, source_file_id,
            local_path, file_url, file_hash, telegram_file_id, telegram_file_unique_id, media_type, is_watermarked,
            is_cover, sort_order, width, height, duration, file_size, mime_type, meta_json, status,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute(
            query,
            (
                asset_id,
                owner_type,
                owner_ref_id,
                owner_ref_key,
                asset_type,
                source_type,
                source_url,
                source_file_id,
                local_path,
                file_url,
                file_hash,
                telegram_file_id,
                telegram_file_unique_id,
                media_type,
                is_watermarked,
                is_cover,
                sort_order,
                width,
                height,
                duration,
                file_size,
                mime_type,
                json.dumps(meta_json) if meta_json else None,
                status,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def create_post_record(self, post_id, listing_id, draft_id, platform, channel_chat_id, channel_message_id, media_group_id, caption_message_id, button_message_id, discuss_chat_id, discuss_thread_id, discuss_message_id, notion_page_id, platform_post_id, post_url, publish_version=1, publish_status='published', post_text=None, comment_text=None, published_by=None):
        query = """
        INSERT INTO posts (
            post_id, listing_id, draft_id, platform, channel_chat_id, channel_message_id, media_group_id, caption_message_id,
            button_message_id, discuss_chat_id, discuss_thread_id, discuss_message_id, notion_page_id, platform_post_id,
            post_url, publish_version, publish_status, post_text, comment_text, published_by, published_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        self._execute_query(query, (
            post_id, listing_id, draft_id, platform, channel_chat_id, channel_message_id, media_group_id, caption_message_id,
            button_message_id, discuss_chat_id, discuss_thread_id, discuss_message_id, notion_page_id, platform_post_id,
            post_url, publish_version, publish_status, post_text, comment_text, published_by
        ))
        return self._fetch_one("SELECT last_insert_rowid()")[0]

    def write_publish_log(self, log_id, post_id, draft_id, listing_id, target_type, target_ref, action, status, attempt_no=1, request_payload=None, response_payload=None, error_code=None, error_message=None, log_message=None, log_level='INFO', started_at=None, finished_at=None):
        query = """
        INSERT INTO publish_logs (
            log_id, post_id, draft_id, listing_id, target_type, target_ref, action, status, attempt_no,
            request_payload, response_payload, error_code, error_message, log_message, log_level, started_at, finished_at,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        self._execute_query(query, (
            log_id, post_id, draft_id, listing_id, target_type, target_ref, action, status, attempt_no,
            json.dumps(request_payload) if request_payload else None, json.dumps(response_payload) if response_payload else None, error_code, error_message, log_message, log_level, started_at, finished_at
        ))
        return self._fetch_one("SELECT last_insert_rowid()")[0]
