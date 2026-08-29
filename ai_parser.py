"""Canonical intake orchestrator: source_posts -> canonical facts -> drafts.

No template, caption, package or review-note text is a fact input.  New intake
and every later refresh call the same canonicalization function.
"""
from __future__ import annotations

import json
import uuid
from collections import Counter
from typing import Any

from collector_db_compat import DatabaseManager
from qiaolian_dual.canonical_facts import canonicalize_source, draft_projection
from qiaolian_dual.canonical_listing_materializer import materialize_draft_facts


class LLMClient:
    """Compatibility seam; any future model output is only a candidate source."""

    def parse_text_with_llm(self, raw_text: str) -> dict[str, Any]:
        return canonicalize_source(raw_text)


class AIParserModule:
    _SOURCE_COLUMNS = (
        "id", "source_id", "source_type", "source_post_id", "raw_text",
        "raw_meta_json", "raw_images_json", "raw_videos_json",
    )

    def __init__(self, db_path: str):
        self.db_manager = DatabaseManager(db_path)
        self.llm_client = LLMClient()

    @staticmethod
    def _as_mapping(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
        if hasattr(row, "keys"):
            return {key: row[key] for key in row.keys()}
        return {key: value for key, value in zip(columns, row)}

    @staticmethod
    def _json_object(value: object) -> dict[str, Any]:
        try:
            decoded = json.loads(str(value or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _json_list_count(value: object) -> int:
        if isinstance(value, list):
            return len(value)
        try:
            decoded = json.loads(str(value or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return 0
        return len(decoded) if isinstance(decoded, list) else 0

    @classmethod
    def _inputs(cls, row: dict[str, Any]) -> tuple[int, str, str, str, dict[str, Any], dict[str, Any]]:
        source_meta = cls._json_object(row.get("raw_meta_json"))
        raw_text = str(row.get("raw_text") or "")
        sanitized_text = str(source_meta.get("sanitized_text") or raw_text)
        image_count = int(source_meta.get("raw_image_count") or 0) or cls._json_list_count(row.get("raw_images_json"))
        video_count = int(source_meta.get("raw_video_count") or 0) or cls._json_list_count(row.get("raw_videos_json"))
        media_summary = {
            "image_count": image_count,
            "video_count": video_count,
            "media_type": "mixed" if image_count and video_count else ("video" if video_count else ("image" if image_count else "none")),
            "visual_review_required": bool(source_meta.get("visual_review_required")),
        }
        identity = {
            "source_post_id": int(row["id"]),
            "source_id": str(row.get("source_id") or ""),
            "source_type": str(row.get("source_type") or ""),
            "source_post_identity": str(row.get("source_post_id") or ""),
        }
        return int(row["id"]), str(row.get("source_type") or ""), raw_text, sanitized_text, identity, media_summary

    @staticmethod
    def _review_note(facts: dict[str, Any]) -> str:
        quality = dict(facts.get("quality") or {})
        flags = ",".join(str(flag) for flag in (quality.get("all_flags") or [])) or "ok"
        level = str(facts.get("publication_location_level") or "unknown")
        return f"quality:{flags} | location_level:{level}"[:500]

    def _mark_source_post(self, post_id: int, status: str, error: str = "") -> None:
        self.db_manager._execute_query(
            "UPDATE source_posts SET parse_status=?, parse_error=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (status, error[:500], post_id),
        )

    def _canonicalize(self, source_row: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any]]:
        post_id, _source_type, raw_text, sanitized_text, identity, media_summary = self._inputs(source_row)
        facts = canonicalize_source(
            raw_text=raw_text,
            sanitized_text=sanitized_text,
            source_identity=identity,
            media_summary=media_summary,
        )
        return post_id, facts, draft_projection(facts)

    def _write_existing_draft(self, draft_id: str, facts: dict[str, Any], projection: dict[str, Any]) -> None:
        """Replace the complete legacy projection from canonical facts.

        A non-empty legacy column is not evidence of a manual override. Audited
        overrides must be represented in facts[manual_overrides], otherwise a
        source reparse would silently create projection drift.
        """
        conn = self.db_manager._get_connection()
        materialize_draft_facts(conn, draft_id=draft_id, facts=facts)
        conn.execute(
            "UPDATE drafts SET review_note=?, updated_at=CURRENT_TIMESTAMP WHERE draft_id=?",
            (self._review_note(facts), draft_id),
        )
        conn.commit()

    def _create_draft(self, source_post_id: int, facts: dict[str, Any], projection: dict[str, Any]) -> str:
        facts_json = json.dumps(facts, ensure_ascii=False, sort_keys=True)
        draft_id = f"DRF_{uuid.uuid4()}"
        cost_notes = str(projection.get("cost_notes") or "")
        self.db_manager.create_draft(
            draft_id=draft_id,
            source_post_id=source_post_id,
            title=projection["title"],
            project=projection["project"],
            community=projection["community"],
            area=projection["area"],
            property_type=projection["property_type"],
            price=projection["price"],
            layout=projection["layout"],
            size=projection["size"],
            floor=projection["floor"],
            deposit=projection["deposit"],
            available_date=projection.get("available_date", ""),
            highlights=projection["highlights"],
            drawbacks=[],
            advisor_comment="",
            cost_notes=cost_notes,
            extracted_data=facts_json,
            normalized_data=facts_json,
            review_status="pending",
            queue_score=projection["quality_score"],
            review_note=self._review_note(facts),
            water_rate=projection.get("water_rate") or None,
            electric_rate=projection.get("electric_rate") or None,
        )
        conn = self.db_manager._get_connection()
        materialize_draft_facts(conn, draft_id=draft_id, facts=facts)
        conn.commit()
        return draft_id

    def _process_single_source_post_with_status(self, source_post_db_id: int) -> tuple[str, str | None]:
        raw = self.db_manager._fetch_one(
            """
            SELECT id, source_id, source_type, source_post_id, raw_text,
                   raw_meta_json, raw_images_json, raw_videos_json
            FROM source_posts WHERE id=? AND parse_status='pending'
            """,
            (source_post_db_id,),
        )
        if not raw:
            return "not_pending", None
        source = self._as_mapping(raw, self._SOURCE_COLUMNS)
        post_id = int(source["id"])
        try:
            _post_id, facts, projection = self._canonicalize(source)
            hard_flags = set((facts.get("quality") or {}).get("hard_flags") or [])
            if "non_rental_source" in hard_flags:
                self._mark_source_post(post_id, "skipped_non_rental", "non_rental_source")
                return "skipped_non_rental", None
            if "missing_price" in hard_flags:
                self._mark_source_post(post_id, "skipped_no_price", "missing_price")
                return "skipped_no_price", None
            existing = self.db_manager._fetch_one(
                "SELECT draft_id FROM drafts WHERE source_post_id=? ORDER BY id DESC LIMIT 1", (post_id,)
            )
            if existing:
                draft_id = str(existing[0])
                self._write_existing_draft(draft_id, facts, projection)
                status = "recanonicalized"
            else:
                draft_id = self._create_draft(post_id, facts, projection)
                status = "parsed"
            self._mark_source_post(post_id, "parsed", "")
            return status, draft_id
        except Exception as exc:
            self._mark_source_post(post_id, "failed", str(exc))
            return "failed", None

    def process_single_source_post(self, source_post_db_id: int) -> str | None:
        _status, draft_id = self._process_single_source_post_with_status(source_post_db_id)
        return draft_id

    def process_pending_source_posts(self) -> dict[str, int]:
        rows = self.db_manager._fetch_all("SELECT id FROM source_posts WHERE parse_status='pending'")
        stats: Counter[str] = Counter()
        for (post_id,) in rows or []:
            status, _draft_id = self._process_single_source_post_with_status(int(post_id))
            stats[status] += 1
        stats["total_pending"] = len(rows or [])
        return dict(stats)

    def _recanonicalize_pending_drafts(self, limit: int) -> int:
        rows = self.db_manager._fetch_all(
            """
            SELECT d.draft_id, sp.id, sp.source_id, sp.source_type, sp.source_post_id,
                   sp.raw_text, sp.raw_meta_json, sp.raw_images_json, sp.raw_videos_json
            FROM drafts d JOIN source_posts sp ON sp.id=d.source_post_id
            WHERE d.review_status='pending'
            ORDER BY d.id DESC LIMIT ?
            """,
            (int(limit),),
        )
        columns = ("draft_id",) + self._SOURCE_COLUMNS
        count = 0
        for raw in rows or []:
            row = self._as_mapping(raw, columns)
            source = {key: row[key] for key in self._SOURCE_COLUMNS}
            _post_id, facts, projection = self._canonicalize(source)
            self._write_existing_draft(str(row["draft_id"]), facts, projection)
            count += 1
        return count

    def refresh_low_quality_drafts(self, limit: int = 50) -> int:
        return self._recanonicalize_pending_drafts(limit)

    def refresh_pending_drafts(self, limit: int = 200) -> int:
        return self._recanonicalize_pending_drafts(limit)

    def repair_pending_drafts(self, limit: int = 200) -> int:
        return self._recanonicalize_pending_drafts(limit)

    def normalize_pending_area_labels(self, limit: int = 500) -> int:
        """Deprecated compatibility entry point; canonical reparse is the only repair."""
        return self._recanonicalize_pending_drafts(limit)
