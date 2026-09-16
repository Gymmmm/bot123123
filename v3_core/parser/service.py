"""Canonical parse service: source_posts -> canonical_records.

Extracted from ``AIParserModule`` without draft creation, package building or
publication decisions.  Every parse stores the canonical result, including sale
and incomplete-but-reviewable property evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from v3_core.ingest.source_repository import SourceRepository
from v3_core.inventory.canonical_facts import canonicalize_source
from v3_core.parser.authoritative_enrichment import enrich_authoritative_facts
from v3_core.storage.inventory_repository import InventoryRepository


@dataclass(frozen=True)
class CanonicalParseResult:
    source_post_pk: int
    canonical_record_id: str
    facts: dict[str, Any]
    status: str


class CanonicalParseService:
    def __init__(
        self,
        source_repository: SourceRepository,
        inventory_repository: InventoryRepository,
    ):
        self.sources = source_repository
        self.inventory = inventory_repository

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

    def parse_source_post(self, source_post_pk: int) -> CanonicalParseResult:
        row = self.sources.get_source_post(int(source_post_pk))
        if row is None:
            raise KeyError(source_post_pk)
        if str(row["parse_status"] or "") == "insufficient_media":
            raise ValueError("source post is blocked by insufficient_media")

        raw_meta = self._json_object(row["raw_meta_json"])
        raw_text = str(row["raw_text"] or "")
        sanitized_text = str(raw_meta.get("sanitized_text") or raw_text)
        image_count = int(raw_meta.get("raw_image_count") or 0) or self._json_list_count(
            row["raw_images_json"]
        )
        video_count = int(raw_meta.get("raw_video_count") or 0) or self._json_list_count(
            row["raw_videos_json"]
        )
        media_summary = {
            "image_count": image_count,
            "video_count": video_count,
            "media_type": (
                "mixed"
                if image_count and video_count
                else ("video" if video_count else ("image" if image_count else "none"))
            ),
            "visual_review_required": bool(raw_meta.get("visual_review_required")),
        }
        identity = {
            "source_post_id": int(row["id"]),
            "source_id": str(row["source_id"] or ""),
            "source_type": str(row["source_type"] or ""),
            "source_post_identity": str(row["source_post_id"] or ""),
        }

        try:
            facts = canonicalize_source(
                raw_text=raw_text,
                sanitized_text=sanitized_text,
                source_identity=identity,
                media_summary=media_summary,
            )
            facts = enrich_authoritative_facts(sanitized_text, facts)
            canonical = self.inventory.store_canonical(
                source_post_id=int(row["id"]), facts=facts
            )
            self.sources.update_parse_status(int(row["id"]), "parsed")
            return CanonicalParseResult(
                source_post_pk=int(row["id"]),
                canonical_record_id=str(canonical["canonical_record_id"]),
                facts=facts,
                status="parsed",
            )
        except Exception:
            self.sources.update_parse_status(int(row["id"]), "parse_failed")
            raise


__all__ = ["CanonicalParseResult", "CanonicalParseService"]
